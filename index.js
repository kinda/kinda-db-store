"use strict";

var _ = require('lodash');
var wait = require('co-wait');
var log = require('kinda-log').create();
var util = require('kinda-util').create();
var KindaDB = require('kinda-db/database');
var Store = require('kinda-store');

var VERSION = 1;

var KindaStoreDB = KindaDB.extend('KindaStoreDB', function() {
  this.Table = require('./table');

  this.isFinal = true; // TODO: remove this (should not be necessary)

  this.setCreator(function(name, url, options) {
    if (!name) throw new Error('name is missing');
    if (!url) throw new Error('url is missing');
    this.name = name;
    this.store = Store.create(url);
    this.database = this;
    this.tables = [];
    this.migrations = [];
  });

  // === Database ====

  this.registerMigration = function(number, fn) {
    if (!_.isNumber(number))
      throw new Error('invalid migration number');
    if (number < 1)
      throw new Error('migration number should be greater than 0');
    if (!_.isFunction(fn))
      throw new Error('migration function is missing');
    if (_.some(this.migrations, { number: number }))
      throw new Error('duplicated migration number');
    this.migrations.push({ number: number, fn: fn });
  };

  this.initializeDatabase = function *() {
    if (this.database._isInitialized) return;
    if (this._isInitializing) return;
    yield this.transaction(function *(tr) {
      try {
        tr._isInitializing = true;
        yield tr.createDatabase();
        yield tr.upgradeDatabase();
        yield tr.verifyDatabase();
        yield tr.migrateDatabase();
      } finally {
        tr._isInitializing = false;
      }
    }, { longTransaction: true, initializeDatabase: false });
    this.database._isInitialized = true;
    yield this.database.emitAsync('didInitialize');
  };

  this.createDatabase = function *() {
    yield this.store.transaction(function *(tr) {
      if (!(yield this.loadDatabase(tr, false))) {
        this.version = VERSION;
        this.lastMigrationNumber = 0;
        this.isLocked = false;
        yield this.saveDatabase(tr, true);
        log.info("Database '" + this.name + "' created");
      }
    }.bind(this));
  };

  this.lockDatabaseIf = function *(fn) {
    var done = false;
    while (!done) {
      yield this.store.transaction(function *(tr) {
        yield this.loadDatabase(tr);
        if (this.isLocked) return;
        if (fn(tr)) {
          this.isLocked = true;
          yield this.saveDatabase(tr);
        }
        done = true;
      }.bind(this));
      if (!done) {
        log.info("Waiting Database '" + this.name + "'...");
        yield wait(5000); // wait 5 secs before retrying
      }
    }
    return this.isLocked;
  };

  this.unlockDatabase = function *() {
    this.isLocked = false;
    yield this.saveDatabase(this.store);
  };

  this.upgradeDatabase = function *() {
    var upgradeIsNeeded = yield this.lockDatabaseIf(function() {
      return this.version !== VERSION;
    }.bind(this));
    if (!upgradeIsNeeded) return;

    // ... upgrading

    this.version = VERSION;
    yield this.unlockDatabase();
    log.info("Database '" + this.name + "' upgraded to version " + VERSION);
  };

  this.verifyDatabase = function *() {
    // TODO: test isCreating and isDeleting flags to
    // detect incompletes indexes
  };

  this.migrateDatabase = function *(transaction) {
    if (!this.migrations.length) return;
    var maxMigrationNumber = _.max(this.migrations, 'number').number;

    var migrationIsNeeded = yield this.lockDatabaseIf(function() {
      if (this.lastMigrationNumber === maxMigrationNumber)
        return false;
      if (this.lastMigrationNumber > maxMigrationNumber)
        throw new Error('incompatible database (lastMigrationNumber > maxMigrationNumber)');
      return true;
    }.bind(this));
    if (!migrationIsNeeded) return;

    try {
      var number = this.lastMigrationNumber;
      var migration;
      do {
        number++;
        migration = _.find(this.migrations, { number: number });
        if (!migration) continue;
        yield migration.fn.call(this);
        log.info("Migration #" + number + " (database '" + this.name + "') done");
        this.lastMigrationNumber = number;
        yield this.saveDatabase(this.store);
      } while (number < maxMigrationNumber);
    } finally {
      yield this.unlockDatabase();
    }
  };

  this.loadDatabase = function *(tr, errorIfMissing) {
    if (!tr) tr = this.store;
    if (errorIfMissing == null) errorIfMissing = true;
    var json = yield tr.get([this.name], { errorIfMissing: errorIfMissing });
    if (json) {
      this.unserialize(json);
      return true;
    }
  };

  this.saveDatabase = function *(tr, errorIfExists) {
    if (!tr) tr = this.store;
    var json = this.serialize();
    yield tr.put([this.name], json, {
      errorIfExists: errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.serialize = function() {
    return {
      version: this.version,
      name: this.name,
      lastMigrationNumber: this.lastMigrationNumber,
      isLocked: this.isLocked,
      tables: _.compact(_.invoke(this.tables, 'serialize'))
    };
  };

  this.unserialize = function(json) {
    this.version = json.version;
    this.name = json.name;
    this.lastMigrationNumber = json.lastMigrationNumber;
    this.isLocked = json.isLocked;
    json.tables.forEach(function(jsonTable) {
      var table = this.getTable(jsonTable.name);
      table.unserialize(jsonTable);
    }, this);
  };

  this.transaction = function *(fn, options) {
    if (this.database !== this)
      return yield fn(this); // we are already in a transaction
    if (!options) options = {};
    if (!options.hasOwnProperty('initializeDatabase'))
      options.initializeDatabase = true;
    if (options.initializeDatabase)
      yield this.initializeDatabase();
    if (options.longTransaction) {
      // For now, just use the regular store
      var transaction = Object.create(this);
      return yield fn(transaction);
    }
    return yield this.store.transaction(function *(tr) {
      var transaction = Object.create(this);
      transaction.store = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.resetDatabase = function *() {
    this.database._isInitialized = false;
    yield this.store.delRange({ prefix: this.name });
    yield this.saveDatabase();
  };

  this.close = function *() {
    yield this.store.close();
  };

  // === Tables ====

  this.initializeTable = function *(table) {
    yield this.initializeDatabase();
    if (table.isVirtual)
      throw new Error("Table '" + table.name + "' (database '" + this.name + "') is missing");
  };

  this.addTable = function *(name) {
    var table = this.getTable(name);
    if (!table.isVirtual)
      throw new Error("Table '" + name + "' (database '" + this.name + "') already exists");
    table.isVirtual = false;
    yield this.saveDatabase(this.store);
    log.info("Table '" + name + "' (database '" + this.name + "') created");
    return table;
  };

  this.addIndex = function *(table, keys, options) {
    table = this.normalizeTable(table);
    keys = table.normalizeKeys(keys);
    if (table.findIndexIndex(keys) !== -1)
      throw new Error('an index with the same keys already exists');
    var index = {
      name: keys.join('+'),
      keys: keys,
      isCreating: true // TODO: use this flag to detect incomplete index creation
    };
    log.info("Creating index '" + index.name + "' (database '" + this.name + "', table '" + table.name + "')...");
    table.indexes.push(index);
    yield this.saveDatabase();
    yield this.forRange(table, {}, function *(item, key) {
      yield this.updateIndex(table, key, undefined, item, index);
    }, this);
    delete index.isCreating;
    yield this.saveDatabase();
  };

  this.delIndex = function *(table, keys, options) {
    table = this.normalizeTable(table);
    keys = table.normalizeKeys(keys);
    var i = table.findIndexIndex(keys);
    if (i === -1) throw new Error('index not found');
    var index = table.indexes[i];
    log.info("Deleting index '" + index.name + "' (database '" + this.name + "', table '" + table.name + "')...");
    index.isDeleting = true; // TODO: use this flag to detect incomplete index deletion
    yield this.saveDatabase();
    yield this.forRange(table, {}, function *(item, key) {
      // TODO: can be optimized with direct delete of the index records
      yield this.updateIndex(table, key, item, undefined, index);
    }, this);
    table.indexes.splice(i, 1);
    yield this.saveDatabase();
  };

  // === Operations ====

  this.get = function *(table, key, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    var item = yield this.store.get(this.makeItemKey(table, key), options);
    return item;
  };

  this.put = function *(table, key, item, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    key = this.normalizeKey(key);
    item = this.normalizeItem(item);
    options = this.normalizeOptions(options);
    yield this.transaction(function *(tr) {
      var itemKey = tr.makeItemKey(table, key);
      var oldItem = yield tr.store.get(itemKey, { errorIfMissing: false });
      yield tr.store.put(itemKey, item, options);
      yield tr.updateIndexes(table, key, oldItem, item);
      yield tr.emitAsync('didPut', table, key, item, options);
    });
  };

  this.del = function *(table, key, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    yield this.transaction(function *(tr) {
      var itemKey = tr.makeItemKey(table, key);
      var oldItem = yield tr.store.get(itemKey, options);
      if (oldItem) {
        yield tr.store.del(itemKey, options);
        yield tr.updateIndexes(table, key, oldItem, undefined);
        yield tr.emitAsync('didDel', table, key, oldItem, options);
      }
    });
  };

  this.getMany = function *(table, keys, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    if (!_.isArray(keys))
      throw new Error('invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    options = this.normalizeOptions(options);
    if (!options.hasOwnProperty('returnValues'))
      options.returnValues = true;
    var itemKeys = keys.map(function(key) {
      return this.makeItemKey(table, key)
    }, this);
    var items = yield this.store.getMany(itemKeys, options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

  this.getRange = function *(table, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    options = this.normalizeOptions(options);
    if (options.by)
      return yield this.getRangeBy(table, options.by, options);
    if (!options.hasOwnProperty('returnValues'))
      options.returnValues = true;
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    var items = yield this.store.getRange(options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

  this.getRangeBy = function *(table, index, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    index = table.normalizeIndex(index);
    options = this.normalizeOptions(options);
    if (!options.hasOwnProperty('returnValues'))
      options.returnValues = true;
    var queryOptions = _.clone(options);
    queryOptions.prefix = [this.name,
      this.makeIndexTableName(table, index)];
    if (options.prefix)
      queryOptions.prefix = queryOptions.prefix.concat(options.prefix);
    queryOptions.returnValues = false;
    var items = yield this.store.getRange(queryOptions);
    items = items.map(function(item) {
      return { key: _.last(item.key) };
    });
    if (!options.returnValues) return items;
    var keys = items.map(function(item) { return item.key; });
    items = yield this.getMany(table, keys, { errorIfMissing: false });
    return items;
  };

  this.getCount = function *(table, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    options = this.normalizeOptions(options);
    if (options.by)
      return yield this.getCountBy(table, options.by, options);
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    return yield this.store.getCount(options);
  };

  this.getCountBy = function *(table, index, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    index = table.normalizeIndex(index);
    options = this.normalizeOptions(options);
    var queryOptions = _.clone(options);
    queryOptions.prefix = [this.name,
      this.makeIndexTableName(table, index)];
    if (options.prefix)
      queryOptions.prefix = queryOptions.prefix.concat(options.prefix);
    return yield this.store.getCount(queryOptions);
  };

  this.updateIndexes = function *(table, key, oldItem, newItem) {
    for (var i = 0; i < table.indexes.length; i++) {
      var index = table.indexes[i];
      yield this.updateIndex(table, key, oldItem, newItem, index);
    }
  };

  this.updateIndex = function *(table, key, oldItem, newItem, index) {
    oldItem = util.flattenObject(oldItem);
    newItem = util.flattenObject(newItem);
    var oldValues = [];
    var newValues = [];
    index.keys.forEach(function(key) {
      oldValues.push(oldItem[key]);
      newValues.push(newItem[key]);
    });
    if (_.isEqual(oldValues, newValues)) return;
    if (!_.contains(oldValues, undefined)) {
      var indexKey = this.makeIndexKey(table, index, oldValues, key);
      yield this.store.del(indexKey);
    };
    if (!_.contains(newValues, undefined)) {
      var indexKey = this.makeIndexKey(table, index, newValues, key);
      yield this.store.put(indexKey);
    };
  };

  this.makeItemKey = function(table, key) {
    return [this.name, table.name, key];
  };

  this.makeIndexKey = function(table, index, values, key) {
    var indexKey = [this.name, this.makeIndexTableName(table, index)];
    indexKey.push.apply(indexKey, values);
    indexKey.push(key);
    return indexKey;
  };

  this.makeIndexTableName = function(table, index) {
    return table.name + ':' + index.name;
  };
});

module.exports = KindaStoreDB;
