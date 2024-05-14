import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import * as promise from 'lib0/promise'
import { Observable } from 'lib0/observable'

const customStoreName = 'custom'
const updatesStoreName = 'updates'

export const PREFERRED_TRIM_SIZE = 500

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {function(IDBObjectStore):void} [beforeApplyUpdatesCallback]
 * @param {function(IDBObjectStore):void} [afterApplyUpdatesCallback]
 */
export const fetchUpdates = (idbPersistence, beforeApplyUpdatesCallback = () => {}, afterApplyUpdatesCallback = () => {}) => {
  const [updatesStore] = idb.transact(/** @type {IDBDatabase} */ (idbPersistence.db), [updatesStoreName]) // , 'readonly')
  return idb.getAll(updatesStore, idb.createIDBKeyRangeLowerBound(idbPersistence._dbref, false)).then(updates => {
    if (!idbPersistence._destroyed) {
      beforeApplyUpdatesCallback(updatesStore)
      Y.transact(idbPersistence.doc, () => {
        updates.forEach(val => Y.applyUpdate(idbPersistence.doc, val));
      }, idbPersistence, false)
      afterApplyUpdatesCallback(updatesStore)
    }
  })
    .then(() => idb.getLastKey(updatesStore).then(lastKey => { idbPersistence._dbref = lastKey + 1 }))
    .then(() => idb.count(updatesStore).then(cnt => { idbPersistence._dbsize = cnt }))
    .then(() => updatesStore)
}

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {boolean} forceStore
 */
export const storeState = (idbPersistence, forceStore = true) =>
  fetchUpdates(idbPersistence)
    .then(updatesStore => {
      if (forceStore || idbPersistence._dbsize >= PREFERRED_TRIM_SIZE) {
        idb.addAutoKey(updatesStore, Y.encodeStateAsUpdate(idbPersistence.doc))
          .then(() => idb.del(updatesStore, idb.createIDBKeyRangeUpperBound(idbPersistence._dbref, true)))
          .then(() => idb.count(updatesStore).then(cnt => { idbPersistence._dbsize = cnt }))
      }
    })

/**
 * Same as fetchUpdates above, but uses async/await and changes for promise based ydoc sync
 * @param {IDBInstace} idbInstance
 * @param {Y.Doc} doc
 * @returns {Promise<IDBObjectStore>}
 */
export const asyncFetchUpdates = async (idbInstance, doc) => {
  /**
   * Retrieve a list of object stores in the scope (readonly) of the given transaction 
   * Returns the 'updates' store for the current idb instance with readonly scope
   */
  const [updatesStore] = idb.transact(/** @type {IDBDatabase} */ (idbInstance.db), [updatesStoreName]) // , 'readonly')
  /**
   * idb.getAll() gets all the objects (updates) from the 'updates' store for the given range of keys (all the keys starting from the lower bound)
   * 
   * idb.createIDBKeyRangeLowerBound(idbPersistence._dbref, false) returns the entire range of IDB keys starting from 
   * the lower bound (idbPersistence._dbref) with no upper bound
   */
  const updates = await idb.getAll(updatesStore, idb.createIDBKeyRangeLowerBound(idbInstance._dbref, false))
  /** Apply the retrieved updates to the ydoc */
  Y.transact(doc, () => {
    updates.forEach(val => Y.applyUpdate(doc, val))
  }, idbInstance, false)
  /** set lower bound as the last key? */
  const lastKey = await idb.getLastKey(updatesStore)
  idbInstance._dbref = lastKey + 1
  /** get the number of objects (update records) and set it as _dbsize */
  const record_count = await idb.count(updatesStore)
  idbInstance._dbsize = record_count
  /** returns the 'updates' store(table) */
  return updatesStore
}

/**
 * Same as storeState above, but uses async/await and changes for promise based ydoc sync
 * @param {IDBInstace} idbInstance
 * @param {boolean} forceStore
 */
export const asyncStoreState = async (idbInstance, forceStore = true) => {
  const tempYDoc = new Y.Doc();
  const updatesStore = await asyncFetchUpdates(idbInstance, tempYDoc);
  /**
   * save entire document state as a single update under a auto generated key, 
   * delete all the other records upto the newly generated key
   * update new db size 
   */
  await idb.addAutoKey(updatesStore, Y.encodeStateAsUpdate(tempYDoc));
  await idb.del(updatesStore, idb.createIDBKeyRangeUpperBound(idbInstance._dbref, true));
  const record_count = await idb.count(updatesStore);
  idbInstance._dbsize = record_count;
  tempYDoc.destroy();
}

/**
 * @param {string} name
 */
export const clearDocument = name => idb.deleteDB(name)

/** Additional interfaces for promise based document sync */
export class IDBInstace {
  /**
   * @param {string} dbName Same as the name of the YDoc to sync / chapter ID
   */
  constructor(dbName){
    /**
     * @type {IDBDatabase | null} 
     */
    this.db = null;
    this.name = dbName;
    this._dbref = 0;
    this._dbsize = 0;
  }

  async initializeConnection() {
    if(this.db) throw new Error("y-indexeddb error: Database already initialized");
    const database = await idb.openDB(this.name, database => {
      idb.createStores(database, [
        ['updates', { autoIncrement: true }],
        ['custom']
      ])
    });
    this.db = database;
  }

  /**
   * Fetch document updates from IDB and apply to provided ydoc
   * @param {Y.Doc} doc
   */
  async syncUpdatesFromDBToDoc(doc) {
    if(!this.db) throw new Error("y-indexeddb error: DB needs to be initialized before calling this method");
    await asyncFetchUpdates(this, doc);
  }

  /**
   * Save a provided update entry in IDB
   * @param {Uint8Array} update
   */
  async updateDB(update){
    if(!this.db) throw new Error("y-indexeddb error: DB needs to be initialized before calling this method");
    const [updatesStore] = idb.transact(/** @type {IDBDatabase} */ (this.db), [updatesStoreName])
    await idb.addAutoKey(updatesStore, update)
    if (++this._dbsize >= PREFERRED_TRIM_SIZE) {
      /** No need to debounce this as in the original function since this method is manually invoked, not on ydoc onUpdate */
      await asyncStoreState(this, false);
    }
    return updatesStore;
  }

  closeConnection(){
    if(!this.db) throw new Error("y-indexeddb error: DB needs to be initialized before calling this method");
    this.db.close()
  }
}

/** Delete database during logout */

/**
 * @param {string} dbName
 */
export const deleteYChapterDB = async(dbName) => {
  await idb.deleteDB(dbName)
}


/**
 * @extends Observable<string>
 */
export class IndexeddbPersistence extends Observable {
  /**
   * @param {string} name
   * @param {Y.Doc} doc
   */
  constructor (name, doc) {
    super()
    this.doc = doc
    this.name = name
    this._dbref = 0
    this._dbsize = 0
    this._destroyed = false
    /**
     * @type {IDBDatabase|null}
     */
    this.db = null
    this.synced = false
    this._db = idb.openDB(name, db =>
      idb.createStores(db, [
        ['updates', { autoIncrement: true }],
        ['custom']
      ])
    )
    /**
     * @type {Promise<IndexeddbPersistence>}
     */
    this.whenSynced = promise.create(resolve => this.on('synced', () => resolve(this)))

    /**
     * This method gets invoked when when IDBPersistence object is initiated
     */
    this._db.then(db => {
      this.db = db
      /**
       * @param {IDBObjectStore} updatesStore
       */
      const beforeApplyUpdatesCallback = (updatesStore) => idb.addAutoKey(updatesStore, Y.encodeStateAsUpdate(doc))
      const afterApplyUpdatesCallback = () => {
        if (this._destroyed) return this
        this.synced = true
        this.emit('synced', [this])
      }
      fetchUpdates(this, beforeApplyUpdatesCallback, afterApplyUpdatesCallback)
    })
    /**
     * Timeout in ms untill data is merged and persisted in idb.
     */
    this._storeTimeout = 1000
    /**
     * @type {any}
     */
    this._storeTimeoutId = null
    /**
     * @param {Uint8Array} update
     * @param {any} origin
     */
    this._storeUpdate = (update, origin) => {
      if (this.db && origin !== this) {
        const [updatesStore] = idb.transact(/** @type {IDBDatabase} */ (this.db), [updatesStoreName])
        idb.addAutoKey(updatesStore, update)
        if (++this._dbsize >= PREFERRED_TRIM_SIZE) {
          // debounce store call
          if (this._storeTimeoutId !== null) {
            clearTimeout(this._storeTimeoutId)
          }
          this._storeTimeoutId = setTimeout(() => {
            storeState(this, false)
            this._storeTimeoutId = null
          }, this._storeTimeout)
        }
      }
    }
    doc.on('update', this._storeUpdate)
    this.destroy = this.destroy.bind(this)
    doc.on('destroy', this.destroy)
  }

  destroy () {
    if (this._storeTimeoutId) {
      clearTimeout(this._storeTimeoutId)
    }
    this.doc.off('update', this._storeUpdate)
    this.doc.off('destroy', this.destroy)
    this._destroyed = true
    return this._db.then(db => {
      db.close()
    })
  }

  /**
   * Destroys this instance and removes all data from indexeddb.
   *
   * @return {Promise<void>}
   */
  clearData () {
    return this.destroy().then(() => {
      idb.deleteDB(this.name)
    })
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @return {Promise<String | number | ArrayBuffer | Date | any>}
   */
  get (key) {
    return this._db.then(db => {
      const [custom] = idb.transact(db, [customStoreName], 'readonly')
      return idb.get(custom, key)
    })
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @param {String | number | ArrayBuffer | Date} value
   * @return {Promise<String | number | ArrayBuffer | Date>}
   */
  set (key, value) {
    return this._db.then(db => {
      const [custom] = idb.transact(db, [customStoreName])
      return idb.put(custom, value, key)
    })
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @return {Promise<undefined>}
   */
  del (key) {
    return this._db.then(db => {
      const [custom] = idb.transact(db, [customStoreName])
      return idb.del(custom, key)
    })
  }
}
