// Restored from express-x-client commit 944c398.
import Dexie from "dexie";
import { from, defer } from 'rxjs';
import { distinctUntilChanged, finalize, startWith, switchMap } from 'rxjs/operators';
import { liveQuery } from "dexie";
// uuidv7 are monotonically increasing and much improve database performance amid B-tree indexes
import { v7 as uuidv7 } from 'uuid';
import { tryOnScopeDispose } from '@vueuse/core';



//////////////////////////       OFFLINE PLUGIN       //////////////////////////
// Enrich `app` with methods, attributes and listeners to handle offline-first crud database access

export function offlinePlugin(app, options = {}) {
   if (typeof options.cacheNamespace !== 'string' || !options.cacheNamespace.trim()) {
      throw new TypeError('offlinePlugin requires a non-empty cacheNamespace')
   }

   const modelSyncFunctions = new Set()
   const syncScopeRefCounts = new Map()

   function createOfflineModel(modelName, fields) {

      const dbName = `${options.cacheNamespace}:${modelName}`;
      const db = getOrCreateDB(dbName, fields);
      const synchronizedWhereKeys = new Set();
      const synchronizeWherePromises = new Map();
      const ownedWhereCounts = new Map();

      const reset = async () => {
         console.log('reset', modelName);
         synchronizedWhereKeys.clear()
         await db.transaction('rw', [db.values, db.metadata], async () => {
            await db.values.clear();
            await db.metadata.clear();
         })
      };


      /////////////          PUB / SUB          /////////////

      const removeCreateListener = app.service(modelName).on('createWithMeta', async ([value, meta]) => {
         console.log(`${modelName} EVENT createWithMeta`, value);
         if (!isValidServiceResultIdentity(value, meta)) return
         await db.transaction('rw', [db.values, db.metadata], async () => {
            if (await isIncomingEventStale(value?.uid ?? meta?.uid, meta)) return
            if (meta?.deleted_at) {
               await db.values.delete(meta.uid)
               await db.metadata.delete(meta.uid)
               return
            }
            if (value?.uid) await db.values.put(sanitizeServerValue(value));
            if (meta?.uid) await db.metadata.put({ ...meta, __dirty__: false });
         })
      });

      const removeUpdateListener = app.service(modelName).on('updateWithMeta', async ([value, meta]) => {
         console.log(`${modelName} EVENT updateWithMeta`, value);
         if (!isValidServiceResultIdentity(value, meta)) return
         // value may be undefined when the server's update yielded 0 rows
         // (concurrent delete race: record was removed between the sync's findMany
         // snapshot and the actual update). Guard to avoid a TypeError crash that
         // would prevent db.metadata.put(meta) from running.
         await db.transaction('rw', [db.values, db.metadata], async () => {
            if (await isIncomingEventStale(value?.uid ?? meta?.uid, meta)) return
            if (meta?.deleted_at) {
               await db.values.delete(meta.uid)
               await db.metadata.delete(meta.uid)
               return
            }
            if (value?.uid) await db.values.put(sanitizeServerValue(value));
            if (meta?.uid) await db.metadata.put({ ...meta, __dirty__: false });
         })
      });

      const removeDeleteListener = app.service(modelName).on('deleteWithMeta', async ([value, meta]) => {
         console.log(`${modelName} EVENT deleteWithMeta`, value)
         if (!isValidServiceResultIdentity(value, meta)) return
         // value may be undefined when the server's delete yielded 0 rows
         // (double-delete race).
         // delete, not put: synchronize() step 2 also deletes idbMetadata for the same
         // uid. If the pub/sub handler fires AFTER step 2, put() would re-create the
         // metadata row as a permanent orphan. delete() is idempotent regardless of order.
         const uid = value?.uid ?? meta?.uid
         await db.transaction('rw', [db.values, db.metadata], async () => {
            if (await isIncomingEventStale(uid, meta)) return
            if (value?.uid) await db.values.delete(value.uid)
            if (uid) await db.metadata.delete(uid)
         })
      });

      async function isIncomingEventStale(uid, incomingMeta) {
         if (!uid || !incomingMeta) return false
         const currentMeta = await db.metadata.get(uid)
         if (!currentMeta) return false
         return compareMetadataTime(currentMeta, incomingMeta) > 0
      }


      /////////////          CREATE/UPDATE/REMOVE          /////////////

      async function create(data) {
         // in offline-first context, uid is created client-side, since server may not be accessible
         const uid = uuidv7()
         // optimistic update
         const now = new Date()
         const safeData = sanitizeMutationData(data)
         await db.transaction('rw', [db.values, db.metadata], async () => {
            await db.values.add({ ...safeData, uid })
            await db.metadata.add({ uid, created_at: now, __dirty__: true, __operation__: 'create' })
         })
         // execute on server, asynchronously, if connection is active
         if (app.isConnected) {
            app.service(modelName).createWithMeta(uid, safeData, now)
            .then(result => applyCreateAcknowledgement(uid, now, result))
            .catch(async err => {
               console.log(`*** err sync ${modelName} create`, err)
               if (!isDefinitiveServiceError(err)) return
               const currentMetadata = await db.metadata.get(uid)
               if (!isCreateRequestStillCurrent(currentMetadata, now)) return
               // rollback
               await db.values.delete(uid)
               await db.metadata.delete(uid)
            })
         }
         return await db.values.get(uid)
      }

      async function applyCreateAcknowledgement(uid, requestCreatedAt, result) {
         const [value, meta] = validateServiceResult(result, uid)
         await db.transaction('rw', [db.values, db.metadata], async () => {
            const currentMetadata = await db.metadata.get(uid)
            if (!isCreateRequestStillCurrent(currentMetadata, requestCreatedAt)) return
            if (meta?.deleted_at) {
               await db.values.delete(uid)
               await db.metadata.delete(uid)
               return
            }
            if (value?.uid) await db.values.put(sanitizeServerValue(value))
            await db.metadata.put({ ...meta, __dirty__: false })
         })
      }

      function isCreateRequestStillCurrent(currentMetadata, requestCreatedAt) {
         return currentMetadata
            && sameTimestamp(currentMetadata.created_at, requestCreatedAt)
            && !currentMetadata.updated_at
            && !currentMetadata.deleted_at
      }

      async function applyUpdateAcknowledgement(uid, requestMetadata, result) {
         const [value, meta] = validateServiceResult(result, uid)
         await db.transaction('rw', [db.values, db.metadata], async () => {
            const currentMetadata = await db.metadata.get(uid)
            if (!metadataUnchangedSinceRequest(currentMetadata, requestMetadata)) return
            if (meta?.deleted_at) {
               await db.values.delete(uid)
               await db.metadata.delete(uid)
               return
            }
            if (value?.uid) await db.values.put(sanitizeServerValue(value))
            await db.metadata.put({ ...meta, __dirty__: false })
         })
      }

      async function applyDeleteAcknowledgement(uid, requestMetadata, result) {
         const [value, meta] = validateServiceResult(result, uid)
         await db.transaction('rw', [db.values, db.metadata], async () => {
            const currentMetadata = await db.metadata.get(uid)
            if (!metadataUnchangedSinceRequest(currentMetadata, requestMetadata)) return
            if (meta?.uid && compareMetadataTime(currentMetadata, meta) > 0) return
            if (meta?.deleted_at && !sameTimestamp(meta.deleted_at, requestMetadata.deleted_at)) {
               await db.values.delete(uid)
               await db.metadata.delete(uid)
               return
            }
            if (value?.uid && !meta?.deleted_at) {
               const restoredValue = { ...value }
               delete restoredValue.__deleted__
               await db.values.put(restoredValue)
            }
            await db.metadata.put({ ...meta, __dirty__: false })
         })
      }

      const update = async (uid, data) => {
         const safeData = sanitizeMutationData(data)
         let previousValue
         let previousMetadata
         let fullUpdatedData
         let now
         const updated = await db.transaction('rw', [db.values, db.metadata], async () => {
            const existingValue = await db.values.get(uid)
            if (!existingValue) return false
            previousValue = { ...existingValue }
            previousMetadata = { ...(await db.metadata.get(uid)) }
            now = nextMutationTimestamp(previousMetadata)
            await db.values.update(uid, safeData)
            fullUpdatedData = sanitizeMutationData({ ...existingValue, ...safeData })
            const operation = previousMetadata.__operation__ === 'create' ? 'create' : 'update'
            await db.metadata.put({ uid, ...previousMetadata, updated_at: now, __dirty__: true, __operation__: operation })
            return true
         })
         if (!updated) return undefined
         // execute on server, asynchronously, if connection is active
         if (app.isConnected) {
            app.service(modelName).updateWithMeta(uid, fullUpdatedData, now)
            .then(result => applyUpdateAcknowledgement(uid, { ...previousMetadata, updated_at: now }, result))
            .catch(async err => {
               console.log(`*** err sync ${modelName} update`, err)
               if (!isDefinitiveServiceError(err)) return
               await db.transaction('rw', [db.values, db.metadata], async () => {
                  const currentMetadata = await db.metadata.get(uid)
                  if (!currentMetadata || !sameTimestamp(currentMetadata.updated_at, now)) return
                  const currentValue = await db.values.get(uid)
                  const currentDeletedAt = currentMetadata.deleted_at ?? (currentValue?.__deleted__ ? new Date() : null)
                  delete previousValue.uid
                  await db.values.update(uid, previousValue)
                  const rollbackMetadata = {
                     updated_at: previousMetadata.updated_at ?? null,
                     __dirty__: currentDeletedAt ? currentMetadata.__dirty__ : (previousMetadata.__dirty__ ?? false),
                  }
                  if (currentDeletedAt) rollbackMetadata.deleted_at = currentDeletedAt
                  await db.metadata.update(uid, rollbackMetadata)
               })
            })
         }
         return await db.values.get(uid)
      }

      const remove = async (uid) => {
         let previousMetadata
         let previousDeletedMarker
         let deleted_at
         const removed = await db.transaction('rw', [db.values, db.metadata], async () => {
            const existingValue = await db.values.get(uid)
            if (!existingValue) return false
            previousDeletedMarker = existingValue.__deleted__
            previousMetadata = { ...(await db.metadata.get(uid)) }
            deleted_at = nextMutationTimestamp(previousMetadata)
            await db.values.update(uid, { __deleted__: true })
            await db.metadata.put({ uid, ...previousMetadata, deleted_at, __dirty__: true, __operation__: 'delete' })
            return true
         })
         if (!removed) return undefined
         // and in database, if connected
         if (app.isConnected) {
            app.service(modelName).deleteWithMeta(uid, deleted_at)
            .then(result => applyDeleteAcknowledgement(uid, { ...previousMetadata, deleted_at }, result))
            .catch(async err => {
               console.log(`*** err sync ${modelName} remove`, err)
               if (!isDefinitiveServiceError(err)) return
               await db.transaction('rw', [db.values, db.metadata], async () => {
                  const currentMetadata = await db.metadata.get(uid)
                  if (!metadataUnchangedSinceRequest(currentMetadata, { ...previousMetadata, deleted_at })) return
                  await db.values.update(uid, { __deleted__: previousDeletedMarker ?? null })
                  await db.metadata.put({ uid, ...previousMetadata })
               })
            })
         }
      }

      /////////////          DIRECT CACHE ACCESS          /////////////

      async function findByUID(uid) {
         const value = await db.values.get(uid)
         return value?.__deleted__ ? undefined : value
      }

      function findWhere(where = {}) {
         const predicate = wherePredicate(where)
         return db.values.filter(value => !value.__deleted__ && predicate(value)).toArray()
      }

      /////////////          REAL-TIME OBSERVABLE          /////////////

      function getObservable(where = {}) {
         const predicate = wherePredicate(where)
         const liveQuery$ = from(liveQuery(() => db.values.filter(value => !value.__deleted__ && predicate(value)).toArray())).pipe(
            distinctUntilChanged((prev, curr) => {
               // Deep equality check to prevent unnecessary emissions (in particular on database write)
               return JSON.stringify(prev) === JSON.stringify(curr)
            })
         )
         
         // Delay subscribing to liveQuery until the cache is up to date: if `where` is a
         // newly-registered filter and we're online, wait for synchronize() to complete
         // first, so the first emission is already the fully synced data (or [] if truly
         // empty). Otherwise (warm cache or offline) there's nothing to wait for.
         //
         // defer() gates the liveQuery subscription:
         //   - defer re-runs the setup on each subscription, calling addSynchroWhere(where) to get isNew.
         //   - If isNew && app.isConnected, it chains synchronize(...) before switching to liveQuery$, so the first emission already reflects
         //   the fully-synced cache ([] only if genuinely empty).
         //   - If !isNew or offline, the .then resolves to undefined immediately and switchMap subscribes to liveQuery$ right away — same
         //   behavior as before.
         return defer(() => {
            const ready = addSynchroWhere(where).then((isNew) => {
               const whereKey = stringifyWithSortedKeys(where)
               if (app.isConnected && (isNew || !synchronizedWhereKeys.has(whereKey))) {
                  return synchronizeWhere(where)
               }
            })
            return from(ready).pipe(
               switchMap(() => liveQuery$),
               finalize(() => { void removeSynchroWhere(where) }),
            )
         })
      }

      let count = 0;
      
      function addSynchroWhere(where) {
         validateWhere(where)
         const whereKey = stringifyWithSortedKeys(where)
         ownedWhereCounts.set(whereKey, (ownedWhereCounts.get(whereKey) ?? 0) + 1)
         const refKey = syncScopeRefKey(dbName, whereKey)
         syncScopeRefCounts.set(refKey, (syncScopeRefCounts.get(refKey) ?? 0) + 1)
         const promise = addSynchroDBWhere(where, db.whereList)
         promise.then(isNew => isNew && console.log(`addSynchroWhere (${++count})`, dbName, modelName, where))
         return promise
      }

      function removeSynchroWhere(where) {
         console.log('removeSynchroWhere', dbName, modelName, where)
         const whereKey = stringifyWithSortedKeys(where)
         const ownedCount = ownedWhereCounts.get(whereKey) ?? 0
         if (ownedCount > 1) {
            ownedWhereCounts.set(whereKey, ownedCount - 1)
         } else {
            ownedWhereCounts.delete(whereKey)
            synchronizedWhereKeys.delete(whereKey)
         }

         const refKey = syncScopeRefKey(dbName, whereKey)
         const nextRefCount = (syncScopeRefCounts.get(refKey) ?? 1) - 1
         if (nextRefCount > 0) {
            syncScopeRefCounts.set(refKey, nextRefCount)
            return Promise.resolve(false)
         }

         syncScopeRefCounts.delete(refKey)
         count -= 1
         return removeSynchroDBWhere(where, db.whereList)
      }

      async function synchronizeAll() {
         await flushDirtyMutations(modelName, db.values, db.metadata)
         await synchronizeModelWhereList(modelName, db.values, db.metadata, db.whereList, synchronizeWhere)
      }

      async function synchronizeWhere(where) {
         const whereKey = stringifyWithSortedKeys(where)
         if (!synchronizeWherePromises.has(whereKey)) {
            const promise = synchronize(modelName, db.values, db.metadata, where)
               .then(() => {
                  synchronizedWhereKeys.add(whereKey)
               })
               .finally(() => {
                  synchronizeWherePromises.delete(whereKey)
               })
            synchronizeWherePromises.set(whereKey, promise)
         }
         return synchronizeWherePromises.get(whereKey)
      }

      // Automatically clean up when the component using this composable unmounts
      tryOnScopeDispose(async () => {
         console.log('CLEANING', dbName, modelName)
         modelSyncFunctions.delete(synchronizeAll)
         removeCreateListener()
         removeUpdateListener()
         removeDeleteListener()
         const ownedWhereEntries = [...ownedWhereCounts.entries()]
         for (const [whereKey, ownedCount] of ownedWhereEntries) {
            const where = JSON.parse(whereKey)
            for (let i = 0; i < ownedCount; i++) {
               await removeSynchroWhere(where)
            }
         }
      })

      modelSyncFunctions.add(synchronizeAll)

      return {
         db, reset,
         create, update, remove,
         findByUID, findWhere,
         getObservable,
         synchronizeAll,
         addSynchroWhere,
      }
   }

   let hasConnected = false

   app.addConnectListener(async (_socket) => {
      app.connectedDate = new Date()
      console.log('onConnect', app.connectedDate)
      app.isConnected = true
      const disconnectedDate = app.disconnectedDate
      const isInitialConnect = !hasConnected
      hasConnected = true
      if (disconnectedDate || isInitialConnect) {
         const results = await Promise.allSettled([...modelSyncFunctions].map(sync => sync()))
         const failures = results.filter(result => result.status === 'rejected')
         if (failures.length > 0) {
            console.error('err reconnect synchronizeAll', failures.map(result => result.reason))
            return
         }
      }
      app.disconnectedDate = null
   })

   app.addDisconnectListener(async (_socket) => {
      app.connectedDate = null
      app.disconnectedDate = new Date()
      console.log('onDisconnect', app.disconnectedDate)
      app.isConnected = false
   })


   const mutex = new Mutex()

   async function flushDirtyMutations(modelName, idbValues, idbMetadata) {
      await mutex.acquire()
      try {
         const dirtyMetadataList = await idbMetadata
            .filter(metadata => metadata.__dirty__ && metadata.__operation__)
            .toArray()

         for (const requestMetadata of dirtyMetadataList) {
            if (requestMetadata.uid == null) continue
            const currentMetadata = await idbMetadata.get(requestMetadata.uid)
            if (!metadataUnchangedSinceRequest(currentMetadata, requestMetadata)) continue

            try {
               let result
               if (requestMetadata.__operation__ === 'delete' || requestMetadata.deleted_at) {
                  result = await app.service(modelName).deleteWithMeta(requestMetadata.uid, requestMetadata.deleted_at)
               } else {
                  const fullValue = await idbValues.get(requestMetadata.uid)
                  if (fullValue == null || fullValue.__deleted__) continue
                  const safeData = sanitizeMutationData(fullValue)

                  if (requestMetadata.__operation__ === 'create' && requestMetadata.created_at) {
                     result = await app.service(modelName).createWithMeta(requestMetadata.uid, safeData, requestMetadata.created_at)
                     if (requestMetadata.updated_at) {
                        const [, createMeta] = validateServiceResult(result, requestMetadata.uid)
                        if (createMeta.deleted_at && compareMetadataTime(createMeta, requestMetadata) >= 0) {
                           // A newer/equal server tombstone wins; keep it as the final
                           // acknowledgement instead of resurrecting with the local update.
                        } else {
                           result = await app.service(modelName).updateWithMeta(requestMetadata.uid, safeData, requestMetadata.updated_at)
                        }
                     }
                  } else if (requestMetadata.updated_at) {
                     result = await app.service(modelName).updateWithMeta(requestMetadata.uid, safeData, requestMetadata.updated_at)
                  } else if (requestMetadata.created_at) {
                     result = await app.service(modelName).createWithMeta(requestMetadata.uid, safeData, requestMetadata.created_at)
                  } else {
                     continue
                  }
               }

               const [serverValue, serverMeta] = validateServiceResult(result, requestMetadata.uid)
               await idbValues.db.transaction('rw', [idbValues, idbMetadata], async () => {
                  const latestMetadata = await idbMetadata.get(requestMetadata.uid)
                  if (!metadataUnchangedSinceRequest(latestMetadata, requestMetadata)) return
                  if (serverMeta.deleted_at) {
                     await idbValues.delete(requestMetadata.uid)
                     await idbMetadata.delete(requestMetadata.uid)
                     return
                  }
                  if (serverValue?.uid) await idbValues.put(sanitizeServerValue(serverValue))
                  await idbMetadata.put({ ...serverMeta, __dirty__: false })
               })
            } catch(err) {
               console.log("*** err flush dirty mutation", modelName, requestMetadata.uid, err)
               // Keep the durable dirty marker. The mutation is idempotent and will
               // be retried on the next reconnect or explicit synchronizeAll().
            }
         }
      } finally {
         mutex.release()
      }
   }

   // ex: where = { uid: 'azer' }
   async function synchronize(modelName, idbValues, idbMetadata, where) {
      await mutex.acquire()
      console.log('synchronize', modelName, where)

      try {
         const requestPredicate = wherePredicate(where)

         // collect meta-data of local values
         // NOTE: __delete__ on values allows to collect metadata from cache-deleted values
         const valueList = await idbValues.filter(requestPredicate).toArray()
         const clientMetadataDict = Object.create(null)
         for (const value of valueList) {
            const metadata = await idbMetadata.get(value.uid)
            if (metadata) {
               clientMetadataDict[value.uid] = metadata
            } else {
               // Repair old/corrupt IndexedDB state where a visible value exists
               // without metadata; otherwise the server receives {} and cannot sync it.
               // Use the oldest possible timestamp: a repaired cache row has unknown
               // provenance and must not beat a real server version or tombstone.
               const repairedMetadata = { uid: value.uid, created_at: new Date(0), __dirty__: true, __repaired__: true }
               await idbMetadata.put(repairedMetadata)
               clientMetadataDict[value.uid] = repairedMetadata
            }
         }
         const dirtyMetadataList = await idbMetadata.filter(metadata => metadata.__dirty__).toArray()
         for (const metadata of dirtyMetadataList) {
            if (Object.hasOwn(clientMetadataDict, metadata.uid)) continue
            const value = await idbValues.get(metadata.uid)
            if (value) {
               if (requestPredicate(value)) clientMetadataDict[metadata.uid] = metadata
            } else if (metadata.deleted_at && Object.keys(where).length === 0) {
               clientMetadataDict[metadata.uid] = metadata
            }
         }

         // call sync service on `where` perimeter
         const syncResult = validateSyncResult(
            await app.service('sync').go(modelName, where, clientMetadataDict),
         )
         const { addClient, updateClient, deleteClient, addDatabase, updateDatabase } = syncResult
         console.log('-> service.sync', modelName, where, addClient, updateClient, deleteClient, addDatabase, updateDatabase)

         // 1- add missing elements in indexedDB cache
         // Use a single transaction for all adds to ensure atomicity.
         // put() instead of add() for metadata: a deleteWithMeta pub/sub event leaves
         // an orphaned metadata row (value deleted, metadata kept with deleted_at).
         // add() would throw a ConstraintError on that orphan; put() upserts safely.
         if (addClient.length > 0) {
            await idbValues.db.transaction('rw', [idbValues, idbMetadata], async () => {
               for (const [value, metaData] of addClient) {
                  // put() instead of add(): if create() ran concurrently and added this
                  // uid to Dexie between the idbValues.filter snapshot and this step,
                  // add() would throw ConstraintError and abort the entire transaction,
                  // silently dropping every other addClient record in the batch.
                  const currentMetadata = await idbMetadata.get(value.uid)
                  if (currentMetadata
                     && compareMetadataTime(metaData, currentMetadata) <= 0
                     && !(currentMetadata.deleted_at && !metaData.deleted_at)) continue
                  await idbValues.put(sanitizeServerValue(value))
                  await idbMetadata.put({ ...metaData, __dirty__: false })
               }
            })
         }
         // 2- delete elements from indexedDB cache
         if (deleteClient.length > 0) {
            await idbValues.db.transaction('rw', [idbValues, idbMetadata], async () => {
               for (const [uid, deletedAt] of deleteClient) {
                  const currentMetadata = await idbMetadata.get(uid)
                  // A deleteWithMeta pub/sub event may already have removed both rows
                  // while this sync request was in flight. Do not recreate a clean
                  // tombstone after that successful acknowledgement.
                  if (!currentMetadata && clientMetadataDict[uid]) continue
                  const unchanged = metadataUnchangedSinceRequest(currentMetadata, clientMetadataDict[uid])
                  if (!unchanged && compareMetadataTime(currentMetadata, { uid, deleted_at: deletedAt }) > 0) continue
                  await idbValues.delete(uid)
                  if (unchanged) await idbMetadata.delete(uid)
                  else await idbMetadata.put({ uid, deleted_at: deletedAt, __dirty__: false })
               }
            })
         }
         // 3- update elements of cache with server's newer version
         for (const [elt, serverMeta] of updateClient) {
            await idbValues.db.transaction('rw', [idbValues, idbMetadata], async () => {
               const currentMetadata = await idbMetadata.get(elt.uid)
               if (!metadataUnchangedSinceRequest(currentMetadata, clientMetadataDict[elt.uid])
                  && compareMetadataTime(currentMetadata, serverMeta) > 0) return
               const value = { ...elt }
               delete value.__deleted__
               await idbValues.put(value)
               await idbMetadata.put({ uid: elt.uid, ...serverMeta, __dirty__: false })
            })
         }

         // 4- create elements of `addDatabase` with full data from cache
         for (const elt of addDatabase) {
            // elt.uid is undefined when the clientMetadataDict fallback {} was used
            // (record exists in idbValues but metadata is missing).  Guard before the
            // get() call: idbValues.get(undefined) itself throws before fullValue is
            // assigned, so checking fullValue == null afterwards is too late.
            if (elt.uid == null) continue
            let currentMetadata = await idbMetadata.get(elt.uid)
            if (!metadataUnchangedSinceRequest(currentMetadata, elt)) continue
            const fullValue = await idbValues.get(elt.uid)
            if (fullValue == null) continue  // record deleted concurrently
            if (!requestPredicate(fullValue)) continue
            delete fullValue.uid
            delete fullValue.__deleted__
            try {
               const result = await app.service(modelName).createWithMeta(elt.uid, fullValue, elt.created_at)
               const [, serverMeta] = validateServiceResult(result, elt.uid)
               await idbValues.db.transaction('rw', [idbValues, idbMetadata], async () => {
                  currentMetadata = await idbMetadata.get(elt.uid)
                  if (!metadataUnchangedSinceRequest(currentMetadata, elt)) return
                  if (serverMeta.deleted_at) {
                     await idbValues.delete(elt.uid)
                     await idbMetadata.delete(elt.uid)
                     return
                  }
                  if (result[0]?.uid) {
                     const returnedValue = { ...result[0] }
                     delete returnedValue.__deleted__
                     await idbValues.put(returnedValue)
                  }
                  await idbMetadata.put({ ...serverMeta, __dirty__: false })
               })
            } catch(err) {
               console.log("*** err sync user addDatabase", err, elt.uid, fullValue, elt.created_at)
               // A failed request may be a timeout or disconnect after the server
               // committed. Keep the local mutation dirty so the idempotent server
               // operation can reconcile it on the next sync.
            }
         }

         // 5- update elements of `updateDatabase` with full data from cache
         for (const elt of updateDatabase) {
            if (elt.uid == null) continue
            let currentMetadata = await idbMetadata.get(elt.uid)
            if (!metadataUnchangedSinceRequest(currentMetadata, elt)) continue
            const fullValue = await idbValues.get(elt.uid)
            if (fullValue == null) continue  // record deleted concurrently
            delete fullValue.uid
            delete fullValue.__deleted__
            try {
               const updateTimestamp = elt.updated_at ?? elt.created_at
               const result = await app.service(modelName).updateWithMeta(elt.uid, fullValue, updateTimestamp)
               const [, serverMeta] = validateServiceResult(result, elt.uid)
               await idbValues.db.transaction('rw', [idbValues, idbMetadata], async () => {
                  currentMetadata = await idbMetadata.get(elt.uid)
                  if (!metadataUnchangedSinceRequest(currentMetadata, elt)) return
                  if (serverMeta.deleted_at) {
                     await idbValues.delete(elt.uid)
                     await idbMetadata.delete(elt.uid)
                     return
                  }
                  if (result[0]?.uid) {
                     const returnedValue = { ...result[0] }
                     delete returnedValue.__deleted__
                     await idbValues.put(returnedValue)
                  }
                  await idbMetadata.put({ ...serverMeta, __dirty__: false })
               })
            } catch(err) {
               console.log("*** err sync user updateDatabase", err)
               // Leave client's local version intact; it will be retried on the next sync.
            }
         }
      } catch(err) {
         console.log('err synchronize', modelName, where, err)
         throw err
      } finally {
         mutex.release()
      }
   }

   function metadataUnchangedSinceRequest(currentMetadata, requestMetadata) {
      return currentMetadata
         && requestMetadata
         && sameTimestamp(currentMetadata.created_at, requestMetadata.created_at)
         && sameTimestamp(currentMetadata.updated_at, requestMetadata.updated_at)
         && sameTimestamp(currentMetadata.deleted_at, requestMetadata.deleted_at)
   }

   // Singleton map to reuse Dexie instances per database name
   const dbInstances = new Map();

   function getOrCreateDB(dbName, fields) {
      if (!dbInstances.has(dbName)) {
         const db = new Dexie(dbName);
         db.version(1).stores({
            whereList: "sortedjson",
            values: ['uid', '__deleted__', ...fields].join(','),
            metadata: "uid, created_at, updated_at, deleted_at",
         });
         dbInstances.set(dbName, db);
      }
      return dbInstances.get(dbName);
   }

   async function getWhereList(whereDb) {
      const list = await whereDb.toArray()
      return list.map(elt => JSON.parse(elt.sortedjson))
   }

   async function addSynchroDBWhere(where, whereDb) {
      await mutex.acquire()
      let modified = false
      try {
         const sortedjson = stringifyWithSortedKeys(where)
         const existing = await whereDb.get(sortedjson)
         if (!existing) {
            // sortedjson is used as a unique standardized representation of a 'where' object ; it is used both as key and value in 'wheredb' database
            await whereDb.add({ sortedjson })
            modified = true
         }
      } catch(err) {
         console.log('err addSynchroDBWhere', where, err)
      } finally {
         mutex.release()
      }
      return modified
   }

   async function removeSynchroDBWhere(where, whereDb) {
      await mutex.acquire()
      try {
         const swhere = stringifyWithSortedKeys(where)
         await whereDb.filter(value => (value.sortedjson === swhere)).delete()
      } catch(err) {
         console.log('err removeSynchroDBWhere', err)
      } finally {
         mutex.release()
      }
   }

   async function synchronizeModelWhereList(modelName, idbValues, idbMetadata, whereDb, syncWhere = null) {
      const whereList = await getWhereList(whereDb)
      for (const where of whereList) {
         if (syncWhere) await syncWhere(where)
         else await synchronize(modelName, idbValues, idbMetadata, where)
      }
   }

   // enrich `app` with new methods and attributes
   return Object.assign(app, {
      createOfflineModel,
   })
}


//////////////////////////       UTILITIES       //////////////////////////


function stringifyWithSortedKeys(obj, space = null) {
   return JSON.stringify(obj, (key, value) => {
      // If the value is a plain object (not an array, null, or other object type like Date)
      if (value && typeof value === 'object' && !Array.isArray(value) && Object.prototype.toString.call(value) === '[object Object]') {
         const sorted = {};
         // Get all keys, sort them, and then re-add them to a new object
         // This new object will maintain the sorted order when stringified
         Object.keys(value).sort().forEach(k => {
            sorted[k] = value[k];
         });
         return sorted;
      }
      // For all other types (primitives, arrays, null, etc.), return the value as is
      return value;
   }, space); // 'space' is optional for pretty-printing (e.g., 2 or 4)
}
// console.log('stringifyWithSortedKeys({ age: 30, name: "Alice", data: { city: "Paris", color: "red" }})', stringifyWithSortedKeys({ age: 30, name: "Alice", data: { city: "Paris", color: "red" } }))

function sameTimestamp(a, b) {
   if (!a || !b) return a === b
   return new Date(a).getTime() === new Date(b).getTime()
}

function compareMetadataTime(a, b) {
   const aTime = metadataTime(a)
   const bTime = metadataTime(b)
   if (aTime == null || bTime == null) return 0
   return aTime - bTime
}

function syncScopeRefKey(dbName, whereKey) {
   return `${dbName}\0${whereKey}`
}

function metadataTime(meta) {
   const value = meta?.deleted_at ?? meta?.updated_at ?? meta?.created_at
   if (!value) return null
   const time = new Date(value).getTime()
   return Number.isNaN(time) ? null : time
}

function nextMutationTimestamp(meta) {
   const previousTime = metadataTime(meta)
   const now = Date.now()
   return new Date(previousTime == null ? now : Math.max(now, previousTime + 1))
}

function isDefinitiveServiceError(err) {
   // Server responses are serialized plain objects. Socket.IO timeouts and
   // disconnects reject with Error instances and are ambiguous: the server may
   // already have committed the mutation before the acknowledgement was lost.
   return Boolean(
      err
      && typeof err === 'object'
      && !(err instanceof Error)
      && typeof err.code === 'string',
   )
}

function sanitizeMutationData(data) {
   if (!data || typeof data !== 'object' || Array.isArray(data)) {
      throw new TypeError('mutation data must be an object')
   }
   const { uid: _uid, __deleted__: _deleted, ...safeData } = data
   return safeData
}

function isValidServiceResultIdentity(value, meta) {
   return Boolean(
      meta
      && typeof meta === 'object'
      && typeof meta.uid === 'string'
      && (!value || (typeof value === 'object' && value.uid === meta.uid)),
   )
}

function validateServiceResult(result, expectedUid) {
   if (!Array.isArray(result) || result.length < 2) {
      throw new TypeError('service mutation result must be a [value, metadata] tuple')
   }
   const [value, meta] = result
   if (!isValidServiceResultIdentity(value, meta) || meta.uid !== expectedUid) {
      throw new TypeError(`service mutation result uid must equal '${expectedUid}'`)
   }
   validateMetadata(meta)
   return [value, meta]
}

function validateSyncResult(result) {
   if (!result || typeof result !== 'object') throw new TypeError('sync result must be an object')
   const keys = ['addClient', 'updateClient', 'deleteClient', 'addDatabase', 'updateDatabase']
   for (const key of keys) {
      if (!Array.isArray(result[key])) throw new TypeError(`sync result '${key}' must be an array`)
   }
   for (const tuple of [...result.addClient, ...result.updateClient]) {
      const uid = tuple?.[0]?.uid
      if (typeof uid !== 'string') throw new TypeError('sync value uid must be a string')
      validateServiceResult(tuple, uid)
   }
   for (const tuple of result.deleteClient) {
      if (!Array.isArray(tuple) || typeof tuple[0] !== 'string'
         || Number.isNaN(new Date(tuple[1]).getTime())) {
         throw new TypeError('sync deleteClient entry must be a [uid, timestamp] tuple')
      }
   }
   for (const metadata of [...result.addDatabase, ...result.updateDatabase]) {
      validateMetadata(metadata)
   }
   return result
}

function validateMetadata(metadata) {
   if (!metadata || typeof metadata !== 'object' || typeof metadata.uid !== 'string') {
      throw new TypeError('sync metadata must contain a string uid')
   }
   const timestampFields = ['created_at', 'updated_at', 'deleted_at']
   if (!timestampFields.some(field => metadata[field] != null)) {
      throw new TypeError(`sync metadata '${metadata.uid}' must contain a timestamp`)
   }
   for (const field of timestampFields) {
      if (metadata[field] != null && Number.isNaN(new Date(metadata[field]).getTime())) {
         throw new TypeError(`sync metadata '${metadata.uid}.${field}' must be a valid timestamp`)
      }
   }
}

function sanitizeServerValue(value) {
   const { __deleted__: _deleted, ...safeValue } = value
   return safeValue
}

function validateWhere(where) {
   if (!where || typeof where !== 'object' || Array.isArray(where)
      || Object.prototype.toString.call(where) !== '[object Object]') {
      throw new TypeError('where must be a plain object')
   }
   validateJsonFilterValue(where, 'where')
   return where
}

function validateJsonFilterValue(value, path) {
   if (value === undefined || typeof value === 'function'
      || typeof value === 'symbol' || typeof value === 'bigint') {
      throw new TypeError(`${path} contains a non-serializable value`)
   }
   if (typeof value === 'number' && !Number.isFinite(value)) {
      throw new TypeError(`${path} contains a non-finite number`)
   }
   if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) throw new TypeError(`${path} contains an invalid Date`)
      return
   }
   if (Array.isArray(value)) {
      value.forEach((entry, index) => validateJsonFilterValue(entry, `${path}[${index}]`))
      return
   }
   if (value && typeof value === 'object') {
      if (Object.prototype.toString.call(value) !== '[object Object]') {
         throw new TypeError(`${path} contains a non-plain object`)
      }
      for (const [key, entry] of Object.entries(value)) {
         validateJsonFilterValue(entry, `${path}.${key}`)
      }
   }
}

export class Mutex {
   constructor() {
      this.locked = false;
      this.queue = [];
   }

   async acquire() {
      if (this.locked) {
         return new Promise(resolve => this.queue.push(resolve));
      }
      this.locked = true;
   }

   release() {
      if (this.queue.length > 0) {
         const next = this.queue.shift();
         next();
      } else {
         this.locked = false;
      }
   }
}

function wherePredicate(where) {
   return (elt) => {
      for (const [attr, value] of Object.entries(where)) {
         const eltAttrValue = elt[attr]

         if (typeof(value) === 'string' || typeof(value) === 'number' || typeof(value) === 'boolean') {
            // 'attr = value' clause
            if (!sameWhereValue(eltAttrValue, value)) return false

         } else if (value === null) {
            // 'attr = null' clause
            if (eltAttrValue !== null) return false

         } else if (hasRangeOperator(value)) {
            // 'attr = { lt/lte/gt/gte: value }' clause — all bounds apply.
            // A missing (undefined) or null field never satisfies a range constraint,
            // consistent with SQL NULL behaviour (NULL op anything = NULL = unknown).
            // JS coerces null → 0 so range guards like `null > 10` silently pass;
            // undefined coerces to NaN and all NaN comparisons return false — both
            // must be excluded explicitly.
            if (eltAttrValue === undefined || eltAttrValue === null) return false
            if ('lte' in value && compareWhereValues(eltAttrValue, value.lte) > 0) return false
            if ('lt'  in value && compareWhereValues(eltAttrValue, value.lt) >= 0)  return false
            if ('gte' in value && compareWhereValues(eltAttrValue, value.gte) < 0)  return false
            if ('gt'  in value && compareWhereValues(eltAttrValue, value.gt) <= 0)  return false
         } else if (!sameWhereValue(eltAttrValue, value)) {
            return false
         }
      }
      return true
   }
}

function hasRangeOperator(value) {
   return value
      && typeof value === 'object'
      && !Array.isArray(value)
      && Object.prototype.toString.call(value) === '[object Object]'
      && ['gte', 'gt', 'lte', 'lt'].some(key => key in value)
}

function sameWhereValue(a, b) {
   if (a instanceof Date || b instanceof Date) {
      const aTime = new Date(a).getTime()
      const bTime = new Date(b).getTime()
      return !Number.isNaN(aTime) && !Number.isNaN(bTime) && aTime === bTime
   }
   if (a && b && typeof a === 'object' && typeof b === 'object') {
      return stringifyWithSortedKeys(a) === stringifyWithSortedKeys(b)
   }
   return a === b
}

function compareWhereValues(a, b) {
   if (a instanceof Date || b instanceof Date) {
      const aTime = new Date(a).getTime()
      const bTime = new Date(b).getTime()
      if (!Number.isNaN(aTime) && !Number.isNaN(bTime)) return aTime - bTime
   }
   if (a === b) return 0
   return a > b ? 1 : -1
}

function isSubset(subset, fullObject) {
   for (const key in fullObject) {
      const fVal = fullObject[key]
      const sVal = subset[key]
      // Primitive values: use reference/value equality (works for string, number, boolean).
      // Object values (e.g. range operators { gte: 1 }): use structural equality via
      // sorted JSON so that two freshly-created identical objects compare as equal.
      if (typeof fVal === 'object' && fVal !== null) {
         if (stringifyWithSortedKeys(fVal) !== stringifyWithSortedKeys(sVal)) return false
      } else {
         if (fVal !== sVal) return false
      }
   }
   return true
}
// console.log('isSubset({a: 1, b: 2}, {b: 2})=true', isSubset({a: 1, b: 2}, {b: 2}))
// console.log('isSubset({}, {})=true', isSubset({}, {}))
// console.log('isSubset({a: 1}, {})=true', isSubset({a: 1}, {}))
// console.log('isSubset({a: 1}, {b: 2})=false', isSubset({a: 1}, {b: 2}))
// console.log('isSubset({a: 1}, {a: 1})=true', isSubset({a: 1}, {a: 1}))

function isSubsetAmong(subset, fullObjectList) {
   return fullObjectList.some(fullObject => isSubset(subset, fullObject));
}
// console.log('isSubsetAmong({a: 1, b: 2}, [{c: 3}, {b: 2}])=true', isSubsetAmong({a: 1, b: 2}, [{c: 3}, {b: 2}]))

export { offlinePlugin as syncClientPlugin }
export default offlinePlugin
