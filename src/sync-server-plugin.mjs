// Restored from express-x-drizzle commit ddf6804.
import { and, eq, gt, gte, lt, lte, inArray, isNull, getTableName } from "drizzle-orm";

import { computeSyncResult } from './sync-algorithm.mjs'

export function truncateString(str, maxLength = 300, ellipsis = '...') {
   // Check if the string already fits
   if (str.length <= maxLength) return str;
   // Calculate the cut-off point, accounting for the ellipsis length
   const cutLength = maxLength - ellipsis.length;
   // Ensure the string is long enough to be cut
   if (cutLength < 0) return str.substring(0, maxLength); // Just cut it off if ellipsis doesn't fit
   // Truncate the string and add the ellipsis
   return str.substring(0, cutLength) + ellipsis;
}





//////////////////////////       UTILITIES       //////////////////////////

function whereToDrizzleFilters(table, where) {
   validateWhere(where)
   const conditions = Object.entries(where)
      .filter(([_, value]) => value !== undefined)
      .map(([key, value]) => {
         const column = table[key]
         if (value === null) return isNull(column)
         if (typeof value === 'object') {
            // Collect ALL range bounds — a compound { gte:1, lte:10 } needs both
            const bounds = []
            if ('gte' in value) bounds.push(gte(column, coerceWhereValue(column, value.gte)))
            if ('gt'  in value) bounds.push(gt(column,  coerceWhereValue(column, value.gt)))
            if ('lte' in value) bounds.push(lte(column, coerceWhereValue(column, value.lte)))
            if ('lt'  in value) bounds.push(lt(column,  coerceWhereValue(column, value.lt)))
            if (bounds.length > 0) return and(...bounds)
         }
         return eq(column, coerceWhereValue(column, value))
      })
   return conditions.length ? and(...conditions) : undefined;
}

function validateWhere(where) {
   if (!where || typeof where !== 'object' || Array.isArray(where)
      || Object.prototype.toString.call(where) !== '[object Object]') {
      throw new TypeError('where must be a plain object')
   }
   validateJsonFilterValue(where, 'where')
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
      for (const [key, entry] of Object.entries(value)) validateJsonFilterValue(entry, `${path}.${key}`)
   }
}

function coerceWhereValue(column, value) {
   if (column?.columnType === 'PgTimestamp' && typeof value === 'string') return new Date(value)
   return value
}

function normalizeMutationTimestamp(value, field) {
   const timestamp = new Date(value)
   if (Number.isNaN(timestamp.getTime())) throw new TypeError(`${field} must be a valid timestamp`)
   const now = Date.now()
   const maxFutureSkew = 5 * 60 * 1000
   return timestamp.getTime() > now + maxFutureSkew ? new Date(now) : timestamp
}

function snapshotChanged() {
   const error = new Error('Sync snapshot changed during its read; retry synchronization')
   error.code = 'sync-snapshot-changed'
   return error
}

function metadataStorageUid(modelName, uid) {
   return JSON.stringify([modelName, uid])
}

function externalizeMetadata(row, uid) {
   return row ? { ...row, uid } : null
}

function metadataTimestamp(meta, fields = ['deleted_at', 'updated_at', 'created_at']) {
   if (!meta) return null
   for (const field of fields) {
      if (meta[field] != null) return new Date(meta[field])
   }
   return null
}

function sanitizeMutationData(data) {
   if (!data || typeof data !== 'object' || Array.isArray(data)) {
      throw new TypeError('mutation data must be an object')
   }
   const { uid: _uid, __deleted__: _deleted, ...safeData } = data
   return safeData
}

function isPlainObject(value) {
   return value && typeof value === 'object' && !Array.isArray(value) && Object.prototype.toString.call(value) === '[object Object]'
}

function hasRangeOperator(value) {
   return isPlainObject(value) && ['gte', 'gt', 'lte', 'lt'].some(key => key in value)
}

function valueWithinRange(value, range) {
   if (value === null || value === undefined) return false
   if ('gte' in range && compareWhereValues(value, range.gte) < 0) return false
   if ('gt' in range && compareWhereValues(value, range.gt) <= 0) return false
   if ('lte' in range && compareWhereValues(value, range.lte) > 0) return false
   if ('lt' in range && compareWhereValues(value, range.lt) >= 0) return false
   return true
}

function rangesOverlap(a, b) {
   const lower = [
      'gte' in a && { value: a.gte, inclusive: true },
      'gt' in a && { value: a.gt, inclusive: false },
      'gte' in b && { value: b.gte, inclusive: true },
      'gt' in b && { value: b.gt, inclusive: false },
   ].filter(Boolean).sort((x, y) => compareRangeBounds(y, x) || Number(x.inclusive) - Number(y.inclusive))[0]

   const upper = [
      'lte' in a && { value: a.lte, inclusive: true },
      'lt' in a && { value: a.lt, inclusive: false },
      'lte' in b && { value: b.lte, inclusive: true },
      'lt' in b && { value: b.lt, inclusive: false },
   ].filter(Boolean).sort((x, y) => compareRangeBounds(x, y) || Number(x.inclusive) - Number(y.inclusive))[0]

   if (!lower || !upper) return true
   const diff = compareWhereValues(lower.value, upper.value)
   if (diff < 0) return true
   if (diff > 0) return false
   return lower.inclusive && upper.inclusive
}

function compareRangeBounds(a, b) {
   return compareWhereValues(a.value, b.value)
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

function constraintsOverlap(a, b) {
   if (a === undefined || b === undefined) return true
   if (a === null || b === null) return a === null && b === null

   const aRange = hasRangeOperator(a)
   const bRange = hasRangeOperator(b)

   if (aRange && bRange) return rangesOverlap(a, b)
   if (aRange) return valueWithinRange(b, a)
   if (bRange) return valueWithinRange(a, b)
   if (isPlainObject(a) || isPlainObject(b)) return true
   return a === b
}

function whereScopesOverlap(a = {}, b = {}) {
   const sharedKeys = Object.keys(a).filter(key => Object.hasOwn(b, key))
   for (const key of sharedKeys) {
      if (!constraintsOverlap(a[key], b[key])) return false
   }
   return true
}

class OverlapLock {
   constructor() {
      this.active = []
      this.queue = []
   }

   acquire(where) {
      return new Promise(resolve => {
         const entry = { where, resolve }
         this.queue.push(entry)
         this.pump()
      })
   }

   pump() {
      for (let i = 0; i < this.queue.length; i++) {
         const entry = this.queue[i]
         if (this.active.some(active => whereScopesOverlap(active.where, entry.where))) continue
         // Preserve fairness: a later scope may bypass earlier disjoint scopes,
         // but never an earlier queued scope that it overlaps.
         if (this.queue.slice(0, i).some(queued => whereScopesOverlap(queued.where, entry.where))) continue
         this.queue.splice(i, 1)
         i -= 1
         this.active.push(entry)
         entry.resolve(() => {
            this.active = this.active.filter(active => active !== entry)
            this.pump()
         })
      }
   }

   get idle() {
      return this.active.length === 0 && this.queue.length === 0
   }
}


//////////////////////////       DRIZZLE OFFLINE PLUGIN       //////////////////////////

export function drizzleOfflinePlugin(app, db, metadata, models, options = {}) {
   if (typeof options.authorize !== 'function') {
      throw new TypeError('drizzleOfflinePlugin requires an authorize(context, operation) policy')
   }
   const computeSynchronization = options.computeSyncResult ?? computeSyncResult
   const configuredModels = new Map(models.map(model => [getTableName(model), model]))
   const storageUidFor = (modelName, uid) =>
      models.length === 1 ? uid : metadataStorageUid(modelName, uid)
   const maxSyncRecords = options.maxSyncRecords ?? 10_000
   if (!Number.isInteger(maxSyncRecords) || maxSyncRecords < 1) {
      throw new TypeError('maxSyncRecords must be a positive integer')
   }

   async function lockMetadataRow(tx, modelName, uid) {
      const storageUid = storageUidFor(modelName, uid)
      await tx.insert(metadata).values({ uid: storageUid }).onConflictDoNothing({ target: metadata.uid })
      let row = (await tx.select().from(metadata).where(eq(metadata.uid, storageUid)).for('update'))[0]

      return { storageUid, meta: externalizeMetadata(row, uid) }
   }

   async function lockUidOwnership(tx, uid) {
      if (models.length === 1) return
      const lockUid = JSON.stringify(['__uid_owner__', uid])
      await tx.insert(metadata).values({ uid: lockUid }).onConflictDoNothing({ target: metadata.uid })
      await tx.select({ uid: metadata.uid }).from(metadata).where(eq(metadata.uid, lockUid)).for('update')
   }

   async function authorize(context, modelName, action, args) {
      const allowed = await options.authorize(context, { modelName, action, args })
      if (!allowed) {
         const error = new Error(`not authorized to ${action} '${modelName}'`)
         error.code = 'forbidden'
         throw error
      }
   }

   async function assertUidUniqueToModel(tx, currentModel, uid) {
      for (const candidate of models) {
         if (candidate === currentModel) continue
         const row = (await tx.select({ uid: candidate.uid }).from(candidate).where(eq(candidate.uid, uid)))[0]
         if (row) throw new Error(`uid '${uid}' already belongs to model '${getTableName(candidate)}'`)
      }
   }

   // add a database service for each model
   for (const model of models) {
      const modelName = getTableName(model)

      app.createService(modelName, {

         findUnique: async function(where) {
            await authorize(this, modelName, 'findUnique', [where])
            const rows = await db.select().from(model).where(whereToDrizzleFilters(model, where));
            return rows[0] ?? null;
         },

         findMany: async function(where, queryOptions = {}) {
            await authorize(this, modelName, 'findMany', [where, queryOptions])
            let query = db.select().from(model).where(whereToDrizzleFilters(model, where))
            if (queryOptions.limit != null) query = query.limit(queryOptions.limit)
            return await query;
         },

         createWithMeta: async function(uid, data, created_at) {
            await authorize(this, modelName, 'createWithMeta', [uid, data, created_at])
            const safeData = sanitizeMutationData(data)
            const ts = normalizeMutationTimestamp(created_at, 'created_at')
            return await db.transaction(async (tx) => {
               await lockUidOwnership(tx, uid)
               const { storageUid, meta: existingMeta } = await lockMetadataRow(tx, modelName, uid)
               await assertUidUniqueToModel(tx, model, uid)
               const existingTime = metadataTimestamp(existingMeta)
               if (existingTime && existingTime >= ts) {
                  const value = (await tx.select().from(model).where(eq(model.uid, uid)))[0] ?? undefined
                  if (existingMeta.deleted_at) {
                     if (value) await tx.delete(model).where(eq(model.uid, uid))
                     return [undefined, existingMeta]
                  }
                  if (!value && !existingMeta.deleted_at) {
                     const [meta] = await tx.insert(metadata)
                        .values({ uid: storageUid, deleted_at: existingTime })
                        .onConflictDoUpdate({ target: metadata.uid, set: { deleted_at: existingTime } })
                        .returning();
                     return [undefined, externalizeMetadata(meta, uid)]
                  }
                  return [value, existingMeta]
               }
               // Upsert: if the model row already exists (e.g. a concurrent createWithMeta
               // from the direct create() path landed before the sync's addDatabase step),
               // update it instead of throwing a PK conflict that would rollback the
               // client's Dexie record.
               const [value] = await tx.insert(model)
                  .values({ ...safeData, uid })
                  .onConflictDoUpdate({ target: model.uid, set: safeData })
                  .returning();
               // Upsert metadata: handles re-creation after a prior deleteWithMeta.
               const [meta] = await tx.insert(metadata)
                  .values({ uid: storageUid, created_at: ts })
                  .onConflictDoUpdate({
                     target: metadata.uid,
                     set: { created_at: ts, deleted_at: null, updated_at: null },
                  })
                  .returning();
               return [value, externalizeMetadata(meta, uid)]
            })
         },

         updateWithMeta: async function(uid, data, updated_at) {
            await authorize(this, modelName, 'updateWithMeta', [uid, data, updated_at])
            const safeData = sanitizeMutationData(data)
            const ts = updated_at ? normalizeMutationTimestamp(updated_at, 'updated_at') : null
            return await db.transaction(async (tx) => {
               await lockUidOwnership(tx, uid)
               const { storageUid, meta: existingMeta } = await lockMetadataRow(tx, modelName, uid)
               await assertUidUniqueToModel(tx, model, uid)
               if (existingMeta?.deleted_at) {
                  const deletedAt = new Date(existingMeta.deleted_at)
                  if (ts && ts > deletedAt) {
                     const [value] = await tx.insert(model)
                        .values({ ...safeData, uid })
                        .onConflictDoUpdate({ target: model.uid, set: safeData })
                        .returning();
                     const [meta] = await tx.insert(metadata)
                        .values({ uid: storageUid, updated_at: ts })
                        .onConflictDoUpdate({ target: metadata.uid, set: { updated_at: ts, deleted_at: null } })
                        .returning();
                     return [value, externalizeMetadata(meta, uid)]
                  }
                  await tx.delete(model).where(eq(model.uid, uid))
                  return [undefined, existingMeta]
               }
               const existingTime = metadataTimestamp(existingMeta, ['updated_at', 'created_at'])
               if (existingTime && (!ts || existingTime >= ts)) {
                  const value = (await tx.select().from(model).where(eq(model.uid, uid)))[0] ?? undefined
                  if (!value) {
                     const [meta] = await tx.insert(metadata)
                        .values({ uid: storageUid, deleted_at: existingTime })
                        .onConflictDoUpdate({ target: metadata.uid, set: { deleted_at: existingTime } })
                        .returning();
                     return [undefined, externalizeMetadata(meta, uid)]
                  }
                  return [value, existingMeta]
               }
               const [value] = await tx.insert(model)
                  .values({ ...safeData, uid })
                  .onConflictDoUpdate({ target: model.uid, set: safeData })
                  .returning();
               // Upsert metadata: if metadata is missing (data-integrity gap where the
               // model row exists but no metadata row), create it so the loop stops.
               const [meta] = await tx.insert(metadata)
                  .values({ uid: storageUid, updated_at: ts })
                  .onConflictDoUpdate({ target: metadata.uid, set: { updated_at: ts, deleted_at: null } })
                  .returning();
               return [value, externalizeMetadata(meta, uid)]
            })
         },

         deleteWithMeta: async function(uid, deleted_at) {
            await authorize(this, modelName, 'deleteWithMeta', [uid, deleted_at])
            return await db.transaction(async (tx) => {
               await lockUidOwnership(tx, uid)
               const { storageUid, meta: existingMeta } = await lockMetadataRow(tx, modelName, uid)
               await assertUidUniqueToModel(tx, model, uid)
               const ts = normalizeMutationTimestamp(deleted_at, 'deleted_at')
               const existingTime = metadataTimestamp(existingMeta)
               if (existingTime && existingTime > ts) {
                  const value = (await tx.select().from(model).where(eq(model.uid, uid)))[0] ?? undefined
                  if (existingMeta.deleted_at) {
                     if (value) await tx.delete(model).where(eq(model.uid, uid))
                     return [undefined, existingMeta]
                  }
                  if (!value && !existingMeta.deleted_at) {
                     const [meta] = await tx.insert(metadata)
                        .values({ uid: storageUid, deleted_at: existingTime })
                        .onConflictDoUpdate({ target: metadata.uid, set: { deleted_at: existingTime } })
                        .returning();
                     return [undefined, externalizeMetadata(meta, uid)]
                  }
                  return [value, existingMeta]
               }
               const [value] = await tx.delete(model).where(eq(model.uid, uid)).returning();
               const [meta] = await tx.insert(metadata)
                  .values({ uid: storageUid, deleted_at: ts })
                  .onConflictDoUpdate({ target: metadata.uid, set: { deleted_at: ts } })
                  .returning();
               return [value, externalizeMetadata(meta, uid)]
            })
         },
      })
   }

   const syncLocks = new Map()

   // add a synchronization service
   app.createService('sync', {

      go: async function(modelName, where, clientMetadataDict) {
         if (!configuredModels.has(modelName)) throw new Error(`model '${modelName}' is not configured for synchronization`)
         await authorize(this, modelName, 'sync', [where, clientMetadataDict])
         if (!clientMetadataDict || typeof clientMetadataDict !== 'object' || Array.isArray(clientMetadataDict)) {
            throw new TypeError('client metadata must be an object')
         }
         const clientUids = Object.keys(clientMetadataDict)
         if (clientUids.length > maxSyncRecords) {
            throw new Error(`sync metadata exceeds maxSyncRecords (${maxSyncRecords})`)
         }

         // overlap-aware lock so independent scopes can still run in parallel, but overlapping where predicates do not
         if (!syncLocks.has(modelName)) syncLocks.set(modelName, new OverlapLock())
         const syncLock = syncLocks.get(modelName)
         const releaseSyncLock = await syncLock.acquire(where)

         try {
            console.log('>>>>> SYNC', modelName, where)
            const databaseService = app.service(modelName)

            const readSnapshot = async () => {
               // STEP1: get existing database `where` values and build a dictionary
               const databaseValues = await databaseService.callWithContext(
                  this, 'findMany', where, { limit: maxSyncRecords + 1 },
               )
               if (databaseValues.length > maxSyncRecords) {
                  throw new Error(`sync result exceeds maxSyncRecords (${maxSyncRecords})`)
               }
               const databaseValuesDict = databaseValues.reduce((accu, value) => {
                  accu[value.uid] = value
                  return accu
               }, Object.create(null))

               // STEP 2: fetch metadata for each database record and each client-sent uid.
               // Client uids matter when the server row was deleted: only the tombstone remains.
               const effectiveClientMetadata = Object.assign(Object.create(null), clientMetadataDict)

               // A client-supplied uid whose real row exists outside `where` must not
               // participate in this scoped sync. Treating it as a missing row can
               // otherwise schedule an out-of-scope deletion.
               const candidateOutsideUids = clientUids.filter(uid => !Object.hasOwn(databaseValuesDict, uid))
               if (candidateOutsideUids.length > 0) {
                  const outsideRows = await db.select({ uid: configuredModels.get(modelName).uid })
                     .from(configuredModels.get(modelName))
                     .where(inArray(configuredModels.get(modelName).uid, candidateOutsideUids))
                  for (const row of outsideRows) delete effectiveClientMetadata[row.uid]
               }

               const databaseMetadataDict = Object.create(null)
               const metadataUids = new Set([
                  ...Object.keys(databaseValuesDict),
                  ...Object.keys(effectiveClientMetadata),
               ])
               const metadataUidList = [...metadataUids]
               if (metadataUidList.length > 0) {
                  const storageToUid = new Map(metadataUidList.map(uid => [storageUidFor(modelName, uid), uid]))
                  const rows = await db.select().from(metadata)
                     .where(inArray(metadata.uid, [...storageToUid.keys()]))
                  for (const row of rows) {
                     const uid = storageToUid.get(row.uid)
                     databaseMetadataDict[uid] = externalizeMetadata(row, uid)
                  }

               }
               const metadataRepairTimestamp = new Date()
               for (const uid of Object.keys(databaseValuesDict)) {
                  if (databaseMetadataDict[uid]) continue
                  if (Object.hasOwn(effectiveClientMetadata, uid)
                     && !effectiveClientMetadata[uid]?.__repaired__) continue

                  const [meta] = await db.insert(metadata)
                     .values({ uid: storageUidFor(modelName, uid), created_at: metadataRepairTimestamp })
                     .onConflictDoNothing({ target: metadata.uid })
                     .returning()
                  if (!meta) throw snapshotChanged()
                  databaseMetadataDict[uid] = externalizeMetadata(meta, uid)
               }

               // Row values must lie between two identical metadata reads. Otherwise
               // a concurrent mutation can pair old data with its new version forever.
               const confirmedValues = await databaseService.callWithContext(
                  this, 'findMany', where, { limit: maxSyncRecords + 1 },
               )
               const initialIds = Object.keys(databaseValuesDict).sort()
               const confirmedIds = confirmedValues.map(value => value.uid).sort()
               if (JSON.stringify(initialIds) !== JSON.stringify(confirmedIds)) throw snapshotChanged()
               if (metadataUidList.length) {
                  const storageToUid = new Map(metadataUidList.map(uid => [storageUidFor(modelName, uid), uid]))
                  const confirmedMetadata = Object.create(null)
                  const rows = await db.select().from(metadata).where(inArray(metadata.uid, [...storageToUid.keys()]))
                  for (const row of rows) {
                     const uid = storageToUid.get(row.uid)
                     confirmedMetadata[uid] = externalizeMetadata(row, uid)
                  }
                  for (const uid of metadataUidList) {
                     if (JSON.stringify(databaseMetadataDict[uid] ?? null) !== JSON.stringify(confirmedMetadata[uid] ?? null)) {
                        throw snapshotChanged()
                     }
                  }
               }
               for (const value of confirmedValues) databaseValuesDict[value.uid] = value
               return { databaseValuesDict, databaseMetadataDict, effectiveClientMetadata }
            }
            let snapshot
            for (let attempt = 0; ; attempt++) {
               try { snapshot = await readSnapshot(); break } catch (error) {
                  if (error.code !== 'sync-snapshot-changed' || attempt >= 2) throw error
               }
            }
            const { databaseValuesDict, databaseMetadataDict, effectiveClientMetadata } = snapshot

            // STEP 3: compute sync result
            const result = computeSynchronization(databaseValuesDict, effectiveClientMetadata, databaseMetadataDict)

            // STEP 4: execute server-side deletions
            for (const uid of result.deleteDatabase) {
               const deletedAt = effectiveClientMetadata[uid]?.deleted_at
                  ?? databaseMetadataDict[uid]?.deleted_at
                  ?? databaseMetadataDict[uid]?.updated_at
                  ?? databaseMetadataDict[uid]?.created_at
               await databaseService.callWithContext(this, 'deleteWithMeta', uid, deletedAt)
            }

            console.log('addDatabase', truncateString(JSON.stringify(result.addDatabase)))
            console.log('updateDatabase', truncateString(JSON.stringify(result.updateDatabase)))
            console.log('addClient', truncateString(JSON.stringify(result.addClient)))
            console.log('deleteClient', truncateString(JSON.stringify(result.deleteClient)))
            console.log('updateClient', truncateString(JSON.stringify(result.updateClient)))

            return {
               addClient: result.addClient,
               updateClient: result.updateClient,
               deleteClient: result.deleteClient,
               addDatabase: result.addDatabase,
               updateDatabase: result.updateDatabase,
            }
         } catch(err) {
            console.log('*** err sync', err)
            throw err
         } finally {
            releaseSyncLock()
            if (syncLock.idle) syncLocks.delete(modelName)
         }
      },
   })

}

export { computeSyncResult, drizzleOfflinePlugin as syncServerPlugin }
export default drizzleOfflinePlugin
