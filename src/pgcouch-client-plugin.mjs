import { Observable, Subject, startWith, switchMap, distinctUntilChanged, takeUntil } from 'rxjs'
import { applyOperations, documentOf, json, modelsOf, putImmutable, quote, readRows, transaction, whereSQL } from './pgcouch-common.mjs'

/** Offline relational models; localDb is a PGLite instance with the application schema. */
export function pgcouchClientPlugin(app, options = {}) {
   const models = modelsOf(options.models)
   const couch = options.couch
   const db = options.localDb
   if (!couch?.put || !couch?.allDocs || !db?.transaction) throw new TypeError('options.couch (local PouchDB), localDb (PGLite), and models are required')
   
   // Signals that local data may have changed
   // Fires after the transaction commits, so observers see the complete transaction’s result, never its intermediate operations
   const changed = new Subject()

   const cache = new Map()
   let queue = Promise.resolve(), closed = false, feed, replication, timer, started = false

   const report = error => { try { options.onError?.(error) } catch {} }

   const enqueue = run => {
      if (closed) return Promise.reject(new Error('PGCouch sync has been disposed'))
      const result = queue.then(run)
      queue = result.catch(report)
      return result
   }

   async function publish() {
      const { rows } = await db.query("SELECT document FROM pgcouch_outbox WHERE status = 'pending' ORDER BY ordinal")
      for (const { document } of rows) await putImmutable(couch, document)
   }

   async function reconcile() {
      try { await publish() } catch (error) { report(error) }
      const { rows } = await couch.allDocs({ include_docs: true, conflicts: true, startkey: 'result:', endkey: 'result:\uffff' })
      const decisions = rows.map(row => row.doc).filter(Boolean)
      for (const doc of decisions) {
         if (doc._conflicts?.length) throw new Error(`Conflicting server decisions: ${doc._id}`)
         if (doc.type !== 'result' || !['accepted', 'rejected'].includes(doc.status) || !/^\d+$/.test(doc.sequence) || !doc.snapshot) throw new Error('Invalid server decision')
      }
      if (!decisions.length) return
      decisions.sort((a, b) => BigInt(a.sequence) < BigInt(b.sequence) ? -1 : BigInt(a.sequence) > BigInt(b.sequence) ? 1 : 0)
      const latest = decisions.at(-1)
      let updated = false
      const rejections = []
      await transaction(db, async tx => {
         const state = (await tx.query('SELECT sequence FROM pgcouch_local_state WHERE id = 1')).rows[0]
         for (const decision of decisions) {
            if (!decision.transactionId) continue
            const result = await tx.query(`UPDATE pgcouch_outbox SET status = $2, error = $3::jsonb
               WHERE id = $1 AND status = 'pending' RETURNING id`, [decision.transactionId, decision.status, json(decision.error ?? null)])
            if (result.rows.length) {
               updated = true
               if (decision.status === 'rejected') rejections.push(Object.assign(new Error(decision.error?.message ?? 'Transaction rejected'), decision.error, { transactionId: decision.transactionId }))
            }
         }
         if (!updated && BigInt(state.sequence) >= BigInt(latest.sequence)) return
         if (BigInt(state.sequence) > BigInt(latest.sequence)) throw new Error('Replication is missing the current authoritative snapshot')
         await tx.query('SET CONSTRAINTS ALL DEFERRED')
         for (const model of [...models].reverse()) await tx.query(`DELETE FROM ${quote(model.table)}`)
         for (const model of models) {
            const data = latest.snapshot[model.table]
            if (!Array.isArray(data)) throw new Error(`Snapshot is missing table '${model.table}'`)
            for (const row of data) {
               const entries = Object.entries(row)
               await tx.query(`INSERT INTO ${quote(model.table)} (${entries.map(([key]) => quote(key)).join(', ')}) OVERRIDING SYSTEM VALUE VALUES (${entries.map((_, i) => `$${i + 1}`).join(', ')})`, entries.map(([, value]) => value))
            }
         }
         await tx.query('SET CONSTRAINTS ALL IMMEDIATE')
         const pending = (await tx.query("SELECT id, document FROM pgcouch_outbox WHERE status = 'pending' ORDER BY ordinal")).rows
         for (const item of pending) {
            await tx.query('SAVEPOINT pgcouch_replay')
            try {
               await applyOperations(tx, item.document.operations, models)
               await tx.query('UPDATE pgcouch_outbox SET error = NULL WHERE id = $1', [item.id])
            } catch (error) {
               await tx.query('ROLLBACK TO SAVEPOINT pgcouch_replay')
               await tx.query('UPDATE pgcouch_outbox SET error = $2::jsonb WHERE id = $1', [item.id, json({ message: error.message, code: error.code ?? 'rebase-failed' })])
            }
            await tx.query('RELEASE SAVEPOINT pgcouch_replay')
         }
         await tx.query('UPDATE pgcouch_local_state SET sequence = $1 WHERE id = 1', [latest.sequence])
         updated = true
      })
      if (updated) changed.next()
      rejections.forEach(report)
   }

   function start() {
      if (closed) throw new Error('PGCouch sync has been disposed')
      if (started) return
      started = true
      feed = couch.changes({ since: 'now', live: true }).on('change', change => {
         if (change.id.startsWith('result:')) void enqueue(reconcile).catch(report)
      }).on('error', report)
      if (options.remote) {
         replication = couch.sync(options.remote, { ...options.replicationOptions, live: true, retry: true })
            .on('error', report).on('denied', report)
      }
      timer = setInterval(() => { void enqueue(reconcile).catch(report) }, options.retryMs ?? 1000)
      timer.unref?.()
   }

   function stop() {
      started = false; clearInterval(timer); feed?.cancel(); replication?.cancel()
   }

   const ready = enqueue(async () => {
      await db.exec(`CREATE TABLE IF NOT EXISTS pgcouch_outbox (
         ordinal BIGSERIAL UNIQUE, id TEXT PRIMARY KEY, document JSONB NOT NULL,
         status TEXT NOT NULL DEFAULT 'pending', error JSONB
      ); CREATE TABLE IF NOT EXISTS pgcouch_local_state (id INTEGER PRIMARY KEY, sequence BIGINT NOT NULL);
      INSERT INTO pgcouch_local_state VALUES (1, -1) ON CONFLICT DO NOTHING;`)
      if (!closed && options.autoStart !== false) start()
      await reconcile()
   })
   void ready.catch(report)

   async function transact(operations) {
      const callback = typeof operations === 'function' ? operations : null
      operations = callback ? [] : structuredClone(operations)
      await ready
      return enqueue(async () => {
         const id = `transaction:${globalThis.crypto.randomUUID()}`
         let doc, results
         await transaction(db, async tx => {
            if (callback) {
               let active = true, pending = Promise.resolve(), failed = false, failure
               results = []
               const run = work => {
                  if (!active) return Promise.reject(new Error('Transaction callback has finished'))
                  const result = pending.then(work)
                  pending = result.catch(error => { if (!failed) failure = error; failed = true })
                  return result
               }
               const scope = Object.create(null)
               for (const model of models) {
                  const write = op => {
                     // Capture inputs before the caller can mutate them.
                     let operation
                     try {
                        operation = documentOf({ _id: id, type: 'transaction', status: 'pending', operations: [op] }, models).operations[0]
                     } catch (error) { return run(() => { throw error }) }
                     return run(async () => {
                        if (operations.length >= 1000) throw new TypeError('Expected 1–1000 operations')
                        const [value] = await applyOperations(tx, [operation], models, { deferConstraints: true })
                        operations.push(operation)
                        results.push(value)
                        return value
                     })
                  }
                  const findMany = (where = {}, queryOptions = {}) => {
                     let filter, query
                     try { filter = structuredClone(where); query = structuredClone(queryOptions) }
                     catch (error) { return run(() => { throw error }) }
                     return run(() => readRows(tx, model, filter, query))
                  }
                  const remove = rowId => write({ action: 'delete', table: model.table, id: rowId })
                  scope[model.name] = {
                     create(idOrData, data) {
                        const value = data ?? idOrData
                        const rowId = data === undefined ? value?.[model.primaryKey] ?? globalThis.crypto.randomUUID() : idOrData
                        return write({ action: 'insert', table: model.table, id: rowId, data: value })
                     },
                     update: (rowId, data) => write({ action: 'update', table: model.table, id: rowId, data }),
                     delete: remove, remove, findMany,
                     async findUnique(where) { return (await findMany(where, { limit: 1 }))[0] ?? null },
                  }
               }
               try { await callback(scope) }
               finally { active = false; await pending }
               if (failed) throw failure
               await tx.query('SET CONSTRAINTS ALL IMMEDIATE')
            }
            const previous = (await tx.query('SELECT id FROM pgcouch_outbox ORDER BY ordinal DESC LIMIT 1')).rows[0]?.id
            doc = documentOf({ _id: id, type: 'transaction', status: 'pending', operations, ...(previous ? { previous } : {}) }, models)
            if (!callback) results = await applyOperations(tx, doc.operations, models)
            await tx.query('INSERT INTO pgcouch_outbox (id, document) VALUES ($1, $2::jsonb)', [id, json(doc)])
         })
         changed.next()
         try { await publish() } catch (error) { report(error) }
         return { id, results }
      })
   }

   async function getStatus() {
      await ready
      return enqueue(async () => {
         const { rows } = await db.query(`SELECT count(*) FILTER (WHERE status = 'pending')::int AS pending,
            count(*) FILTER (WHERE status = 'rejected')::int AS failed,
            count(*) FILTER (WHERE status = 'pending' AND error IS NOT NULL)::int AS blocked FROM pgcouch_outbox`)
         return { ...rows[0], online: started && (options.network ?? globalThis.navigator)?.onLine !== false }
      })
   }

   function createPGCouchModel(name, modelOptions = {}) {
      const model = models.find(m => m.name === name)
      if (!model) throw new TypeError(`Unknown model '${name}'`)
      if (modelOptions.primaryKey && modelOptions.primaryKey !== model.primaryKey) throw new TypeError('Primary key must match options.models')
      if (modelOptions.idGeneration === 'server') throw new TypeError('PGCouch offline mutations require client-generated ids')

      if (cache.has(name)) return cache.get(name)

      let disposed = false
      const ended = new Subject()
      const assertActive = () => { if (disposed || closed) throw new Error('PGCouch model has been disposed') }

      async function findMany(where = {}, queryOptions = {}) {
         assertActive(); whereSQL(where)
         where = structuredClone(where); queryOptions = structuredClone(queryOptions)
         await ready
         return enqueue(() => readRows(db, model, where, queryOptions))
      }

      function getObservable(where = {}, queryOptions = {}) {
         assertActive(); whereSQL(where)
         const filter = structuredClone(where)
         let observable = changed.pipe(startWith(null), switchMap(() => findMany(filter, queryOptions)), distinctUntilChanged((a, b) => json(a) === json(b)), takeUntil(ended))
         if (Object.hasOwn(queryOptions, 'initialValue')) observable = observable.pipe(startWith(queryOptions.initialValue))
         return new Observable(subscriber => observable.subscribe(subscriber))
      }

      const api = {
         findMany,
         getObservable,
         async findUnique(where) { return (await findMany(where, { limit: 1 }))[0] ?? null },
         async create(idOrData, data) {
            assertActive()
            const value = data ?? idOrData
            const id = data === undefined ? value[model.primaryKey] ?? globalThis.crypto.randomUUID() : idOrData
            return (await transact([{ action: 'insert', table: model.table, id, data: value }])).results[0]
         },
         async update(id, data) { assertActive(); return (await transact([{ action: 'update', table: model.table, id, data }])).results[0] },
         async remove(id) { assertActive(); return (await transact([{ action: 'delete', table: model.table, id }])).results[0] },

         prepare: () => { assertActive(); return ready }, start: () => { assertActive(); return ready.then(start) }, stop, getStatus,
         async dispose() { if (disposed) return; disposed = true; ended.next(); ended.complete(); cache.delete(name); await queue },
         flushQueue: () => ready.then(() => enqueue(reconcile)), waitForIdle: () => queue,
         subscribe: callback => { const subscription = changed.subscribe(callback); return () => subscription.unsubscribe() },
      }
      cache.set(name, api)
      return api
   }

   async function disposePGCouchSync() {
      stop(); closed = true; await queue; stop(); changed.complete()
      for (const model of [...cache.values()]) await model.dispose()
   }

   return Object.assign(app, { createPGCouchModel, createElectricModel: createPGCouchModel,
      pgcouchTransaction: transact, disposePGCouchSync, disposeElectricSync: disposePGCouchSync,
      pgcouchReady: ready })
}

export default pgcouchClientPlugin
