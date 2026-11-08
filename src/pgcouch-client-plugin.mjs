import { Observable, Subject, startWith, switchMap, distinctUntilChanged, takeUntil } from 'rxjs'
import { applyOperations, documentOf, json, modelsOf, mutationTarget, putImmutable, quote, readRows, transaction, whereSQL } from './pgcouch-common.mjs'

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

   // Keep authoritative rows on disk, separate from optimistic application tables.
   async function acceptDecisions(tx) {
      const state = (await tx.query('SELECT * FROM pgcouch_local_state WHERE id = 1')).rows[0]
      let sequence = state.initialized ? BigInt(state.sequence) : -1n
      let updated = false
      const rejections = []
      // A checkpoint can bridge missing earlier history during initial replication.
      const checkpoint = (await tx.query(`SELECT document FROM pgcouch_inbox
         WHERE sequence > $1 AND document ? 'snapshot' ORDER BY sequence DESC LIMIT 1`, [String(sequence)])).rows[0]?.document
      if (checkpoint) {
         await tx.query('DELETE FROM pgcouch_base')
         for (const model of models) {
            const rows = checkpoint.snapshot[model.table]
            if (!Array.isArray(rows)) throw new Error(`Snapshot is missing table '${model.table}'`)
            for (const row of rows) await tx.query('INSERT INTO pgcouch_base VALUES ($1, $2::jsonb, $3::jsonb)', [model.table, json(row[model.primaryKey]), json(row)])
         }
         sequence = BigInt(checkpoint.sequence)
         updated = true
      }
      // Process one document at a time; never materialize transaction history in JS.
      while (true) {
         const decision = (await tx.query('SELECT document FROM pgcouch_inbox WHERE sequence = $1', [String(sequence + 1n)])).rows[0]?.document
         if (!decision) break
         if (!Array.isArray(decision.changes)) throw new Error('Incremental decision is missing changes')
         for (const change of decision.changes) {
            const model = models.find(model => model.table === change.table)
            if (!model || !['string', 'number'].includes(typeof change.id)) throw new Error('Invalid row change')
            if (change.data === null) await tx.query('DELETE FROM pgcouch_base WHERE table_name = $1 AND key = $2::jsonb', [change.table, json(change.id)])
            else {
               if (!change.data || change.data[model.primaryKey] !== change.id) throw new Error('Invalid changed row')
               await tx.query(`INSERT INTO pgcouch_base VALUES ($1, $2::jsonb, $3::jsonb)
                  ON CONFLICT (table_name, key) DO UPDATE SET row_data = EXCLUDED.row_data`, [change.table, json(change.id), json(change.data)])
            }
         }
         sequence++
         updated = true
      }
      // Acknowledge even older proposals skipped by a checkpoint. Their decisions
      // may arrive later than the checkpoint and must still resolve local outbox entries.
      while (true) {
         const decision = (await tx.query('SELECT document FROM pgcouch_inbox WHERE sequence <= $1 ORDER BY sequence LIMIT 1', [String(sequence)])).rows[0]?.document
         if (!decision) break
         if (decision.transactionId) {
            const result = await tx.query(`UPDATE pgcouch_outbox SET status = $2, error = $3::jsonb
               WHERE id = $1 AND status = 'pending' RETURNING id`, [decision.transactionId, decision.status, json(decision.error ?? null)])
            if (result.rows.length) {
               updated = true
               if (decision.status === 'rejected') rejections.push(Object.assign(new Error(decision.error?.message ?? 'Transaction rejected'), decision.error, { transactionId: decision.transactionId }))
            }
         }
         await tx.query('DELETE FROM pgcouch_inbox WHERE sequence = $1', [decision.sequence])
      }
      if (!updated) return { updated, rejections }
      await tx.query('SET CONSTRAINTS ALL DEFERRED')
      for (const model of [...models].reverse()) await tx.query(`DELETE FROM ${quote(model.table)}`)
      for (const model of models) {
         // Populate from disk in SQL rather than loading an entire dataset in JS.
         await tx.query(`INSERT INTO ${quote(model.table)} OVERRIDING SYSTEM VALUE
            SELECT (jsonb_populate_record(NULL::${quote(model.table)}, row_data)).*
            FROM pgcouch_base WHERE table_name = $1`, [model.table])
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
      await tx.query('UPDATE pgcouch_local_state SET sequence = $1, initialized = true WHERE id = 1', [String(sequence)])
      return { updated, rejections }
   }

   async function reconcile() {
      try { await publish() } catch (error) { report(error) }
      while (true) {
         const state = (await db.query('SELECT cursor FROM pgcouch_local_state WHERE id = 1')).rows[0]
         const page = await couch.changes({ since: state.cursor ?? 0, include_docs: true, conflicts: true, limit: 100 })
         const { updated, rejections } = await transaction(db, async tx => {
            for (const change of page.results) {
               if (!change.id.startsWith('result:')) continue
               const doc = change.doc
               if (doc?._conflicts?.length) throw new Error(`Conflicting server decisions: ${doc._id}`)
               if (!doc || doc.type !== 'result' || !['accepted', 'rejected'].includes(doc.status) || !/^\d+$/.test(doc.sequence) || (!doc.snapshot && !Array.isArray(doc.changes))) throw new Error('Invalid server decision')
               await tx.query(`INSERT INTO pgcouch_inbox VALUES ($1, $2::jsonb)
                  ON CONFLICT (sequence) DO UPDATE SET document = EXCLUDED.document`, [doc.sequence, json(doc)])
            }
            const result = await acceptDecisions(tx)
            await tx.query('UPDATE pgcouch_local_state SET cursor = $1::jsonb WHERE id = 1', [json(page.last_seq)])
            return result
         })
         if (updated) changed.next()
         rejections.forEach(report)
         if (page.results.length < 100) break
      }
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
      await db.exec(`ALTER TABLE pgcouch_local_state ADD COLUMN IF NOT EXISTS cursor JSONB;
         ALTER TABLE pgcouch_local_state ADD COLUMN IF NOT EXISTS initialized BOOLEAN NOT NULL DEFAULT false;
         CREATE TABLE IF NOT EXISTS pgcouch_base (table_name TEXT NOT NULL, key JSONB NOT NULL, row_data JSONB NOT NULL, PRIMARY KEY (table_name, key));
         CREATE TABLE IF NOT EXISTS pgcouch_inbox (sequence BIGINT PRIMARY KEY, document JSONB NOT NULL);`)
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
                  const remove = idOrWhere => write({ action: 'delete', table: model.table, ...mutationTarget(idOrWhere) })
                  scope[model.name] = {
                     create(idOrData, data) {
                        const value = data ?? idOrData
                        const rowId = data === undefined ? value?.[model.primaryKey] ?? globalThis.crypto.randomUUID() : idOrData
                        return write({ action: 'insert', table: model.table, id: rowId, data: value })
                     },
                     update: (idOrWhere, data) => write({ action: 'update', table: model.table, ...mutationTarget(idOrWhere), data }),
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
         async update(idOrWhere, data) { assertActive(); return (await transact([{ action: 'update', table: model.table, ...mutationTarget(idOrWhere), data }])).results[0] },
         async remove(idOrWhere) { assertActive(); return (await transact([{ action: 'delete', table: model.table, ...mutationTarget(idOrWhere) }])).results[0] },
         async delete(idOrWhere) { return api.remove(idOrWhere) },

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
