import { randomUUID } from 'node:crypto'
import { applyOperations, canonical, documentOf, json, modelsOf, putImmutable, quote, readRows, snapshot, transaction } from './pgcouch-common.mjs'

export async function preparePGCouchSchema(db) {
   await db.query(`CREATE TABLE IF NOT EXISTS pgcouch_state (id INTEGER PRIMARY KEY CHECK (id = 1), sequence BIGINT NOT NULL)`)
   await db.query('INSERT INTO pgcouch_state VALUES (1, 0) ON CONFLICT DO NOTHING')
   await db.query(`CREATE TABLE IF NOT EXISTS pgcouch_receipts (
      id TEXT PRIMARY KEY, proposal JSONB, decision JSONB NOT NULL, published BOOLEAN NOT NULL DEFAULT false
   )`)
}

/** PostgreSQL is authoritative; CouchDB transports immutable proposals and decisions. */
export function pgcouchServerPlugin(app, db, models, options = {}) {
   const configured = modelsOf(models)
   const couch = options.couch
   const checkpointInterval = options.checkpointInterval ?? 100
   if (!Number.isSafeInteger(checkpointInterval) || checkpointInterval < 1) throw new TypeError('checkpointInterval must be a positive integer')
   if (configured.some(model => model.name === (options.serviceName ?? 'pgcouch'))) throw new TypeError('Transaction service name conflicts with a model name')
   if (!couch?.allDocs || !couch?.put) throw new TypeError('options.couch must be a PouchDB-compatible CouchDB connection')
   if (!db?.connect && !db?.transaction) throw new TypeError('db must be a pg Pool or transaction-capable database')
   let closed = false, timer, feed, queue = Promise.resolve()
   const report = error => { try { options.onError?.(error) } catch {} }

   function enqueue(run) {
      if (closed) return Promise.reject(new Error('PGCouch sync has been disposed'))
      const next = queue.then(run)
      queue = next.catch(report)
      return next
   }

   async function authorize(context, model, action, args) {
      if (options.authorize && !await options.authorize(context, { modelName: model.name, action, args })) {
         throw Object.assign(new Error(`Not authorized to ${action} '${model.name}'`), { code: 'forbidden' })
      }
   }

   async function publish() {
      while (true) {
         const row = (await db.query('SELECT id, proposal, decision FROM pgcouch_receipts WHERE NOT published ORDER BY (decision->>\'sequence\')::bigint LIMIT 1')).rows[0]
         if (!row) break
         if (row.proposal) await putImmutable(couch, row.proposal)
         await putImmutable(couch, row.decision)
         await db.query('UPDATE pgcouch_receipts SET published = true WHERE id = $1', [row.id])
      }
   }

   async function process(value, context) {
      const doc = documentOf(value, configured)
      return transaction(db, async tx => {
         // 'select for update' = mutex at the database level. One lock defines the global commit order across server instances.
         const { rows: state } = await tx.query('SELECT sequence FROM pgcouch_state WHERE id = 1 FOR UPDATE')
         
         const prior = (await tx.query('SELECT proposal, decision FROM pgcouch_receipts WHERE id = $1', [doc._id])).rows[0]
         if (prior) {
            if (canonical(prior.proposal) !== canonical(doc)) throw new Error('Transaction id reused with different operations')
            return prior.decision
         }
         if (doc.previous && !(await tx.query('SELECT id FROM pgcouch_receipts WHERE id = $1', [doc.previous])).rows.length) return null
         const sequence = String(BigInt(state[0].sequence) + 1n)
         await tx.query(`CREATE TEMP TABLE pgcouch_changes (
            table_name TEXT, key JSONB, PRIMARY KEY (table_name, key)
         ) ON COMMIT DROP`)
         let status = 'accepted', results, rejection
         await tx.query('SAVEPOINT pgcouch_mutation')
         try {
            for (const op of doc.operations) {
               const model = configured.find(m => m.table === op.table)
               const action = op.action === 'insert' ? 'create' : op.action
               await authorize(context, model, action, op.action === 'delete' ? [op.id] : [op.id, op.data])
            }
            results = await applyOperations(tx, doc.operations, configured)
         } catch (error) {
            await tx.query('ROLLBACK TO SAVEPOINT pgcouch_mutation')
            // Connection/transaction failures are retried, never persisted as domain rejections.
            if (error.code && !/^(23|22|42|P0001)/.test(error.code) && !['forbidden', 'not-found'].includes(error.code) && !(error instanceof TypeError)) throw error
            status = 'rejected'
            rejection = { code: error.code ?? 'invalid-argument', message: error.code === 'forbidden' ? 'Transaction forbidden' : 'Transaction could not satisfy the database constraints' }
         }
         await tx.query('RELEASE SAVEPOINT pgcouch_mutation')
         const changes = []
         for (const model of configured) {
            // Read final rows after every AFTER trigger has completed. Captured NEW
            // images alone can be stale when another trigger updates the same row.
            const rows = (await tx.query(`SELECT c.key AS id, to_jsonb(r) AS data
               FROM pgcouch_changes c LEFT JOIN ${quote(model.table)} r
               ON r.${quote(model.primaryKey)} = (jsonb_populate_record(NULL::${quote(model.table)}, jsonb_build_object('${model.primaryKey}', c.key))).${quote(model.primaryKey)}
               WHERE c.table_name = $1 ORDER BY c.key`, [model.table])).rows
            changes.push(...rows.map(row => ({ table: model.table, ...row })))
         }
         const decision = JSON.parse(json({ _id: `result:${doc._id.slice(12)}`, type: 'result', transactionId: doc._id,
            status, sequence, ...(results ? { results } : {}), ...(rejection ? { error: rejection } : {}),
            changes, ...(BigInt(sequence) % BigInt(checkpointInterval) === 0n ? { snapshot: await snapshot(tx, configured) } : {}) }))
         await tx.query('UPDATE pgcouch_state SET sequence = $1 WHERE id = 1', [sequence])
         await tx.query('INSERT INTO pgcouch_receipts (id, proposal, decision) VALUES ($1, $2::jsonb, $3::jsonb)', [doc._id, json(doc), json(decision)])
         return decision
      })
   }

   async function drain() {
      await publish()
      const { rows } = await couch.allDocs({ include_docs: true, conflicts: true, startkey: 'transaction:', endkey: 'transaction:\uffff' })
      // Repeat until predecessor dependencies stop making progress.
      let pending = rows.map(row => row.doc).filter(Boolean)
      while (pending.length) {
         const deferred = []
         for (const doc of pending) {
            if (doc._conflicts?.length) { report(new Error(`Conflicting transaction revisions: ${doc._id}`)); continue }
            try {
               const context = options.contextForDocument ? await options.contextForDocument(doc) : { app, transport: 'couch' }
               if (!await process(doc, context)) deferred.push(doc)
            } catch (error) { report(error) }
         }
         if (deferred.length === pending.length) break
         pending = deferred
      }
      await publish()
   }

   const ready = enqueue(async () => {
      await preparePGCouchSchema(db)
      await transaction(db, async tx => {
         await tx.query('SELECT sequence FROM pgcouch_state WHERE id = 1 FOR UPDATE')
         // Capture final row images, including SQL trigger and foreign-key cascade effects.
         // Writes outside this plugin are not replicated.
         await tx.query(`CREATE OR REPLACE FUNCTION pgcouch_capture() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
            IF to_regclass('pg_temp.pgcouch_changes') IS NOT NULL THEN
               IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND to_jsonb(OLD)->TG_ARGV[0] IS DISTINCT FROM to_jsonb(NEW)->TG_ARGV[0]) THEN
                  INSERT INTO pg_temp.pgcouch_changes VALUES (TG_TABLE_NAME, to_jsonb(OLD)->TG_ARGV[0]) ON CONFLICT DO NOTHING;
               END IF;
               IF TG_OP <> 'DELETE' THEN
                  INSERT INTO pg_temp.pgcouch_changes VALUES (TG_TABLE_NAME, to_jsonb(NEW)->TG_ARGV[0]) ON CONFLICT DO NOTHING;
               END IF;
            END IF;
            RETURN NULL;
         END $$`)
         for (const model of configured) {
            await tx.query(`DROP TRIGGER IF EXISTS pgcouch_capture ON ${quote(model.table)}`)
            await tx.query(`CREATE TRIGGER pgcouch_capture AFTER INSERT OR UPDATE OR DELETE ON ${quote(model.table)}
               FOR EACH ROW EXECUTE FUNCTION pgcouch_capture('${model.primaryKey}')`)
         }
         if (!(await tx.query("SELECT id FROM pgcouch_receipts WHERE id = 'bootstrap'")).rows.length) {
            const doc = { _id: 'result:bootstrap', type: 'result', status: 'accepted', sequence: '0', snapshot: await snapshot(tx, configured) }
            await tx.query('INSERT INTO pgcouch_receipts (id, decision) VALUES ($1, $2::jsonb)', ['bootstrap', json(doc)])
         }
      })
      if (closed) return
      feed = couch.changes({ since: 'now', live: true }).on('change', change => {
         if (change.id.startsWith('transaction:')) void enqueue(drain).catch(report)
      }).on('error', report)
      timer = setInterval(() => { void enqueue(drain).catch(report) }, options.retryMs ?? 1000)
      timer.unref?.()
      try { await drain() } catch (error) { report(error) }
   })
   void ready.catch(report)

   async function submit(operations, context, id = `transaction:${randomUUID()}`) {
      const doc = documentOf({ _id: id, type: 'transaction', status: 'pending', operations }, configured)
      await ready
      return enqueue(async () => {
         // SQL receipt is the durable outbox; replication outages do not undo committed writes.
         const decision = await process(doc, context)
         try { await publish() } catch (error) { report(error) }
         if (decision.status === 'rejected') throw Object.assign(new Error(decision.error.message), decision.error)
         return decision.results
      })
   }

   for (const model of configured) app.createService(model.name, {
      async findMany(where = {}, queryOptions = {}) {
         await ready; await authorize(this, model, 'findMany', [where, queryOptions])
         return readRows(db, model, where, queryOptions)
      },
      async findUnique(where) {
         await ready; await authorize(this, model, 'findUnique', [where])
         return (await readRows(db, model, where, { limit: 1 }))[0] ?? null
      },
      async create(idOrData, data) {
         const id = data === undefined ? idOrData[model.primaryKey] ?? randomUUID() : idOrData
         return (await submit([{ action: 'insert', table: model.table, id, data: data ?? idOrData }], this))[0]
      },
      async update(id, data) { return (await submit([{ action: 'update', table: model.table, id, data }], this))[0] },
      async delete(id) { return (await submit([{ action: 'delete', table: model.table, id }], this))[0] },
   })
   app.createService(options.serviceName ?? 'pgcouch', {
      async transaction(operations, id) { return submit(operations, this, id) },
   })
   return { ready, models: configured, submit, flushQueue: () => ready.then(() => enqueue(drain)),
      async dispose() { closed = true; clearInterval(timer); feed?.cancel(); await queue; clearInterval(timer); feed?.cancel() } }
}

/** Install with an admin connection before exposing the replication database. */
export function pgcouchValidationDocument() {
   return { _id: '_design/pgcouch', validate_doc_update: function(doc, old, user) {
      if (doc._id.indexOf('_design/') === 0 && user.roles.indexOf('_admin') >= 0) return
      var server = user.roles.indexOf('pgcouch-server') >= 0 || user.roles.indexOf('_admin') >= 0
      if (old) throw { forbidden: 'PGCouch documents are immutable' }
      if (doc._deleted) throw { forbidden: 'PGCouch documents cannot be deleted' }
      if (doc._id.indexOf('result:') === 0 && doc.type === 'result' && server) return
      if (doc._id === 'transaction:bootstrap' || doc._id.indexOf('transaction:') !== 0 || doc.type !== 'transaction' || doc.status !== 'pending' || !Array.isArray(doc.operations) || !doc.operations.length || doc.operations.length > 1000) throw { forbidden: 'Invalid PGCouch document' }
   }.toString() }
}
export default pgcouchServerPlugin
