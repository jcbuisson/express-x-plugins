import test from 'node:test'
import assert from 'node:assert/strict'
import { PGlite } from '@electric-sql/pglite'
import PouchDB from 'pouchdb'
import memory from 'pouchdb-adapter-memory'
import { pgcouchServerPlugin, pgcouchValidationDocument } from '../src/pgcouch-server-plugin.mjs'
import { pgcouchClientPlugin } from '../src/pgcouch-client-plugin.mjs'
PouchDB.plugin(memory)
const models = [{ name: 'horses', primaryKey: 'id' }, { name: 'examinations', primaryKey: 'id' }]
const schema = `CREATE TABLE horses (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, last_examination_id INTEGER);
CREATE TABLE examinations (id INTEGER PRIMARY KEY, horse_id INTEGER NOT NULL REFERENCES horses(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);
ALTER TABLE horses ADD CONSTRAINT examination_fk FOREIGN KEY (last_examination_id) REFERENCES examinations(id) DEFERRABLE INITIALLY DEFERRED;`
function application() {
   const services = {}
   return { createService(name, methods) { services[name] = methods }, service(name) { return services[name] } }
}
async function fixture(t, options = {}) {
   const db = new PGlite(); await db.exec(schema)
   const couch = new PouchDB(crypto.randomUUID(), { adapter: 'memory' })
   const app = application()
   const errors = []
   const server = pgcouchServerPlugin(app, db, models, { couch, retryMs: 60_000, onError: e => errors.push(e), ...options })
   t.after(async () => { await server.dispose(); await db.close(); await couch.destroy() })
   await server.ready
   async function client(existing) {
      const localDb = existing?.db ?? new PGlite()
      if (!existing) await localDb.exec(schema)
      const localCouch = existing?.couch ?? new PouchDB(crypto.randomUUID(), { adapter: 'memory' })
      const clientErrors = []
      const api = pgcouchClientPlugin({}, { models, localDb, couch: localCouch, autoStart: false, onError: e => clientErrors.push(e) })
      await api.pgcouchReady
      t.after(async () => { await api.disposePGCouchSync(); if (!existing) { await localDb.close(); await localCouch.destroy() } })
      return { api, db: localDb, couch: localCouch, errors: clientErrors, horses: api.createPGCouchModel('horses'), examinations: api.createPGCouchModel('examinations') }
   }
   async function roundTrip(client) {
      await client.horses.flushQueue()
      await client.couch.replicate.to(couch)
      await server.flushQueue()
      await client.couch.replicate.from(couch)
      await client.horses.flushQueue()
   }
   return { db, couch, app, server, errors, client, roundTrip }
}

test('PGCouch: offline atomic multi-table transaction, replication, cold bootstrap and cascades', async t => {
   const f = await fixture(t)
   const a = await f.client()
   await a.horses.create(42, { name: 'Luna' })
   await a.api.pgcouchTransaction([
      { action: 'insert', table: 'examinations', id: 100, data: { horse_id: 42 } },
      { action: 'update', table: 'horses', id: 42, data: { last_examination_id: 100 } },
   ])
   assert.equal((await a.horses.getStatus()).pending, 2)
   assert.equal((await a.horses.findUnique({ id: 42 })).last_examination_id, 100)
   await f.roundTrip(a)
   assert.equal((await a.horses.getStatus()).pending, 0)
   assert.equal((await f.db.query('SELECT * FROM examinations')).rows.length, 1)
   const b = await f.client()
   await f.roundTrip(b)
   assert.deepEqual(await b.horses.findMany(), await a.horses.findMany())
   await a.api.pgcouchTransaction([
      { action: 'update', table: 'horses', id: 42, data: { last_examination_id: null } },
      { action: 'delete', table: 'horses', id: 42 },
   ])
   await f.roundTrip(a); await f.roundTrip(b)
   assert.deepEqual(await b.examinations.findMany(), [])
   assert.deepEqual(await b.horses.findMany(), [])
   assert.deepEqual(f.errors, [])
})

test('PGCouch: callback reads its writes, defers constraints and replicates one atomic proposal', async t => {
   const f = await fixture(t), a = await f.client()
   let saved
   let notifications = 0
   const unsubscribe = a.horses.subscribe(() => { notifications++ })
   const result = await a.api.pgcouchTransaction(async tx => {
      saved = tx.horses
      await tx.horses.create({ id: 42, name: 'Luna', last_examination_id: 100 })
      assert.equal((await tx.horses.findUnique({ id: 42 })).name, 'Luna')
      await tx.examinations.create(100, { horse_id: 42 })
      await tx.horses.update(42, { name: 'Moon' })
      assert.equal((await tx.horses.findMany({ name: 'Moon' })).length, 1)
      assert.equal(notifications, 0)
   })
   assert.equal(result.results.length, 3)
   assert.equal(notifications, 1)
   assert.equal((await a.horses.getStatus()).pending, 1)
   await assert.rejects(saved.update(42, { name: 'Late' }), /finished/)
   await f.roundTrip(a)
   assert.equal((await f.db.query('SELECT name FROM horses WHERE id = 42')).rows[0].name, 'Moon')
   await a.api.pgcouchTransaction(async tx => {
      await tx.horses.update(42, { last_examination_id: null })
      await tx.examinations.delete(100)
      await tx.horses.remove(42)
   })
   await f.roundTrip(a)
   assert.deepEqual((await f.db.query('SELECT * FROM horses')).rows, [])
   unsubscribe()
})

test('PGCouch: callback errors, caught operation errors and deferred violations roll back the outbox', async t => {
   const f = await fixture(t), a = await f.client()
   for (const callback of [
      async tx => { await tx.horses.create(1, { name: 'Luna' }); throw new Error('Abort') },
      async tx => { await tx.horses.create(1, { name: 'Luna' }); await tx.examinations.create(1, { horse_id: 999 }) },
      async tx => {
         await tx.horses.create(1, { name: 'Luna' })
         await assert.rejects(tx.horses.update(999, { name: 'Missing' }))
      },
      async tx => { await tx.horses.create(1, { name: 'Luna' }); await tx.horses.update(1, {}) },
      async () => {},
   ]) {
      await assert.rejects(a.api.pgcouchTransaction(callback))
      assert.deepEqual(await a.horses.findMany(), [])
      assert.equal((await a.horses.getStatus()).pending, 0)
   }
})

test('PGCouch: local constraint violation rolls back all operations and outbox', async t => {
   const f = await fixture(t), a = await f.client()
   await assert.rejects(a.api.pgcouchTransaction([
      { action: 'insert', table: 'horses', id: 1, data: { name: 'Valid' } },
      { action: 'insert', table: 'examinations', id: 1, data: { horse_id: 999 } },
   ]))
   assert.deepEqual(await a.horses.findMany(), [])
   assert.equal((await a.horses.getStatus()).pending, 0)
})

test('PGCouch: concurrent unique conflict is rejected, client converges and reports rejection', async t => {
   const f = await fixture(t), a = await f.client(), b = await f.client()
   await a.horses.create(1, { name: 'Same' }); await b.horses.create(2, { name: 'Same' })
   await f.roundTrip(a); await f.roundTrip(b); await f.roundTrip(a)
   assert.deepEqual(await a.horses.findMany(), await b.horses.findMany())
   assert.equal((await b.horses.getStatus()).failed, 1)
   assert.equal((await b.horses.getStatus()).pending, 0)
   assert.equal(b.errors.at(-1).code, '23505')
})

test('PGCouch: server services, authorization and idempotent replay', async t => {
   const f = await fixture(t, { authorize: (_context, op) => op.action !== 'update' })
   const horse = await f.app.service('horses').create(7, { name: 'Seven' })
   assert.equal(horse.id, 7)
   await assert.rejects(f.app.service('horses').update(7, { name: 'Denied' }), { code: 'forbidden' })
   const doc = { _id: 'transaction:fixed', type: 'transaction', status: 'pending', operations: [{ action: 'insert', table: 'horses', id: 8, data: { name: 'Eight' } }] }
   await f.couch.put(doc)
   await f.server.flushQueue(); await f.server.flushQueue()
   const count = await f.db.query('SELECT count(*)::int AS n FROM horses')
   assert.equal(count.rows[0].n, 2)
   assert.equal((await f.couch.get('result:fixed')).status, 'accepted')
   assert.equal((await f.app.service('horses').findMany({ id: { gte: 7 } })).length, 2)
})

test('PGCouch: a failed Couch write is recovered from the durable local outbox after restart', async t => {
   const f = await fixture(t), a = await f.client()
   const put = a.couch.put.bind(a.couch)
   a.couch.put = async () => { throw new Error('simulated crash boundary') }
   await a.horses.create(1, { name: 'Durable' })
   assert.equal((await a.horses.getStatus()).pending, 1)
   await a.api.disposePGCouchSync()
   a.couch.put = put
   const restarted = await f.client(a)
   await f.roundTrip(restarted)
   assert.equal((await f.db.query('SELECT name FROM horses')).rows[0].name, 'Durable')
   assert.equal((await restarted.horses.getStatus()).pending, 0)
})

test('PGCouch: server SQL commit survives publication failure', async t => {
   const f = await fixture(t)
   const put = f.couch.put.bind(f.couch)
   f.couch.put = async () => { throw new Error('Couch offline') }
   await f.app.service('horses').create(1, { name: 'Committed' })
   assert.equal((await f.db.query('SELECT * FROM horses')).rows.length, 1)
   f.couch.put = put
   await f.server.flushQueue()
   const a = await f.client(); await f.roundTrip(a)
   assert.equal((await a.horses.findUnique({ id: 1 })).name, 'Committed')
})

test('PGCouch: query observables emit committed local changes', async t => {
   const f = await fixture(t), a = await f.client()
   const emissions = []
   const subscription = a.horses.getObservable({ id: { gte: 1 } }).subscribe(rows => emissions.push(rows))
   t.after(() => subscription.unsubscribe())
   await new Promise(resolve => setImmediate(resolve))
   await a.horses.create(1, { name: 'Reactive' })
   await a.horses.waitForIdle()
   await new Promise(resolve => setImmediate(resolve))
   assert.equal(emissions.at(-1)[0].name, 'Reactive')
})

test('PGCouch: protocol validation and Couch role validation', async t => {
   const f = await fixture(t)
   await assert.rejects(f.server.submit([{ action: 'insert', table: 'horses; DROP TABLE horses', id: 1, data: {} }]))
   await assert.rejects(f.server.submit([{ action: 'insert', table: 'horses', id: 1, data: { name: 'Reserved' } }], {}, 'transaction:bootstrap'))
   const validate = Function(`return (${pgcouchValidationDocument().validate_doc_update})`)()
   assert.throws(() => validate({ _id: 'result:fake', type: 'result' }, null, { roles: [] }))
   assert.doesNotThrow(() => validate({ _id: 'result:trusted', type: 'result' }, null, { roles: ['pgcouch-server'] }))
   assert.throws(() => validate({ _id: 'transaction:x' }, {}, { roles: ['pgcouch-server'] }))
})

test('PGCouch: predecessor order survives reverse lexical delivery', async t => {
   const f = await fixture(t)
   await f.couch.put({ _id: 'transaction:z-first', type: 'transaction', status: 'pending', operations: [
      { action: 'insert', table: 'horses', id: 42, data: { name: 'Before' } },
   ] })
   await f.couch.put({ _id: 'transaction:a-second', previous: 'transaction:z-first', type: 'transaction', status: 'pending', operations: [
      { action: 'update', table: 'horses', id: 42, data: { name: 'After' } },
   ] })
   await f.server.flushQueue()
   const first = await f.couch.get('result:z-first'), second = await f.couch.get('result:a-second')
   assert.equal(first.status, 'accepted'); assert.equal(second.status, 'accepted')
   assert.ok(BigInt(first.sequence) < BigInt(second.sequence))
   assert.equal((await f.db.query('SELECT name FROM horses WHERE id = 42')).rows[0].name, 'After')
})

test('PGCouch: remote invalid transaction rolls back every server operation', async t => {
   const f = await fixture(t)
   await f.couch.put({ _id: 'transaction:invalid-fk', type: 'transaction', status: 'pending', operations: [
      { action: 'insert', table: 'horses', id: 42, data: { name: 'Rolled back' } },
      { action: 'insert', table: 'examinations', id: 100, data: { horse_id: 999 } },
   ] })
   await f.server.flushQueue()
   assert.equal((await f.couch.get('result:invalid-fk')).status, 'rejected')
   assert.deepEqual((await f.db.query('SELECT * FROM horses')).rows, [])
   assert.deepEqual((await f.db.query('SELECT * FROM examinations')).rows, [])
})

test('PGCouch: rebases pending local work over remote state without losing either change', async t => {
   const f = await fixture(t), a = await f.client(), b = await f.client()
   await a.horses.create(1, { name: 'Initial' }); await f.roundTrip(a); await f.roundTrip(b)
   await a.horses.update(1, { name: 'Pending name' })
   await b.api.pgcouchTransaction([
      { action: 'insert', table: 'examinations', id: 10, data: { horse_id: 1 } },
      { action: 'update', table: 'horses', id: 1, data: { last_examination_id: 10 } },
   ])
   await f.roundTrip(b)
   // Pull the remote decision without pushing a's pending proposal yet.
   await a.couch.replicate.from(f.couch); await a.horses.flushQueue()
   assert.deepEqual(await a.horses.findUnique({ id: 1 }), { id: 1, name: 'Pending name', last_examination_id: 10 })
   assert.equal((await a.horses.getStatus()).pending, 1)
   await f.roundTrip(a); await f.roundTrip(b)
   assert.deepEqual(await a.horses.findMany(), await b.horses.findMany())
})

test('PGCouch: live replication, start/stop and model disposal', async t => {
   const f = await fixture(t), a = await f.client()
   // Use the same protocol with native live PouchDB replication.
   const replication = a.couch.sync(f.couch, { live: true, retry: true })
   t.after(() => replication.cancel())
   await a.horses.start()
   const emissions = []
   let complete = false
   const subscription = a.horses.getObservable().subscribe({ next: rows => emissions.push(rows), complete: () => { complete = true } })
   t.after(() => subscription.unsubscribe())
   await a.horses.create(12, { name: 'Live' })
   for (let attempt = 0; attempt < 100; attempt++) {
      await f.server.flushQueue(); await a.horses.flushQueue()
      if ((await a.horses.getStatus()).pending === 0) break
      await new Promise(resolve => setTimeout(resolve, 10))
   }
   assert.equal((await a.horses.getStatus()).pending, 0)
   assert.equal((await f.db.query('SELECT name FROM horses WHERE id = 12')).rows[0].name, 'Live')
   a.horses.stop()
   await a.horses.dispose()
   assert.equal(complete, true)
   await assert.rejects(a.horses.create(13, { name: 'Disposed' }), /disposed/)
   assert.notEqual(a.api.createElectricModel('horses'), a.horses)
})


test('PGCouch: incremental changes capture cascades and trigger effects; checkpoints bootstrap across gaps', async t => {
   const f = await fixture(t, { checkpointInterval: 3 })
   await f.db.exec(`CREATE FUNCTION rename_horse() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN UPDATE horses SET name = upper(name) WHERE id = NEW.horse_id; RETURN NEW; END $$;
      CREATE TRIGGER rename_horse AFTER INSERT ON examinations FOR EACH ROW EXECUTE FUNCTION rename_horse();`)
   await f.server.submit([{ action: 'insert', table: 'horses', id: 1, data: { name: 'Luna' } }], {}, 'transaction:delta1')
   await f.server.submit([{ action: 'insert', table: 'examinations', id: 2, data: { horse_id: 1 } }], {}, 'transaction:delta2')
   await f.server.submit([{ action: 'update', table: 'horses', id: 1, data: { name: 'Moon' } }], {}, 'transaction:delta3')
   await f.server.submit([{ action: 'delete', table: 'horses', id: 1 }], {}, 'transaction:delta4')
   const docs = (await Promise.all([1, 2, 3, 4].map(i => f.couch.get(`result:delta${i}`)))).map(({ _rev, ...doc }) => doc)
   assert.equal(docs[0].snapshot, undefined)
   assert.equal(docs[1].snapshot, undefined)
   assert.equal(docs[1].changes.find(row => row.table === 'horses').data.name, 'LUNA')
   assert.equal(docs[2].snapshot.horses[0].name, 'Moon')
   assert.deepEqual(docs[3].changes.map(row => [row.table, row.data]).sort(), [['examinations', null], ['horses', null]])
   const a = await f.client()
   // Replication can deliver later decisions before their predecessors.
   await a.couch.put(docs[1]); await a.horses.flushQueue()
   assert.deepEqual(await a.horses.findMany(), [])
   await a.couch.put(docs[2]); await a.horses.flushQueue()
   assert.equal((await a.horses.findUnique({ id: 1 })).name, 'Moon')
   await a.couch.put(docs[3]); await a.horses.flushQueue()
   assert.deepEqual(await a.horses.findMany(), [])
   assert.deepEqual(await a.examinations.findMany(), [])
   // Older arrivals must not restore deleted rows.
   await a.couch.put(docs[0]); await a.horses.flushQueue()
   assert.deepEqual(await a.horses.findMany(), [])
   assert.equal((await a.db.query('SELECT count(*)::int AS n FROM pgcouch_inbox')).rows[0].n, 0)
})

test('PGCouch: decision ingestion is paged, durable across restart, and accepts legacy snapshots', async t => {
   const f = await fixture(t), a = await f.client()
   const { _rev, ...bootstrap } = await f.couch.get('result:bootstrap')
   const docs = [bootstrap]
   for (let i = 1; i <= 105; i++) docs.push({
      _id: `result:page${i}`, type: 'result', status: 'accepted', sequence: String(i),
      changes: [{ table: 'horses', id: 1, data: { id: 1, name: `Horse ${i}`, last_examination_id: null } }],
   })
   await a.couch.bulkDocs(docs)
   const original = a.couch.changes.bind(a.couch)
   const limits = []
   a.couch.changes = options => { limits.push(options.limit); return original(options) }
   await a.horses.flushQueue()
   assert.ok(limits.length >= 2)
   assert.ok(limits.every(limit => limit === 100))
   assert.equal((await a.horses.findUnique({ id: 1 })).name, 'Horse 105')
   await a.api.disposePGCouchSync()
   const restarted = await f.client(a)
   assert.equal((await restarted.horses.findUnique({ id: 1 })).name, 'Horse 105')
   // Old servers sent a snapshot with every decision, without a changes field.
   await a.couch.put({ _id: 'result:legacy', type: 'result', status: 'accepted', sequence: '106',
      snapshot: { horses: [{ id: 2, name: 'Legacy', last_examination_id: null }], examinations: [] } })
   await restarted.horses.flushQueue()
   assert.equal((await restarted.horses.findUnique({ id: 2 })).name, 'Legacy')
   assert.equal(await restarted.horses.findUnique({ id: 1 }), null)
})

test('PGCouch: row deltas use final state after recursive AFTER triggers', async t => {
   const f = await fixture(t)
   await f.db.exec(`CREATE FUNCTION normalize_horse() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
         IF NEW.name <> upper(NEW.name) THEN UPDATE horses SET name = upper(NEW.name) WHERE id = NEW.id; END IF;
         RETURN NEW;
      END $$;
      CREATE TRIGGER a_normalize AFTER INSERT OR UPDATE ON horses FOR EACH ROW EXECUTE FUNCTION normalize_horse();`)
   await f.server.submit([{ action: 'insert', table: 'horses', id: 1, data: { name: 'Luna' } }], {}, 'transaction:normalized')
   const decision = await f.couch.get('result:normalized')
   assert.equal(decision.changes.length, 1)
   assert.equal(decision.changes[0].data.name, 'LUNA')
   const a = await f.client()
   await f.roundTrip(a)
   assert.equal((await a.horses.findUnique({ id: 1 })).name, 'LUNA')
})

test('PGCouch: where updates and deletes replicate atomically and use server-time matches', async t => {
   const f = await fixture(t), a = await f.client(), b = await f.client()
   await a.api.pgcouchTransaction(async tx => {
      for (const id of [1, 2, 3]) await tx.horses.create(id, { name: `Horse ${id}` })
      await tx.examinations.create(100, { horse_id: 1 })
      await tx.examinations.create(101, { horse_id: 2 })
   })
   await f.roundTrip(a); await f.roundTrip(b)
   // This row is unknown locally when the filter is queued, but matches on the server.
   await f.app.service('horses').create(4, { name: 'Horse 4' })
   const updated = await a.horses.update({ id: { gte: 2, lt: 5 }, last_examination_id: null }, { last_examination_id: 100 })
   assert.deepEqual(updated.map(row => row.id).sort(), [2, 3])
   await f.roundTrip(a); await f.roundTrip(b)
   assert.deepEqual((await b.horses.findMany({ last_examination_id: 100 })).map(row => row.id), [2, 3, 4])
   const removed = await a.horses.delete({ last_examination_id: 100 })
   assert.deepEqual(removed.map(row => row.id).sort(), [2, 3, 4])
   await f.roundTrip(a); await f.roundTrip(b)
   assert.deepEqual((await b.horses.findMany()).map(row => row.id), [1])
   assert.deepEqual((await b.examinations.findMany()).map(row => row.id), [100])
   assert.deepEqual(await a.horses.update({ id: 999 }, { name: 'Missing' }), [])
   assert.deepEqual(await a.horses.remove({ id: 999 }), [])
   await assert.rejects(a.horses.update(999, { name: 'Missing' }), /does not exist/)
})

test('PGCouch: callback and array transactions support where, preserve results, and roll back bulk failures', async t => {
   const f = await fixture(t), a = await f.client()
   await a.api.pgcouchTransaction(async tx => {
      await tx.horses.create(1, { name: 'One' })
      await tx.horses.create(2, { name: 'Two' })
   })
   const before = (await a.horses.getStatus()).pending
   await assert.rejects(a.api.pgcouchTransaction(async tx => {
      await tx.horses.update({}, { last_examination_id: null })
      await tx.horses.update({}, { name: 'Duplicate' })
   }))
   assert.deepEqual((await a.horses.findMany()).map(row => row.name), ['One', 'Two'])
   assert.equal((await a.horses.getStatus()).pending, before)
   const { results } = await a.api.pgcouchTransaction(async tx => {
      const rows = await tx.horses.update({ id: { gt: 0, lte: 2 } }, { last_examination_id: null })
      assert.equal(rows.length, 2)
      assert.equal((await tx.horses.findMany({ last_examination_id: null })).length, 2)
      await tx.horses.delete({ name: 'Two' })
   })
   assert.equal(results[0].length, 2)
   assert.equal(results[1][0].id, 2)
   const arrayResult = await a.api.pgcouchTransaction([
      { action: 'update', table: 'horses', where: { id: 1 }, data: { name: 'First' } },
      { action: 'delete', table: 'horses', where: { name: 'Absent' } },
   ])
   assert.equal(arrayResult.results[0][0].name, 'First')
   assert.deepEqual(arrayResult.results[1], [])
   await f.roundTrip(a)
   assert.equal((await f.app.service('horses').update({ name: 'First' }, { name: 'Last' }))[0].id, 1)
   assert.deepEqual(await f.app.service('horses').update({ id: 999 }, { name: 'Absent' }), [])
   assert.equal((await f.app.service('horses').delete({})).length, 1)
   await f.roundTrip(a)
   assert.deepEqual(await a.horses.findMany(), [])
})

test('PGCouch: mutation filters validate protocol and authorization receives the predicate', async t => {
   const validation = await fixture(t)
   for (const op of [
      { action: 'insert', table: 'horses', where: {}, data: { name: 'Invalid' } },
      { action: 'delete', table: 'horses', id: 1, where: {} },
      ...[null, undefined, [], { id: undefined }, { id: NaN }, { id: () => 1 }, { id: { gt: undefined } }, { id: { sql: '1=1' } }, { 'id; DROP TABLE horses': 1 }].map(where => ({ action: 'delete', table: 'horses', where })),
      { action: 'update', table: 'horses', where: {}, data: { id: 42 } },
   ]) await assert.rejects(validation.server.submit([op]))
   const seen = []
   const f = await fixture(t, { authorize(context, operation) {
      seen.push(operation)
      return typeof operation.args[0] !== 'object'
   } })
   await f.app.service('horses').create(1, { name: 'One' })
   await assert.rejects(f.app.service('horses').update({ id: { gte: 1 } }, { name: 'Forbidden' }), /forbidden/i)
   await assert.rejects(f.app.service('horses').delete({}), /forbidden/i)
   assert.deepEqual(seen.at(-2).args, [{ id: { gte: 1 } }, { name: 'Forbidden' }])
   assert.deepEqual(seen.at(-1).args, [{}])
   assert.equal((await f.db.query('SELECT name FROM horses')).rows[0].name, 'One')
})


test('PGCouch: wrapper preserves aliases, service names and authorization contexts', async t => {
   const seen = []
   const f = await fixture(t, {
      serviceName: 'transactions',
      authorize(context, operation) { seen.push({ context, operation }); return true },
   })
   const context = { user: { id: 'authenticated-user' } }
   await f.app.service('horses').create.call(context, 1, { name: 'Context' })
   await f.app.service('horses').findMany.call(context)
   await f.app.service('horses').findUnique.call(context, { id: 1 })
   await f.app.service('transactions').transaction.call(context, [
      { action: 'update', table: 'horses', id: 1, data: { name: 'Updated' } },
   ], 'transaction:context')
   assert.ok(seen.every(item => item.context === context))
   assert.deepEqual(seen.map(item => item.operation.action), ['create', 'findMany', 'findUnique', 'update'])
   await f.couch.put({ _id: 'transaction:replicated-context', type: 'transaction', status: 'pending', operations: [
      { action: 'update', table: 'horses', id: 1, data: { name: 'Replicated' } },
   ] })
   await f.server.flushQueue()
   assert.equal(seen.at(-1).context.app, f.app)
   assert.equal(seen.at(-1).context.transport, 'couch')
   const a = await f.client()
   assert.equal(a.api.createPGCouchModel, a.api.createElectricModel)
   assert.equal(a.api.disposePGCouchSync, a.api.disposeElectricSync)
   assert.equal(a.api.createElectricModel('horses'), a.horses)
   assert.throws(() => pgcouchServerPlugin(application(), f.db, models, { couch: f.couch, serviceName: 'horses' }), /conflicts/)
})
