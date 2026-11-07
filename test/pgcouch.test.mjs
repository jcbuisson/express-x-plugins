import test from 'node:test'
import assert from 'node:assert/strict'
import { PGlite } from '@electric-sql/pglite'
import PouchDB from 'pouchdb'
import memory from 'pouchdb-adapter-memory'
import { pgcouchServerPlugin, pgcouchValidationDocument } from '../src/pgcouch-server-plugin.mjs'
import { pgcouchClientPlugin } from '../src/pgcouch-client-plugin.mjs'
import { documentOf, modelsOf } from '../src/pgcouch-common.mjs'
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

test('PGCouch: protocol validation and Couch role validation', () => {
   const normalized = modelsOf(models)
   assert.throws(() => documentOf({ _id: 'transaction:x', type: 'transaction', status: 'pending', operations: [{ action: 'insert', table: 'horses; DROP TABLE horses', id: 1, data: {} }] }, normalized))
   assert.throws(() => documentOf({ _id: 'transaction:bootstrap', type: 'transaction', status: 'pending', operations: [{ action: 'insert', table: 'horses', id: 1, data: { name: 'Reserved' } }] }, normalized))
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
