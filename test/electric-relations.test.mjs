import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricServerPlugin, prepareElectricSyncSchema } from '../src/electric-server-plugin.mjs'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

const models = [
   { name: 'user', table: 'people', tombstoneData: { email: null } },
   { name: 'group', table: 'groups', tombstoneData: { name: null } },
   { name: 'membership', tombstoneData: { user_uid: null, group_uid: null } },
   { name: 'note', primaryKey: 'id', tombstoneData: { membership_uid: null } },
]

// Pool semantics on a single PGlite connection: queue complete transactions,
// never interleave BEGIN/COMMIT from concurrently flushing model queues.
function poolFor(db) {
   let tail = Promise.resolve()
   return {
      query: (...args) => db.query(...args),
      async connect() {
         let release
         const next = new Promise(resolve => { release = resolve })
         const previous = tail
         tail = next
         await previous
         return { query: (...args) => db.query(...args), release }
      },
   }
}

async function fixture(t) {
   const db = new PGlite()
   t.after(() => db.close())
   await db.exec(`
      CREATE TABLE people (uid UUID PRIMARY KEY, email TEXT UNIQUE);
      CREATE TABLE groups (uid UUID PRIMARY KEY, name TEXT UNIQUE);
      CREATE TABLE membership (
         uid UUID PRIMARY KEY,
         user_uid UUID REFERENCES people(uid) ON DELETE CASCADE,
         group_uid UUID REFERENCES groups(uid) ON DELETE CASCADE,
         label TEXT,
         UNIQUE (user_uid, group_uid)
      );
      CREATE TABLE note (id UUID PRIMARY KEY, membership_uid UUID REFERENCES membership(uid) ON DELETE CASCADE);
   `)
   await prepareElectricSyncSchema(db, models)
   const services = new Map()
   const app = { createService(name, methods) { services.set(name, methods) }, get() {} }
   electricServerPlugin(app, poolFor(db), models, { sync: true })
   const metadata = () => ({ clientId: crypto.randomUUID(), revision: '1' })
   const user = crypto.randomUUID()
   const group = crypto.randomUUID()
   await services.get('user').create(user, { email: 'member@example.com' }, metadata())
   await services.get('group').create(group, { name: 'Team' }, metadata())
   return { db, services, metadata, user, group }
}

test('membership committed before an offline group delete is cascaded, including descendants', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   const membership = crypto.randomUUID()
   const note = crypto.randomUUID()
   const created = await services.get('membership').create(membership, { user_uid: user, group_uid: group }, metadata())
   await services.get('note').create(note, { membership_uid: membership }, metadata())
   const deleted = await services.get('group').delete(group, metadata())
   assert.equal(deleted.deleted, true)
   const row = (await db.query('SELECT * FROM membership')).rows[0]
   assert.equal(row.deleted, true)
   assert.equal(row.user_uid, null)
   assert.equal(row.group_uid, null)
   assert.ok(BigInt(row.version) > BigInt(created.version))
   assert.equal((await db.query('SELECT * FROM note')).rows[0].deleted, true)
   assert.equal((await db.query('SELECT * FROM people')).rows[0].deleted, false)
})

test('membership sent after parent deletion becomes a replayable rejection tombstone', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   await services.get('group').delete(group, metadata())
   const id = crypto.randomUUID()
   const request = [id, { user_uid: user, group_uid: group }, metadata()]
   const rejected = await services.get('membership').create(...request)
   assert.equal(rejected.deleted, true)
   assert.equal(rejected.syncRejection.code, 'sync-parent-deleted')
   assert.deepEqual(await services.get('membership').create(...request), rejected)
   assert.equal((await db.query('SELECT count(*)::int AS count FROM membership WHERE NOT deleted')).rows[0].count, 0)
   assert.equal((await db.query('SELECT * FROM membership')).rows[0].group_uid, null)
})

test('user deletion also cascades; delayed updates cannot resurrect the membership', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   const id = crypto.randomUUID()
   await services.get('membership').create(id, { user_uid: user, group_uid: group }, metadata())
   await services.get('user').delete(user, metadata())
   const delayed = await services.get('membership').update(id, { label: 'late edit' }, metadata())
   assert.equal(delayed.deleted, true)
   assert.equal((await db.query('SELECT * FROM membership')).rows[0].deleted, true)
   assert.equal((await db.query('SELECT * FROM groups')).rows[0].deleted, false)
})

test('membership reassignment checks deleted parents and colliding creates cannot delete valid rows', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   const id = crypto.randomUUID()
   const other = crypto.randomUUID()
   await services.get('group').create(other, { name: 'Other' }, metadata())
   await services.get('group').delete(other, metadata())
   const original = await services.get('membership').create(id, { user_uid: user, group_uid: group }, metadata())
   const collider = metadata()
   for (const revision of ['1', '2']) {
      await assert.rejects(services.get('membership').create(id, { user_uid: user, group_uid: other }, { ...collider, revision }),
         { code: 'sync-create-conflict' })
      assert.deepEqual((await db.query('SELECT * FROM membership WHERE uid = $1', [id])).rows, [original])
   }
   const reassigned = await services.get('membership').update(id, { group_uid: other }, metadata())
   assert.equal(reassigned.deleted, true)
   assert.equal(reassigned.syncRejection.code, 'sync-parent-deleted')
   assert.equal((await db.query('SELECT * FROM membership')).rows[0].deleted, true)
})

test('a missing parent is retriable so parent and child can be created offline together', async t => {
   const { services, metadata, user } = await fixture(t)
   const group = crypto.randomUUID()
   const request = [crypto.randomUUID(), { user_uid: user, group_uid: group }, metadata()]
   await assert.rejects(services.get('membership').create(...request), { code: 'sync-parent-missing' })
   await services.get('group').create(group, { name: 'Queued parent' }, metadata())
   assert.equal((await services.get('membership').create(...request)).deleted, false)
})

test('partial updates validate retained foreign keys, including fields passed as undefined', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   const id = crypto.randomUUID()
   await services.get('membership').create(id, { user_uid: user, group_uid: group }, metadata())
   // Simulate a legacy orphan left by an older server instance.
   await db.query('UPDATE groups SET deleted = true WHERE uid = $1', [group])
   const result = await services.get('membership').update(id, { group_uid: undefined, label: 'edit' }, metadata())
   assert.equal(result.deleted, true)
   assert.equal(result.syncRejection.code, 'sync-parent-deleted')
})

test('schema preparation repairs existing soft-delete orphans and is idempotent', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   const id = crypto.randomUUID()
   await services.get('membership').create(id, { user_uid: user, group_uid: group }, metadata())
   await services.get('note').create(crypto.randomUUID(), { membership_uid: id }, metadata())
   await db.query('UPDATE groups SET deleted = true WHERE uid = $1', [group])
   await prepareElectricSyncSchema(db, models)
   const repaired = (await db.query('SELECT * FROM membership')).rows[0]
   assert.equal(repaired.deleted, true)
   assert.equal((await db.query('SELECT * FROM note')).rows[0].deleted, true)
   await prepareElectricSyncSchema(db, models)
   assert.deepEqual((await db.query('SELECT * FROM membership')).rows[0], repaired)
})

test('cascade failure rolls back the parent, children, and mutation cursor together', async t => {
   const { db, services, metadata, user, group } = await fixture(t)
   await services.get('membership').create(crypto.randomUUID(), { user_uid: user, group_uid: group }, metadata())
   const broken = models.map(model => model.name === 'membership' ? { ...model, tombstoneData: () => { throw new Error('factory failed') } } : model)
   const brokenServices = new Map()
   electricServerPlugin({ createService(name, methods) { brokenServices.set(name, methods) }, get() {} }, poolFor(db), broken, { sync: true })
   const mutation = metadata()
   await assert.rejects(brokenServices.get('group').delete(group, mutation), /factory failed/)
   assert.equal((await db.query('SELECT * FROM groups')).rows[0].deleted, false)
   assert.equal((await db.query('SELECT * FROM membership')).rows[0].deleted, false)
   assert.equal((await db.query('SELECT count(*)::int AS count FROM electric_mutation_cursor WHERE client_id = $1', [mutation.clientId])).rows[0].count, 0)
})

test('cyclic CASCADE relationships terminate and tombstone every descendant', async t => {
   const db = new PGlite()
   t.after(() => db.close())
   await db.exec('CREATE TABLE nodes (uid UUID PRIMARY KEY, parent_uid UUID REFERENCES nodes(uid) ON DELETE CASCADE)')
   const models = [{ name: 'nodes', tombstoneData: { parent_uid: null } }]
   await prepareElectricSyncSchema(db, models)
   let service
   electricServerPlugin({ createService(_name, methods) { service = methods }, get() {} }, poolFor(db), models, { sync: true })
   const first = crypto.randomUUID()
   const second = crypto.randomUUID()
   await service.create(first, { parent_uid: first }, { clientId: crypto.randomUUID(), revision: '1' })
   await service.create(second, { parent_uid: first }, { clientId: crypto.randomUUID(), revision: '1' })
   await service.update(first, { parent_uid: second }, { clientId: crypto.randomUUID(), revision: '1' })
   await service.delete(first, { clientId: crypto.randomUUID(), revision: '1' })
   assert.ok((await db.query('SELECT * FROM nodes')).rows.every(row => row.deleted))
})

class Stream {
   subscribe() {}
   isConnected() { return true }
   unsubscribeAll() {}
}
class Shape {
   static current
   constructor() { Shape.current = this }
   subscribe(callback) { this.emit = callback }
}

test('offline rejected memberships disappear immediately, report a conflict, and stop retrying', async t => {
   const { db: server, services, metadata, user, group } = await fixture(t)
   const db = new PGlite()
   t.after(() => db.close())
   const errors = []
   let requests = 0
   const app = { service: () => ({ async create(...args) { requests++; return services.get('membership').create(...args) } }) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const network = { onLine: false }
   const model = app.createElectricModel('membership', { localDb: db, network, retryMs: 60_000, onError: error => errors.push(error) })
   await model.prepare()
   const local = await model.create({ user_uid: user, group_uid: group })
   assert.equal((await model.findMany()).length, 1)
   await services.get('group').delete(group, metadata())
   network.onLine = true
   await model.flushQueue()
   assert.equal((await model.findMany()).length, 0)
   assert.equal(errors[0].code, 'sync-parent-deleted')
   assert.equal(errors[0].rowId, local.uid)
   await model.flushQueue()
   assert.equal(requests, 1)
   model.start()
   t.after(async () => { model.stop(); await model.waitForIdle() })
   // An older snapshot cannot restore the rejected optimistic row.
   Shape.current.emit({ rows: [{ ...local, version: '0', deleted: false }] })
   await model.waitForIdle()
   assert.equal((await model.findMany()).length, 0)
   Shape.current.emit({ rows: (await server.query('SELECT * FROM membership')).rows })
   await model.waitForIdle()
   assert.equal((await model.getStatus()).pending, 0)
   assert.equal((await model.findMany()).length, 0)
})
