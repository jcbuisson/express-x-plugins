import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { createClient } from '@jcbuisson/express-x/client'
import { FetchError, Shape as ElectricShape } from '@electric-sql/client'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'
import { electricServerPlugin, prepareElectricSyncSchema } from '../src/electric-server-plugin.mjs'

class Stream {
   static current
   constructor(options) { this.options = options; Stream.current = this }
   subscribe() { return () => {} }
   isConnected() { return true }
   unsubscribeAll() {}
}

class Shape {
   static current
   constructor() { Shape.current = this }
   subscribe(callback) { this.emit = callback; return () => {} }
}

test('edits survive a committed create whose response was lost', async () => {
   const server = new PGlite()
   const db = new PGlite()
   let model
   try {
      await server.exec('CREATE TABLE todos (id UUID PRIMARY KEY, title TEXT NOT NULL)')
      await prepareElectricSyncSchema(server, [{ name: 'todos', primaryKey: 'id', tombstoneData: { title: '' } }])
      let service
      const pool = {
         query: server.query.bind(server),
         async connect() { return { query: server.query.bind(server), release() {} } },
      }
      electricServerPlugin({ createService(_name, methods) { service = methods }, get() {} }, pool,
         [{ name: 'todos', primaryKey: 'id', tombstoneData: { title: '' } }], { sync: true })
      let loseResponse = true
      const requests = []
      const proxy = {
         async create(...args) {
            requests.push(args)
            const result = await service.create(...args)
            if (loseResponse) { loseResponse = false; throw new Error('response lost') }
            return result
         },
      }
      const app = { service: () => proxy }
      electricClientPlugin(app, { ShapeStream: Stream, Shape })
      model = app.createElectricModel('todos', { primaryKey: 'id', localDb: db, retryMs: 60_000 })
      await model.prepare()
      const row = await model.create({ title: 'original' })
      await model.flushQueue()
      const original = (await server.query('SELECT * FROM todos')).rows[0]
      await model.update(row.id, { title: 'edited' })
      await model.flushQueue()
      const edited = (await server.query('SELECT * FROM todos')).rows[0]
      assert.equal(edited.title, 'edited')
      assert.ok(BigInt(edited.version) > BigInt(original.version))

      // Replaying either revision must preserve the edited row and its version.
      await service.create(...requests[0])
      await service.create(...requests[1])
      assert.deepEqual((await server.query('SELECT * FROM todos')).rows, [edited])
      model.start()
      Shape.current.emit({ rows: [edited] })
      await model.waitForIdle()
      assert.equal((await model.findUnique({ id: row.id })).title, 'edited')
      assert.equal((await model.getStatus()).pending, 0)

      // Another client's colliding create still cannot overwrite the row.
      const collider = crypto.randomUUID()
      for (const revision of ['1', '2']) {
         await assert.rejects(service.create(row.id, { title: 'collision' }, { clientId: collider, revision }),
            { code: 'sync-create-conflict' })
      }
      assert.deepEqual((await server.query('SELECT * FROM todos')).rows, [edited])

      // A delayed edited create must not resurrect a tombstone.
      const metadata = requests[1][2]
      await service.delete(row.id, { ...metadata, revision: '100' })
      const tombstone = await service.create(row.id, { title: 'delayed' }, { ...metadata, revision: '101' })
      assert.equal(tombstone.deleted, true)
   } finally {
      model?.stop()
      await model?.waitForIdle()
      await db.close()
      await server.close()
   }
})

async function syncFixture(t, schema, models) {
   const server = new PGlite()
   const local = new PGlite()
   t.after(async () => { await local.close(); await server.close() })
   await server.exec(schema)
   await prepareElectricSyncSchema(server, models)
   const services = new Map()
   const pool = { query: server.query.bind(server), async connect() {
      return { query: server.query.bind(server), release() {} }
   } }
   electricServerPlugin({ createService(name, service) { services.set(name, service) }, get() {} }, pool, models, { sync: true })
   const app = { service: name => services.get(name) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   return { server, local, services, app }
}

test('updates coalesce changed fields without resending generated snapshot columns', async t => {
   const { app, local, server, services } = await syncFixture(t,
      `CREATE TABLE todos (uid UUID PRIMARY KEY, qty INTEGER, title TEXT,
         doubled INTEGER GENERATED ALWAYS AS (qty * 2) STORED)`, ['todos'])
   const service = services.get('todos')
   const original = await service.create(crypto.randomUUID(), { qty: 2, title: 'original' },
      { clientId: crypto.randomUUID(), revision: '1' })
   const requests = []
   const update = service.update
   service.update = async (...args) => { requests.push(args[1]); return update(...args) }
   const network = { onLine: false }
   const model = app.createElectricModel('todos', { localDb: local, network, retryMs: 60_000 })
   t.after(async () => { model.stop(); await model.waitForIdle() })
   await model.prepare()
   model.start()
   Shape.current.emit({ rows: [original] })
   await model.waitForIdle()
   await model.update(original.uid, { qty: 3 })
   await model.update(original.uid, { title: 'changed' })
   await model.update(original.uid, {})
   network.onLine = true
   await model.flushQueue()
   assert.deepEqual(requests, [{ qty: 3, title: 'changed' }])
   const updated = (await server.query('SELECT * FROM todos WHERE uid = $1', [original.uid])).rows[0]
   assert.equal(updated.doubled, 6)
   Shape.current.emit({ rows: [updated] })
   await model.waitForIdle()
   assert.equal((await model.getStatus()).pending, 0)
   assert.equal((await model.findUnique({ uid: original.uid })).doubled, 6)

   network.onLine = false
   const created = await model.create({ qty: 5, title: 'queued create' })
   await model.update(created.uid, { title: 'edited before send' })
   network.onLine = true
   await model.flushQueue()
   const inserted = (await server.query('SELECT * FROM todos WHERE uid = $1', [created.uid])).rows[0]
   assert.equal(inserted.qty, 5)
   assert.equal(inserted.title, 'edited before send')
   assert.equal(inserted.doubled, 10)
})

test('offline sync rejects partial Shapes and supports local query filters', async t => {
   const db = new PGlite()
   let model
   let subscription
   t.after(async () => {
      subscription?.unsubscribe()
      model?.stop()
      await model?.waitForIdle()
      await db.close()
   })
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const invalidOptions = [
      { streamOptions: { params: { where: 'qty > 0' } } },
      { streamOptions: { params: { where: 'NOT deleted' } } },
      { streamOptions: { params: { where: () => 'qty > 0' } } },
      { url: '/electric/v1/shape/todos?where=qty%20%3E%200' },
      { streamOptions: { params: { columns: ['uid', 'qty'] } } },
      { url: '/electric/v1/shape/todos?columns=uid,qty' },
      { streamOptions: { params: { log: 'changes_only' } } },
      { streamOptions: { offset: 'now' } },
      { url: '/electric/v1/shape/todos?offset=100_0' },
      { streamOptions: { params: { subset__where: 'qty > 0' } } },
   ]
   for (const options of invalidOptions) {
      assert.throws(() => app.createElectricModel('todos', { localDb: db, ...options }), /complete Shape including tombstones/)
   }
   const params = { log: 'full' }
   model = app.createElectricModel('todos', { localDb: db, network: { onLine: false }, retryMs: 60_000, streamOptions: { params } })
   // Options supplied by the caller cannot later turn the validated stream into a partial Shape.
   params.where = 'qty > 0'
   await model.prepare()
   model.start()
   const zero = { uid: crypto.randomUUID(), qty: 0, version: '1', deleted: false }
   const positive = { uid: crypto.randomUUID(), qty: 2, version: '2', deleted: false }
   Shape.current.emit({ rows: [zero, positive] })
   await model.waitForIdle()
   assert.deepEqual(await model.findMany({ qty: { gt: 0 } }), [positive])
   let observed
   subscription = model.getObservable({ qty: { gt: 0 } }).subscribe(rows => { observed = rows })
   await new Promise(resolve => setTimeout(resolve, 0))
   assert.deepEqual(observed, [positive])
})

test('offline filters validate empty caches and match SQL null and Date behavior', async t => {
   const db = new PGlite()
   t.after(() => db.close())
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('todos', { localDb: db })
   await model.prepare()
   await assert.rejects(model.findMany({ qty: { equals: 1 } }), /unsupported/)
   await assert.rejects(model.findMany({ qty: { lt: null } }), /unsupported/)
   assert.throws(() => model.getObservable({ qty: {} }), /unsupported/)
   const before = new Date('2026-01-01')
   const at = new Date('2026-01-02')
   const after = new Date('2026-01-03')
   await model.create({ qty: null, date: before })
   const matching = await model.create({ qty: 2, date: at })
   await model.create({ date: after })
   assert.deepEqual((await model.findMany({ qty: { lt: 5 } })).map(row => row.uid), [matching.uid])
   assert.deepEqual((await model.findMany({ date: at })).map(row => row.uid), [matching.uid])
   assert.deepEqual((await model.findMany({ date: { gt: before, lt: after } })).map(row => row.uid), [matching.uid])
})

for (const onDelete of ['', 'ON DELETE CASCADE']) {
   test(`a flush resolves reverse-ordered parents with ${onDelete || 'default foreign-key behavior'}`,  async t => {
      const { app, local, server, services } = await syncFixture(t,
         `CREATE TABLE nodes (uid UUID PRIMARY KEY, parent_uid UUID REFERENCES nodes(uid) ${onDelete})`,
         [{ name: 'nodes', tombstoneData: { parent_uid: null } }])
      const service = services.get('nodes')
      let calls = 0
      const create = service.create
      service.create = async (...args) => { calls++; return create(...args) }
      const model = app.createElectricModel('nodes', { localDb: local })
      await model.prepare()
      const root = crypto.randomUUID()
      const middle = crypto.randomUUID()
      await model.create({ parent_uid: middle })
      await model.create({ uid: middle, parent_uid: root })
      await model.create({ uid: root, parent_uid: null })
      await model.flushQueue()
      assert.equal((await server.query('SELECT * FROM nodes WHERE NOT deleted')).rows.length, 3)
      assert.equal((await local.query('SELECT * FROM electric_mutation_queue WHERE acknowledged_version IS NULL')).rows.length, 0)

      const orphan = await model.create({ parent_uid: crypto.randomUUID() })
      const before = calls
      await model.flushQueue()
      assert.equal(calls, before + 1)
      await model.flushQueue()
      assert.equal(calls, before + 2)
      const queued = (await local.query('SELECT * FROM electric_mutation_queue WHERE row_id = $1', [orphan.uid])).rows[0]
      assert.equal(queued.status, 'pending')
      assert.equal(queued.acknowledged_version, null)
   })
}

for (const staleFailure of [false, true]) {
   test(`new observable reads supersede stale ${staleFailure ? 'errors' : 'results'}`, async t => {
      const db = new PGlite()
      let release
      let entered
      let hold = false
      const gate = new Promise(resolve => { release = resolve })
      const reading = new Promise(resolve => { entered = resolve })
      const delayed = {
         exec: (...args) => db.exec(...args),
         transaction: callback => db.transaction(callback),
         async query(sql, ...args) {
            const result = await db.query(sql, ...args)
            if (hold && sql.startsWith('SELECT data') && sql.includes('FROM electric_local_rows')) {
               hold = false
               entered()
               await gate
               if (staleFailure) throw new Error('stale read failed')
            }
            return result
         },
      }
      const app = { service: () => ({}) }
      electricClientPlugin(app, { ShapeStream: Stream, Shape })
      const model = app.createElectricModel('todos', { localDb: delayed })
      let subscription
      t.after(async () => { subscription?.unsubscribe(); release(); await db.close() })
      await model.prepare()
      const row = await model.create({ title: 'old' })
      hold = true
      const observed = []
      const errors = []
      let sawNew
      const newer = new Promise(resolve => { sawNew = resolve })
      subscription = model.getObservable().subscribe({
         next(rows) { observed.push(rows[0].title); if (rows[0].title === 'new') sawNew() },
         error(error) { errors.push(error) },
      })
      await reading
      await model.update(row.uid, { title: 'new' })
      await newer
      release()
      await new Promise(resolve => setImmediate(resolve))
      assert.deepEqual(observed, ['new'])
      assert.deepEqual(errors, [])
      assert.equal(subscription.closed, false)
   })
}

test('constraint failures do not block valid writes and an edit can retry a failed row', async t => {
   const { app, local, server, services } = await syncFixture(t,
      "CREATE TABLE todos (uid UUID PRIMARY KEY, title TEXT NOT NULL UNIQUE CHECK (length(title) > 2))",
      [{ name: 'todos', tombstoneData: { title: 'deleted' } }])
   await services.get('todos').create(crypto.randomUUID(), { title: 'taken' }, { clientId: crypto.randomUUID(), revision: '1' })
   const model = app.createElectricModel('todos', { localDb: local })
   await model.prepare()
   const duplicate = await model.create({ title: 'taken' })
   await model.create({ title: null })
   await model.create({ title: 'x' })
   const valid = await model.create({ title: 'valid' })
   await model.flushQueue()
   assert.equal((await model.getStatus()).failed, 3)
   assert.equal((await model.getStatus()).pending, 1)
   assert.equal((await server.query('SELECT title FROM todos WHERE uid = $1', [valid.uid])).rows[0].title, 'valid')
   await model.update(duplicate.uid, { title: 'repaired' })
   await model.flushQueue()
   assert.equal((await model.getStatus()).failed, 2)
   assert.equal((await server.query('SELECT title FROM todos WHERE uid = $1', [duplicate.uid])).rows[0].title, 'repaired')
})

test('serialized validation and data errors fail their rows while transient errors remain retryable', async t => {
   const { local, server, services } = await syncFixture(t,
      'CREATE TABLE todos (uid UUID PRIMARY KEY, qty INTEGER)', ['todos'])
   const errors = []
   const requests = []
   let transient = false
   const socket = {
      on() {},
      timeout() { return this },
      async emitWithAck(_event, { name, action, args }) {
         requests.push(args[0])
         let envelope
         try {
            if (transient) throw Object.assign(new Error('serialization failure'), { code: '40001' })
            envelope = { result: await services.get(name)[action](...args) }
         } catch (error) {
            // Match Express-X's server envelope: Error prototypes are not sent.
            envelope = { error: { code: error.code || 'unknown-error', message: error.message, stack: error.stack } }
         }
         return JSON.parse(JSON.stringify(envelope))
      },
   }
   const app = createClient(socket)
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('todos', { localDb: local, onError: error => errors.push(error) })
   await model.prepare()
   await model.create({ qty: 1, 'invalid-column': true })
   const invalidValue = await model.create({ qty: 'bad' })
   await model.create({ qty: '2147483648' })
   const valid = await model.create({ qty: 7 })
   await model.flushQueue()
   assert.deepEqual(errors.map(error => error.code), ['invalid-argument', '22P02', '22003'])
   assert.ok(errors.every(error => !(error instanceof Error)))
   assert.equal((await model.getStatus()).failed, 3)
   assert.equal((await model.getStatus()).pending, 1)
   assert.equal((await server.query('SELECT qty FROM todos WHERE uid = $1', [valid.uid])).rows[0].qty, 7)
   await model.flushQueue()
   assert.equal(requests.length, 4)
   await model.update(invalidValue.uid, { qty: 5 })
   await model.flushQueue()
   assert.equal((await model.getStatus()).failed, 2)
   assert.equal((await server.query('SELECT qty FROM todos WHERE uid = $1', [invalidValue.uid])).rows[0].qty, 5)

   const retryable = await model.create({ qty: 8 })
   const following = await model.create({ qty: 9 })
   const before = requests.length
   transient = true
   await model.flushQueue()
   assert.equal(requests.length, before + 1)
   assert.equal(requests.at(-1), retryable.uid)
   assert.equal((await model.getStatus()).failed, 2)
   transient = false
   await model.flushQueue()
   assert.deepEqual(requests.slice(before), [retryable.uid, retryable.uid, following.uid])
   assert.equal((await server.query('SELECT count(*)::int AS count FROM todos')).rows[0].count, 4)
})

test('an edited colliding create with a lost rejection never overwrites the existing row', async t => {
   const { app, local, server, services } = await syncFixture(t,
      'CREATE TABLE todos (uid UUID PRIMARY KEY, title TEXT)', ['todos'])
   const id = crypto.randomUUID()
   const service = services.get('todos')
   const original = await service.create(id, { title: 'original' }, { clientId: crypto.randomUUID(), revision: '1' })
   const create = service.create
   let loseResponse = true
   service.create = async (...args) => {
      try { return await create(...args) } catch (error) {
         if (loseResponse) { loseResponse = false; throw new Error('response lost') }
         throw error
      }
   }
   const model = app.createElectricModel('todos', { localDb: local })
   await model.prepare()
   await model.create({ uid: id, title: 'collision' })
   await model.flushQueue()
   await model.update(id, { title: 'edited collision' })
   await model.flushQueue()
   assert.equal((await model.getStatus()).failed, 1)
   assert.deepEqual((await server.query('SELECT * FROM todos WHERE uid = $1', [id])).rows, [original])
   assert.equal((await server.query('SELECT * FROM electric_mutation_cursor')).rows.length, 1)
})

test('stop ends an explicit flush after its current request and idle waits for its acknowledgement', async t => {
   const db = new PGlite()
   let release
   let entered
   const gate = new Promise(resolve => { release = resolve })
   const sending = new Promise(resolve => { entered = resolve })
   const calls = []
   const app = { service: () => ({ async create(id, data) {
      calls.push(id)
      if (calls.length === 1) { entered(); await gate }
      return { uid: id, ...data, version: String(calls.length) }
   } }) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const network = { onLine: false }
   const model = app.createElectricModel('todos', { localDb: db, network, retryMs: 60_000 })
   t.after(async () => { release(); model.stop(); await model.waitForIdle(); await db.close() })
   await model.prepare()
   const first = await model.create({ title: 'first' })
   const second = await model.create({ title: 'second' })
   model.start()
   network.onLine = true
   const flush = model.flushQueue()
   await sending
   assert.equal(model.flushQueue(), flush)
   let idle = false
   const drained = model.waitForIdle().then(() => { idle = true })
   await Promise.resolve()
   assert.equal(idle, false)
   model.stop()
   assert.equal(Stream.current.options.signal.aborted, true)
   release()
   await flush
   await drained
   assert.deepEqual(calls, [first.uid])
   const pending = await db.query('SELECT row_id FROM electric_mutation_queue WHERE acknowledged_version IS NULL')
   assert.deepEqual(pending.rows, [{ row_id: second.uid }])
   model.start()
   assert.equal(Stream.current.options.signal.aborted, false)
   await model.waitForIdle()
   assert.deepEqual(calls, [first.uid, second.uid])
})

test('network TypeErrors keep queued mutations retryable', async t => {
   const db = new PGlite()
   t.after(() => db.close())
   let failing = true
   const calls = []
   const app = { service: () => ({ async create(id, data) {
      calls.push(id)
      if (failing) throw new TypeError('Failed to fetch')
      return { uid: id, ...data, version: '1' }
   } }) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('todos', { localDb: db })
   await model.prepare()
   const first = await model.create({ title: 'first' })
   const second = await model.create({ title: 'second' })
   await model.flushQueue()
   assert.deepEqual(calls, [first.uid])
   assert.equal((await model.getStatus()).failed, 0)
   failing = false
   await model.flushQueue()
   assert.deepEqual(calls, [first.uid, first.uid, second.uid])
   assert.equal((await db.query('SELECT count(*)::int AS count FROM electric_mutation_queue WHERE acknowledged_version IS NULL')).rows[0].count, 0)
})

test('a Shape error cannot erase an existing offline cache or mark it connected', async t => {
   const db = new PGlite()
   class ErrorStream extends Stream {
      listeners = []
      subscribe(_messages, onError) { this.listeners.push(onError); return () => {} }
      fail(error) { for (const callback of this.listeners) callback?.(error) }
   }
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: ErrorStream, Shape: ElectricShape })
   const model = app.createElectricModel('todos', { localDb: db, retryMs: 60_000 })
   t.after(async () => { model.stop(); await model.waitForIdle(); await db.close() })
   await model.prepare()
   const cached = { uid: crypto.randomUUID(), title: 'saved offline', version: '1', deleted: false }
   await db.query('INSERT INTO electric_local_rows (model_name, row_id, data) VALUES ($1, $2, $3)', ['todos', cached.uid, cached])
   model.start()
   Stream.current.fail(new FetchError(503, 'unavailable', undefined, {}, 'http://localhost/shape'))
   await model.waitForIdle()
   assert.deepEqual(await model.findMany(), [cached])
   assert.equal((await model.getStatus()).online, false)
})

test('offline writes stay local until the acknowledged Electric version arrives', async () => {
   const db = new PGlite()
   const calls = []
   const service = {
      async create(id, data, metadata) {
         calls.push({ id, data, metadata })
         return { id, ...data, version: '42' }
      },
      async delete(id, metadata) {
         calls.push({ id, metadata, action: 'delete' })
         return { id, deleted: true, version: '43' }
      },
   }
   const app = { service: () => service }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('todos', { primaryKey: 'id', localDb: db, network: { onLine: false }, retryMs: 60_000 })
   try {
      await model.prepare()
      const local = await model.create({ title: 'offline' })
      assert.match(local.id, /^[0-9a-f-]{36}$/)
      assert.equal((await model.findMany())[0].title, 'offline')
      assert.equal((await model.getStatus()).pending, 1)
      assert.equal(calls.length, 0)

      // Simulate reconnect, then an old Shape snapshot followed by confirmation.
      const onlineModel = app.createElectricModel('todos', { primaryKey: 'id', localDb: db, network: { onLine: true }, retryMs: 60_000 })
      await onlineModel.flushQueue()
      assert.equal(calls.length, 1)
      assert.equal(calls[0].metadata.revision, '1')
      assert.equal((await onlineModel.getStatus()).pending, 1)
      onlineModel.start()
      Shape.current.emit({ rows: [{ id: local.id, title: 'older', version: '41', deleted: false }] })
      await new Promise(resolve => setTimeout(resolve, 10))
      assert.equal((await onlineModel.findMany())[0].title, 'offline')
      Shape.current.emit({ rows: [{ id: local.id, title: 'offline', version: '42', deleted: false }] })
      await new Promise(resolve => setTimeout(resolve, 10))
      assert.equal((await onlineModel.getStatus()).pending, 0)
      await onlineModel.remove(local.id)
      await onlineModel.flushQueue()
      assert.equal((await onlineModel.findMany()).length, 0)
      Shape.current.emit({ rows: [{ id: local.id, title: 'offline', version: '42', deleted: false }] })
      await new Promise(resolve => setTimeout(resolve, 10))
      assert.equal((await onlineModel.findMany()).length, 0)
      Shape.current.emit({ rows: [{ id: local.id, title: '', version: '43', deleted: true }] })
      await new Promise(resolve => setTimeout(resolve, 10))
      assert.equal((await onlineModel.getStatus()).pending, 0)
      onlineModel.stop()
   } finally {
      await db.close()
   }
})

test('BIGINT filters stay numeric and lossless across snapshots, local edits, and reopen', async t => {
   const db = new PGlite()
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('rows', { localDb: db, network: { onLine: false }, retryMs: 60_000 })
   t.after(async () => { model.stop(); await model.waitForIdle(); await db.close() })
   await model.prepare()
   model.start()
   const small = { uid: crypto.randomUUID(), quantity: 10n, code: '10', version: 1n, deleted: false }
   const huge = { uid: crypto.randomUUID(), quantity: 9007199254740993n, code: '2', version: 2n, deleted: false }
   const negative = { uid: crypto.randomUUID(), quantity: -3n, code: '3', version: 3n, deleted: false }
   Shape.current.emit({ rows: [small, huge, negative] })
   await model.waitForIdle()
   const ids = async where => (await model.findMany(where)).map(row => row.uid).sort()
   assert.deepEqual(await ids({ quantity: 10 }), [small.uid])
   assert.deepEqual(await ids({ quantity: { gt: '2' } }), [small.uid, huge.uid].sort())
   assert.deepEqual(await ids({ quantity: { gt: 9007199254740992n } }), [huge.uid])
   assert.deepEqual(await ids({ quantity: '9007199254740993' }), [huge.uid])
   assert.deepEqual(await ids({ quantity: { lt: '-2' } }), [negative.uid])
   // A text field containing digits must retain PostgreSQL text ordering.
   assert.deepEqual(await ids({ code: { gt: '2' } }), [negative.uid])
   await assert.rejects(model.findMany({ quantity: { gt: '2.5' } }), /BIGINT filters require integer/)
   await model.update(small.uid, { quantity: '12' })
   assert.deepEqual(await ids({ quantity: 12n }), [small.uid])
   const reopened = app.createElectricModel('rows', { localDb: db })
   await reopened.prepare()
   assert.equal((await reopened.findMany({ quantity: 12 }))[0].quantity, '12')
   const created = await reopened.create({ quantity: 20n })
   assert.equal((await reopened.findMany({ quantity: 20 }))[0].uid, created.uid)
})

test('nullable snapshots retain known BIGINT types for existing and newly appearing rows', async t => {
   const db = new PGlite()
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('rows', { localDb: db, network: { onLine: false }, retryMs: 60_000 })
   t.after(async () => { model.stop(); await model.waitForIdle(); await db.close() })
   await model.prepare()
   model.start()
   const first = { uid: crypto.randomUUID(), quantity: 10n, code: '10', version: 1n, deleted: false }
   Shape.current.emit({ rows: [first] })
   await model.waitForIdle()
   const second = { uid: crypto.randomUUID(), quantity: null, code: null, version: 3n, deleted: false }
   Shape.current.emit({ rows: [{ ...first, quantity: null, code: null, version: 2n }, second] })
   await model.waitForIdle()
   await model.update(first.uid, { quantity: '12', code: '10' })
   await model.update(second.uid, { quantity: '13', code: '10' })
   assert.equal((await model.findMany({ quantity: 12 }))[0].uid, first.uid)
   assert.equal((await model.findMany({ quantity: 13n }))[0].uid, second.uid)
   assert.equal((await model.findMany({ quantity: { gt: '2' } })).length, 2)
   assert.deepEqual(await model.findMany({ code: { gt: '2' } }), [])
})

test('preparing an older cache adds BIGINT type metadata without deleting rows', async t => {
   const db = new PGlite()
   let model
   t.after(async () => { model?.stop(); await model?.waitForIdle(); await db.close() })
   await db.exec(`CREATE TABLE electric_local_rows (
      model_name TEXT NOT NULL, row_id TEXT NOT NULL, data JSONB NOT NULL,
      PRIMARY KEY (model_name, row_id))`)
   const row = { uid: crypto.randomUUID(), quantity: '10', version: '1', deleted: false }
   await db.query('INSERT INTO electric_local_rows VALUES ($1, $2, $3)', ['rows', row.uid, row])
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   model = app.createElectricModel('rows', { localDb: db, network: { onLine: false }, retryMs: 60_000 })
   await model.prepare()
   assert.deepEqual(await model.findMany(), [row])
   assert.deepEqual((await db.query('SELECT bigint_columns FROM electric_local_rows')).rows, [{ bigint_columns: [] }])
   model.start()
   Shape.current.emit({ rows: [{ ...row, quantity: 10n, version: 1n }] })
   await model.waitForIdle()
   assert.deepEqual(await model.findMany({ quantity: 10 }), [row])
})

test('large snapshots use three queries and preserve bigint versions and tombstones', async () => {
   const db = new PGlite()
   const { createOfflineElectricModel } = await import('../src/electric-offline-model.mjs')
   let queries = 0
   const countedDb = {
      exec: (...args) => db.exec(...args),
      query: (...args) => db.query(...args),
      transaction: callback => db.transaction(tx => callback({
         query: (...args) => { queries++; return tx.query(...args) },
      })),
   }
   const model = createOfflineElectricModel({
      db: countedDb, service: {}, modelName: 'range', primaryKey: 'uid',
      ShapeStreamClass: Stream, ShapeClass: Shape, ownsSync: true,
      network: { onLine: true }, retryMs: 60_000,
   })
   try {
      await model.prepare()
      model.start()
      await model.waitForIdle()
      const rows = Array.from({ length: 100 }, () => ({
         uid: crypto.randomUUID(), label: 'shared', version: '9007199254740993', deleted: false,
      }))
      Shape.current.emit({ rows })
      await model.waitForIdle()
      assert.equal(queries, 3)
      assert.equal((await model.findMany()).length, 100)
      assert.equal((await model.findMany())[0].version, '9007199254740993')
      queries = 0
      Shape.current.emit({ rows: [{ ...rows[0], deleted: true }, { ...rows[1], label: 'changed' }] })
      await model.waitForIdle()
      assert.equal(queries, 3)
      assert.deepEqual(await model.findMany(), [{ ...rows[1], label: 'changed' }])
   } finally {
      model.stop()
      await model.waitForIdle()
      await db.close()
   }
})

test('a running flush pauses remaining mutations when the network goes offline', async () => {
   const db = new PGlite()
   const network = { onLine: true }
   const calls = []
   let finish
   const app = { service: () => ({ async create(id) {
      calls.push(id)
      if (calls.length === 1) await new Promise(resolve => { finish = resolve })
      return { version: String(calls.length) }
   } }) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('todos', { primaryKey: 'id', localDb: db, network })
   try {
      await model.prepare()
      await model.create({ title: 'first' })
      await model.create({ title: 'second' })
      const flushing = model.flushQueue()
      while (!finish) await new Promise(resolve => setTimeout(resolve, 1))
      network.onLine = false
      finish()
      await flushing
      assert.equal(calls.length, 1)
      network.onLine = true
      await model.flushQueue()
      assert.equal(calls.length, 2)
   } finally {
      model.stop()
      await model.waitForIdle()
      await db.close()
   }
})

test('stop cancels a manual flush even before the Shape stream has started', async () => {
   const db = new PGlite()
   const calls = []
   let release
   const app = { service: () => ({ async create(id) {
      calls.push(id)
      if (calls.length === 1) await new Promise(resolve => { release = resolve })
      return { version: String(calls.length) }
   } }) }
   electricClientPlugin(app, { ShapeStream: Stream, Shape })
   const model = app.createElectricModel('todos', { primaryKey: 'id', localDb: db })
   try {
      await model.prepare()
      await model.create({ title: 'first' })
      await model.create({ title: 'second' })
      const flushing = model.flushQueue()
      while (!release) await new Promise(resolve => setTimeout(resolve, 1))
      model.stop()
      release()
      await flushing
      await model.waitForIdle()
      assert.equal(calls.length, 1)
      assert.equal((await db.query('SELECT count(*)::int AS count FROM electric_mutation_queue WHERE acknowledged_version IS NULL')).rows[0].count, 1)
      await model.flushQueue()
      assert.equal(calls.length, 2)
   } finally {
      release?.()
      model.stop()
      await model.waitForIdle()
      await db.close()
   }
})
