import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

class Locks {
   queues = new Map()
   request(name, { signal }, callback) {
      return new Promise((resolve, reject) => {
         const queue = this.queues.get(name) ?? []
         this.queues.set(name, queue)
         const job = { callback, resolve, reject, active: false }
         signal.addEventListener('abort', () => {
            if (job.active) return
            const index = queue.indexOf(job)
            if (index !== -1) queue.splice(index, 1)
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))
         }, { once: true })
         queue.push(job)
         if (queue.length === 1) this.run(name)
      })
   }
   run(name) {
      const queue = this.queues.get(name)
      const job = queue[0]
      if (!job) return
      job.active = true
      Promise.resolve().then(job.callback).then(job.resolve, job.reject).finally(() => {
         queue.shift()
         this.run(name)
      })
   }
}
class Channel extends EventTarget {
   static instances = new Set()
   constructor(name) { super(); this.name = name; Channel.instances.add(this) }
   postMessage(data) {
      for (const channel of Channel.instances) {
         if (channel !== this && channel.name === this.name) channel.dispatchEvent(new MessageEvent('message', { data }))
      }
   }
   close() { Channel.instances.delete(this) }
}
class Stream {
   static instances = []
   constructor(options) { this.options = options; Stream.instances.push(this) }
   subscribe() { return () => {} }
   isConnected() { return true }
   unsubscribeAll() { this.stopped = true }
}
class Shape {
   constructor(stream) { stream.shape = this }
   subscribe(callback) { this.emit = callback; return () => {} }
}
async function until(predicate) {
   for (let i = 0; i < 100; i++) {
      if (await predicate()) return
      await new Promise(resolve => setTimeout(resolve, 5))
   }
   assert.fail('condition did not become true')
}
function configure(db, locks, service = {}, extra = {}) {
   const app = { service: () => service }
   electricClientPlugin(app, {
      sync: true, databaseName: 'test-db', localDb: db, locks,
      BroadcastChannel: Channel, ShapeStream: Stream, Shape, ...extra,
   })
   return app
}

test('managed models initialize automatically, share one database, and cache models', async () => {
   const db = new PGlite()
   let created = 0
   const locks = new Locks()
   const app = configure(undefined, locks, {}, { createDatabase: async ({ dataDir }) => {
      assert.equal(dataDir, 'idb://test-db'); created++; return db
   } })
   try {
      const users = app.createElectricModel('users', { primaryKey: 'uid' })
      const ranges = app.createElectricModel('ranges', { primaryKey: 'uid' })
      assert.equal(app.createElectricModel('users', { primaryKey: 'uid' }), users)
      assert.throws(() => app.createElectricModel('users'), /primaryKey/)
      const row = await users.create({ name: 'Works before prepare' })
      await ranges.prepare()
      assert.equal(created, 1)
      assert.equal((await users.findUnique({ uid: row.uid })).name, row.name)
   } finally {
      await app.disposeElectricSync()
   }
   assert.equal(db.closed, true)
})

test('only one tab streams and flushes; follower edits broadcast and ownership transfers', async () => {
   const db = new PGlite()
   const locks = new Locks()
   const calls = []
   const service = { async create(id, data) { calls.push(id); return { id, ...data, version: '42' } } }
   const first = configure(db, locks, service)
   const second = configure(db, locks, service)
   const before = Stream.instances.length
   let subscription
   try {
      const owner = first.createElectricModel('todos')
      const follower = second.createElectricModel('todos')
      await owner.prepare()
      await follower.prepare()
      await until(() => Stream.instances.length === before + 1)
      let observed = []
      subscription = owner.getObservable().subscribe(rows => { observed = rows })
      const row = await follower.create({ title: 'Written in follower' })
      await until(() => calls.length === 1 && observed.length === 1)
      assert.equal(observed[0].id, row.id)
      assert.equal(Stream.instances.length, before + 1)
      const stream = Stream.instances.at(-1)
      stream.shape.emit({ rows: [{ ...row, version: 42n, deleted: false }] })
      await until(async () => (await follower.getStatus()).pending === 0)
      assert.equal((await follower.findMany())[0].version, '42')
      await owner.dispose()
      await until(() => Stream.instances.length === before + 2)
      assert.equal(stream.stopped, true)
      const next = Stream.instances.at(-1)
      next.shape.emit({ rows: [{ ...row, version: 42n, deleted: false }] })
      await until(async () => (await follower.findMany()).length === 1)
      assert.equal(calls.length, 1)
   } finally {
      subscription?.unsubscribe()
      await first.disposeElectricSync()
      await second.disposeElectricSync()
      await db.close()
   }
})

test('stopping a waiting follower cancels takeover; restart can acquire ownership', async () => {
   const db = new PGlite()
   const locks = new Locks()
   const first = configure(db, locks)
   const second = configure(db, locks)
   const before = Stream.instances.length
   try {
      const owner = first.createElectricModel('todos')
      const follower = second.createElectricModel('todos')
      await owner.prepare(); await follower.prepare()
      await until(() => Stream.instances.length === before + 1)
      follower.stop()
      owner.stop()
      await new Promise(resolve => setTimeout(resolve, 10))
      assert.equal(Stream.instances.length, before + 1)
      await follower.start()
      await until(() => Stream.instances.length === before + 2)
   } finally {
      await first.disposeElectricSync(); await second.disposeElectricSync(); await db.close()
   }
})

test('initialization errors reject writes and observable reads and call onError', async () => {
   const error = new Error('Worker could not load assets')
   const reported = []
   const app = configure(undefined, new Locks(), {}, {
      createDatabase: async () => { throw error },
      onError: cause => reported.push(cause),
   })
   const model = app.createElectricModel('todos')
   await assert.rejects(model.create({ title: 'No silent failure' }), /load assets/)
   await new Promise(resolve => model.getObservable().subscribe({ error: cause => {
      assert.equal(cause, error); resolve()
   } }))
   assert.deepEqual(reported, [error])
   await assert.rejects(app.disposeElectricSync(), /load assets/)
})

test('sync mode validates browser capabilities and persistent database identity', () => {
   const app = { service: () => ({}) }
   assert.throws(() => electricClientPlugin(app, { sync: true }), /databaseName/)
   assert.throws(() => electricClientPlugin(app, { sync: true, databaseName: 'todos', locks: {} }), /Web Locks/)
})

test('default worker errors and timeouts reject preparation instead of hanging', async () => {
   for (const triggerError of [true, false]) {
      let terminated = false
      class Worker extends EventTarget {
         constructor() {
            super()
            if (triggerError) setTimeout(() => {
               const event = new Event('error')
               event.message = 'worker module not found'
               this.dispatchEvent(event)
            }, 10)
         }
         terminate() { terminated = true }
      }
      const app = configure(undefined, new Locks(), {}, { workerFactory: () => new Worker(), initTimeoutMs: 30 })
      const model = app.createElectricModel('todos')
      await assert.rejects(model.prepare(), triggerError ? /module not found/ : /timed out/)
      assert.equal(terminated, true)
      await assert.rejects(app.disposeElectricSync())
   }
})

test('snapshots and reads recover after a database leader changes', async () => {
   const db = new PGlite()
   let failSnapshot = false
   let failRead = false
   let snapshotRetries = 0
   const leaderError = () => Object.assign(new Error('Leader changed, pending operation in indeterminate state'), { name: 'LeaderChangedError' })
   const shared = {
      exec: db.exec.bind(db),
      async query(...args) {
         if (failRead) { failRead = false; throw leaderError() }
         return db.query(...args)
      },
      async transaction(callback) {
         if (failSnapshot) { failSnapshot = false; snapshotRetries++; throw leaderError() }
         return db.transaction(callback)
      },
   }
   const app = configure(shared, new Locks())
   const before = Stream.instances.length
   try {
      const model = app.createElectricModel('todos')
      await model.prepare()
      await until(() => Stream.instances.length === before + 1)
      failSnapshot = true
      const id = crypto.randomUUID()
      Stream.instances.at(-1).shape.emit({ rows: [{ id, title: 'After election', version: 99n, deleted: false }] })
      await until(async () => (await model.findMany()).length === 1)
      assert.equal(snapshotRetries, 1)
      failRead = true
      assert.equal((await model.findUnique({ id })).version, '99')
      assert.equal((await model.getStatus()).error, null)
   } finally { await app.disposeElectricSync(); await db.close() }
})

test('an indeterminate foreground mutation is not replayed automatically', async () => {
   const db = new PGlite()
   let writes = 0
   const app = { service: () => ({}) }
   const localDb = {
      exec: db.exec.bind(db), query: db.query.bind(db),
      async transaction(callback) {
         writes++
         await db.transaction(callback)
         throw Object.assign(new Error('Leader changed, pending operation in indeterminate state'), { name: 'LeaderChangedError' })
      },
   }
   electricClientPlugin(app)
   const model = app.createElectricModel('todos', { localDb, ownsSync: false })
   try {
      await model.prepare()
      await assert.rejects(model.create({ title: 'Committed but reply lost' }), /Leader changed/)
      assert.equal(writes, 1)
      assert.equal((await model.findMany()).length, 1)
      assert.equal((await model.getStatus()).pending, 1)
   } finally { await db.close() }
})
