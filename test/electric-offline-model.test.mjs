import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'
import { electricServerPlugin, prepareElectricSyncSchema } from '../src/electric-server-plugin.mjs'

class Stream {
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
      await service.create(row.id, { title: 'collision' }, { clientId: crypto.randomUUID(), revision: '1' })
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
