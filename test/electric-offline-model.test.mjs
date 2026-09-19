import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

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
   const model = app.createElectricModel('todos', { localDb: db, network: { onLine: false }, retryMs: 60_000 })
   try {
      await model.prepare()
      const local = await model.create({ title: 'offline' })
      assert.match(local.id, /^[0-9a-f-]{36}$/)
      assert.equal((await model.findMany())[0].title, 'offline')
      assert.equal((await model.getStatus()).pending, 1)
      assert.equal(calls.length, 0)

      // Simulate reconnect, then an old Shape snapshot followed by confirmation.
      const onlineModel = app.createElectricModel('todos', { localDb: db, network: { onLine: true }, retryMs: 60_000 })
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
