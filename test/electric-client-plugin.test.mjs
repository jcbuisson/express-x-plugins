import assert from 'node:assert/strict'
import test from 'node:test'
import { effectScope, isRef } from 'vue'

import { electricClientPlugin, whereToElectricParams } from '../src/electric-client-plugin.mjs'

class FakeStream {
   static instances = []
   constructor(options) {
      this.options = options
      FakeStream.instances.push(this)
   }
}

class FakeShape {
   constructor(stream) { this.stream = stream; stream.shape = this }
   subscribe(callback) {
      callback({ rows: [{ uid: 'one', completed: false }] })
      return () => { this.unsubscribed = true }
   }
}

test('translates where objects into parameterized Electric filters', () => {
   assert.deepEqual(whereToElectricParams({ completed: false, priority: { gte: 2, lt: 5 }, owner: null }), {
      where: '"completed" = $1 AND "priority" >= $2 AND "priority" < $3 AND "owner" IS NULL',
      params: ['false', '2', '5'],
   })
   assert.deepEqual(whereToElectricParams({}), {})
   assert.throws(() => whereToElectricParams({ 'bad;drop': 1 }), /simple SQL identifier/)
})

test('getObservable emits Shape rows and cleans up its subscription', () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')
   let rows
   const subscription = todo.getObservable({ completed: false }).subscribe(value => { rows = value })

   assert.deepEqual(rows, [{ uid: 'one', completed: false }])
   assert.equal(FakeStream.instances.at(-1).options.url, '/electric/v1/shape/todos')
   assert.deepEqual(FakeStream.instances.at(-1).options.params, {
      where: '"completed" = $1', params: ['false'],
   })
   subscription.unsubscribe()
})

test('getVueRef returns Shape rows in a Vue ref and cleans up with its scope', () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')
   const scope = effectScope()
   let rows

   scope.run(() => { rows = todo.getVueRef({ completed: false }) })

   assert.equal(isRef(rows), true)
   assert.deepEqual(rows.value, [{ uid: 'one', completed: false }])
   const shape = FakeStream.instances.at(-1).shape
   assert.equal(shape.unsubscribed, undefined)
   scope.stop()
   assert.equal(shape.unsubscribed, true)
})

test('firstResult resolves with the first Shape rows and cleans up its subscription', async () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')

   const rows = await todo.firstResult({ completed: false })

   assert.deepEqual(rows, [{ uid: 'one', completed: false }])
   const stream = FakeStream.instances.at(-1)
   assert.deepEqual(stream.options.params, {
      where: '"completed" = $1', params: ['false'],
   })
   assert.equal(stream.shape.unsubscribed, true)
})

test('model mutations retain the simple Express-X API', async () => {
   const calls = []
   const service = {
      async createWithMeta(...args) { calls.push(['create', ...args]); return [{ uid: args[0], ...args[1] }, {}] },
      async updateWithMeta(...args) { calls.push(['update', ...args]); return [{ uid: args[0], ...args[1] }, {}] },
      async deleteWithMeta(...args) { calls.push(['remove', ...args]); return [{ uid: args[0] }, {}] },
   }
   const app = { service: () => service }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')

   const created = await todo.create({ title: 'Test' })
   await todo.update(created.uid, { completed: true })
   await todo.remove(created.uid)
   assert.deepEqual(calls.map(call => call[0]), ['create', 'update', 'remove'])
})
