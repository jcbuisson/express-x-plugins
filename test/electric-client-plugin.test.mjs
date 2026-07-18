import assert from 'node:assert/strict'
import test from 'node:test'

import { electricClientPlugin, whereToElectricParams } from '../src/electric-client-plugin.mjs'

class FakeStream {
   static instances = []
   constructor(options) {
      this.options = options
      FakeStream.instances.push(this)
   }
}

class FakeShape {
   constructor(stream) { this.stream = stream }
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
