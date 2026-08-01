import assert from 'node:assert/strict'
import test from 'node:test'
import { effectScope, isRef } from 'vue'

import {
   DisposableShape,
   electricClientPlugin,
   whereToElectricParams,
} from '../src/electric-client-plugin.mjs'

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

class DeferredShape {
   constructor(stream) { this.stream = stream; stream.shape = this }
   subscribe(callback) {
      this.callback = callback
      return () => { this.unsubscribed = true }
   }
}

class EmptyShape {
   constructor(stream) { this.stream = stream; stream.shape = this }
   subscribe(callback) {
      callback({ rows: [] })
      return () => { this.unsubscribed = true }
   }
}

test('DisposableShape tears down its stream with its last subscriber', () => {
   const stream = {
      subscribe() { return () => {} },
      unsubscribeAll() { this.unsubscribed = true },
   }
   const shape = new DisposableShape(stream)
   const unsubscribe = shape.subscribe(() => {})

   unsubscribe()

   assert.equal(stream.unsubscribed, true)
})

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

test('findMany resolves with the first Shape rows and cleans up its subscription', async () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')

   const rows = await todo.findMany({ completed: false })

   assert.deepEqual(rows, [{ uid: 'one', completed: false }])
   const stream = FakeStream.instances.at(-1)
   assert.deepEqual(stream.options.params, {
      where: '"completed" = $1', params: ['false'],
   })
   assert.equal(stream.shape.unsubscribed, true)
})

test('findMany unsubscribes when its Vue scope is disposed before a result', async () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: DeferredShape })
   const todo = app.createElectricModel('todos')
   const scope = effectScope()
   let result

   scope.run(() => { result = todo.findMany({ completed: false }) })
   const rejection = assert.rejects(result, error => error.name === 'EmptyError')
   const shape = FakeStream.instances.at(-1).shape
   assert.equal(shape.unsubscribed, undefined)

   scope.stop()

   await rejection
   assert.equal(shape.unsubscribed, true)
})

test('findUnique resolves with the first matching row or null', async () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')

   assert.deepEqual(
      await todo.findUnique({ uid: 'one' }),
      { uid: 'one', completed: false },
   )
   assert.equal(FakeStream.instances.at(-1).shape.unsubscribed, true)

   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: EmptyShape })
   const emptyTodo = app.createElectricModel('todos')
   assert.equal(await emptyTodo.findUnique({ uid: 'missing' }), null)
   assert.equal(FakeStream.instances.at(-1).shape.unsubscribed, true)
})

test('model mutations retain the simple Express-X API', async () => {
   const calls = []
   const service = {
      async create(...args) { calls.push(['create', ...args]); return { uid: args[0], ...args[1] } },
      async update(...args) { calls.push(['update', ...args]); return { uid: args[0], ...args[1] } },
      async delete(...args) { calls.push(['remove', ...args]); return { uid: args[0] } },
   }
   const app = { service: () => service }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos')

   const created = await todo.create({ title: 'Test' })
   await todo.update(created.uid, { completed: true })
   await todo.remove(created.uid)
   assert.deepEqual(calls.map(call => call[0]), ['create', 'update', 'remove'])
})

test('model creation supports server-generated IDs', async () => {
   const calls = []
   const service = {
      async create(...args) { calls.push(args); return { id: 42, ...args[0] } },
   }
   const app = { service: () => service }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   const todo = app.createElectricModel('todos', { idGeneration: 'server' })

   assert.deepEqual(await todo.create({ title: 'Test' }), { id: 42, title: 'Test' })
   assert.deepEqual(calls, [[{ title: 'Test' }]])
})

test('rejects an unsupported ID generation strategy', () => {
   const app = { service: () => ({}) }
   electricClientPlugin(app, { ShapeStream: FakeStream, Shape: FakeShape })
   assert.throws(
      () => app.createElectricModel('todos', { idGeneration: 'database-ish' }),
      /idGeneration/,
   )
})
