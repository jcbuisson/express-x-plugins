import 'fake-indexeddb/auto'
import test from 'node:test'
import assert from 'node:assert/strict'
import { electricSqlServerPlugin } from '../src/electric-sql-server-plugin.mjs'
import { electricSqlClientPlugin } from '../src/electric-sql-client-plugin.mjs'

function server(authorize = () => true) {
   const state = { queries: [] }
   const app = {
      createService(name, methods) { state.service = methods },
      get(path, handler) { state.route = handler },
   }
   electricSqlServerPlugin(app, { async query(...args) { state.queries.push(args); return { rows: [{ id: 1 }] } } }, {
      tables: ['todos'], statements: { add: 'INSERT INTO todos (label) VALUES ($1) RETURNING *' },
      authorize, sourceSecret: 'server-secret',
      async fetch(url) { state.target = url; return new Response('[]', { headers: { 'electric-handle': 'abc' } }) },
   })
   return state
}

test('executes only registered SQL with bound values and caller context', async () => {
   const context = { user: 1 }
   const state = server((caller, operation) => caller === context && operation.name === 'add')
   const value = "'); DROP TABLE todos; --"
   assert.deepEqual(await state.service.query.call(context, 'add', [value]), [{ id: 1 }])
   assert.deepEqual(state.queries[0], ['INSERT INTO todos (label) VALUES ($1) RETURNING *', [value]])
   await assert.rejects(state.service.query.call(context, 'toString'), /unknown/)
   await assert.rejects(state.service.query.call({}, 'add', ['blocked']), { code: 'forbidden' })
   assert.equal(state.queries.length, 1)
})

test('shape proxy pins table and credentials and preserves SQL parameters', async () => {
   const state = server()
   const response = { status() { return this }, setHeader() {}, send(body) { this.body = body } }
   await state.route({ params: { table: 'todos' }, query: {
      table: 'private', secret: 'attacker', source_id: 'attacker', offset: '-1', where: 'id = $1', 'params[1]': '123',
   } }, response, error => { throw error })
   assert.equal(state.target.searchParams.get('table'), 'todos')
   assert.equal(state.target.searchParams.get('secret'), 'server-secret')
   assert.equal(state.target.searchParams.get('source_id'), null)
   assert.equal(state.target.searchParams.get('params[1]'), '123')
   assert.equal(response.body.toString(), '[]')
   const denied = server(() => false)
   let failure
   await denied.route({ params: { table: 'todos' }, query: {} }, response, error => { failure = error })
   assert.equal(failure.code, 'forbidden')
   assert.equal(denied.target, undefined)
})

test('client binds SQL filters and stops network work on unsubscribe or disposal', async () => {
   const streams = []
   class Stream {
      constructor(options) { this.options = options; streams.push(this) }
      unsubscribeAll() { this.stopped = true }
   }
   class Shape {
      constructor(stream) { this.stream = stream }
      subscribe(callback) { this.stream.emit = callback; return () => { this.stream.unsubscribed = true } }
   }
   const app = { service: () => ({ query: async (...args) => args }) }
   electricSqlClientPlugin(app, { persistence: false, ShapeStream: Stream, Shape })
   assert.deepEqual(await app.sql.query('add', ['hello']), ['add', ['hello']])
   let rows
   const stop = app.sql.watch('todos', { where: 'done = $1', params: [false] }, value => { rows = value })
   assert.deepEqual(streams[0].options.params, { where: 'done = $1', params: ['false'] })
   streams[0].emit({ rows: [{ id: 1 }] })
   assert.deepEqual(rows, [{ id: 1 }])
   stop()
   stop()
   assert.ok(streams[0].stopped && streams[0].unsubscribed && streams[0].options.signal.aborted)
   app.sql.watch('todos', () => {})
   assert.throws(() => app.sql.watch('todos', { params: [null] }, () => {}), /IS NULL/)
   await app.sql.dispose()
   assert.ok(streams[1].options.signal.aborted)
})

test('persistent watches restore filtered snapshots after disposal, including empty results', async () => {
   await import('fake-indexeddb/auto')
   const streams = []
   class Stream {
      constructor() { streams.push(this) }
      unsubscribeAll() {}
   }
   class Shape {
      constructor(stream) { this.stream = stream }
      subscribe(callback) { this.stream.emit = callback; return () => {} }
   }
   const databaseName = `sql-test-${crypto.randomUUID()}`
   function client() {
      const app = { service: () => ({ query: async () => [] }) }
      electricSqlClientPlugin(app, { databaseName, ShapeStream: Stream, Shape })
      return app.sql
   }
   const first = client()
   first.watch('todos', { where: 'done = $1', params: [false] }, () => {})
   const rows = [{ id: 1n, created: new Date('2026-01-01'), label: 'saved' }]
   streams.at(-1).emit({ rows })
   rows[0].label = 'mutated by caller'
   first.watch('todos', { where: 'done = $1', params: [true] }, () => {})
   streams.at(-1).emit({ rows: [] })
   await first.dispose()
   const second = client()
   const restored = await new Promise(resolve => second.watch('todos', {
      where: 'done = $1', params: [false],
   }, resolve))
   assert.deepEqual(restored, [{ id: 1n, created: new Date('2026-01-01'), label: 'saved' }])
   assert.deepEqual(await new Promise(resolve => second.watch('todos', {
      where: 'done = $1', params: [true],
   }, resolve)), [])
   // A fresh network snapshot must win against a pending IndexedDB read.
   const seen = []
   second.watch('todos', { where: 'done = $1', params: [false] }, value => seen.push(value))
   streams.at(-1).emit({ rows: [{ id: 2 }] })
   await second.dispose()
   assert.deepEqual(seen, [[{ id: 2 }]])
   assert.throws(() => second.watch('todos', () => {}), /disposed/)
   const third = client()
   await third.clearLocalData()
   let delivered = false
   const stop = third.watch('todos', () => { delivered = true })
   stop()
   await third.dispose()
   assert.equal(delivered, false)
})
