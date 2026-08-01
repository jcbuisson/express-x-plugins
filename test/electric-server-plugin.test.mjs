import assert from 'node:assert/strict'
import test from 'node:test'

import { electricOfflinePlugin } from '../src/electric-server-plugin.mjs'

function fixture({ authorize = async () => true, fetch } = {}) {
   const services = new Map()
   const routes = new Map()
   const queries = []
   const db = {
      async query(sql, values = []) {
         queries.push({ sql, values })
         return { rows: [{ uid: values.at(-1) ?? 'one', label: 'row' }] }
      },
   }
   const app = {
      createService(name, methods) { services.set(name, methods) },
      get(path, handler) { routes.set(path, handler) },
   }
   const registration = electricOfflinePlugin(app, db, ['todos'], { authorize, fetch: fetch ?? globalThis.fetch })
   return { app, db, services, routes, queries, registration }
}

test('registers the mutation API', async () => {
   const { services, queries } = fixture()
   const service = services.get('todos')
   assert.deepEqual(Object.keys(service), [
      'create', 'update', 'delete',
   ])

   const value = await service.create.call({}, 'one', { label: 'new' })
   assert.equal(value.uid, 'one')

   await service.create.call({}, 'only-id', {})
   assert.match(queries[1].sql, /DO UPDATE SET "uid" = EXCLUDED\."uid"/)
   await assert.rejects(service.create.call({}, 'bad', []), /plain object/)
})

test('allows the database to generate a primary key', async () => {
   const { services, queries } = fixture()
   const service = services.get('todos')

   await service.create.call({}, { label: 'generated id' })
   assert.match(queries[0].sql, /^INSERT INTO "todos" \("label"\) VALUES \(\$1\) RETURNING \*$/)
   assert.doesNotMatch(queries[0].sql, /ON CONFLICT/)

   await service.create.call({}, {})
   assert.equal(queries[1].sql, 'INSERT INTO "todos" DEFAULT VALUES RETURNING *')
})

test('protects the configured primary key during updates', async () => {
   const services = new Map()
   const queries = []
   const db = {
      async query(sql, values = []) {
         queries.push({ sql, values })
         return { rows: [] }
      },
   }
   const app = {
      createService(name, methods) { services.set(name, methods) },
      get() {},
   }
   electricOfflinePlugin(app, db, [{ name: 'people', primaryKey: 'id' }], {
      authorize: async () => true,
   })

   await services.get('people').update.call({}, 7, { id: 99, uid: 'editable', name: 'Ada' })

   assert.equal(
      queries[0].sql,
      'UPDATE "people" SET "uid" = $1, "name" = $2 WHERE "id" = $3 RETURNING *',
   )
   assert.deepEqual(queries[0].values, ['editable', 'Ada', 7])
})

test('requires authorization and reports forbidden calls', async () => {
   const { services } = fixture({ authorize: async () => false })
   await assert.rejects(
      services.get('todos').create.call({}, 'one', { label: 'no' }),
      error => error.code === 'forbidden',
   )
})

test('shape proxy pins the configured table and keeps credentials server-side', async () => {
   let fetched
   const fetch = async (url) => {
      fetched = new URL(url)
      return new Response('[{"value":{"uid":"one"}}]', {
         status: 200,
         headers: { 'content-type': 'application/json', 'electric-handle': 'abc' },
      })
   }
   const services = new Map()
   const routes = new Map()
   const db = { query: async () => ({ rows: [] }) }
   const app = {
      createService(name, methods) { services.set(name, methods) },
      get(path, handler) { routes.set(path, handler) },
   }
   electricOfflinePlugin(app, db, [{ name: 'todo', table: 'todos' }], {
      authorize: async () => true,
      electricUrl: 'https://electric.example/v1/shape',
      sourceId: 'source',
      sourceSecret: 'secret',
      fetch,
   })
   const response = {
      headers: {},
      status(value) { this.statusCode = value; return this },
      setHeader(key, value) { this.headers[key] = value },
      send(value) { this.body = value },
      json(value) { this.body = value },
   }
   await routes.get('/electric/v1/shape/:model')({
      params: { model: 'todo' },
      query: { table: 'secrets', offset: '10' },
      get: () => 'application/json',
   }, response, error => { throw error })

   assert.equal(fetched.searchParams.get('table'), 'todos')
   assert.equal(fetched.searchParams.get('offset'), '10')
   assert.equal(fetched.searchParams.get('source_id'), 'source')
   assert.equal(fetched.searchParams.get('secret'), 'secret')
   assert.equal(response.statusCode, 200)
   assert.equal(response.headers['electric-handle'], 'abc')
})

test('rejects unsafe SQL identifiers', () => {
   assert.throws(
      () => electricOfflinePlugin(
         { createService() {}, get() {} },
         { query() {} },
         ['todos; DROP TABLE users'],
         { authorize: async () => true, fetch: async () => {} },
      ),
      /simple SQL identifier/,
   )
})
