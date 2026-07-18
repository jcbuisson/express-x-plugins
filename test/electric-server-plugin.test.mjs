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
         if (sql.startsWith('SELECT pg_current')) return { rows: [{ txid: '42' }] }
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

test('registers the familiar mutation API', async () => {
   const { services, queries } = fixture()
   const service = services.get('todos')
   assert.deepEqual(Object.keys(service), [
      'createWithMeta', 'updateWithMeta', 'deleteWithMeta',
   ])

   const [, meta] = await service.createWithMeta.call({}, 'one', { label: 'new' }, '2026-01-01T00:00:00Z')
   assert.equal(meta.uid, 'one')
   assert.equal(meta.txid, '42')
   assert.equal(meta.created_at, '2026-01-01T00:00:00.000Z')

   await service.createWithMeta.call({}, 'only-id', {}, '2026-01-01T00:00:00Z')
   assert.match(queries[2].sql, /DO UPDATE SET "uid" = EXCLUDED\."uid"/)
   await assert.rejects(service.createWithMeta.call({}, 'bad', [], new Date()), /plain object/)
})

test('requires authorization and reports forbidden calls', async () => {
   const { services } = fixture({ authorize: async () => false })
   await assert.rejects(
      services.get('todos').createWithMeta.call({}, 'one', { label: 'no' }, new Date()),
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
