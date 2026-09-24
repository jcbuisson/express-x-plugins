import assert from 'node:assert/strict'
import test from 'node:test'

import { electricServerPlugin, prepareElectricSyncSchema } from '../src/electric-server-plugin.mjs'

function fixture({ authorize = async () => true, fetch } = {}) {
   const services = new Map()
   const routes = new Map()
   const queries = []
   const db = {
      async query(sql, values = []) {
         queries.push({ sql, values })
         return { rows: [{ id: values.at(-1) ?? 'one', label: 'row' }] }
      },
   }
   const app = {
      createService(name, methods) { services.set(name, methods) },
      get(path, handler) { routes.set(path, handler) },
   }
   const registration = electricServerPlugin(app, db, ['todos'], { authorize, fetch: fetch ?? globalThis.fetch })
   return { app, db, services, routes, queries, registration }
}

test('registers the mutation API', async () => {
   const { services, queries } = fixture()
   const service = services.get('todos')
   assert.deepEqual(Object.keys(service), [
      'findUnique', 'findMany', 'create', 'update', 'delete',
   ])

   const value = await service.create.call({}, 'one', { label: 'new' })
   assert.equal(value.id, 'one')

   await service.create.call({}, 'only-id', {})
   assert.match(queries[1].sql, /DO UPDATE SET "id" = EXCLUDED\."id"/)
   assert.equal(queries[1].values[0], 'only-id')
   assert.deepEqual(fixture().registration.models, [
      { name: 'todos', table: 'todos', primaryKey: 'id' },
   ])
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
   electricServerPlugin(app, db, [{ name: 'people', primaryKey: 'id' }], {
      authorize: async () => true,
   })

   await services.get('people').update.call({}, 7, { id: 99, uid: 'editable', name: 'Ada' })

   assert.equal(
      queries[0].sql,
      'UPDATE "people" SET "uid" = $1, "name" = $2 WHERE "id" = $3 RETURNING *',
   )
   assert.deepEqual(queries[0].values, ['editable', 'Ada', 7])
})

test('accepts Date values in server-side filters', async () => {
   const { services, queries } = fixture()
   const createdAt = new Date('2026-08-01T12:00:00.000Z')

   await services.get('todos').findUnique.call({}, { createdAt })

   assert.equal(
      queries[0].sql,
      'SELECT * FROM "todos" WHERE "createdAt" = $1 LIMIT 1',
   )
   assert.deepEqual(queries[0].values, [createdAt])
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
   electricServerPlugin(app, db, [{ name: 'todo', table: 'todos' }], {
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
      () => electricServerPlugin(
         { createService() {}, get() {} },
         { query() {} },
         ['todos; DROP TABLE users'],
         { authorize: async () => true, fetch: async () => {} },
      ),
      /simple SQL identifier/,
   )
})

test('sync mode commits a versioned write and rejects replayed or stale revisions', async () => {
   const queries = []
   let cursor = { revision: '0', result: null }
   let inserts = 0
   const client = {
      async query(sql, values = []) {
         queries.push({ sql, values })
         if (sql.includes('SELECT revision, result FROM electric_mutation_cursor')) return { rows: [cursor] }
         if (sql.includes('INSERT INTO "todos"')) {
            inserts++
            return { rows: [{ id: values.at(-1), version: '42', deleted: false }] }
         }
         if (sql.includes('UPDATE electric_mutation_cursor')) cursor = { revision: values[3], result: JSON.parse(values[4]) }
         return { rows: [] }
      },
      release() {},
   }
   const db = { query: client.query.bind(client), async connect() { return client } }
   const services = new Map()
   electricServerPlugin({ createService(name, methods) { services.set(name, methods) }, get() {} }, db,
      [{ name: 'todos', tombstoneData: { title: '' } }], { sync: true })
   const service = services.get('todos')
   const id = '64d76168-775f-481e-8974-18d31d835d9e'
   const clientId = '5e199e1c-23a4-473e-9989-14d4a1fb857e'
   const metadata = { clientId, revision: '2' }

   const result = await service.create.call({}, id, { title: 'first' }, metadata)
   assert.equal(result.version, '42')
   assert.equal((await service.create.call({}, id, { title: 'first' }, metadata)).version, '42')
   assert.equal((await service.create.call({}, id, { title: 'stale' }, { clientId, revision: '1' })).version, '42')
   assert.equal(inserts, 1)
   assert.equal(queries.filter(({ sql }) => sql === 'COMMIT').length, 3)
   await assert.rejects(service.create.call({}, id, { title: 'bad' }), /metadata is required/)
})

test('sync schema prepares version and tombstone columns', async () => {
   const queries = []
   await prepareElectricSyncSchema({ query: async sql => { queries.push(sql) } }, ['todos'])
   assert.equal(queries.length, 4)
   assert.match(queries[2], /ADD COLUMN IF NOT EXISTS version BIGINT/)
   assert.match(queries[3], /ADD COLUMN IF NOT EXISTS deleted BOOLEAN/)
})

test('sync delete writes a versioned tombstone and clears configured fields', async () => {
   const queries = []
   const id = '64d76168-775f-481e-8974-18d31d835d9e'
   const client = {
      async query(sql, values = []) {
         queries.push({ sql, values })
         if (sql.includes('SELECT revision, result FROM electric_mutation_cursor')) return { rows: [{ revision: '0', result: null }] }
         if (sql.includes('INSERT INTO "todos"')) return { rows: [{ id, title: '', deleted: true, version: '7' }] }
         return { rows: [] }
      },
      release() {},
   }
   const db = { query: client.query.bind(client), async connect() { return client } }
   const services = new Map()
   electricServerPlugin({ createService(name, methods) { services.set(name, methods) }, get() {} }, db,
      [{ name: 'todos', tombstoneData: { title: '' } }], { sync: true })
   const result = await services.get('todos').delete.call({}, id, {
      clientId: '5e199e1c-23a4-473e-9989-14d4a1fb857e', revision: '1',
   })
   assert.equal(result.deleted, true)
   const tombstone = queries.find(({ sql }) => sql.includes('INSERT INTO "todos"'))
   assert.match(tombstone.sql, /"title" = EXCLUDED\."title"/)
   assert.match(tombstone.sql, /version = nextval/)
   assert.deepEqual(tombstone.values, [id, ''])
})
