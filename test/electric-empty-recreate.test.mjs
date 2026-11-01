import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'
import { electricServerPlugin, prepareElectricSyncSchema } from '../src/electric-server-plugin.mjs'

for (const requiredColumn of [false, true]) {
   test(`an ID-only replacement create is acknowledged with requiredColumn=${requiredColumn}`, async () => {
      const server = new PGlite()
      const local = new PGlite()
      try {
         await server.exec(`CREATE TABLE flags (uid UUID PRIMARY KEY${requiredColumn ? ', label TEXT NOT NULL' : ''})`)
         const models = [{ name: 'flags', tombstoneData: requiredColumn ? { label: '' } : {} }]
         await prepareElectricSyncSchema(server, models)
         let service
         const pool = { query: server.query.bind(server), async connect() {
            return { query: server.query.bind(server), release() {} }
         } }
         electricServerPlugin({ createService(_name, methods) { service = methods }, get() {} }, pool, models, { sync: true })
         const app = { service: () => service }
         electricClientPlugin(app)
         const model = app.createElectricModel('flags', { localDb: local })
         await model.prepare()
         const row = await model.create(requiredColumn ? { label: 'original' } : {})
         await model.flushQueue()
         await model.remove(row.uid)
         await model.create({ uid: row.uid })
         await model.flushQueue()
         assert.equal((await model.getStatus()).failed, 0)
         assert.equal((await local.query('SELECT count(*)::int AS count FROM electric_mutation_queue WHERE acknowledged_version IS NULL')).rows[0].count, 0)
         assert.equal((await server.query('SELECT deleted FROM flags')).rows[0].deleted, false)
         await assert.rejects(service.create(row.uid, requiredColumn ? { label: 'collision' } : {}, { clientId: crypto.randomUUID(), revision: '1' }), { code: 'sync-create-conflict' })

         // Once a delete actually reaches the server, recreating must still lose to its tombstone.
         await model.remove(row.uid)
         await model.flushQueue()
         await model.create({ uid: row.uid })
         await model.flushQueue()
         assert.deepEqual(await model.findMany(), [])
         assert.equal((await model.getStatus()).failed, 0)
         assert.equal((await server.query('SELECT deleted FROM flags')).rows[0].deleted, true)
      } finally { await local.close(); await server.close() }
   })
}
