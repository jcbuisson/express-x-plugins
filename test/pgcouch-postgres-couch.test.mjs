import test from 'node:test'
import assert from 'node:assert/strict'
import { PGlite } from '@electric-sql/pglite'
import { Pool } from 'pg'
import PouchDB from 'pouchdb'
import memory from 'pouchdb-adapter-memory'
import { pgcouchServerPlugin, pgcouchValidationDocument } from '../src/pgcouch-server-plugin.mjs'
import { pgcouchClientPlugin } from '../src/pgcouch-client-plugin.mjs'
PouchDB.plugin(memory)
const connectionString = process.env.PGCOUCH_TEST_DATABASE_URL
const couchURL = process.env.PGCOUCH_TEST_COUCH_URL

test('PGCouch: real PostgreSQL, CouchDB, offline replication and role enforcement', {
   skip: !connectionString, timeout: 60_000,
}, async t => {
   const suffix = crypto.randomUUID().replaceAll('-', '')
   const schema = `pgcouch_test_${suffix}`
   const admin = new Pool({ connectionString })
   const pool = new Pool({ connectionString, options: `-c search_path=${schema}` })
   const remote = couchURL ? new PouchDB(`${couchURL.replace(/\/$/, '')}/pgcouch_test_${suffix}`) : new PouchDB(`pgcouch_server_${suffix}`, { adapter: 'memory' })
   const local = new PouchDB(`pgcouch_client_${suffix}`, { adapter: 'memory' })
   const db = new PGlite()
   let server, client, second
   t.after(async () => {
      await client?.disposePGCouchSync(); await server?.dispose(); await second?.dispose()
      await db.close(); await local.destroy(); await remote.destroy()
      await pool.end()
      await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      await admin.end()
   })
   await admin.query(`CREATE SCHEMA "${schema}"`)
   const sql = `CREATE TABLE horses (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, last_examination_id INTEGER);
      CREATE TABLE examinations (id INTEGER PRIMARY KEY, horse_id INTEGER NOT NULL REFERENCES horses(id) DEFERRABLE INITIALLY DEFERRED);
      ALTER TABLE horses ADD FOREIGN KEY (last_examination_id) REFERENCES examinations(id) DEFERRABLE INITIALLY DEFERRED;`
   await pool.query(sql); await db.exec(sql)
   await remote.info()
   if (couchURL) await remote.put(pgcouchValidationDocument())
   const models = [{ name: 'horses', primaryKey: 'id' }, { name: 'examinations', primaryKey: 'id' }]
   const services = new Map()
   const app = { createService(name, methods) { services.set(name, methods) } }
   const errors = []
   server = pgcouchServerPlugin(app, pool, models, { couch: remote, retryMs: 60_000, onError: e => errors.push(e) })
   await server.ready
   client = pgcouchClientPlugin({}, { models, localDb: db, couch: local, autoStart: false })
   await client.pgcouchReady
   const horses = client.createElectricModel('horses')
   await horses.create(42, { name: 'Real server' })
   await client.pgcouchTransaction([
      { action: 'insert', table: 'examinations', id: 100, data: { horse_id: 42 } },
      { action: 'update', table: 'horses', id: 42, data: { last_examination_id: 100 } },
   ])
   await local.replicate.to(remote); await server.flushQueue()
   await local.replicate.from(remote); await horses.flushQueue()
   assert.equal((await horses.getStatus()).pending, 0)
   assert.equal((await pool.query('SELECT last_examination_id FROM horses WHERE id = 42')).rows[0].last_examination_id, 100)
   // Two server instances share the same SQL receipt lock and sequence.
   second = pgcouchServerPlugin({ createService() {} }, pool, models, { couch: remote, retryMs: 60_000, onError: e => errors.push(e) })
   await second.ready
   const operation = [{ action: 'insert', table: 'horses', id: 50, data: { name: 'Exactly once' } }]
   const results = await Promise.all([server.submit(operation, {}, 'transaction:once'), second.submit(operation, {}, 'transaction:once')])
   assert.deepEqual(results[0], results[1])
   assert.equal((await pool.query('SELECT count(*)::int AS n FROM horses WHERE id = 50')).rows[0].n, 1)
   await assert.rejects(second.submit([{ ...operation[0], data: { name: 'Reused id' } }], {}, 'transaction:once'), /reused/)
   assert.equal(errors.length, 1)
   assert.match(errors[0].message, /reused/)
   errors.length = 0
   if (!couchURL) return
   // An ordinary replication user can create proposals but cannot forge decisions.
   const base = new URL(couchURL)
   const auth = `Basic ${Buffer.from(`${decodeURIComponent(base.username)}:${decodeURIComponent(base.password)}`).toString('base64')}`
   base.username = ''; base.password = ''
   await fetch(new URL('/_users', base), { method: 'PUT', headers: { authorization: auth } })
   const user = `pgcouch_user_${suffix}`
   await fetch(new URL(`/_users/org.couchdb.user:${user}`, base), { method: 'PUT', headers: { authorization: auth, 'content-type': 'application/json' }, body: JSON.stringify({ _id: `org.couchdb.user:${user}`, name: user, password: 'test-password', roles: [], type: 'user' }) })
   t.after(async () => {
      const url = new URL(`/_users/org.couchdb.user:${user}`, base)
      const doc = await (await fetch(url, { headers: { authorization: auth } })).json()
      url.searchParams.set('rev', doc._rev)
      await fetch(url, { method: 'DELETE', headers: { authorization: auth } })
   })
   const usernameURL = new URL(`${base.href.replace(/\/$/, '')}/pgcouch_test_${suffix}`)
   usernameURL.username = user; usernameURL.password = 'test-password'
   // Explicit membership allows this user into this shared replication scope.
   await fetch(new URL(`/pgcouch_test_${suffix}/_security`, base), { method: 'PUT', headers: { authorization: auth, 'content-type': 'application/json' }, body: JSON.stringify({ admins: { names: [], roles: [] }, members: { names: [user], roles: [] } }) })
   const ordinary = new PouchDB(usernameURL.href)
   try {
      await assert.rejects(ordinary.put({ _id: 'result:forged', type: 'result', sequence: '999', snapshot: {} }), error => error.status === 403)
      await assert.rejects(ordinary.put({ ...(await remote.get('result:once')), status: 'rejected' }), error => error.status === 403)
      await ordinary.put({ _id: 'transaction:ordinary', type: 'transaction', status: 'pending', operations: [{ action: 'update', table: 'horses', id: 42, data: { name: 'Authenticated proposal' } }] })
      await server.flushQueue()
      assert.equal((await pool.query('SELECT name FROM horses WHERE id = 42')).rows[0].name, 'Authenticated proposal')
   } finally { await ordinary.close() }
   assert.deepEqual(errors, [])
})
