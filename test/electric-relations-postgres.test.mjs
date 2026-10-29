import assert from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { electricServerPlugin, prepareElectricSyncSchema } from '../src/electric-server-plugin.mjs'

const connectionString = process.env.ELECTRIC_TEST_DATABASE_URL

function register(db, models) {
   const services = new Map()
   electricServerPlugin({ createService(name, methods) { services.set(name, methods) }, get() {} }, db, models, { sync: true })
   return services
}

function commitGate(pool) {
   let reached
   let release
   const waiting = new Promise(resolve => { reached = resolve })
   const released = new Promise(resolve => { release = resolve })
   return {
      waiting, release,
      db: {
         query: (...args) => pool.query(...args),
         async connect() {
            const client = await pool.connect()
            return {
               async query(sql, values) {
                  if (sql === 'COMMIT') { reached(); await released }
                  return client.query(sql, values)
               },
               release: () => client.release(),
            }
         },
      },
   }
}

test('PostgreSQL serializes concurrent membership writes and soft deletes across plugin instances', {
   skip: !connectionString,
   timeout: 30_000,
}, async t => {
   const { Pool } = await import('pg')
   const schema = `electric_sync_test_${crypto.randomUUID().replaceAll('-', '')}`
   const admin = new Pool({ connectionString })
   let pool
   t.after(async () => {
      try {
         await pool?.end()
         await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      } finally { await admin.end() }
   })
   await admin.query(`CREATE SCHEMA "${schema}"`)
   pool = new Pool({ connectionString, options: `-c search_path=${schema}`, statement_timeout: 10_000 })
   await pool.query(`
      CREATE TABLE people (uid UUID PRIMARY KEY);
      CREATE TABLE groups (uid UUID PRIMARY KEY);
      CREATE TABLE membership (
         uid UUID PRIMARY KEY,
         user_uid UUID REFERENCES people(uid) ON DELETE CASCADE,
         group_uid UUID REFERENCES groups(uid) ON DELETE CASCADE
      );
   `)
   const models = [
      { name: 'user', table: 'people' },
      { name: 'group', table: 'groups' },
      { name: 'membership', tombstoneData: { user_uid: null, group_uid: null } },
      { name: 'user_alias', table: 'people' },
      { name: 'group_alias', table: 'groups' },
      { name: 'membership_alias', table: 'membership', tombstoneData: { user_uid: null, group_uid: null } },
   ]
   await prepareElectricSyncSchema(pool, models)
   const services = register(pool, models)
   const metadata = () => ({ clientId: crypto.randomUUID(), revision: '1' })

   for (const first of ['delete', 'create']) {
      await t.test(`${first} reaches the server first`, async () => {
         const user = crypto.randomUUID()
         const group = crypto.randomUUID()
         const membership = crypto.randomUUID()
         await services.get('user').create(user, {}, metadata())
         await services.get('group').create(group, {}, metadata())
         const gate = commitGate(pool)
         const gated = register(gate.db, models)
         const create = service => service.get('membership').create(membership, { user_uid: user, group_uid: group }, metadata())
         const remove = service => service.get('group').delete(group, metadata())
         const firstWrite = first === 'delete' ? remove(gated) : create(gated)
         let secondWrite
         try {
            // Keep the first transaction open after its mutation. The second
            // must wait for the same advisory lock on another connection.
            await Promise.race([gate.waiting, firstWrite.then(() => { throw new Error('commit gate was not reached') })])
            secondWrite = first === 'delete' ? create(services) : remove(services)
            let blocked = false
            for (let attempt = 0; attempt < 100; attempt++) {
               const { rows } = await pool.query(`SELECT EXISTS (
                  SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
                     AND classid = 178693771 AND objid = 1
                     AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
               ) AS blocked`)
               if (rows[0].blocked) { blocked = true; break }
               await delay(10)
            }
            assert.equal(blocked, true, 'the second mutation must block until the first commits')
         } finally {
            gate.release()
            await Promise.allSettled([firstWrite, secondWrite])
         }
         const results = await Promise.all([firstWrite, secondWrite])
         if (first === 'delete') assert.equal(results[1].syncRejection.code, 'sync-parent-deleted')
         const row = (await pool.query('SELECT * FROM membership WHERE uid = $1', [membership])).rows[0]
         assert.equal(row.deleted, true)
         assert.equal(row.user_uid, null)
         assert.equal(row.group_uid, null)
         assert.equal((await pool.query('SELECT deleted FROM groups WHERE uid = $1', [group])).rows[0].deleted, true)
      })
   }
})
