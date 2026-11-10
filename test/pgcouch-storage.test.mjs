import assert from 'node:assert/strict'
import test from 'node:test'
import { storageForRemote } from '../src/pgcouch-storage.mjs'
import { PGlite } from '@electric-sql/pglite'
import { prepareLocalTransport } from '../src/pgcouch-storage.mjs'

const names = ({ remote, ...storage }) => storage

test('equivalent remote database URLs share storage across reloads and credential changes', async () => {
   const origin = 'https://offline.example.com'
   const first = await storageForRemote('/couchdb/offline-v3', origin)
   const equivalent = await storageForRemote('https://reader:secret@offline.example.com:443/couchdb/offline-v3/?option=1#fragment', origin)
   assert.deepEqual(names(equivalent), names(first))
   assert.equal(JSON.stringify(names(equivalent)).includes('secret'), false)
})

test('different databases and hosts isolate both stores, the worker and its lock', async () => {
   const origin = 'https://offline.example.com'
   const first = await storageForRemote('/couchdb/offline-v2', origin)
   for (const remote of ['/couchdb/offline-v3', 'https://other.example.com/couchdb/offline-v2']) {
      const other = await storageForRemote(remote, origin)
      for (const key of ['relational', 'transactions', 'lock', 'worker']) assert.notEqual(first[key], other[key], key)
   }
})

test('changing transport preserves cached rows and pending edits while resetting only its change cursor', async t => {
   const db = new PGlite()
   t.after(() => db.close())
   await db.exec(`CREATE TABLE pgcouch_local_state (id INTEGER PRIMARY KEY, sequence BIGINT, initialized BOOLEAN, cursor JSONB);
      INSERT INTO pgcouch_local_state VALUES (1, 12, true, '999');
      CREATE TABLE pgcouch_outbox (id TEXT PRIMARY KEY, status TEXT, document JSONB);
      INSERT INTO pgcouch_outbox VALUES ('pending-edit', 'pending', '{"operations":[]}');
      CREATE TABLE "group" (uid TEXT PRIMARY KEY, name TEXT);
      INSERT INTO "group" VALUES ('cached', 'Cached group');`)
   await prepareLocalTransport(db, 'new-transport')
   assert.deepEqual((await db.query('SELECT * FROM pgcouch_local_state')).rows, [{ id: 1, sequence: 12, initialized: true, cursor: 0 }])
   assert.equal((await db.query('SELECT * FROM pgcouch_outbox')).rows[0].status, 'pending')
   assert.equal((await db.query('SELECT * FROM "group"')).rows[0].name, 'Cached group')
   await db.query("UPDATE pgcouch_local_state SET cursor = '5'")
   await prepareLocalTransport(db, 'new-transport')
   assert.equal((await db.query('SELECT cursor FROM pgcouch_local_state')).rows[0].cursor, 5)
})


test('storage prefixes distinguish apps while preserving existing offline names', async () => {
   const legacy = await storageForRemote('/couchdb/demo', 'https://example.com')
   assert.ok(legacy.relational.startsWith('idb://express-x-pgcouch-relational-'))
   assert.deepEqual(legacy, await storageForRemote('/couchdb/demo', 'https://example.com', 'express-x'))
   const first = await storageForRemote('/couchdb/demo', 'https://example.com', 'offline')
   const second = await storageForRemote('/couchdb/demo', 'https://example.com', 'other-app')
   assert.ok(first.relational.startsWith('idb://offline-pgcouch-relational-'))
   assert.notEqual(first.relational, second.relational)
   await assert.rejects(storageForRemote('/demo', 'https://example.com', '../bad'), TypeError)
})
