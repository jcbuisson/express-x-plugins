import test from 'node:test'
import assert from 'node:assert/strict'
import { PGlite } from '@electric-sql/pglite'
import { drizzle } from 'drizzle-orm/pglite'
import { pgTable, text, timestamp } from 'drizzle-orm/pg-core'
import { syncServerPlugin } from '../src/sync-server-plugin.mjs'

const T0 = new Date('2026-01-01')
const T1 = new Date('2026-01-02')

async function fixture(t, withMetadata = true) {
   const pg = new PGlite()
   t.after(() => pg.close())
   await pg.exec(`CREATE TABLE rows (uid TEXT PRIMARY KEY, label TEXT);
      CREATE TABLE metadata (uid TEXT PRIMARY KEY, created_at TIMESTAMP, updated_at TIMESTAMP, deleted_at TIMESTAMP)`)
   const db = drizzle(pg)
   const rows = pgTable('rows', { uid: text().primaryKey(), label: text() })
   const metadata = pgTable('metadata', { uid: text().primaryKey(), created_at: timestamp(), updated_at: timestamp(), deleted_at: timestamp() })
   const services = new Map()
   const app = {
      createService(name, methods) {
         methods.callWithContext = (context, action, ...args) => methods[action].apply(context, args)
         services.set(name, methods)
      },
      service: name => services.get(name),
   }
   syncServerPlugin(app, db, metadata, [rows], { authorize: () => true })
   await db.insert(rows).values({ uid: 'row', label: 'original' })
   if (withMetadata) await db.insert(metadata).values({ uid: 'row', created_at: T0 })
   return { db, metadata, service: services.get('rows'), sync: services.get('sync') }
}

for (const writeAfterRead of [1, 2]) {
   test(`sync pairs values with their own metadata when a write follows read ${writeAfterRead}`, async t => {
      const { service, sync } = await fixture(t)
      const findMany = service.findMany
      let reads = 0
      service.findMany = async function(...args) {
         const rows = await findMany.apply(this, args)
         if (++reads === writeAfterRead) await service.updateWithMeta('row', { label: 'changed' }, T1)
         return rows
      }
      const result = await sync.go('rows', {}, {})
      assert.equal(result.addClient[0][0].label, 'changed')
      assert.equal(result.addClient[0][1].updated_at.getTime(), T1.getTime())
      assert.equal(reads, writeAfterRead === 1 ? 2 : 4)
   })
}

test('metadata repair cannot overwrite a concurrent mutation version', async t => {
   const { db, metadata, service, sync } = await fixture(t, false)
   const insert = db.insert.bind(db)
   let inject = true
   db.insert = table => {
      const builder = insert(table)
      if (table !== metadata || !inject) return builder
      const values = builder.values.bind(builder)
      builder.values = data => {
         const query = values(data)
         const returning = query.returning.bind(query)
         query.returning = async (...args) => {
            const pending = returning(...args)
            inject = false
            await service.updateWithMeta('row', { label: 'concurrent' }, T1)
            return pending
         }
         return query
      }
      return builder
   }
   const result = await sync.go('rows', {}, {})
   assert.equal(result.addClient[0][0].label, 'concurrent')
   const [stored] = await db.select().from(metadata)
   assert.equal(stored.updated_at.getTime(), T1.getTime())
   assert.equal(stored.created_at, null)
})

test('continuously changing snapshots fail after bounded retries', async t => {
   const { service, sync } = await fixture(t)
   const findMany = service.findMany
   let reads = 0
   service.findMany = async function(...args) {
      const rows = await findMany.apply(this, args)
      reads++
      await service.updateWithMeta('row', { label: String(reads) }, new Date(T1.getTime() + reads))
      return rows
   }
   await assert.rejects(sync.go('rows', {}, {}), { code: 'sync-snapshot-changed' })
   assert.equal(reads, 6)
})
