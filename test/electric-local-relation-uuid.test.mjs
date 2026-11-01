import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

test('local cascades normalize valid UUID foreign keys without rejecting unsynced invalid values', async () => {
   const db = new PGlite()
   const app = { service: () => ({}) }
   electricClientPlugin(app, { relations: [
      { child: 'children', parent: 'parents', column: 'parent_uid' },
      { child: 'details', parent: 'children', column: 'child_uid' },
   ] })
   const make = name => app.createElectricModel(name, { localDb: db, network: { onLine: false } })
   const parents = make('parents'), children = make('children'), details = make('details')
   try {
      await parents.prepare()
      const parent = await parents.create({ uid: `a${crypto.randomUUID().slice(1)}` })
      const formats = [id => id.toUpperCase(), id => `{${id}}`, id => id.replaceAll('-', '')]
      for (const format of formats) {
         const child = await children.create({ uid: `a${crypto.randomUUID().slice(1)}`, parent_uid: format(parent.uid) })
         await details.create({ child_uid: format(child.uid) })
      }
      const invalid = await children.create({ parent_uid: 'invalid pending input' })
      const nullable = await children.create({ parent_uid: null })
      assert.equal((await children.findMany()).length, 5)
      await parents.remove(parent.uid)
      assert.deepEqual((await children.findMany()).map(row => row.uid).sort(), [invalid.uid, nullable.uid].sort())
      assert.deepEqual(await details.findMany(), [])
   } finally { await db.close() }
})
