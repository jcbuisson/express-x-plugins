import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

test('a rejected parent mutation hides descendants before its Shape arrives', async () => {
   const db = new PGlite()
   const app = { service: () => ({
      async create() { return { version: '1', deleted: false } },
      async update() { return { version: '2', deleted: true } },
   }) }
   electricClientPlugin(app, { relations: [
      { child: 'children', parent: 'parents', column: 'parent_uid' },
      { child: 'details', parent: 'children', column: 'child_uid' },
   ] })
   const make = name => app.createElectricModel(name, { localDb: db })
   const parents = make('parents'), children = make('children'), details = make('details')
   try {
      await parents.prepare()
      const parent = await parents.create({ title: 'cached parent' })
      await parents.flushQueue()
      const child = await children.create({ parent_uid: parent.uid })
      await details.create({ child_uid: child.uid })
      assert.equal((await children.findMany()).length, 1)
      await parents.update(parent.uid, { title: 'edited after remote deletion' })
      await parents.flushQueue()
      assert.equal(await parents.findUnique({ uid: parent.uid }), null)
      assert.deepEqual(await children.findMany(), [])
      assert.deepEqual(await details.findMany(), [])
      assert.deepEqual(await make('children').findMany(), [])
      assert.equal((await db.query('SELECT count(*)::int AS count FROM electric_local_rows WHERE model_name = $1', ['children'])).rows[0].count, 1)
   } finally { await db.close() }
})
