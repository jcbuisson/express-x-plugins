import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

for (const deleted of [false, true]) {
   test(`a replacement create clears its superseded local delete, then respects server deleted=${deleted}`, async () => {
      const db = new PGlite()
      const app = { service: () => ({ async create() { return { version: '1', deleted } } }) }
      electricClientPlugin(app, { relations: [{ child: 'children', parent: 'parents', column: 'parent_uid' }] })
      const make = name => app.createElectricModel(name, { localDb: db })
      const parents = make('parents'), children = make('children')
      try {
         await parents.prepare()
         const parent = await parents.create({ title: 'original' })
         const child = await children.create({ parent_uid: parent.uid })
         await parents.remove(parent.uid)
         assert.deepEqual(await children.findMany(), [])
         await parents.create({ uid: parent.uid, title: 'replacement' })
         assert.equal((await parents.findUnique({ uid: parent.uid })).title, 'replacement')
         assert.equal((await children.findMany())[0].uid, child.uid)
         await parents.flushQueue()
         assert.equal((await parents.findMany()).length, deleted ? 0 : 1)
         assert.equal((await children.findMany()).length, deleted ? 0 : 1)
      } finally { await db.close() }
   })
}
