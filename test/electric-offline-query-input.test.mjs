import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

test('offline reads retain their filters while database requests are pending', async () => {
   const db = new PGlite()
   let release
   let gate
   const delayed = {
      exec: db.exec.bind(db), transaction: db.transaction.bind(db),
      async query(...args) {
         const result = await db.query(...args)
         if (gate && args[0].startsWith('SELECT data')) await gate
         return result
      },
   }
   const app = { service: () => ({}) }
   electricClientPlugin(app)
   const model = app.createElectricModel('todos', { localDb: delayed, network: { onLine: false } })
   try {
      await model.prepare()
      const first = await model.create({ score: 1 })
      const second = await model.create({ score: 2 })
      gate = new Promise(resolve => { release = resolve })
      const where = { score: { gte: 1, lte: 1 } }
      const reading = model.findMany(where)
      where.score.gte = 2
      where.score.lte = 2
      release()
      assert.deepEqual((await reading).map(row => row.uid), [first.uid])

      gate = new Promise(resolve => { release = resolve })
      const uniqueWhere = { uid: first.uid }
      const unique = model.findUnique(uniqueWhere)
      uniqueWhere.uid = second.uid
      release()
      assert.equal((await unique).uid, first.uid)
   } finally {
      release?.()
      await db.close()
   }
})
