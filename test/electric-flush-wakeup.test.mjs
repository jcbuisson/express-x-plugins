import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

class Stream {
   subscribe() {}
   unsubscribeAll() {}
}
class Shape {
   subscribe() {}
}

for (const mode of ['running', 'stopped', 'restarted']) {
   test(`a write joining an empty queue flush is handled when ${mode}`, async () => {
      const db = new PGlite()
      let release
      let entered
      let hold = true
      const gate = new Promise(resolve => { release = resolve })
      const selected = new Promise(resolve => { entered = resolve })
      const delayed = {
         exec: db.exec.bind(db), transaction: db.transaction.bind(db),
         async query(...args) {
            const result = await db.query(...args)
            if (hold && args[0].startsWith('SELECT * FROM electric_mutation_queue')) {
               hold = false
               assert.equal(result.rows.length, 0)
               entered()
               await gate
            }
            return result
         },
      }
      const calls = []
      const app = { service: () => ({ async create(id) { calls.push(id); return { version: '1' } } }) }
      electricClientPlugin(app, { ShapeStream: Stream, Shape })
      const model = app.createElectricModel('todos', { localDb: delayed, retryMs: 60_000 })
      try {
         await model.prepare()
         model.start()
         await selected
         const row = await model.create({ title: 'joined the active flush' })
         if (mode !== 'running') model.stop()
         if (mode === 'restarted') model.start()
         release()
         await model.waitForIdle()
         assert.deepEqual(calls, mode === 'stopped' ? [] : [row.uid])
      } finally {
         release()
         model.stop()
         await model.waitForIdle()
         await db.close()
      }
   })
}
