import 'fake-indexeddb/auto'
import test from 'node:test'
import assert from 'node:assert/strict'
import { offlinePlugin } from '../src/sync-client-plugin.mjs'

function deferredResponse() {
   let resolve
   let applied
   const gate = new Promise(done => { resolve = done })
   return {
      resolve,
      result: { then(callback) { applied = gate.then(callback); return applied } },
      applied: () => applied,
   }
}

async function fixture(t, methods) {
   const app = {
      isConnected: true,
      service: () => ({ on: () => () => {}, ...methods }),
      addConnectListener() {}, addDisconnectListener() {},
   }
   offlinePlugin(app, { cacheNamespace: `sync-race-${crypto.randomUUID()}` })
   const model = app.createOfflineModel('rows', ['label'])
   t.after(() => model.db.delete())
   await model.db.values.put({ uid: 'row', label: 'original' })
   await model.db.metadata.put({ uid: 'row', created_at: new Date('2026-01-01'), __dirty__: false })
   return { app, model }
}

test('a late update acknowledgement preserves a newer offline delete', async t => {
   const response = deferredResponse()
   let sent
   const { app, model } = await fixture(t, { updateWithMeta(...args) { sent = args; return response.result } })
   await model.update('row', { label: 'updated' })
   app.isConnected = false
   await model.remove('row')
   const expected = await model.db.metadata.get('row')
   response.resolve([{ uid: 'row', ...sent[1] }, { uid: 'row', created_at: new Date('2026-01-01'), updated_at: sent[2] }])
   await response.applied()
   assert.deepEqual(await model.db.metadata.get('row'), expected)
   assert.equal(await model.findByUID('row'), undefined)
   assert.equal(expected.__dirty__, true)
})

test('a late delete acknowledgement preserves a newer offline update', async t => {
   const response = deferredResponse()
   let sent
   const { app, model } = await fixture(t, { deleteWithMeta(...args) { sent = args; return response.result } })
   await model.remove('row')
   app.isConnected = false
   await model.update('row', { label: 'restored' })
   const expected = await model.db.metadata.get('row')
   response.resolve([undefined, { uid: 'row', created_at: new Date('2026-01-01'), deleted_at: sent[1] }])
   await response.applied()
   assert.deepEqual(await model.db.metadata.get('row'), expected)
   assert.equal((await model.db.values.get('row')).label, 'restored')
   assert.equal(expected.__dirty__, true)
})
