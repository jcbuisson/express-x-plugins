import 'fake-indexeddb/auto'
import test from 'node:test'
import assert from 'node:assert/strict'
import { offlinePlugin } from '../src/sync-client-plugin.mjs'

function deferredResponse() {
   let resolve
   let reject
   let applied
   const gate = new Promise((done, fail) => { resolve = done; reject = fail })
   return {
      resolve, reject,
      result: { then(callback) {
         applied = gate.then(callback)
         const catchError = applied.catch.bind(applied)
         applied.catch = handler => { applied = catchError(handler); return applied }
         return applied
      } },
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

test('restoring an offline deleted row uploads an update rather than replaying its deletion', async t => {
   const requests = []
   const { app, model } = await fixture(t, {
      async updateWithMeta(uid, data, updated_at) {
         requests.push(['update', uid])
         return [{ uid, ...data }, { uid, created_at: new Date('2026-01-01'), updated_at }]
      },
      async deleteWithMeta(uid) { requests.push(['delete', uid]); throw new Error('unexpected delete') },
   })
   app.isConnected = false
   await model.remove('row')
   await model.update('row', { label: 'restored offline' })
   assert.equal((await model.findByUID('row')).label, 'restored offline')
   assert.equal((await model.db.metadata.get('row')).deleted_at, null)
   await model.synchronizeAll()
   assert.deepEqual(requests, [['update', 'row']])
   assert.equal((await model.db.metadata.get('row')).__dirty__, false)
})

test('a rejected restoration restores the preceding pending deletion', async t => {
   const response = deferredResponse()
   const { app, model } = await fixture(t, { updateWithMeta: () => response.result })
   app.isConnected = false
   await model.remove('row')
   const previousValue = await model.db.values.get('row')
   const previousMetadata = await model.db.metadata.get('row')
   app.isConnected = true
   await model.update('row', { label: 'rejected restoration' })
   response.reject({ code: 'forbidden', message: 'restoration rejected' })
   await response.applied()
   assert.deepEqual(await model.db.values.get('row'), previousValue)
   assert.deepEqual(await model.db.metadata.get('row'), previousMetadata)
})

test('another client releasing a shared scope cannot disable an active client synchronization', async t => {
   const namespace = `shared-sync-${crypto.randomUUID()}`
   let syncCalls = 0
   const clients = []
   const subscriptions = []
   t.after(async () => {
      for (const subscription of subscriptions) subscription.unsubscribe()
      await new Promise(resolve => setTimeout(resolve, 10))
      for (const { model } of clients) model.db.close()
   })
   function client() {
      const app = {
         isConnected: false,
         addConnectListener() {}, addDisconnectListener() {},
         service: () => ({
            on: () => () => {},
            async go() {
               syncCalls++
               return { addClient: [], updateClient: [], deleteClient: [], addDatabase: [], updateDatabase: [] }
            },
         }),
      }
      offlinePlugin(app, { cacheNamespace: namespace })
      const model = app.createOfflineModel('rows', ['label'])
      const result = { app, model }
      clients.push(result)
      return result
   }
   const first = client()
   const second = client()
   await Promise.all(clients.map(({ model }) => new Promise((resolve, reject) => {
      subscriptions.push(model.getObservable({ label: 'active' }).subscribe({ next: resolve, error: reject }))
   })))
   subscriptions[0].unsubscribe()
   for (let attempt = 0; attempt < 100 && await first.model.db.whereList.count(); attempt++) {
      await new Promise(resolve => setTimeout(resolve, 2))
   }
   assert.equal(await first.model.db.whereList.count(), 0)
   second.app.isConnected = true
   await second.model.synchronizeAll()
   assert.equal(syncCalls, 1)
})
