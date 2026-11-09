import { PGlite } from '@electric-sql/pglite'
import PouchDB from 'pouchdb-browser'
import indexeddb from 'pouchdb-adapter-indexeddb'
import { pgcouchClientPlugin } from './pgcouch-client-plugin.mjs'
import { storageForRemote, prepareLocalTransport } from './pgcouch-storage.mjs'

PouchDB.plugin(indexeddb)

// Call synchronously from the app's worker entry, before any top-level await.
// Schema/model definitions belong to the app; runtime and transport belong here.
export function startPGCouchWorker(options) {
   if (typeof options?.remote !== 'string' || !options.remote) throw new TypeError('options.remote is required')
   if (typeof options.schema !== 'string') throw new TypeError('options.schema must be SQL text')
   if (!Array.isArray(options.models) || !options.models.length) throw new TypeError('options.models must be a non-empty array')
   options = { ...options, models: structuredClone(options.models) }
   // One database, transaction queue and replication feed for every tab.
   const ports = new Set()
   const errors = new Map()
   const app = {}
   let syncEnabled = true
   let localDb
   const runtime = { ready: false, syncing: false, stage: 'waiting-lock' }
   function updateRuntime(value) {
      Object.assign(runtime, value)
      for (const port of ports) port.postMessage({ type: 'sync-state', ...runtime })
   }
   function errorData(error) {
      return { message: error?.message ?? String(error), status: error?.status,
         code: error?.code, transactionId: error?.transactionId }
   }
   function report(error) {
      const value = errorData(error)
      errors.set(value.message, value)
      for (const port of ports) port.postMessage({ type: 'sync-error', error: value })
   }

   // Only workers for the same remote database compete for database ownership.
   const ready = new Promise((resolve, reject) => {
      // Register onconnect synchronously below: WebKit can dispatch the first
      // connection while a module's top-level await is still pending.
      void storageForRemote(options.remote, self.location.origin, options.storagePrefix)
         .then(pgcouchConfig => navigator.locks.request(pgcouchConfig.lock, async () => {
            try {
               updateRuntime({ stage: 'loading-database' })
               localDb = new PGlite(pgcouchConfig.relational)
               await localDb.waitReady
               updateRuntime({ stage: 'preparing-schema' })
               await localDb.exec(options.schema)
               await prepareLocalTransport(localDb, pgcouchConfig.transactions)
               updateRuntime({ stage: 'opening-transport' })
               const couch = new PouchDB(pgcouchConfig.transactions, { adapter: 'indexeddb' })
               await couch.info()
               const replicate = couch.sync.bind(couch)
               couch.sync = (...args) => replicate(...args)
                  .on('active', () => updateRuntime({ syncing: true }))
                  .on('paused', (error) => { updateRuntime({ syncing: false }); if (error) report(error) })
                  .on('error', () => updateRuntime({ syncing: false }))
                  .on('complete', () => updateRuntime({ syncing: false }))
               pgcouchClientPlugin(app, {
                  models: options.models, localDb, couch,
                  remote: pgcouchConfig.remote,
                  network: { get onLine() { return syncEnabled && navigator.onLine } },
                  onError: report,
               })
               updateRuntime({ stage: 'starting-sync' })
               await app.pgcouchReady
               updateRuntime({ ready: true, stage: 'ready' })
               resolve()
               await new Promise(() => {})
            } catch (error) { updateRuntime({ ready: true, syncing: false, stage: 'failed' }); reject(error) }
      })).catch(error => { updateRuntime({ ready: true, syncing: false, stage: 'failed' }); reject(error) })
   })
   void ready.catch(report)

   const methods = new Set(['findMany', 'findUnique', 'create', 'update', 'remove', 'delete',
      'prepare', 'getStatus', 'flushQueue', 'waitForIdle'])

   self.onconnect = ({ ports: [port] }) => {
      ports.add(port)
      let active = true
      const cancelled = new Set()
      const subscriptions = new Map()
      port.postMessage({ type: 'sync-state', ...runtime })
      for (const error of errors.values()) port.postMessage({ type: 'sync-error', error })
      port.onmessage = async ({ data }) => {
         const { id, type, model, method, args = [] } = data
         try {
            if (type === 'dispose') {
               active = false
               for (const subscription of subscriptions.values()) subscription.unsubscribe()
               subscriptions.clear(); ports.delete(port); port.close()
               return
            }
            if (type === 'unsubscribe') {
               const subscription = subscriptions.get(id)
               if (subscription) { subscription.unsubscribe(); subscriptions.delete(id) }
               else cancelled.add(id)
               return
            }
            await ready
            if (!active) return
            if (type === 'subscribe') {
               if (cancelled.delete(id)) return
               subscriptions.set(id, app.createPGCouchModel(model).getObservable(...args).subscribe({
                  next: (value) => port.postMessage({ type: 'value', id, value }),
                  error: (error) => port.postMessage({ type: 'subscription-error', id, error: errorData(error) }),
               }))
               return
            }
            let value
            if (type === 'network') {
               syncEnabled = args[0]
               const sync = app.createPGCouchModel(options.models[0].name ?? options.models[0])
               if (syncEnabled) await sync.start()
               else sync.stop()
            } else if (type === 'transaction') value = await app.pgcouchTransaction(...args)
            else if (type === 'call' && methods.has(method)) {
               value = await app.createPGCouchModel(model)[method](...args)
               if (method === 'getStatus') {
                  const { rows } = await localDb.query('SELECT initialized FROM pgcouch_local_state WHERE id = 1')
                  value = { ...value, initialized: rows[0]?.initialized === true }
               }
            }
            else throw new Error('Unknown sync request')
            port.postMessage({ type: 'result', id, value })
         } catch (error) { port.postMessage({ type: type === 'subscribe' ? 'subscription-error' : 'result', id, error: errorData(error) }) }
      }
      port.start()
   }

   return { ready }
}
