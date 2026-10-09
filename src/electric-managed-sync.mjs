import { Observable } from 'rxjs'

/** One database and one cached model per plugin instance. Browser locks span tabs. */
export function createManagedSync(options, createModel) {
   const databaseName = options.databaseName
   if (typeof databaseName !== 'string' || !databaseName.trim()) {
      throw new TypeError('sync: true requires a non-empty databaseName')
   }
   const locks = options.locks ?? globalThis.navigator?.locks
   const Channel = options.BroadcastChannel ?? globalThis.BroadcastChannel
   if (!locks?.request || !Channel) throw new Error('Managed sync requires Web Locks and BroadcastChannel (use HTTPS or localhost)')
   let database
   let preparation = Promise.resolve()
   let closed = false
   const models = new Map()

   function getDatabase() {
      database ??= Promise.resolve().then(async () => {
         if (options.localDb) return options.localDb
         if (options.createDatabase) return options.createDatabase({ dataDir: `idb://${databaseName}` })
         const { PGliteWorker } = await import('@electric-sql/pglite/worker')
         const worker = options.workerFactory
            ? options.workerFactory()
            : new Worker(new URL('./electric-pglite-worker.mjs', import.meta.url), { type: 'module' })
         let timer
         let rejectFailure
         const failure = new Promise((_, reject) => { rejectFailure = reject })
         const fail = event => rejectFailure(new Error(event.message ?? 'PGlite worker failed to initialize'))
         worker.addEventListener('error', fail)
         worker.addEventListener('messageerror', fail)
         timer = setTimeout(() => rejectFailure(new Error('PGlite worker initialization timed out')), options.initTimeoutMs ?? 30_000)
         try {
            return await Promise.race([PGliteWorker.create(worker, { dataDir: `idb://${databaseName}` }), failure])
         } catch (error) {
            worker.terminate()
            throw error
         } finally {
            clearTimeout(timer)
            worker.removeEventListener('error', fail)
            worker.removeEventListener('messageerror', fail)
         }
      })
      return database
   }

   function model(name, modelOptions) {
      if (closed) throw new Error('Electric sync has been disposed')
      const existing = models.get(name)
      if (existing) {
         if (existing.primaryKey !== (modelOptions.primaryKey ?? 'id')) throw new TypeError('A cached sync model cannot change primaryKey')
         return existing.api
      }
      let active = true
      let disposed = false
      let controller
      let release
      let generation = 0
      let channel
      let offline
      let error = null
      function report(cause) {
         error = cause
         try { (modelOptions.onError ?? options.onError)?.(cause, { modelName: name }) } catch (callbackError) { console.error(callbackError) }
      }
      const ready = getDatabase().then(async db => {
         channel = new Channel(`express-x:${databaseName}:${name}`)
         offline = createModel(name, { ...modelOptions, localDb: db, channel, ownsSync: false, onError: report })
         const prepared = preparation.then(() => offline.prepare())
         preparation = prepared.catch(() => {})
         await prepared
         if (disposed) { channel.close(); return offline }
         if (active) startOwnership()
         return offline
      })
      // Report initialization failure even when the first consumer only subscribes.
      void ready.catch(report)

      function startOwnership() {
         if (controller || disposed || !active) return
         offline.start()
         controller = new AbortController()
         const ownerGeneration = ++generation
         void locks.request(`express-x:${databaseName}:${name}`, { signal: controller.signal }, async () => {
            if (!active || disposed || generation !== ownerGeneration) return
            try {
               offline.setSyncOwner(true)
               await new Promise(resolve => { release = resolve })
            } finally {
               await offline.waitForIdle()
               if (generation === ownerGeneration) { offline.setSyncOwner(false); release = undefined }
            }
         }).catch(cause => { if (cause.name !== 'AbortError') report(cause) })
      }
      function stop() {
         active = false
         ++generation
         controller?.abort()
         controller = undefined
         release?.()
         release = undefined
         offline?.setSyncOwner(false)
         offline?.stop()
      }
      function start() {
         if (disposed) throw new Error('Electric model has been disposed')
         active = true
         return ready.then(() => startOwnership())
      }
      function invoke(method, ...args) {
         if (disposed) return Promise.reject(new Error('Electric model has been disposed'))
         return ready.then(model => model[method](...args))
      }
      const api = {
         prepare: () => ready.then(() => undefined), start, stop,
         async dispose() {
            if (disposed) return
            disposed = true
            stop()
            try { await ready; await offline.waitForIdle() } finally { channel?.close(); models.delete(name) }
         },
         getObservable(where = {}) {
            return new Observable(subscriber => {
               if (disposed) { subscriber.error(new Error('Electric model has been disposed')); return }
               let subscription
               let cancelled = false
               const fail = cause => subscriber.error(cause)
               void ready.then(model => {
                  if (!cancelled) subscription = model.getObservable(where).subscribe(subscriber)
               }).catch(fail)
               return () => { cancelled = true; subscription?.unsubscribe() }
            })
         },
         findMany: (where = {}) => invoke('findMany', where),
         findUnique: (where = {}) => invoke('findUnique', where),
         create: data => invoke('create', data),
         update: (id, data) => invoke('update', id, data),
         remove: id => invoke('remove', id),
         flushQueue: () => invoke('flushQueue'),
         getStatus: async () => ({ ...await invoke('getStatus'), error: error?.message ?? null }),
      }
      models.set(name, { api, primaryKey: modelOptions.primaryKey ?? 'id' })
      return api
   }
   async function dispose() {
      closed = true
      await Promise.allSettled([...models.values()].map(({ api }) => api.dispose()))
      if (database && !options.localDb) await (await database).close()
   }
   return { model, dispose }
}
