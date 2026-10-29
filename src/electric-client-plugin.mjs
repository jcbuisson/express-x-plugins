import { assertPlainObject, quoteIdentifier, whereToElectricParams } from './electric-where.mjs'
export { whereToElectricParams } from './electric-where.mjs'
import { shareQueryObservables } from './electric-query-observable.mjs'
import { Shape, ShapeStream } from '@electric-sql/client'
import { Observable, firstValueFrom, Subject, takeUntil } from 'rxjs'
import { getCurrentScope, onScopeDispose } from 'vue'
import { createOfflineElectricModel } from './electric-offline-model.mjs'
import { createManagedSync } from './electric-managed-sync.mjs'
import { streamAbort } from './electric-stream-abort.mjs'

/**
 * Add Electric-backed reactive models to an Express-X client.
 *
 * Usage:
 *   electricClientPlugin(app)
 *   const todoModel = app.createElectricModel('todos') // client-generated UUID in the default uid primary key
 *   const todoMilk = await todoModel.create({ label: "Buy milk" })
 *   todoMilk.update({ completed: true })
 *   todoModel.getObservable({ completed: false }).subscribe(...) // subscribe to an observable for todos with completed===false
 */
export function electricClientPlugin(app, options = {}) {
   const shapePath = options.shapePath ?? '/electric/v1/shape'
   const ShapeStreamClass = options.ShapeStream ?? ShapeStream
   const ShapeClass = options.Shape ?? DisposableShape
   const ObservableClass = options.Observable ?? Observable

   const managed = options.sync === true
      ? createManagedSync(options, createDirectModel)
      : null

   function createElectricModel(modelName, modelOptions = {}) {
      quoteIdentifier(modelName)
      quoteIdentifier(modelOptions.primaryKey ?? 'uid')
      if (managed) {
         if (modelOptions.idGeneration === 'server') throw new TypeError('sync models require client-generated UUIDs')
         return managed.model(modelName, modelOptions)
      }
      return createDirectModel(modelName, modelOptions)
   }

   function createDirectModel(modelName, modelOptions) {
      const service = app.service(modelName)
      const url = modelOptions.url ?? modelPath(shapePath, modelName)
      if (modelOptions.localDb) return createOfflineElectricModel({
         db: modelOptions.localDb, service, modelName, url,
         primaryKey: modelOptions.primaryKey ?? 'uid',
         ShapeStreamClass, ShapeClass,
         ownsSync: modelOptions.ownsSync ?? true,
         channel: modelOptions.channel,
         network: modelOptions.network ?? globalThis.navigator,
         retryMs: modelOptions.retryMs ?? 5000,
         streamOptions: modelOptions.streamOptions,
         onError: modelOptions.onError ?? options.onError,
      })
      const streamOptions = modelOptions.streamOptions ?? {}
      const idGeneration = modelOptions.idGeneration ?? 'client'
      const primaryKey = modelOptions.primaryKey ?? 'uid'
      quoteIdentifier(primaryKey)
      if (!['client', 'server'].includes(idGeneration)) {
         throw new TypeError("idGeneration must be 'client' or 'server'")
      }

      const sharedObservable = shareQueryObservables(createObservable)

      function getObservable(where = {}) {
         whereToElectricParams(where) // Preserve eager filter validation.
         return sharedObservable(where)
      }

      function createObservable(where) {
         // Validate eagerly
         const filterParams = whereToElectricParams(where)
         return new ObservableClass(subscriber => {
            const cancellation = streamAbort(streamOptions.signal)
            const stream = new ShapeStreamClass({
               ...streamOptions,
               signal: cancellation.signal,
               url,
               params: { ...streamOptions.params, ...filterParams },
            })
            const shape = new ShapeClass(stream)
            let previous
            const unsubscribe = shape.subscribe(({ rows }) => {
               if (shape.error) { subscriber.error(shape.error); return }
               const current = [...rows]
               const serialized = JSON.stringify(current, (_key, value) => typeof value === 'bigint' ? String(value) : value)
               if (serialized === previous) return
               previous = serialized
               subscriber.next(current)
            })
            // Shape exposes errors as state. ShapeStream retries transient failures;
            // callers keep one observable subscription across reconnects
            return () => {
               unsubscribe()
               stream.unsubscribeAll?.()
               cancellation.abort()
            }
         })
      }

      function findMany(where = {}) {
         const observable = getObservable(where)
         if (!getCurrentScope()) return firstValueFrom(observable)
         const scopeDisposed = new Subject()
         onScopeDispose(() => {
            scopeDisposed.next()
            scopeDisposed.complete()
         })
         return firstValueFrom(observable.pipe(takeUntil(scopeDisposed)))
      }

      async function findUnique(where) {
         const rows = await findMany(where)
         return rows[0] ?? null
      }

      async function create(uidOrData, data) {
         const hasUid = data !== undefined
         const mutationData = hasUid ? data : uidOrData
         assertPlainObject(mutationData, 'mutation data')
         if (hasUid) return service.create(uidOrData, data)
         if (idGeneration === 'server') return service.create(mutationData)
         const uid = globalThis.crypto?.randomUUID?.()
         if (!uid) throw new Error('crypto.randomUUID() is required')
         return service.create(uid, mutationData)
      }

      async function update(id, data) {
         return service.update(id, data)
      }

      async function remove(id) {
         return service.delete(id)
      }

      return { getObservable, findMany, findUnique, create, update, remove }
   }

   return Object.assign(app, { createElectricModel, ...(managed ? { disposeElectricSync: managed.dispose } : {}) })
}


//////////////////////                   UTILITIES                   //////////////////////

export class DisposableShape extends Shape {
   subscribe(callback) {
      const unsubscribe = super.subscribe(callback)
      return () => {
         unsubscribe()
         if (this.numSubscribers === 0) this.stream.unsubscribeAll()
      }
   }
}

function modelPath(shapePath, modelName) {
   const base = shapePath.replace(/\/$/, '')
   return `${base}/${encodeURIComponent(modelName)}`
}

export default electricClientPlugin
