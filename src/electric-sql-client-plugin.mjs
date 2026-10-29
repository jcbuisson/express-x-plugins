import { Shape, ShapeStream } from '@electric-sql/client'
import Dexie from 'dexie'

/** Install app.sql.query(name, params) and app.sql.watch(table, filter, callback). */
export function electricSqlClientPlugin(app, options = {}) {
   const service = app.service(options.serviceName ?? 'sql')
   const Stream = options.ShapeStream ?? ShapeStream
   const LiveShape = options.Shape ?? Shape
   const active = new Set()
   const persistent = options.persistence ?? Boolean(globalThis.indexedDB)
   const database = persistent ? new Dexie(options.databaseName ?? 'express-x-sql') : null
   if (database) database.version(1).stores({ shapes: '&key' })
   const scope = JSON.stringify([options.serviceName ?? 'sql', options.shapePath ?? '/electric-sql/v1/shape'])
   let writes = Promise.resolve()
   let disposed = false
   const report = error => options.onError?.(error)
   const keyFor = (table, where, params) => JSON.stringify([scope, table, where ?? null, params])
   
   function watch(table, filter, callback) {
      if (disposed) throw new Error('SQL client is disposed')
      if (typeof filter === 'function') { callback = filter; filter = {} }
      const { where, params = [] } = filter ?? {}
      if (typeof table !== 'string' || !table) throw new TypeError('table is required')
      if (typeof callback !== 'function') throw new TypeError('callback is required')
      if (where !== undefined && typeof where !== 'string') throw new TypeError('where must be SQL text')
      if (!Array.isArray(params)) throw new TypeError('params must be an array')
      const values = params.map(value => {
         if (value instanceof Date && !Number.isNaN(value.getTime())) return value.toISOString()
         if (typeof value === 'string' || typeof value === 'boolean' || typeof value === 'bigint'
            || (typeof value === 'number' && Number.isFinite(value))) return String(value)
         throw new TypeError('shape params must be strings, booleans, finite numbers, bigints or Dates; use IS NULL for null')
      })
      const controller = new AbortController()
      const key = keyFor(table, where, values)
      let stopped = false
      let received = false
      // Do not let a slow disk read overwrite a newer network snapshot.
      if (database) database.shapes.get(key).then(snapshot => {
         if (!stopped && !received && snapshot) callback(snapshot.rows)
      }).catch(report)
      const stream = new Stream({
         url: `${(options.shapePath ?? '/electric-sql/v1/shape').replace(/\/$/, '')}/${encodeURIComponent(table)}`,
         headers: options.headers,
         onError: options.onError,
         signal: controller.signal,
         params: { ...(where === undefined ? {} : { where }), params: values },
      })
      const shape = new LiveShape(stream)
      const unsubscribe = shape.subscribe(({ rows }) => {
         if (stopped) return
         if (shape.error) { report(shape.error); return }
         received = true
         const current = [...rows]
         if (database) {
            // Clone before handing rows to application code; preserve Dates and bigints.
            const snapshot = structuredClone({ key, rows: current })
            writes = writes.then(() => database.shapes.put(snapshot)).catch(report)
         }
         callback(current)
      })

      function stop() {
         if (stopped) return
         stopped = true
         unsubscribe()
         stream.unsubscribeAll()
         controller.abort()
         active.delete(stop)
      }
      active.add(stop)
      return stop
   }
   app.sql = {
      query: (name, params = []) => service.query(name, params),
      watch,
      async clearLocalData() {
         await writes
         if (database) await database.shapes.clear()
      },
      async dispose() {
         disposed = true
         for (const stop of active) stop()
         await writes
         database?.close()
      },
   }
   return app
}

export default electricSqlClientPlugin
