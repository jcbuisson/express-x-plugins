import { Shape, ShapeStream } from '@electric-sql/client'

/** Install app.sql.query(name, params) and app.sql.watch(table, filter, callback). */
export function electricSqlClientPlugin(app, options = {}) {
   const service = app.service(options.serviceName ?? 'sql')
   const Stream = options.ShapeStream ?? ShapeStream
   const LiveShape = options.Shape ?? Shape
   const active = new Set()
   
   function watch(table, filter, callback) {
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
      const stream = new Stream({
         url: `${(options.shapePath ?? '/electric-sql/v1/shape').replace(/\/$/, '')}/${encodeURIComponent(table)}`,
         headers: options.headers,
         onError: options.onError,
         signal: controller.signal,
         params: { ...(where === undefined ? {} : { where }), params: values },
      })
      const shape = new LiveShape(stream)
      const unsubscribe = shape.subscribe(({ rows }) => callback([...rows]))
      let stopped = false

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
      dispose() { for (const stop of active) stop() },
   }
   return app
}

export default electricSqlClientPlugin
