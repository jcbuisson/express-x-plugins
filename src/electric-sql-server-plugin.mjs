
/** SQL statements execute on PostgreSQL; Electric supplies live table reads. */
export function electricSqlServerPlugin(app, db, options = {}) {
   const { tables, statements = {}, authorize } = options
   if (!db || typeof db.query !== 'function') throw new TypeError('db must expose query(sql, params)')
   if (!Array.isArray(tables) || !tables.length || tables.some(table => typeof table !== 'string' || !table)) {
      throw new TypeError('tables must be a non-empty array of table names')
   }
   if (typeof authorize !== 'function') throw new TypeError('authorize(context, operation) is required')
   const sql = new Map(Object.entries(statements))
   for (const statement of sql.values()) {
      if (typeof statement !== 'string' || !statement.trim()) throw new TypeError('statements must contain SQL strings')
   }
   const fetchImpl = options.fetch ?? globalThis.fetch
   const upstreamUrl = new URL(options.electricUrl ?? process.env.ELECTRIC_URL ?? 'http://localhost:3000/v1/shape')
   const shapePath = options.shapePath ?? '/electric-sql/v1/shape/:table'
   async function check(context, operation) {
      if (!await authorize(context, operation)) {
         const error = new Error('forbidden')
         error.code = 'forbidden'
         throw error
      }
   }

   app.createService(options.serviceName ?? 'sql', {
      async query(name, params = []) {
         if (!sql.has(name)) throw new TypeError('unknown SQL statement')
         if (!Array.isArray(params)) throw new TypeError('params must be an array')
         await check(this, { action: 'query', name, params })
         return (await db.query(sql.get(name), params)).rows
      },
   })
   
   app.get(shapePath, async (request, response, next) => {
      try {
         const table = request.params.table
         if (!tables.includes(table)) return response.status(404).json({ error: 'unknown table' })
         const target = new URL(upstreamUrl)
         // Only Electric protocol and SQL filter parameters cross the proxy.
         for (const [key, value] of Object.entries(request.query)) {
            if (!['offset', 'handle', 'live', 'cursor', 'where', 'columns', 'replica'].includes(key)
               && !/^params\[[1-9][0-9]*\]$/.test(key)) continue
            if (typeof value !== 'string') throw new TypeError('shape parameters must be strings')
            target.searchParams.set(key, value)
         }
         target.searchParams.set('table', table)
         await check({ app, request, response, transport: 'http' }, {
            action: 'shape', table, params: Object.fromEntries(target.searchParams),
         })
         if (options.sourceId) target.searchParams.set('source_id', options.sourceId)
         if (options.sourceSecret) target.searchParams.set('secret', options.sourceSecret)
         const controller = new AbortController()
         const abort = () => controller.abort()
         response.on?.('close', abort)
         try {
            const upstream = await fetchImpl(target, { signal: controller.signal })
            response.status(upstream.status)
            upstream.headers.forEach((value, key) => {
               if (!['content-encoding', 'content-length', 'transfer-encoding', 'connection', 'set-cookie'].includes(key.toLowerCase())) {
                  response.setHeader(key, value)
               }
            })
            response.send(Buffer.from(await upstream.arrayBuffer()))
         } finally {
            response.off?.('close', abort)
         }
      } catch (error) { next(error) }
   })
   return { shapePath }
}

export default electricSqlServerPlugin
