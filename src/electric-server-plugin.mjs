/**
 * Register Express-X mutation services and an Electric Shape proxy.
 *
 * @param {object} app Express-X/Express application
 * @param {object} db pg-compatible Pool or Client exposing query()
 * @param {(string|{name:string,table?:string,primaryKey?:string})[]} models
 * @param {object} options
 */
export function electricOfflinePlugin(app, db, models, options = {}) {
   if (!db || typeof db.query !== 'function') throw new TypeError('db must expose query(sql, values)')
   if (typeof options.authorize !== 'function') {
      throw new TypeError('electricOfflinePlugin requires an authorize(context, operation) policy')
   }
   const configuredModels = normalizeModels(models)
   const electricUrl = new URL(options.electricUrl ?? process.env.ELECTRIC_URL ?? 'http://localhost:3000/v1/shape')
   const shapePath = options.shapePath ?? '/electric/v1/shape/:model'
   const fetchImpl = options.fetch ?? globalThis.fetch
   if (typeof fetchImpl !== 'function') throw new TypeError('a fetch implementation is required')

   async function authorize(context, modelName, action, args) {
      const allowed = await options.authorize(context, { modelName, action, args })
      if (!allowed) {
         const error = new Error(`not authorized to ${action} '${modelName}'`)
         error.code = 'forbidden'
         throw error
      }
   }

   // for each model 'name', there is a service 'name' with methods 'findUnique', 'findMany', 'create', 'update', 'delete'
   for (const model of configuredModels) {
      app.createService(model.name, {

         findUnique: async function(where) {
            await authorize(this, model.name, 'findUnique', [where])
            const filter = buildWhere(where)
            const result = await db.query(`SELECT * FROM ${model.quotedTable} WHERE ${filter.sql} LIMIT 1`, filter.values)
            return result.rows[0] ?? null
         },

         findMany: async function(where, queryOptions = {}) {
            await authorize(this, model.name, 'findMany', [where, queryOptions])
            const filter = buildWhere(where)
            let sql = `SELECT * FROM ${model.quotedTable} WHERE ${filter.sql}`
            const values = [...filter.values]
            if (queryOptions.limit != null) {
               if (!Number.isInteger(queryOptions.limit) || queryOptions.limit < 1) {
                  throw new TypeError('limit must be a positive integer')
               }
               values.push(queryOptions.limit)
               sql += ` LIMIT $${values.length}`
            }
            return (await db.query(sql, values)).rows
         },

         // create(data): the primary key is server-generated
         // create(uid, data): uid (the primary key) is provided by the client
         create: async function(idOrData, data) {
            const hasClientId = data !== undefined
            const mutationData = hasClientId ? data : idOrData
            await authorize(this, model.name, 'create', hasClientId ? [idOrData, mutationData] : [mutationData])
            assertPlainObject(mutationData, 'mutation data')
            const safeData = hasClientId
               ? { ...mutationData, [model.primaryKey]: idOrData }
               : { ...mutationData }
            const entries = Object.entries(safeData).filter(([, value]) => value !== undefined)
            const columns = entries.map(([column]) => quoteIdentifier(column, 'data column'))
            const values = entries.map(([, value]) => value)
            const parameters = values.map((_, index) => `$${index + 1}`)
            return withTransaction(db, async client => {
               if (entries.length === 0) {
                  const result = await client.query(`INSERT INTO ${model.quotedTable} DEFAULT VALUES RETURNING *`)
                  return result.rows[0]
               }
               const conflictAction = hasClientId
                  ? ' ON CONFLICT (' + model.quotedPrimaryKey + ') DO UPDATE SET '
                     + (entries.some(([column]) => column !== model.primaryKey)
                        ? entries
                           .filter(([column]) => column !== model.primaryKey)
                           .map(([column]) => `${quoteIdentifier(column, 'data column')} = EXCLUDED.${quoteIdentifier(column, 'data column')}`)
                           .join(', ')
                        : `${model.quotedPrimaryKey} = EXCLUDED.${model.quotedPrimaryKey}`)
                  : ''
               const result = await client.query(
                  `INSERT INTO ${model.quotedTable} (${columns.join(', ')}) VALUES (${parameters.join(', ')})`
                  + conflictAction + ' RETURNING *',
                  values,
               )
               return result.rows[0]
            })
         },

         update: async function(id, data) {
            await authorize(this, model.name, 'update', [id, data])
            const set = buildSet(data, model.primaryKey)
            return withTransaction(db, async client => {
               const result = await client.query(
                  `UPDATE ${model.quotedTable} SET ${set.sql} WHERE ${model.quotedPrimaryKey} = $${set.values.length + 1} RETURNING *`,
                  [...set.values, id],
               )
               return result.rows[0]
            })
         },

         delete: async function(id) {
            await authorize(this, model.name, 'delete', [id])
            return withTransaction(db, async client => {
               const result = await client.query(
                  `DELETE FROM ${model.quotedTable} WHERE ${model.quotedPrimaryKey} = $1 RETURNING *`, [id],
               )
               return result.rows[0]
            })
         },
      })
   }

   // uses long-polling request waiting for changes
   app.get(shapePath, async (request, response, next) => {
      try {
         const model = configuredModels.find(candidate => candidate.name === request.params.model)
         if (!model) return response.status(404).json({ error: 'unknown model' })
         await authorize({ app, request, response, transport: 'http' }, model.name, 'shape', [request.query])
         const target = new URL(electricUrl)
         for (const [key, value] of Object.entries(request.query)) {
            if (key === 'table') continue
            if (Array.isArray(value)) value.forEach(entry => target.searchParams.append(key, entry))
            else if (value != null) target.searchParams.set(key, value)
         }
         target.searchParams.set('table', model.table)
         if (options.sourceId) target.searchParams.set('source_id', options.sourceId)
         if (options.sourceSecret) target.searchParams.set('secret', options.sourceSecret)
         const upstream = await fetchImpl(target, { headers: { accept: request.get('accept') ?? '*/*' } })
         response.status(upstream.status)
         copyResponseHeaders(upstream, response)
         response.send(Buffer.from(await upstream.arrayBuffer()))
      } catch (error) {
         next(error)
      }
   })

   return { shapePath, models: configuredModels.map(({ name, table, primaryKey }) => ({ name, table, primaryKey })) }
}


const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
const RANGE_OPERATORS = { gt: '>', gte: '>=', lt: '<', lte: '<=' }

function quoteIdentifier(value, label) {
   if (typeof value !== 'string' || !IDENTIFIER.test(value)) {
      throw new TypeError(`${label} must be a simple SQL identifier`)
   }
   return `"${value}"`
}

function normalizeModels(models) {
   if (!Array.isArray(models) || models.length === 0) {
      throw new TypeError('models must be a non-empty array')
   }
   return models.map(model => {
      const config = typeof model === 'string' ? { name: model } : model
      if (!config || typeof config !== 'object' || Array.isArray(config)) {
         throw new TypeError('each model must be a name or configuration object')
      }
      const name = config.name
      const table = config.table ?? name
      const primaryKey = config.primaryKey ?? 'uid'
      quoteIdentifier(name, 'model name')
      return {
         name,
         table,
         primaryKey,
         quotedTable: quoteIdentifier(table, `table for '${name}'`),
         quotedPrimaryKey: quoteIdentifier(primaryKey, `primary key for '${name}'`),
      }
   })
}

function assertPlainObject(value, label) {
   if (!value || typeof value !== 'object' || Array.isArray(value) || Object.prototype.toString.call(value) !== '[object Object]') {
      throw new TypeError(`${label} must be a plain object`)
   }
}

function buildWhere(where, startIndex = 1) {
   assertPlainObject(where, 'where')
   const clauses = []
   const values = []
   for (const [column, constraint] of Object.entries(where)) {
      const quotedColumn = quoteIdentifier(column, 'where column')
      if (constraint === undefined) continue
      if (constraint === null) {
         clauses.push(`${quotedColumn} IS NULL`)
         continue
      }
      if (constraint && typeof constraint === 'object' && !Array.isArray(constraint) && !(constraint instanceof Date)) {
         const entries = Object.entries(constraint)
         if (entries.length === 0 || entries.some(([operator]) => !RANGE_OPERATORS[operator])) {
            throw new TypeError(`unsupported where constraint for '${column}'`)
         }
         for (const [operator, value] of entries) {
            values.push(value)
            clauses.push(`${quotedColumn} ${RANGE_OPERATORS[operator]} $${startIndex + values.length - 1}`)
         }
         continue
      }
      values.push(constraint)
      clauses.push(`${quotedColumn} = $${startIndex + values.length - 1}`)
   }
   return { sql: clauses.length ? clauses.join(' AND ') : 'TRUE', values }
}

function buildSet(data, primaryKey, startIndex = 1) {
   assertPlainObject(data, 'mutation data')
   const entries = Object.entries(data).filter(([key, value]) => key !== primaryKey && value !== undefined)
   if (entries.length === 0) throw new TypeError('mutation data must contain at least one field')
   return {
      sql: entries.map(([column], index) => `${quoteIdentifier(column, 'data column')} = $${startIndex + index}`).join(', '),
      values: entries.map(([, value]) => value),
   }
}

async function withTransaction(db, operation) {
   if (typeof db?.connect !== 'function') return operation(db)
   const client = await db.connect()
   try {
      await client.query('BEGIN')
      const result = await operation(client)
      await client.query('COMMIT')
      return result
   } catch (error) {
      await client.query('ROLLBACK')
      throw error
   } finally {
      client.release()
   }
}

function copyResponseHeaders(source, target) {
   source.headers.forEach((value, key) => {
      if (!['content-encoding', 'content-length', 'transfer-encoding'].includes(key.toLowerCase())) {
         target.setHeader(key, value)
      }
   })
}
