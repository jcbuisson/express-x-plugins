/**
 * Register Express-X mutation services and an Electric Shape proxy.
 *
 * @param {object} app Express-X/Express application
 * @param {object} db pg-compatible Pool or Client exposing query()
 * @param {(string|{name:string,table?:string,primaryKey?:string})[]} models
 * @param {object} options
 */
export function electricServerPlugin(app, db, models, options = {}) {
   if (!db || typeof db.query !== 'function') throw new TypeError('db must expose query(sql, values)')
   // if (typeof options.authorize !== 'function') {
   //    throw new TypeError('electricServerPlugin requires an authorize(context, operation) policy')
   // }
   const configuredModels = normalizeModels(models)
   const sync = options.sync === true
   if (sync && typeof db.connect !== 'function') throw new TypeError('sync requires a pg Pool with connect()')
   const electricUrl = new URL(options.electricUrl ?? process.env.ELECTRIC_URL ?? 'http://localhost:3000/v1/shape')
   const shapePath = options.shapePath ?? '/electric/v1/shape/:model'
   const fetchImpl = options.fetch ?? globalThis.fetch
   if (typeof fetchImpl !== 'function') throw new TypeError('a fetch implementation is required')
   const authorizeFunc = options.authorize || (() => true)
   if (typeof authorizeFunc !== 'function') throw new TypeError('an authorize function is required')

   async function authorize(context, modelName, action, args) {
      const allowed = await authorizeFunc(context, { modelName, action, args })
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
         // create(id, data): id (the primary key) is provided by the client
         create: async function(idOrData, data, mutation) {
            const hasClientId = data !== undefined
            const mutationData = hasClientId ? data : idOrData
            await authorize(this, model.name, 'create', hasClientId
               ? (sync ? [idOrData, mutationData, mutation] : [idOrData, mutationData])
               : [mutationData])
            assertPlainObject(mutationData, 'mutation data')
            const safeData = hasClientId
               ? { ...mutationData, [model.primaryKey]: idOrData }
               : { ...mutationData }
            if (sync) {
               if (!hasClientId) throw new TypeError('sync mutations require a client-generated id')
               return syncMutation(db, model, 'create', idOrData, safeData, mutation)
            }
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

         update: async function(id, data, mutation) {
            await authorize(this, model.name, 'update', sync ? [id, data, mutation] : [id, data])
            if (sync) return syncMutation(db, model, 'update', id, data, mutation)
            const set = buildSet(data, model.primaryKey)
            return withTransaction(db, async client => {
               const result = await client.query(
                  `UPDATE ${model.quotedTable} SET ${set.sql} WHERE ${model.quotedPrimaryKey} = $${set.values.length + 1} RETURNING *`,
                  [...set.values, id],
               )
               return result.rows[0]
            })
         },

         delete: async function(id, mutation) {
            await authorize(this, model.name, 'delete', sync ? [id, mutation] : [id])
            if (sync) return syncMutation(db, model, 'delete', id, null, mutation)
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

   return { shapePath, sync, models: configuredModels.map(({ name, table, primaryKey }) => ({ name, table, primaryKey })) }
}

/** Run once before accepting sync mutations. Existing rows receive a version. */
export async function prepareElectricSyncSchema(db, models) {
   const configuredModels = normalizeModels(models)
   await db.query('CREATE SEQUENCE IF NOT EXISTS electric_sync_version_seq')
   await db.query(`CREATE TABLE IF NOT EXISTS electric_mutation_cursor (
      table_name TEXT NOT NULL, client_id UUID NOT NULL, row_id TEXT NOT NULL,
      revision BIGINT NOT NULL DEFAULT 0, result JSONB,
      PRIMARY KEY (table_name, client_id, row_id)
   )`)
   for (const model of configuredModels) {
      await db.query(`ALTER TABLE ${model.quotedTable} ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT nextval('electric_sync_version_seq')`)
      await db.query(`ALTER TABLE ${model.quotedTable} ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT false`)
   }
}

async function syncMutation(db, model, action, id, data, mutation) {
   if (!mutation || typeof mutation !== 'object') throw new TypeError('sync mutation metadata is required')
   const { clientId, revision } = mutation
   if (typeof clientId !== 'string' || !UUID.test(clientId)) throw new TypeError('clientId must be a UUID')
   if (!/^[1-9][0-9]*$/.test(String(revision)) || BigInt(revision) > 9223372036854775807n) {
      throw new TypeError('revision must be a positive bigint')
   }
   if (typeof id !== 'string' || !UUID.test(id)) throw new TypeError('sync id must be a UUID')
   if (action !== 'delete') assertPlainObject(data, 'mutation data')
   if (data && ('version' in data || 'deleted' in data)) throw new TypeError('version and deleted are managed by sync')
   return withTransaction(db, async tx => {
      const key = [model.table, clientId, String(id)]
      await tx.query(`INSERT INTO electric_mutation_cursor (table_name, client_id, row_id)
         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, key)
      const { rows } = await tx.query(`SELECT revision, result FROM electric_mutation_cursor
         WHERE table_name = $1 AND client_id = $2 AND row_id = $3 FOR UPDATE`, key)
      const cursor = rows[0]
      if (BigInt(revision) <= BigInt(cursor.revision)) return cursor.result
      let result
      if (action === 'create') {
         const entries = Object.entries({ ...data, [model.primaryKey]: id }).filter(([, value]) => value !== undefined)
         const columns = entries.map(([column]) => quoteIdentifier(column, 'data column'))
         const values = entries.map(([, value]) => value)
         const parameters = values.map((_, index) => `$${index + 1}`)
         const inserted = await tx.query(`INSERT INTO ${model.quotedTable} (${columns.join(', ')}) VALUES (${parameters.join(', ')})
            ON CONFLICT (${model.quotedPrimaryKey}) DO UPDATE SET ${model.quotedPrimaryKey} = EXCLUDED.${model.quotedPrimaryKey}
            RETURNING *`, values)
         result = inserted.rows[0]
      } else if (action === 'update') {
         const set = buildSet(data, model.primaryKey)
         const updated = await tx.query(`UPDATE ${model.quotedTable} SET ${set.sql}, version = nextval('electric_sync_version_seq')
            WHERE ${model.quotedPrimaryKey} = $${set.values.length + 1} AND NOT deleted RETURNING *`, [...set.values, id])
         result = updated.rows[0] ?? await writeTombstone(tx, model, id)
      } else {
         result = await writeTombstone(tx, model, id)
      }
      await tx.query(`UPDATE electric_mutation_cursor SET revision = $4, result = $5::jsonb
         WHERE table_name = $1 AND client_id = $2 AND row_id = $3`, [...key, String(revision), JSON.stringify(result ?? null)])
      return result
   })
}

async function writeTombstone(tx, model, id) {
   const data = typeof model.tombstoneData === 'function'
      ? await model.tombstoneData({ id })
      : model.tombstoneData
   validateTombstoneData(data, model.primaryKey)
   const tombstone = Object.entries(data)
   const columns = [model.quotedPrimaryKey, ...tombstone.map(([column]) => quoteIdentifier(column, 'tombstone column')), 'deleted']
   const values = [id, ...tombstone.map(([, value]) => value)]
   const parameters = values.map((_, index) => `$${index + 1}`)
   const clearFields = tombstone.map(([column]) => `${quoteIdentifier(column, 'tombstone column')} = EXCLUDED.${quoteIdentifier(column, 'tombstone column')}`)
   const { rows } = await tx.query(`INSERT INTO ${model.quotedTable} (${columns.join(', ')}) VALUES (${parameters.join(', ')}, true)
      ON CONFLICT (${model.quotedPrimaryKey}) DO UPDATE SET ${clearFields.length ? clearFields.join(', ') + ', ' : ''}deleted = true, version = nextval('electric_sync_version_seq')
      RETURNING *`, values)
   return rows[0]
}


const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
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
      const primaryKey = config.primaryKey ?? 'id'
      quoteIdentifier(name, 'model name')
      const tombstoneData = config.tombstoneData ?? {}
      if (typeof tombstoneData !== 'function') validateTombstoneData(tombstoneData, primaryKey)
      return {
         name,
         table,
         primaryKey,
         tombstoneData,
         quotedTable: quoteIdentifier(table, `table for '${name}'`),
         quotedPrimaryKey: quoteIdentifier(primaryKey, `primary key for '${name}'`),
      }
   })
}

function validateTombstoneData(data, primaryKey) {
   assertPlainObject(data, 'tombstoneData')
   for (const column of Object.keys(data)) {
      quoteIdentifier(column, 'tombstone column')
      if ([primaryKey, 'version', 'deleted'].includes(column)) throw new TypeError('tombstoneData contains a sync-managed column')
   }
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
