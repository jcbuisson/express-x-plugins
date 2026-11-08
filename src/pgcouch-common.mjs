
// Shared SQL and protocol helpers

export function quote(value) {
   if (typeof value !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) throw new TypeError('Expected a simple SQL identifier')
   return `"${value}"`
}

export function object(value) {
   if (!value || Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError('Expected a plain object')
   return value
}

export function modelsOf(models) {
   if (!Array.isArray(models) || !models.length) throw new TypeError('models must be a non-empty array')
   const result = models.map(value => {
      const config = typeof value === 'string' ? { name: value } : value
      const model = { ...config, table: config.table ?? config.name, primaryKey: config.primaryKey ?? 'uid' }
      for (const key of ['name', 'table', 'primaryKey']) quote(model[key])
      if (model.table.startsWith('pgcouch_')) throw new TypeError('pgcouch_ tables are reserved')
      return model
   })
   if (new Set(result.map(m => m.name)).size !== result.length || new Set(result.map(m => m.table)).size !== result.length) throw new TypeError('Duplicate model or table')
   return result
}

export function json(value) {
   return JSON.stringify(value, (_key, item) => typeof item === 'bigint' ? String(item) : item)
}

export function documentOf(value, models) {
   object(value)
   if (value._id === 'transaction:bootstrap' || !/^transaction:[A-Za-z0-9_-]{1,128}$/.test(value._id) || value.type !== 'transaction' || value.status !== 'pending') throw new TypeError('Invalid transaction document')
   if (value.previous != null && !/^transaction:[A-Za-z0-9_-]{1,128}$/.test(value.previous)) throw new TypeError('Invalid previous transaction')
   if (value.previous === value._id) throw new TypeError('A transaction cannot depend on itself')
   if (!Array.isArray(value.operations) || !value.operations.length || value.operations.length > 1000) throw new TypeError('Expected 1–1000 operations')
   const operations = value.operations.map(op => {
      object(op)
      const model = models.find(m => m.table === op.table)
      if (!model || !['insert', 'update', 'delete'].includes(op.action)) throw new TypeError('Unknown table or action')
      if (!['string', 'number'].includes(typeof op.id) || op.id === '' || (typeof op.id === 'number' && !Number.isSafeInteger(op.id))) throw new TypeError('An explicit string or safe integer id is required')
      const data = op.action === 'delete' ? undefined : object(op.data)
      if (data) for (const key of Object.keys(data)) {
         quote(key)
         if (key === model.primaryKey && data[key] !== op.id) throw new TypeError('Primary key cannot change')
      }
      return { action: op.action, table: op.table, id: op.id, ...(data ? { data } : {}) }
   })
   return JSON.parse(json({ _id: value._id, type: 'transaction', status: 'pending', operations, ...(value.previous ? { previous: value.previous } : {}) }))
}

export async function transaction(db, run) {
   if (typeof db.connect !== 'function' && typeof db.transaction === 'function') return db.transaction(run)
   if (typeof db.connect !== 'function') throw new TypeError('A pg Pool or PGLite transaction-capable database is required')
   const tx = await db.connect()
   try {
      await tx.query('BEGIN')
      const value = await run(tx)
      await tx.query('COMMIT')
      return value
   } catch (error) {
      await tx.query('ROLLBACK')
      throw error
   } finally { tx.release() }
}

export async function applyOperations(tx, operations, models, { deferConstraints = false } = {}) {
   const results = []
   await tx.query('SET CONSTRAINTS ALL DEFERRED')
   for (const op of operations) {
      const model = models.find(m => m.table === op.table)
      const table = quote(model.table), pk = quote(model.primaryKey)
      let result
      if (op.action === 'delete') result = await tx.query(`DELETE FROM ${table} WHERE ${pk} = $1 RETURNING *`, [op.id])
      else {
         const entries = Object.entries({ ...op.data, ...(op.action === 'insert' ? { [model.primaryKey]: op.id } : {}) })
            .filter(([key, value]) => value !== undefined && (op.action === 'insert' || key !== model.primaryKey))
         if (!entries.length) throw new TypeError('An update requires at least one field')
         const values = entries.map(([, value]) => value)
         if (op.action === 'insert') result = await tx.query(`INSERT INTO ${table} (${entries.map(([key]) => quote(key)).join(', ')}) VALUES (${values.map((_, i) => `$${i + 1}`).join(', ')}) RETURNING *`, values)
         else result = await tx.query(`UPDATE ${table} SET ${entries.map(([key], i) => `${quote(key)} = $${i + 1}`).join(', ')} WHERE ${pk} = $${values.length + 1} RETURNING *`, [...values, op.id])
      }
      if (!result.rows.length && op.action === 'update') throw Object.assign(new Error('Row does not exist'), { code: 'not-found' })
      results.push(result.rows[0] ?? null)
   }
   // Surface deferred violations before recording a successful decision.
   if (!deferConstraints) await tx.query('SET CONSTRAINTS ALL IMMEDIATE')
   return results
}

export function whereSQL(where = {}) {
   object(where)
   const clauses = [], values = []
   const operators = { gt: '>', gte: '>=', lt: '<', lte: '<=' }
   for (const [key, value] of Object.entries(where)) {
      const column = quote(key)
      if (value === undefined) continue
      if (value === null) { clauses.push(`${column} IS NULL`); continue }
      if (typeof value === 'object' && !(value instanceof Date)) {
         const entries = Object.entries(object(value))
         if (!entries.length) throw new TypeError('Empty range constraint')
         for (const [op, operand] of entries) {
            if (!operators[op]) throw new TypeError('Unsupported where operator')
            values.push(operand); clauses.push(`${column} ${operators[op]} $${values.length}`)
         }
      } else { values.push(value); clauses.push(`${column} = $${values.length}`) }
   }
   return { sql: clauses.join(' AND ') || 'TRUE', values }
}

export async function readRows(db, model, where = {}, options = {}) {
   const filter = whereSQL(where)
   let sql = `SELECT * FROM ${quote(model.table)} WHERE ${filter.sql} ORDER BY ${quote(model.primaryKey)}`
   if (options.limit != null) {
      if (!Number.isInteger(options.limit) || options.limit < 1) throw new TypeError('limit must be positive')
      filter.values.push(options.limit); sql += ` LIMIT $${filter.values.length}`
   }
   return (await db.query(sql, filter.values)).rows
}

export async function snapshot(tx, models) {
   const tables = {}
   for (const model of models) tables[model.table] = await readRows(tx, model)
   return JSON.parse(json(tables))
}

export async function putImmutable(couch, doc) {
   try { await couch.put(doc) } catch (error) {
      if (error.status !== 409) throw error
      const existing = await couch.get(doc._id)
      const clean = ({ _rev, _conflicts, ...value }) => value
      if (canonical(clean(existing)) !== canonical(clean(doc))) throw new Error(`Conflicting immutable document: ${doc._id}`)
   }
}

export function canonical(value) {
   if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
   if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`
   return JSON.stringify(value)
}
