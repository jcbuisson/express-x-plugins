const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
const RANGE_OPERATORS = { gt: '>', gte: '>=', lt: '<', lte: '<=' }

export function quoteIdentifier(value) {
   if (typeof value !== 'string' || !IDENTIFIER.test(value)) {
      throw new TypeError(`'${value}' must be a simple SQL identifier`)
   }
   return `"${value}"`
}

export function assertPlainObject(value, label) {
   if (!value || typeof value !== 'object' || Array.isArray(value) || Object.prototype.toString.call(value) !== '[object Object]') {
      throw new TypeError(`${label} must be a plain object`)
   }
}

function serializeValue(value, path) {
   if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) throw new TypeError(`${path} contains an invalid Date`)
      return value.toISOString()
   }
   if (['string', 'number', 'bigint', 'boolean'].includes(typeof value) && value !== undefined) {
      if (typeof value === 'number' && !Number.isFinite(value)) {
         throw new TypeError(`${path} contains a non-finite number`)
      }
      return String(value)
   }
   throw new TypeError(`${path} contains an unsupported value`)
}

/** Convert the Express-X object filter into Electric's parameterized SQL filter. */
export function whereToElectricParams(where = {}) {
   assertPlainObject(where, 'where')
   const clauses = []
   const params = []

   for (const [column, constraint] of Object.entries(where)) {
      const quotedColumn = quoteIdentifier(column)
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
            params.push(serializeValue(value, `where.${column}.${operator}`))
            clauses.push(`${quotedColumn} ${RANGE_OPERATORS[operator]} $${params.length}`)
         }
         continue
      }
      params.push(serializeValue(constraint, `where.${column}`))
      clauses.push(`${quotedColumn} = $${params.length}`)
   }

   return clauses.length ? { where: clauses.join(' AND '), params } : {}
}
