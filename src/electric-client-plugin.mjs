import { Shape, ShapeStream } from '@electric-sql/client'
import { firstValueFrom, Observable, Subject, takeUntil } from 'rxjs'
import { getCurrentScope, onScopeDispose, ref } from 'vue'


/**
 * Add Electric-backed reactive models to an Express-X client.
 *
 * Usage:
 *   electricClientPlugin(app)
 *   const todo = app.createElectricModel('todos')
 *   todo.getObservable({ completed: false }).subscribe(...)
 *   const completedTodos = await todo.findMany({ completed: false })
 */
export function electricClientPlugin(app, options = {}) {
   const shapePath = options.shapePath ?? '/electric/v1/shape'
   const ShapeStreamClass = options.ShapeStream ?? ShapeStream
   const ShapeClass = options.Shape ?? Shape
   const ObservableClass = options.Observable ?? Observable

   function createElectricModel(modelName, modelOptions = {}) {
      quoteIdentifier(modelName)
      const service = app.service(modelName)
      const url = modelOptions.url ?? modelPath(shapePath, modelName)
      const streamOptions = modelOptions.streamOptions ?? {}

      function getObservable(where = {}) {
         // Validate eagerly
         const filterParams = whereToElectricParams(where)
         return new ObservableClass(subscriber => {
            const stream = new ShapeStreamClass({
               ...streamOptions,
               url,
               params: { ...streamOptions.params, ...filterParams },
            })
            const shape = new ShapeClass(stream)
            let previous
            const unsubscribe = shape.subscribe(({ rows }) => {
               const current = [...rows]
               const serialized = JSON.stringify(current)
               if (serialized === previous) return
               previous = serialized
               subscriber.next(current)
            })
            // Shape exposes errors as state. ShapeStream retries transient failures;
            // callers keep one observable subscription across reconnects
            return () => unsubscribe()
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

      async function create(data) {
         assertPlainObject(data, 'mutation data')
         const uid = globalThis.crypto?.randomUUID?.()
         if (!uid) throw new Error('crypto.randomUUID() is required')
         return service.create(uid, data)
      }

      async function update(uid, data) {
         return service.update(uid, data)
      }

      async function remove(uid) {
         return service.delete(uid)
      }

      return { getObservable, findMany, create, update, remove }
   }

   return Object.assign(app, { createElectricModel })
}


//////////////////////                   UTILITIES                   //////////////////////

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
const RANGE_OPERATORS = { gt: '>', gte: '>=', lt: '<', lte: '<=' }

function quoteIdentifier(value) {
   if (typeof value !== 'string' || !IDENTIFIER.test(value)) {
      throw new TypeError(`'${value}' must be a simple SQL identifier`)
   }
   return `"${value}"`
}

function assertPlainObject(value, label) {
   if (!value || typeof value !== 'object' || Array.isArray(value) || Object.prototype.toString.call(value) !== '[object Object]') {
      throw new TypeError(`${label} must be a plain object`)
   }
}

function serializeValue(value, path) {
   if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) throw new TypeError(`${path} contains an invalid Date`)
      return value.toISOString()
   }
   if (['string', 'number', 'boolean'].includes(typeof value) && value !== undefined) {
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

function modelPath(shapePath, modelName) {
   const base = shapePath.replace(/\/$/, '')
   return `${base}/${encodeURIComponent(modelName)}`
}

export default electricClientPlugin
