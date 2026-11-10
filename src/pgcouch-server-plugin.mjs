import { createPGCouchServer } from 'pg-couch/server'

export { preparePGCouchSchema, pgcouchValidationDocument } from 'pg-couch/server'

/** Register express-x services backed by pg-couch, preserving request context. */
export function pgcouchServerPlugin(app, db, models, options = {}) {
   const serviceName = options.serviceName ?? 'pgcouch'
   if (models?.some(model => (typeof model === 'string' ? model : model.name) === serviceName)) {
      throw new TypeError('Transaction service name conflicts with a model name')
   }
   const server = createPGCouchServer({
      ...options, db, models,
      contextForDocument: options.contextForDocument ?? (() => ({ app, transport: 'couch' })),
   })
   for (const model of server.models) {
      const methods = {}
      for (const method of ['findMany', 'findUnique', 'create', 'update', 'delete']) {
         methods[method] = function (...args) {
            return server.model(model.name, this)[method](...args)
         }
      }
      app.createService(model.name, methods)
   }
   app.createService(serviceName, {
      transaction(operations, id) { return server.submit(operations, this, id) },
   })
   return server
}

export default pgcouchServerPlugin
