import { createPGCouchClient } from 'pg-couch/client'

/** Attach the pg-couch client API using the existing express-x model aliases. */
export function pgcouchClientPlugin(app, options = {}) {
   const client = createPGCouchClient(options)
   return Object.assign(app, {
      createPGCouchModel: client.model,
      createElectricModel: client.model,
      pgcouchTransaction: client.transaction,
      disposePGCouchSync: client.dispose,
      disposeElectricSync: client.dispose,
      pgcouchReady: client.ready,
   })
}

export default pgcouchClientPlugin
