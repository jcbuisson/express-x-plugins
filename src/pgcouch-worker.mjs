import { startPGCouchWorker as startWorker } from 'pg-couch/worker'

// Preserve existing express-x database names for application worker entries.
export function startPGCouchWorker(options) {
   return startWorker({ ...options, storagePrefix: options?.storagePrefix ?? 'express-x' })
}
