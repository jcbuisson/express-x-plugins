import { storageForRemote as pgCouchStorageForRemote } from 'pg-couch/storage'

export { prepareLocalTransport } from 'pg-couch/storage'

// Preserve existing express-x database names when delegating to pg-couch.
export function storageForRemote(remote, origin, prefix = 'express-x') {
   return pgCouchStorageForRemote(remote, origin, prefix)
}
