// Restored from express-x commit edb1db3 (last complete custom sync algorithm).

// This sync algorithm uses the metadata table as its only source of truth for existence and recency. Manual SQL changes
// create a split reality — the data table and the metadata table diverge — and the algorithm's set-intersection logic
// misidentifies the divergence as intentional client-side changes that must be propagated.
// If you need to seed or manipulate data outside the framework, you must also write the corresponding rows into the
// metadata table with valid created_at/updated_at/deleted_at values.

// The metadata table is a tombstone log that grows monotonically. Every record ever created leaves a permanent entry. For
// long-lived applications with high churn this is worth planning for: you can only safely purge a tombstone once you're certain no
// client still holds a copy of that record (i.e., every client has synced at least once after the deletion). There's currently no
// pruning mechanism in the framework.

export function computeSyncResult(databaseValuesDict, clientMetadataDict, databaseMetadataDict) {
   assertValidMetadataTimestamps(clientMetadataDict, 'client')
   assertValidMetadataTimestamps(databaseMetadataDict, 'database')
   const onlyDatabaseIds = new Set()
   const onlyClientIds = new Set()
   const databaseAndClientIds = new Set()

   for (const uid of Object.keys(databaseValuesDict)) {
      if (Object.hasOwn(clientMetadataDict, uid)) databaseAndClientIds.add(uid)
      else onlyDatabaseIds.add(uid)
   }
   for (const uid of Object.keys(clientMetadataDict)) {
      if (Object.hasOwn(databaseValuesDict, uid)) databaseAndClientIds.add(uid)
      else onlyClientIds.add(uid)
   }

   const addDatabase = [], updateDatabase = [], deleteDatabase = []
   const addClient = [], updateClient = [], deleteClient = []

   for (const uid of onlyDatabaseIds) {
      const databaseMetaData = databaseMetadataDict[uid] || { uid, created_at: null }
      if (databaseMetaData.deleted_at) {
         deleteDatabase.push(uid)
      } else {
         addClient.push([databaseValuesDict[uid], databaseMetaData])
      }
   }

   for (const uid of onlyClientIds) {
      const clientMetaData = clientMetadataDict[uid]
      const databaseMetaData = databaseMetadataDict[uid]
      if (databaseMetaData?.deleted_at) {
         const clientUpdatedAt = new Date(clientMetaData.deleted_at || clientMetaData.updated_at || clientMetaData.created_at)
         const databaseDeletedAt = new Date(databaseMetaData.deleted_at)
         if (databaseDeletedAt >= clientUpdatedAt) {
            deleteClient.push([uid, databaseMetaData.deleted_at])
         } else if (clientMetaData.deleted_at) {
            deleteDatabase.push(uid)
            deleteClient.push([uid, clientMetaData.deleted_at])
         } else {
            addDatabase.push(clientMetaData)
         }
      } else if (databaseMetaData) {
         const clientUpdatedAt = new Date(clientMetaData.deleted_at || clientMetaData.updated_at || clientMetaData.created_at)
         const databaseUpdatedAt = new Date(databaseMetaData.updated_at || databaseMetaData.created_at)
         if (databaseUpdatedAt >= clientUpdatedAt) {
            deleteDatabase.push(uid)
            deleteClient.push([uid, databaseUpdatedAt])
         } else if (clientMetaData.deleted_at) {
            deleteDatabase.push(uid)
            deleteClient.push([uid, clientMetaData.deleted_at])
         } else {
            addDatabase.push(clientMetaData)
         }
      } else if (clientMetaData.deleted_at) {
         deleteClient.push([uid, clientMetaData.deleted_at])
      } else {
         addDatabase.push(clientMetaData)
      }
   }

   for (const uid of databaseAndClientIds) {
      const clientMetaData = clientMetadataDict[uid]
      const databaseMetaData = databaseMetadataDict[uid] || { uid, created_at: null }
      if (databaseMetaData.deleted_at) {
         const clientUpdatedAt = new Date(clientMetaData.deleted_at || clientMetaData.updated_at || clientMetaData.created_at)
         const databaseDeletedAt = new Date(databaseMetaData.deleted_at)
         if (databaseDeletedAt >= clientUpdatedAt) {
            deleteDatabase.push(uid)
            deleteClient.push([uid, databaseMetaData.deleted_at])
         } else if (clientMetaData.deleted_at) {
            deleteDatabase.push(uid)
            deleteClient.push([uid, clientMetaData.deleted_at])
         } else {
            updateDatabase.push(clientMetaData)
         }
      } else if (clientMetaData.deleted_at) {
         const clientDeletedAt = new Date(clientMetaData.deleted_at)
         const databaseUpdatedAt = new Date(databaseMetaData.updated_at || databaseMetaData.created_at)
         const diff = clientDeletedAt - databaseUpdatedAt
         if (diff >= 0) {
            deleteDatabase.push(uid)
            deleteClient.push([uid, clientMetaData.deleted_at])
         } else {
            updateClient.push([databaseValuesDict[uid], databaseMetaData])
         }
      } else {
         const clientUpdatedAt = new Date(clientMetaData.updated_at || clientMetaData.created_at)
         const databaseUpdatedAt = new Date(databaseMetaData.updated_at || databaseMetaData.created_at)
         const diff = clientUpdatedAt - databaseUpdatedAt
         if (diff > 0) updateDatabase.push(clientMetaData)
         else if (diff < 0) updateClient.push([databaseValuesDict[uid], databaseMetaData])
      }
   }

   return {
      addClient,
      updateClient,
      deleteClient,
      addDatabase,
      updateDatabase,
      deleteDatabase,
   }
}

function assertValidMetadataTimestamps(metadataDict, source) {
   if (!metadataDict || typeof metadataDict !== 'object' || Array.isArray(metadataDict)) {
      throw new TypeError(`${source} metadata must be an object`)
   }
   for (const [uid, metadata] of Object.entries(metadataDict)) {
      if (!metadata || typeof metadata !== 'object') {
         throw new TypeError(`${source} metadata for '${uid}' must be an object`)
      }
      if (metadata.uid !== uid) {
         throw new TypeError(`${source} metadata key '${uid}' does not match uid '${metadata.uid}'`)
      }
      const timestampFields = ['created_at', 'updated_at', 'deleted_at']
      // Timestamp-free database metadata can exist transiently while the
      // Drizzle plugin locks/repairs a row. Client metadata must always carry
      // a usable version.
      if (source !== 'database' && !timestampFields.some(field => metadata[field] != null)) {
         throw new TypeError(`${source} metadata '${uid}' must contain a timestamp`)
      }
      for (const field of timestampFields) {
         const value = metadata[field]
         if (value == null) continue
         if (Number.isNaN(new Date(value).getTime())) {
            throw new TypeError(`${source} metadata '${uid}.${field}' must be a valid timestamp`)
         }
      }
   }
}

