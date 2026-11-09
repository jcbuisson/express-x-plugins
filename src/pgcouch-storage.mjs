// Credentials and URL query/fragment options do not identify a CouchDB database.
// Hash the endpoint so storage names contain neither credentials nor URL syntax.
export async function storageForRemote(remote, origin, prefix = 'express-x') {
   if (typeof prefix !== 'string' || !/^[A-Za-z0-9_-]+$/.test(prefix)) throw new TypeError('Invalid storage prefix')
   const url = new URL(remote, origin)
   const identity = new URL(url)
   identity.username = ''
   identity.password = ''
   identity.search = ''
   identity.hash = ''
   identity.pathname = identity.pathname.replace(/\/+$/, '') || '/'
   const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(identity.href))
   const namespace = Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('')
   return {
      remote: url.href,
      relational: `idb://${prefix}-pgcouch-relational-${namespace}`,
      transactions: `${prefix}-pgcouch-transactions-${namespace}-indexeddb`,
      lock: `${prefix}-pgcouch-owner-${namespace}`,
      worker: `${prefix}-pgcouch-${namespace}`,
   }
}

// Changing PouchDB adapters creates a new local change sequence. Reset only the
// transport cursor; keep authoritative rows, applied sequence and offline edits.
export async function prepareLocalTransport(db, name) {
   await db.transaction(async tx => {
      await tx.exec('CREATE TABLE IF NOT EXISTS offline_sync_transport (id INTEGER PRIMARY KEY CHECK (id = 1), name TEXT NOT NULL)')
      const prior = (await tx.query('SELECT name FROM offline_sync_transport WHERE id = 1')).rows[0]
      if (prior?.name === name) return
      const { rows } = await tx.query("SELECT to_regclass('pgcouch_local_state') IS NOT NULL AS exists")
      if (rows[0].exists) await tx.query("UPDATE pgcouch_local_state SET cursor = '0'::jsonb WHERE id = 1")
      await tx.query('INSERT INTO offline_sync_transport VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name', [name])
   })
}
