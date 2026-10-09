import { Observable } from 'rxjs'

/** Offline model backed by a shared PGlite database (normally PGliteWorker). */
export function createOfflineElectricModel({ db, service, modelName, url, primaryKey, ShapeStreamClass, ShapeClass, ownsSync, channel, network, retryMs, streamOptions = {}, onError }) {
   if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(primaryKey)) throw new TypeError('primaryKey must be a simple SQL identifier')
   let snapshot = null
   let operation = Promise.resolve()
   let flushing = false
   let started = false
   let connected = false
   let timer
   let stream
   let streamGeneration = 0
   let lastError = null
   const pendingBackground = new Set()
   function report(error) {
      lastError = error?.message ?? String(error)
      try { onError?.(error, { modelName }) } catch (callbackError) { console.error(callbackError) }
      notify()
   }
   function background(promise) {
      pendingBackground.add(promise)
      void promise.catch(report).finally(() => pendingBackground.delete(promise))
   }
   const listeners = new Set()

   async function prepare() {
      await retryLeader(() => db.exec(`
         CREATE SEQUENCE IF NOT EXISTS electric_mutation_revision_seq;
         CREATE TABLE IF NOT EXISTS electric_sync_client (
            singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
            id UUID NOT NULL DEFAULT gen_random_uuid()
         );
         INSERT INTO electric_sync_client (singleton) VALUES (true) ON CONFLICT DO NOTHING;
         CREATE TABLE IF NOT EXISTS electric_local_rows (
            model_name TEXT NOT NULL, row_id TEXT NOT NULL, data JSONB NOT NULL,
            PRIMARY KEY (model_name, row_id)
         );
         CREATE TABLE IF NOT EXISTS electric_mutation_queue (
            model_name TEXT NOT NULL, row_id TEXT NOT NULL,
            action TEXT NOT NULL CHECK (action IN ('create', 'update', 'delete')),
            data JSONB, revision BIGINT NOT NULL DEFAULT nextval('electric_mutation_revision_seq'),
            acknowledged_version NUMERIC,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'failed')),
            failure_reason TEXT,
            PRIMARY KEY (model_name, row_id)
         );
      `))
   }

   function notify() {
      channel?.postMessage({ modelName, ...(ownsSync ? { online: connected && network?.onLine !== false } : {}) })
      for (const listener of listeners) queueMicrotask(() => { try { listener() } catch (error) { console.error(error) } })
   }

   function subscribe(listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
   }

   async function findMany(where = {}) {
      const { rows } = await retryLeader(() => db.query('SELECT data FROM electric_local_rows WHERE model_name = $1 ORDER BY row_id', [modelName]))
      return rows.map(row => row.data).filter(row => matchesWhere(row, where))
   }

   async function findUnique(where) {
      return (await findMany(where))[0] ?? null
   }

   function getObservable(where = {}) {
      return new Observable(subscriber => {
         let active = true
         const emit = () => { void findMany(where).then(rows => { if (active) subscriber.next(rows) }, error => subscriber.error(error)) }
         const unsubscribe = subscribe(emit)
         emit()
         return () => { active = false; unsubscribe() }
      })
   }

   async function create(data) {
      assertData(data)
      if ('version' in data || 'deleted' in data) throw new TypeError('version and deleted are managed by sync')
      const id = data[primaryKey] ?? globalThis.crypto?.randomUUID?.()
      if (!id) throw new Error('crypto.randomUUID() is required')
      assertId(id)
      const row = { ...data, [primaryKey]: id }
      await db.transaction(async tx => {
         await tx.query('INSERT INTO electric_local_rows (model_name, row_id, data) VALUES ($1, $2, $3::jsonb)', [modelName, String(id), json(row)])
         await queueMutation(tx, id, 'create', row)
      })
      notify()
      if (started) background(flushQueue())
      return row
   }

   async function update(id, data) {
      assertId(id)
      assertData(data)
      if (primaryKey in data || 'version' in data || 'deleted' in data) throw new TypeError('sync-managed columns cannot be updated')
      let row
      await db.transaction(async tx => {
         const current = await tx.query('SELECT data FROM electric_local_rows WHERE model_name = $1 AND row_id = $2', [modelName, String(id)])
         if (!current.rows[0]) throw new Error('row not found')
         row = { ...current.rows[0].data, ...data }
         await tx.query('UPDATE electric_local_rows SET data = $3::jsonb WHERE model_name = $1 AND row_id = $2', [modelName, String(id), json(row)])
         const queued = await tx.query('SELECT action, acknowledged_version FROM electric_mutation_queue WHERE model_name = $1 AND row_id = $2', [modelName, String(id)])
         if (queued.rows[0]?.action === 'delete') throw new Error('row has a pending delete')
         const action = queued.rows[0]?.action === 'create' && queued.rows[0].acknowledged_version == null ? 'create' : 'update'
         await queueMutation(tx, id, action, row)
      })
      notify()
      if (started) background(flushQueue())
      return row
   }

   async function remove(id) {
      assertId(id)
      await db.transaction(async tx => {
         await tx.query('DELETE FROM electric_local_rows WHERE model_name = $1 AND row_id = $2', [modelName, String(id)])
         await queueMutation(tx, id, 'delete', null)
      })
      notify()
      if (started) background(flushQueue())
   }

   async function queueMutation(tx, id, action, data) {
      await tx.query(`INSERT INTO electric_mutation_queue (model_name, row_id, action, data)
         VALUES ($1, $2, $3, $4::jsonb)
         ON CONFLICT (model_name, row_id) DO UPDATE SET
            action = EXCLUDED.action, data = EXCLUDED.data,
            revision = nextval('electric_mutation_revision_seq'),
            acknowledged_version = NULL, status = 'pending', failure_reason = NULL`,
      [modelName, String(id), action, data == null ? null : json(data)])
   }

   function ordered(task) {
      const current = operation.then(task)
      operation = current.catch(() => {})
      return current
   }

   function applySnapshot(rows) {
      return ordered(async () => {
         snapshot = rows
         await reconcileSnapshot(rows)
      })
   }

   function reconcile() {
      return ordered(async () => {
         if (snapshot !== null) await reconcileSnapshot(snapshot)
      })
   }

   function resetSnapshot() {
      return ordered(() => { snapshot = null })
   }

   async function reconcileSnapshot(rows) {
      await retryLeader(() => db.transaction(async tx => {
         for (const row of rows) {
            const id = String(row[primaryKey])
            if (row.version != null) {
               await tx.query(`DELETE FROM electric_mutation_queue
                  WHERE model_name = $1 AND row_id = $2 AND acknowledged_version <= $3::numeric`,
               [modelName, id, String(row.version)])
            }
            if (row.deleted) continue
            await tx.query(`INSERT INTO electric_local_rows (model_name, row_id, data)
               SELECT $1, $2, $3::jsonb WHERE NOT EXISTS
                  (SELECT 1 FROM electric_mutation_queue WHERE model_name = $1 AND row_id = $2)
               ON CONFLICT (model_name, row_id) DO UPDATE SET data = EXCLUDED.data`,
            [modelName, id, json(row)])
         }
         const activeIds = rows.filter(row => !row.deleted).map(row => String(row[primaryKey]))
         await tx.query(`DELETE FROM electric_local_rows AS local
            WHERE local.model_name = $1 AND NOT (local.row_id = ANY($2::text[]))
              AND NOT EXISTS (SELECT 1 FROM electric_mutation_queue AS pending
                 WHERE pending.model_name = local.model_name AND pending.row_id = local.row_id)`,
         [modelName, activeIds])
      }))
      notify()
   }

   async function flushQueue() {
      if (!ownsSync || flushing || network?.onLine === false) return
      flushing = true
      try {
         while (ownsSync) {
            const { rows } = await retryLeader(() => db.query(`SELECT * FROM electric_mutation_queue
               WHERE model_name = $1 AND status = 'pending' AND acknowledged_version IS NULL
               ORDER BY revision LIMIT 1`, [modelName]))
            const mutation = rows[0]
            if (!mutation) break
            try {
               await sendMutation(mutation)
               lastError = null
            } catch (error) {
               report(error)
               if (!isPermanent(error)) break
               await db.query(`UPDATE electric_mutation_queue SET status = 'failed', failure_reason = $3
                  WHERE model_name = $1 AND row_id = $2 AND revision = $4`,
               [modelName, mutation.row_id, error.message, mutation.revision])
               notify()
            }
         }
      } finally {
         flushing = false
      }
   }

   async function sendMutation(mutation) {
      const client = await retryLeader(() => db.query('SELECT id FROM electric_sync_client WHERE singleton = true'))
      const metadata = { clientId: client.rows[0].id, revision: String(mutation.revision) }
      const { row_id: id, data, action } = mutation
      const result = action === 'create'
         ? await service.create(id, withoutKey(data), metadata)
         : action === 'update'
            ? await service.update(id, withoutKey(data), metadata)
            : await service.delete(id, metadata)
      if (result?.version == null) throw new Error('sync mutation did not return a version')
      await retryLeader(() => db.transaction(async tx => {
         const current = await tx.query('SELECT action, revision, data FROM electric_mutation_queue WHERE model_name = $1 AND row_id = $2', [modelName, id])
         const queued = current.rows[0]
         if (!queued) return
         if (String(queued.revision) === String(mutation.revision) && queued.action === action) {
            await tx.query(`UPDATE electric_mutation_queue SET acknowledged_version = $3::numeric
               WHERE model_name = $1 AND row_id = $2`, [modelName, id, String(result.version)])
         } else if (action === 'create' && queued.action === 'create') {
            // The create was sent, then edited locally. Its next request is an update.
            await tx.query(`UPDATE electric_mutation_queue SET action = 'update',
               revision = nextval('electric_mutation_revision_seq') WHERE model_name = $1 AND row_id = $2`, [modelName, id])
         }
      }))
      await reconcile()
      notify()
   }

   function withoutKey(data) {
      const copy = { ...data }
      delete copy[primaryKey]
      delete copy.version
      delete copy.deleted
      return copy
   }

   function start() {
      if (started) return
      started = true
      channel?.addEventListener('message', sharedChange)
      globalThis.addEventListener?.('online', online)
      if (!ownsSync) { channel?.postMessage({ modelName, requestStatus: true }); return }
      const generation = ++streamGeneration
      stream = new ShapeStreamClass({ ...streamOptions, url })
      stream.subscribe(messages => {
         if (generation !== streamGeneration) return
         if (messages.some(message => message.headers?.control === 'must-refetch')) background(resetSnapshot())
         connected = stream.isConnected?.() ?? true
         notify()
      }, error => { if (generation !== streamGeneration) return; connected = false; report(error) })
      const shape = new ShapeClass(stream)
      shape.subscribe(({ rows }) => { if (generation !== streamGeneration) return; connected = true; background(applySnapshot([...rows])) })
      timer = setInterval(() => { background(flushQueue()) }, retryMs)
      background(flushQueue())
   }

   function setSyncOwner(value) {
      if (ownsSync === value) return
      const restart = started
      stop()
      ownsSync = value
      if (restart) start()
   }

   async function waitForIdle() {
      while (pendingBackground.size) await Promise.allSettled([...pendingBackground])
      await operation
   }

   function stop() {
      if (!started) return
      started = false
      ++streamGeneration
      clearInterval(timer)
      stream?.unsubscribeAll()
      channel?.removeEventListener('message', sharedChange)
      globalThis.removeEventListener?.('online', online)
      connected = false
      notify()
   }

   function online() { background(flushQueue()) }
   function sharedChange(event) {
      if (event.data?.modelName !== modelName) return
      if (!ownsSync && typeof event.data.online === 'boolean') connected = event.data.online
      if (ownsSync && event.data.requestStatus) notify()
      for (const listener of listeners) queueMicrotask(() => { try { listener() } catch (error) { console.error(error) } })
      if (ownsSync) background(flushQueue())
   }

   async function getStatus() {
      const { rows } = await retryLeader(() => db.query(`SELECT
         count(*) FILTER (WHERE status = 'pending')::int AS pending,
         count(*) FILTER (WHERE status = 'failed')::int AS failed
         FROM electric_mutation_queue WHERE model_name = $1`, [modelName]))
      return { ...rows[0], online: network?.onLine !== false && connected, ...(lastError ? { error: lastError } : {}) }
   }

   return { prepare, start, stop, setSyncOwner, waitForIdle, subscribe, findMany, findUnique, getObservable,
      create, update, remove, flushQueue, getStatus }
}

function assertData(data) {
   if (!data || typeof data !== 'object' || Array.isArray(data)) throw new TypeError('mutation data must be a plain object')
}

function assertId(id) {
   if (typeof id !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id)) {
      throw new TypeError('sync id must be a UUID')
   }
}

function matchesWhere(row, where) {
   return Object.entries(where).every(([key, constraint]) => {
      if (constraint === undefined) return true
      const value = row[key]
      if (constraint && typeof constraint === 'object' && !(constraint instanceof Date)) {
         return Object.entries(constraint).every(([operator, limit]) => {
            if (operator === 'gt') return value > limit
            if (operator === 'gte') return value >= limit
            if (operator === 'lt') return value < limit
            if (operator === 'lte') return value <= limit
            throw new TypeError(`unsupported where constraint for '${key}'`)
         })
      }
      return value === constraint
   })
}

function isPermanent(error) {
   const status = error?.status ?? error?.response?.status
   return error?.code === 'forbidden' || error instanceof TypeError ||
      status >= 400 && status < 500 && ![408, 425, 429].includes(status)
}

function json(value) {
   return JSON.stringify(value, (_key, item) => typeof item === 'bigint' ? String(item) : item)
}

// Snapshots and reads are safe to repeat if PGlite's worker changes mid-request.
// Foreground local mutation transactions may have committed; callers see their error.
async function retryLeader(action) {
   for (let attempt = 0; ; attempt++) {
      try { return await action() } catch (error) {
         if (!(error?.name === 'LeaderChangedError' || error?.constructor?.name === 'LeaderChangedError' ||
            error?.message === 'Leader changed, pending operation in indeterminate state') || attempt >= 7) throw error
         await new Promise(resolve => setTimeout(resolve, Math.min(25 * 2 ** attempt, 250)))
      }
   }
}
