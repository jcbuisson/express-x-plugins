
# express-x-plugins

Currently includes:

- a plugin which preserves room membership and socket data across page reloads
- a plugin integrating ElectricSQL sync engine into express-x, which greatly simplifies relational database
access and provides powerful local-first features


## Install

```sh
npm install @jcbuisson/express-x-plugins
```


## Electric plugin: local-first Postgres

Express-X handles authorized PostgreSQL mutations; Electric streams committed rows to clients.

The electric plugin has two separate elections:

- PGlite leader: owns the local database engine. IndexedDB shares the stored data, but it doesn’t share the running PostgreSQL engine.
Each PGlite instance has its own in-memory state: caches, transaction state, and buffers.
- Sync owner: chosen by a separate Web Lock per model; runs the Electric stream and sends queued mutations to the server.

They prevent duplicate database engines and duplicate synchronization across tabs. The database leader and sync owner can be different tabs.


### Settings
```bash
# set lc_messages=C for the PostgreSQL role so Electric receives recognizable English errors
ALTER ROLE chris SET lc_messages = 'C';

# ALLOW POSTGRES LOGICAL REPLICATION
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER ROLE chris WITH REPLICATION;

(restart postgres)
```

(?
## Use HTTP2
nginx: `listen 443 ssl http2;`
)



### Server

```sh
npm install pg
```

```js
import { Pool } from 'pg'
import { expressX } from '@jcbuisson/express-x/server'
import { electricServerPlugin, prepareElectricSyncSchema } from '@jcbuisson/express-x-plugins/electric-server'

const app = expressX()
const db = new Pool({ connectionString: process.env.DATABASE_URL })

app.configure(electricServerPlugin, db, [
  { name: 'todos', tombstoneData: { title: '' } },
], {
  sync: true,
  electricUrl: process.env.ELECTRIC_URL, // ElectricSQL sync service, e.g. http://localhost:3000/v1/shape
  sourceId: process.env.ELECTRIC_SOURCE_ID,
  sourceSecret: process.env.ELECTRIC_SOURCE_SECRET,
  authorize: async (context, { modelName, action }) => {
    // `context.transport` is "http" for Shapes and "ws" for Express-X calls.
    return Boolean(context.request?.user || context.socket?.data?.user)
  },
})

// Run this during startup, before serving sync mutations:
await prepareElectricSyncSchema(db, [{ name: 'todos' }])
```

This registers one Express-X service per model. In direct mode, it has the familiar API:

- `findUnique(where)`
- `findMany(where, queryOptions)`
- `create(id, data)` for a client-generated primary key
- `create(data)` for a database-generated primary key
- `update(id, data)`
- `delete(id)`

The client model provides synchronized reads through `findMany(where)` and
`getObservable(where)`. The service also exposes direct one-shot server reads
when synchronization is not required.

In sync mode, create requires a client-generated UUID and every mutation requires
`{ clientId, revision }` as its last argument. The returned row includes `version`.
The server records the latest revision per client and row in the same transaction
as the write. Retries replay the saved result; older requests cannot overwrite a
newer edit from that client. Deletes write versioned tombstones instead of removing
rows. Configure `tombstoneData` for required columns without defaults, such as a
required title. Keep the cursor table and tombstones to protect delayed retries and
offline clients. This does not resolve concurrent edits by different clients.

`tombstoneData` can also be a function receiving `{ id }` and returning a plain
object (or a promise of one). It runs inside the mutation transaction whenever a
tombstone is written, including an update targeting a missing or deleted row.
Replayed or stale revisions return the saved result without calling it again.
For a unique, required email column, derive a dummy value from the row's UUID:

```js
{
  name: 'user',
  primaryKey: 'uid',
  tombstoneData: ({ id }) => ({
    email: `deleted-${id}@tombstone.invalid`,
    name: '',
    color: '',
  }),
}
```

The returned keys must be valid column identifiers and cannot include the primary
key, `version`, or `deleted`. Reserve the dummy email pattern for tombstones so
active users cannot occupy those values. A deterministic value remains stable
across repeated deletions; a function can also generate a fresh random value.

Import `prepareElectricSyncSchema` alongside `electricServerPlugin`. It adds
`version` and `deleted` columns and creates the shared version sequence and cursor
table. The primary key must be UUID in sync mode. All writes to synced tables must
advance `version`; hard deletes bypass tombstone confirmation.

Without `sync: true`, the existing direct service methods remain available and
return created, updated, or deleted rows directly.

### Client

Install the optional client dependencies in the browser application:

```sh
npm install @electric-sql/client rxjs
```

For a server configured without `sync: true`, the direct client API is:

```js
import { electricClientPlugin } from '@jcbuisson/express-x-plugins/electric-client'

app.configure(electricClientPlugin, {
  shapePath: '/electric/v1/shape',
})

// Client-generated UUID stored in the default `uid` primary key:
const todo = app.createElectricModel('todos')

// Or, for a database-generated primary key such as SERIAL/IDENTITY:
const numberedTodo = app.createElectricModel('numberedTodos', {
  primaryKey: 'id',
  idGeneration: 'server',
})

const incompleteTodos = await todo.findMany({ completed: false })

const subscription = todo.getObservable({ completed: false }).subscribe(rows => {
  console.log(rows)
})

// Direct mutations use the matching Express-X service and are reflected by Electric.
await todo.create({ title: 'Learn Shapes', completed: false })
await todo.update(id, { completed: true })
await todo.remove(id)

const created = await numberedTodo.create({ title: 'Assigned by PostgreSQL' })
console.log(created.id)

subscription.unsubscribe()
```

### Managed browser sync

With a server configured with `sync: true`, the client plugin can own the shared
PGlite database and multi-tab synchronization:

```sh
npm install @electric-sql/pglite @electric-sql/client rxjs vue
```

```js
app.configure(electricClientPlugin, {
  sync: true,
  databaseName: 'selommes', // persistent IndexedDB name; unique to this app/backend
  shapePath: '/electric/v1/shape',
  onError: (error, { modelName }) => console.error(modelName, error),
})

const ranges = app.createElectricModel('range')
const subscription = ranges.getObservable().subscribe(rows => console.log(rows))
await ranges.create({ label: 'Works offline' })
await ranges.update(uid, { label: 'Edited offline' })
await ranges.remove(uid)
console.log(await ranges.getStatus()) // pending, failed, online, error

// During app teardown (for example, HMR disposal):
subscription.unsubscribe()
await app.disposeElectricSync()
```

Models prepare and start automatically; operations wait for schema readiness.
Repeated calls for the same model reuse its instance. `findMany` and `findUnique`
read the local cache immediately after preparation, which can be empty before
Electric's first snapshot. Use `app.service('user').findMany({ deleted: false, ...filter })`
when an online authentication flow requires an authoritative server read.

Each plugin instance shares one persistent PGliteWorker. PGlite coordinates its
database worker across tabs. A separate Web Lock per database/model elects one
tab to run the Electric stream and mutation queue. Follower writes notify the
owner through BroadcastChannel. When the owner closes or stops, a waiting tab
takes over. Stream ownership does not depend on PGlite's leader event timing.
BIGINT values in cached rows are stored as lossless decimal strings. Snapshot
reconciliation and reads retry transient database leader changes. An indeterminate
foreground local mutation is reported instead of blindly repeated.

`model.stop()` stops synchronization and releases ownership without deleting local
rows or queued edits; `await model.start()` resumes it. `await model.dispose()`
also closes its channel. `await app.disposeElectricSync()` disposes all managed
models and closes the plugin-owned database. A supplied `localDb` remains owned
by the caller. Background errors appear in `getStatus()` and invoke `onError`;
initialization failures reject reads/writes and error observable subscriptions.
Worker startup times out after 30 seconds (`initTimeoutMs` can override this).

Managed sync requires HTTPS or localhost, Web Locks, BroadcastChannel, workers,
and IndexedDB. Configure Vite to keep the plugin and PGlite worker asset URLs
intact during development and emit workers as ES modules:

```js
export default defineConfig({
  optimizeDeps: {
    exclude: ['@jcbuisson/express-x-plugins', '@electric-sql/pglite'],
  },
  worker: { format: 'es' },
})
```

For other bundlers, provide `workerFactory` if their worker discovery requires
an application-owned entry point:

```js
// electric.worker.js
import '@jcbuisson/express-x-plugins/electric-worker'

// Client configuration:
app.configure(electricClientPlugin, {
  sync: true,
  databaseName: 'selommes',
  workerFactory: () => new Worker(new URL('./electric.worker.js', import.meta.url), {
    type: 'module',
  }),
})
```

PWA applications should cache the emitted PGlite `.wasm` and `.data` assets to
initialize after an offline reload. The WASM asset is about 10 MB; increase the
precache file-size limit accordingly. Apps that already provide a shared database
can pass plugin-level `localDb` to retain automatic ownership management.

### Explicit offline models

For offline writes, install `@electric-sql/pglite`, pass a shared PGlite database as `localDb`, and use the sync server mode above.
`PGliteWorker` with an IndexedDB data directory is recommended for multiple tabs.
Only one worker should use `ownsSync: true`; use a shared `BroadcastChannel` so other tabs receive change notifications.

```js
const todo = app.createElectricModel('todos', {
  localDb: db, // PGliteWorker with a persistent idb:// data directory
  channel: new BroadcastChannel('todos-sync'),
})
await todo.prepare() // creates local rows, mutation queue, client ID, revision sequence
todo.start()         // starts Electric and mutation retries
await todo.create({ title: 'Works offline' })
await todo.update(id, { title: 'Still offline' })
await todo.remove(id)
const rows = await todo.findMany()
const status = await todo.getStatus() // pending, failed, online
```

Local edits and queue entries commit together. Successful service responses leave
queue entries in place until Electric sends the acknowledged `version` or newer.
Rows with `deleted = true` confirm deletes but stay hidden locally. Network errors
remain queued for retry; permanent HTTP 4xx errors are marked failed. The local
database, including its identity and revision sequence, must remain persistent.

`findMany(where)` resolves with all matching rows from the first synchronized Shape emission.

It unsubscribes after the first emission; if called within a Vue scope, it also
unsubscribes if that scope is disposed before a result arrives.

Object filters use parameterized Electric Shape predicates. Exact values,
`null`, and `gt`/`gte`/`lt`/`lte` ranges are supported.

All Electric cursor parameters are forwarded. The client cannot override the
configured table, and Electric source credentials stay server-side.

### Model configuration

Server models may be strings (table, service name, and default `id` key) or objects:

```js
{ name: 'todo', table: 'todos', primaryKey: 'id' }
```

Names are restricted to simple PostgreSQL identifiers. Values are always sent
as query parameters; range filters support `gt`, `gte`, `lt`, and `lte`.

Client models default to `idGeneration: 'client'`, which generates a UUID and calls
`create(id, data)`. Set `idGeneration: 'server'` to call `create(data)` and let a
PostgreSQL default, sequence, or identity column generate the primary key.

Requires Node 18+ for the built-in Fetch API. The PostgreSQL client only needs a
`query(sql, values)` method; a `pg.Pool` is recommended so each mutation and its
transaction ID are captured in the same transaction.

### Run Electric from Docker

A local install of the Electric sync engine requires Elixir and Erlang; it is simpler to use a pre-built Docker image.

```
services:
  electric:
    image: electricsql/electric:latest
    environment:
      DATABASE_URL: postgresql://user:password@host.docker.internal:5432/mydb
      ELECTRIC_INSECURE: "true"
      ELECTRIC_STORAGE: FAST_FILE
      ELECTRIC_PERSISTENT_STATE: FILE
      ELECTRIC_STORAGE_DIR: /var/lib/electric
    ports:
      - "3001:3000"
    volumes:
      - electric_mydb_data:/var/lib/electric

volumes:
  electric_mydb_data:
```

```
docker compose pull electric
docker compose up electric
```



## Reload plugin

### Server

```js
import { reloadPlugin } from '@jcbuisson/express-x-plugins/reload-server'
```


Managed sync automatically pauses network ownership when `navigator.onLine` is
false and resumes active models on the browser's `online` event. Local reads,
writes, queued edits, and cross-tab notifications remain available while offline.
A model explicitly stopped with `model.stop()` stays stopped after reconnection.
Disposing a model removes its network listeners. Browser connectivity is a hint;
requests can still fail while reported online and retain the usual retry behavior.
For custom runtimes/tests, plugin options `network` and `networkEvents` can supply
the connectivity state and an EventTarget respectively.


Models default to `primaryKey: 'uid'` on both client and server. Set
`primaryKey: 'id'` explicitly for tables that use `id`.

With `sync: true`, the sync owner's Electric stream stays active when its tab is
hidden because it serves other tabs too. To customize this behavior, pass
`streamOptions.runtimeVisibility` to `createElectricModel`. Other stream options
are preserved. Independent streams retain Electric's normal visibility behavior.

## Custom metadata sync plugin

The original timestamp/metadata synchronization engine is available separately
from Electric:

```sh
npm install dexie uuid @vueuse/core rxjs vue drizzle-orm
```

On the server, register Drizzle tables with a `uid` primary key and a shared
metadata table (`uid` primary key, `created_at`, `updated_at`, `deleted_at`
timestamps):

```js
import { syncServerPlugin } from '@jcbuisson/express-x-plugins/sync-server'

app.configure(syncServerPlugin, db, metadata, [todos], {
  authorize: async (context, { modelName, action, args }) => {
    return Boolean(context.socket?.data?.user)
  },
})
```

The plugin registers each model's `findUnique`, `findMany`, `createWithMeta`,
`updateWithMeta`, and `deleteWithMeta` methods, plus `sync.go`. Configure each
model service's Express-X publishing policy to send mutation events to its
authorized subscribers. Authorization is required; `maxSyncRecords` defaults
to 10,000. Use separate applications for Electric and custom sync services
when their model names overlap.

On the client:

```js
import { syncClientPlugin } from '@jcbuisson/express-x-plugins/sync-client'

app.configure(syncClientPlugin, { cacheNamespace: 'my-app:current-user' })
const todo = app.createOfflineModel('todos', ['label'])
const subscription = todo.getObservable({}).subscribe(rows => console.log(rows))
await todo.create({ label: 'Available offline' })
await todo.update(uid, { label: 'Edited offline' })
await todo.remove(uid)
subscription.unsubscribe()
```

The client persists rows and dirty mutations in Dexie/IndexedDB and reconciles
subscribed filters on reconnect. `cacheNamespace` must be non-empty and should
isolate each backend/user. Models support `findByUID`, `reset`, and
`synchronizeAll` alongside observable reads and mutations. Vue scope disposal
cleans up model listeners and filter subscriptions.

The original names `offlinePlugin` and `drizzleOfflinePlugin` remain exported.
The pure algorithm is exported as `computeSyncResult` from `/sync-algorithm`
and `/sync-server`. It compares metadata timestamps and returns additions,
updates, and deletions for each side. Writes outside the plugin must maintain
the metadata table too. Tombstones persist; safe pruning requires knowing all
clients have observed a deletion.

Restored from the last committed custom implementations, dated July 8, 2026:
`express-x` **edb1db3** (algorithm and regression tests), `express-x-client`
**944c398** (client), and `express-x-drizzle` **ddf6804** (server). The algorithm
was removed from Express-X in **47c2293** on July 19. Only module imports and
plugin export aliases were adapted; the synchronization logic is preserved.
