
# express-x-plugins

Plugins for [Express-X](https://github.com/jcbuisson/express-x), with separate
server and browser entry points.

| Plugin | Purpose | Server / client imports |
| --- | --- | --- |
| [Electric](#electric-plugin-local-first-postgres) | PostgreSQL Shapes, with optional persistent offline writes | `/electric-server`, `/electric-client` |
| [Reload](#reload-plugin) | Restore socket rooms and data after a page reload | `/reload-server`, `/reload-client` |
| [Metadata sync](#custom-metadata-sync-plugin) | Timestamp reconciliation using Drizzle and Dexie | `/sync-server`, `/sync-client` |
| [PGCouch](#pgcouch-plugin) | Atomic offline transactions replicated through CouchDB/PouchDB | `/pgcouch-server`, `/pgcouch-client`, `/pgcouch-worker` |

All import paths below are prefixed with `@jcbuisson/express-x-plugins`.
`/electric-worker` and `/sync-algorithm` are supporting modules, not plugins.

## Setup

Requires Node.js 18+ and npm 8+. Install the package in each application that
uses it, along with Express-X and the dependencies for your chosen plugin.
Optional peer dependencies are listed in each plugin's setup instructions.

```sh
# Server application
npm install @jcbuisson/express-x @jcbuisson/express-x-plugins express socket.io

# Browser application
npm install @jcbuisson/express-x @jcbuisson/express-x-plugins socket.io-client
```

Create the server application, configure its plugins, then start listening:

```js
import { expressX } from '@jcbuisson/express-x/server'

const app = expressX()
// Register server plugins here, completing any async schema preparation first.
app.httpServer.listen(8000)
```

Create the browser client and register its plugins before connecting:

```js
import { io } from 'socket.io-client'
import { createClient } from '@jcbuisson/express-x/client'

const socket = io('http://localhost:8000', { autoConnect: false })
const app = createClient(socket)
// Register client plugins here.
socket.connect()
```

The examples below use these server/client `app` instances. Configure your
application's authentication and service publishing policies as needed. Choose
one synchronization plugin for a given model: their service names and client
model factories can overlap. Reload can be used alongside any of them.



## Reload plugin

Preserves Socket.IO room membership and `socket.data` across browser reloads.
It uses the previous socket ID and a one-use transfer token stored in the tab's
`sessionStorage`. No additional plugin dependencies are required.

### Server setup

Register before accepting connections:

```js
import { reloadPlugin } from '@jcbuisson/express-x-plugins/reload-server'

await app.configure(reloadPlugin)
```

Disconnected state remains available for two minutes by default. To change this,
set `reloadTransferTtlMs` on the server's `app.get('config')` object before
registering the plugin. The caches are in memory and scoped to the application;
restoration requires reconnecting to the same running server instance.

### Client setup

Register before calling `socket.connect()` in the shared setup example:

```js
import { reloadPlugin } from '@jcbuisson/express-x-plugins/reload-client'

await app.configure(reloadPlugin)
```

Restoration happens automatically on connection. The old socket's own ID room is
excluded, and data already set on the new socket takes precedence over restored
values.




## Electric plugin: local-first Postgres

Without managed sync (sync: false) or a supplied localDb, the client streams Shapes directly and sends CRUD calls directly;
it has no persistent local mutation queue or owner election.

With managed sync (sync: true) Express-X handles authorized PostgreSQL mutations; Electric streams committed rows to clients.

The electric plugin has two separate elections:

- PGlite leader: owns the local database engine. IndexedDB shares the stored data, but it doesn’t share the running PostgreSQL engine.
Each PGlite instance has its own in-memory state: caches, transaction state, and buffers.
- Sync owner: chosen by a separate Web Lock per model; runs the Electric stream and sends queued mutations to the server.

They prevent duplicate database engines and duplicate synchronization across tabs. The database leader and sync owner can be different tabs.


Electric does incremental replication: after the initial snapshot, it streams database changes instead of repeatedly comparing client/server metadata.
Its Shapes can represent filtered subsets of tables



### PostgreSQL setup

Create your application tables through your normal migrations. For example, the
managed sync example below uses a UUID primary key:

```sql
CREATE TABLE todos (
  uid uuid PRIMARY KEY,
  title text NOT NULL,
  completed boolean NOT NULL DEFAULT false
);
```

Run the following as a PostgreSQL administrator, replacing `app_user` with the
role used by Electric. Restart PostgreSQL after changing replication settings.

```sql
ALTER ROLE app_user SET lc_messages = 'C';
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER ROLE app_user WITH REPLICATION;
```

Start the Electric service using the [Docker example](#run-electric-from-docker).
Set `DATABASE_URL` for the application and `ELECTRIC_URL` to the service's Shape
endpoint (for that Docker example, `http://localhost:3001/v1/shape`).

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

#### Relationships and offline deletes

In sync mode, the server discovers PostgreSQL `ON DELETE CASCADE` foreign keys
between configured models. Deleting a parent writes tombstones for its current
children and descendants in the same transaction, including rows the deleting
offline client never saw. A delayed create or update referencing a deleted parent
is resolved as a child tombstone, so deletion wins in either arrival order.
The result includes `syncRejection: { code: 'sync-parent-deleted', message }`.
Retries replay that result through the normal mutation cursor.

The browser removes a rejected optimistic row immediately, acknowledges its
tombstone version, and reports an error through `onError` with `code` and `rowId`.
It does not resend the rejected mutation. Electric's tombstone snapshot completes
queue cleanup. An edit to an already deleted row is handled the same way, with
`code: 'sync-row-deleted'`. If the parent is missing rather than deleted, the
write remains retriable: its parent's offline create may still be queued.

Pass the same complete model list, including `tombstoneData`, to
`prepareElectricSyncSchema` and `electricServerPlugin`. Startup preparation also
repairs existing active children of soft-deleted parents and their descendants.
Clients only need to queue the parent deletion; explicit child deletions remain
compatible. Cascades are part of the authorized parent deletion and do not invoke
separate child authorization callbacks.

These rules apply to single-column `ON DELETE CASCADE` foreign keys referencing
the configured parent's primary key. Other foreign-key delete policies retain
their existing behavior. Unsupported CASCADE keys between configured models
fail discovery rather than silently losing integrity. Configure every model in
the relationship graph. Restart the plugin after changing foreign keys: discovery
is cached after the first sync mutation.

Transactions affecting these relationships acquire one shared PostgreSQL advisory
lock before cursor or row locks. This deliberately serializes relational writes
across plugin instances, including cyclic graphs; models without discovered
relationships remain parallel. All participating instances must use these rules.
Direct SQL writes and non-sync services bypass this coordination.

Regression tests run with `npm test`. To also exercise actual concurrent PostgreSQL
transactions, set `ELECTRIC_TEST_DATABASE_URL` to a test database and run `npm test`.
The PostgreSQL test creates and removes its own isolated schema and requires
permission to create schemas; it does not use application tables.

Without `sync: true`, the existing direct service methods remain available and
return created, updated, or deleted rows directly.

### Client

Install the optional client dependencies in the browser application:

```sh
npm install @electric-sql/client @electric-sql/pglite rxjs vue
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

Equivalent `getObservable(where)` filters share one underlying subscription per model,
including in offline sync mode. New subscribers receive the latest result. The last
unsubscribe releases the source and cache entry, so a later subscription reads fresh
data. Filter key order does not matter; filters are captured when `getObservable` is called.


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
Offline sync streams require complete Shapes, including tombstones and all
columns. Apply filters with `findMany(where)` or `getObservable(where)`;
`streamOptions.params.where`, column projections, and other partial stream
options are rejected because they can prevent mutation confirmation.
Repeated calls for the same model reuse its instance. `findMany` and `findUnique`
read the local cache immediately after preparation, which can be empty before
Electric's first snapshot. Use `app.service('user').findMany({ deleted: false, ...filter })`
when an online authentication flow requires an authoritative server read.

Each plugin instance shares one persistent PGliteWorker. PGlite coordinates its
database worker across tabs. A separate Web Lock per database/model elects one
tab to run the Electric stream and mutation queue. Follower writes notify the
owner through BroadcastChannel. When the owner closes or stops, a waiting tab
takes over. Stream ownership does not depend on PGlite's leader event timing.
BIGINT values in cached rows are stored as lossless decimal strings with type
metadata, so equality and range filters compare them numerically without rounding.
BIGINT filter values can be integers, decimal strings, or JavaScript bigints. Snapshot
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


### Local cascade visibility

Configure the client with the same CASCADE relationships as the server schema:

```js
electricClientPlugin(app, {
   sync: true,
   databaseName: 'my-app',
   relations: [
      { child: 'user_group_relation', parent: 'group', column: 'group_uid' },
      { child: 'user_group_relation', parent: 'user', column: 'user_uid' },
   ],
})
```

Names identify client models, and `column` is the child's foreign key referencing
its parent's configured primary key. This applies to offline models sharing a
local database, including managed sync. `findMany`, `findUnique` and observables
hide descendants of locally deleted parents immediately, including transitive
cascades. Managed tabs broadcast changes across models. Missing parents alone do
not hide rows: Shapes can arrive in any order. Nullable foreign keys remain visible.
The plugin retains local deletion markers across reloads and acknowledgement so
children stay hidden until their server tombstones arrive. Only the parent delete
is queued; the server still discovers PostgreSQL CASCADE constraints and writes
the authoritative tombstones. Relationship configuration is explicit on the client;
it is not downloaded from the server. Direct models without a local database are
unaffected.

UI subscriptions can opt into an immediate placeholder with
`model.getObservable(where, { initialValue: [] })`. The returned observable
replays its latest value and releases its subscription when the last subscriber
unsubscribes. Omit the option to wait for actual rows; `findMany()` and
`findUnique()` always wait for actual data.


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

Server models may be strings (table, service name, and default `uid` key) or objects:

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




## Custom metadata sync plugin

The original timestamp/metadata synchronization engine is available separately
from Electric:

### Server setup

```sh
npm install drizzle-orm pg
```

Define your Drizzle schema and apply it to PostgreSQL through your application's
migration process before registering the plugin:

```js
import { Pool } from 'pg'
import { drizzle } from 'drizzle-orm/node-postgres'
import { pgTable, text, timestamp } from 'drizzle-orm/pg-core'

const todos = pgTable('todos', {
  uid: text('uid').primaryKey(),
  label: text('label').notNull(),
})
const metadata = pgTable('metadata', {
  uid: text('uid').primaryKey(),
  created_at: timestamp('created_at'),
  updated_at: timestamp('updated_at'),
  deleted_at: timestamp('deleted_at'),
})
const db = drizzle(new Pool({ connectionString: process.env.DATABASE_URL }))
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

### Client setup

```sh
npm install dexie uuid @vueuse/core rxjs vue
```

Use IndexedDB in the browser; the model name must match the server table name.
The fields array declares the model's indexed Dexie fields.

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




## PGCouch plugin

[PGCouch](docs/pgcouch.md) provides model CRUD/query API with
PostgreSQL on the server, PGLite on clients, and CouchDB/PouchDB transaction
replication. It supports atomic multi-table offline writes, durable outboxes,
and server-side constraint validation.
Import `pgcouchClientPlugin` from `@jcbuisson/express-x-plugins/pgcouch-client` and
`pgcouchServerPlugin` from `@jcbuisson/express-x-plugins/pgcouch-server`.
See the guide for schema requirements, permissions, conflict policy and the
checkpoint and incremental replication behavior.


### Server setup

```sh
npm install pg pouchdb
```

Create the PostgreSQL business tables, provision a CouchDB database, and install
`pgcouchValidationDocument()` with an admin connection before allowing clients
to replicate. Configure CouchDB membership, authentication and browser CORS;
only the server account should have the `pgcouch-server` role.

```js
import { Pool } from 'pg'
import PouchDB from 'pouchdb'
import { pgcouchServerPlugin } from '@jcbuisson/express-x-plugins/pgcouch-server'

const models = [{ name: 'todos', primaryKey: 'uid' }]
const db = new Pool({ connectionString: process.env.DATABASE_URL })
const couch = new PouchDB(process.env.COUCH_DATABASE_URL)
const sync = pgcouchServerPlugin(app, db, models, {
  couch,
  authorize: async (context, operation) => policy(context, operation),
  onError: console.error,
})
await sync.ready // before app.httpServer.listen(...)
```

Supply your application's `policy`. Replicated proposals have no verified socket
identity; the [PGCouch guide](docs/pgcouch.md) explains `contextForDocument` and
trusted replication scopes. Initialization prepares internal protocol tables;
your application owns business schema migrations.

### Client setup

```sh
npm install @electric-sql/pglite pouchdb rxjs
```

```js
import { PGlite } from '@electric-sql/pglite'
import PouchDB from 'pouchdb' // use the browser build in browser applications
import { pgcouchClientPlugin } from '@jcbuisson/express-x-plugins/pgcouch-client'

const localDb = new PGlite('idb://my-app-relational')
await localDb.exec(`
  CREATE TABLE IF NOT EXISTS todos (
    uid uuid PRIMARY KEY,
    title text NOT NULL,
    completed boolean NOT NULL DEFAULT false
  );
`)
const couch = new PouchDB('my-app-transactions')
pgcouchClientPlugin(app, {
  models: [{ name: 'todos', primaryKey: 'uid' }],
  localDb,
  couch,
  remote: '/couchdb/my-app', // authenticated CouchDB URL or application proxy
  onError: console.error,
})
await app.pgcouchReady
const todos = app.createPGCouchModel('todos')
await todos.create({ title: 'Works offline', completed: false })
const subscription = todos.getObservable({}).subscribe(console.log)

// On teardown:
// subscription.unsubscribe()
// await app.disposePGCouchSync()
// await localDb.close()
// await couch.close()
```

Use matching server/client schemas and globally unique IDs for offline writes.
The direct client API lets the application own both local databases and
coordinate multi-tab access. Version **5.4.2** also provides
`startPGCouchWorker` from `/pgcouch-worker` and storage helpers from
`/pgcouch-storage`: a small application worker entry supplies the SQL schema,
models, remote URL and storage prefix, while the plugin owns the shared database,
replication and request/subscription runtime. The worker includes WebKit startup
handling and preserves cached rows and pending edits during transport upgrades.
See the [shared browser worker guide](docs/pgcouch.md#shared-browser-worker).
PGCouch replicates row changes with periodic full checkpoints; use bounded
scopes and read the guide's schema and permission requirements before deployment.
