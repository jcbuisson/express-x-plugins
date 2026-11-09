# PGCouch

PGCouch implements a model CRUD/query API using PostgreSQL/PGLite and CouchDB/PouchDB.
`@electric-sql/pglite` is the PostgreSQL engine used locally, independently of Electric.

## Server

```js
import { Pool } from 'pg'
import PouchDB from 'pouchdb'
import {
  pgcouchServerPlugin, pgcouchValidationDocument,
} from '@jcbuisson/express-x-plugins/pgcouch-server'

const models = [
  { name: 'horses', primaryKey: 'id' },
  { name: 'examinations', primaryKey: 'id' },
]
const db = new Pool({ connectionString: process.env.DATABASE_URL })
// An authenticated server connection, with CouchDB role pgcouch-server.
const couch = new PouchDB(process.env.COUCH_DATABASE_URL)

// Provision once with an admin connection, before allowing client replication:
// await couch.put(pgcouchValidationDocument())
// Configure CouchDB _security membership and browser CORS separately.

const sync = pgcouchServerPlugin(app, db, models, {
  couch,
  authorize(context, { modelName, action, args }) {
    // Use your application's policy. Couch proposals have transport: 'couch'.
    // A Couch document is untrusted input, not an authenticated HTTP request.
    return policy(context, modelName, action, args)
  },
  onError: console.error,
})
await sync.ready // before accepting requests
// On shutdown: await sync.dispose(); await couch.close(); await db.end()
```

Strings in `models` default to matching table names and primary key `uid`.
Configurations can specify `{ name, table, primaryKey }`. All identifiers must be
simple SQL identifiers. All configured tables share one replication scope.
`preparePGCouchSchema(db)` is also exported; normal initialization calls it.
The application creates its own business tables and migrations.

The plugin registers each model's `findUnique(where)`,
`findMany(where, { limit })`, `create(data)`, `create(id, data)`,
`update(idOrWhere, data)`, and `delete(idOrWhere)` Express-X services. Server `create(data)`
uses a supplied primary key or generates a UUID; integer keys should be supplied
explicitly. Inserts are strict: duplicate keys reject instead of upserting.

It also registers `app.service('pgcouch').transaction(operations, optionalId)`.
The optional id must have the form `transaction:<unique-token>` (`bootstrap` is reserved); retry with the
same operations and id to receive the same result without repeating the write.
The return value is an array of operation results. The returned server controller
also exposes `submit(operations, context, optionalId)`, `flushQueue()`, `models`,
`ready`, and `dispose()`. Set `serviceName` to rename the transaction service.

## Client

```js
import { PGlite } from '@electric-sql/pglite'
import PouchDB from 'pouchdb' // use its browser build in browser applications
import { pgcouchClientPlugin } from '@jcbuisson/express-x-plugins/pgcouch-client'

const localDb = new PGlite('idb://my-app-relational')
// Run your application's local schema migrations before installing the plugin.
const couch = new PouchDB('my-app-transactions')

pgcouchClientPlugin(app, {
  models, // same configuration as the server
  localDb,
  couch,
  remote: '/couchdb/my-app', // authenticated CouchDB database, typically via proxy
  onError(error) {
    console.error(error.transactionId, error.code, error.message)
  },
})
await app.pgcouchReady
const horses = app.createPGCouchModel('horses')
// Compatibility alias: app.createElectricModel('horses')

await horses.create(42, { name: 'Luna' })
await app.pgcouchTransaction([
  { action: 'insert', table: 'examinations', id: 100, data: { horse_id: 42 } },
  { action: 'update', table: 'horses', id: 42, data: { last_examination_id: 100 } },
])

const subscription = horses.getObservable({ id: { gte: 1 } }).subscribe(rows => {
  console.log(rows)
})
console.log(await horses.findUnique({ id: 42 }))
console.log(await horses.getStatus()) // { pending, failed, blocked, online }
// subscription.unsubscribe()
// await app.disposePGCouchSync() // alias: disposeElectricSync()
// await localDb.close(); await couch.close()
```

Client methods: `create(data)`, `create(id, data)`, `update(idOrWhere, data)`,
`delete(idOrWhere)` (alias `remove`), `findMany(where, { limit })`, `findUnique(where)`,
`getObservable(where, { initialValue, limit })`, `prepare()`, `start()`, `stop()`,
`dispose()`, `flushQueue()`, `waitForIdle()`, `subscribe(callback)`, and `getStatus()`.
Filters support equality, null, and `gt/gte/lt/lte` ranges. Observable queries emit
after complete local commits and reconciliation, never intermediate operations.
`pgcouchTransaction(operations)` returns `{ id, results }` after the durable local
commit. Completion means locally committed; `pending` drops once the server's
decision arrives. Server rejections are reported through `onError` and `failed`.
An update by scalar ID to a missing row rejects; deleting a missing ID is a no-op.

Updates and deletes also accept a `where` object, using the same equality, null,
and `gt/gte/lt/lte` filters as `findMany`:

```js
const updated = await horses.update({ id: { gte: 10, lt: 20 } }, {
  last_examination_id: null,
})
const deleted = await horses.delete({ last_examination_id: null })
// Explicitly match all rows:
await horses.update({}, { last_examination_id: null })
```

Object filters always return an array of affected rows (including `[]` for no
matches), with no guaranteed order. Scalar IDs preserve the existing single-row
return value. A primary-key filter such as `{ id: 42 }` still returns an array.
All matching rows are changed atomically. Primary keys cannot be updated by a
filter. Missing arguments, invalid filters and undefined filter values reject;
use an explicit `{}` to match every row.

These forms work on client models, transaction-scoped callback models and server
services. Array transactions can specify `where` instead of `id`:

```js
await app.pgcouchTransaction([
  { action: 'update', table: 'horses', where: { id: { gte: 10 } },
    data: { last_examination_id: null } },
  { action: 'delete', table: 'examinations', where: { horse_id: 42 } },
])
```

Each filtered statement counts as one operation, regardless of how many rows it
matches. Its transaction result is an array of rows. The server authorization
hook receives `args: [where, data]` for an update and `args: [where]` for a delete;
scalar-ID calls keep their existing arguments. Authorization is checked for the
whole statement, so policies must explicitly handle predicate arguments.

Predicates are stored in the proposal and reevaluated during local rebases and
when the server executes the transaction. Server matches may differ from the
original optimistic matches, especially after an offline interval. The server's
row changes determine the final state on every client. A callback's JavaScript
branches are still evaluated only locally. Upgrade servers to support `where`
operations before clients begin submitting them.

The client also accepts a callback with transaction-bound models, keyed by the
configured model name:

```js
const { id, results } = await app.pgcouchTransaction(async tx => {
  await tx.horses.create(42, { name: 'Luna' })
  const horse = await tx.horses.findUnique({ id: 42 })
  if (horse) await tx.examinations.create(100, { horse_id: horse.id })
  await tx.horses.update(42, { last_examination_id: 100 })
})
```

Each scoped model exposes `create`, `update`, `delete` (alias `remove`),
`findMany`, and `findUnique`. Use these scoped methods inside the callback;
ordinary models use the outer queue and awaiting them there would deadlock.
Reads see earlier writes. Await the scoped calls; they execute in invocation
order. Scoped models expire when the callback finishes. Throwing, any failed
scoped operation (even if caught), or a constraint violation rolls back all
writes and the outbox entry. Deferred constraints are checked after the callback.
The callback must perform 1–1000 writes and returns the same `{ id, results }`
envelope as the array form; its own return value is ignored.

Only the resulting writes are replicated as one proposal. The callback and its
reads run locally once; conditions are not reevaluated on the server. Server
constraints and authorization still determine acceptance of the entire proposal.
The server transaction service continues to accept operations arrays.

`create(data)` generates a UUID unless the data includes its primary key.
Offline mutations require client-generated IDs. `idGeneration: 'server'` is
unsupported on clients. Unlike managed Electric mode, the caller supplies and
owns the PGLite and PouchDB instances and initializes their schemas. Electric
Shape URLs, stream options, tombstone configuration, and Electric workers do not
apply. The plugin never closes caller-owned databases. Use one plugin instance
per local database; multi-tab ownership must be coordinated by the application.

`remote` enables native PouchDB live bidirectional replication with retries.
Omit it to manage replication yourself; set `autoStart: false` for explicit
lifecycle control. `replicationOptions` passes authentication/options to PouchDB;
`live` and `retry` are always enabled. `retryMs` controls local reconciliation
and server polling (default 1000 ms). `flushQueue()` publishes/reconciles locally;
it does not wait for an offline remote peer or promise global synchronization.
`stop()` pauses the shared replication/reconciliation loop for all models;
local writes continue to be queued. Model disposal releases that model's query
subscriptions; application disposal stops replication and all subscriptions.

## Transaction protocol and recovery

Proposals use the requested shape:

```json
{
  "_id": "transaction:abc123",
  "type": "transaction",
  "status": "pending",
  "operations": [
    { "action": "insert", "table": "examinations", "id": 100, "data": { "horse_id": 42 } },
    { "action": "update", "table": "horses", "id": 42, "data": { "last_examination_id": 100 } }
  ]
}
```

Each client-generated proposal can also contain `previous`, linking to its
preceding local transaction. This preserves causal order for that client's
queue even when CouchDB delivers proposals in another order. Each document
contains 1–1000 operations. Proposals are immutable. The server writes a separate
immutable `result:abc123` document with `transactionId`, `status` (`accepted` or
`rejected`), a PostgreSQL-assigned `sequence`, authoritative row `changes`, and `results` or
`error`. Each change is `{ table, id, data }`: `data` is the complete server row,
or `null` for a deletion. Server SQL triggers capture defaults, cascades and
changes caused by other database triggers inside the transaction. Multiple
changes to one row are collapsed to its final image; rejected transactions
publish an empty change list. Server service calls use the same receipt/outbox protocol.

`result:bootstrap` contains the initial full snapshot. Subsequent decisions include
a full `snapshot` checkpoint only every `checkpointInterval` decisions (server
option, default `100`, a positive integer). A checkpoint and that decision's
changes describe the same committed state; clients use the checkpoint directly.
The SQL receipt persists changes and any checkpoint atomically with the writes.

1. PGLite atomically commits local operations and their proposal in
   `pgcouch_outbox`. PouchDB publication is retried from this durable outbox.
2. PouchDB replicates proposals to CouchDB, including across offline periods.
3. The server serializes transactions with a PostgreSQL row lock, checks the
   authorization policy for every operation, and applies the whole transaction
   with constraints. Failures roll back all operations. A SQL receipt records
   the immutable decision in the same commit. Multiple workers and retries
   share this idempotency boundary.
4. The server publishes proposals and decisions from the SQL outbox. A crash
   or replication outage between databases does not lose an acknowledged commit.
5. Clients ingest the PouchDB changes feed in pages of at most 100 documents,
   persisting the feed cursor and received decisions atomically in PGLite. They
   maintain authoritative rows in a separate local SQL table, apply consecutive
   decisions in server sequence order, and wait for missing predecessors. A
   received newer checkpoint can bridge a gap. Processed inbox decisions are
   removed. Older acknowledgements still resolve pending local proposals.
6. Clients atomically rebuild application tables from their authoritative SQL
   rows and replay undecided local transactions in order. Failed rebases are
   retained as pending/blocked until the server decides them. Rejected optimistic
   work is removed during reconciliation. New clients bootstrap from an available
   checkpoint and subsequent changes. Older snapshot-only decisions remain readable.

Reconciliation no longer loads all historical snapshots into JavaScript memory.
A feed page and an individual checkpoint still require memory; full checkpoints
remain proportional to dataset size. Authoritative rows occupy an additional
local SQL copy. Rebuilding application tables still touches the full dataset
when authoritative state changes. This reduces replication payload and history
memory use; it does not make reconciliation proportional only to changed rows.
History is streamed on first bootstrap; checkpoint discovery does not bypass
PouchDB's initial replication of historical documents.

Upgrade clients before enabling incremental decisions on the server: older
clients require a snapshot on every decision. Upgraded clients rebuild their
SQL baseline from the existing replicated history on first startup. Existing
CouchDB documents and SQL receipts are retained; they are not rewritten.

CouchDB owns document revisions, durable replication, retry/checkpoint behavior,
and conflict detection. Unique immutable proposals avoid competing revisions
for normal edits; divergent revisions of the same proposal or decision are
reported and are never silently merged. The application must repair such
protocol violations administratively. CouchDB does not resolve SQL conflicts.
The relational policy is serial server commit order: later accepted updates
win for the fields they change; duplicate inserts, missing-row updates, and
constraint violations reject the whole transaction. There is no automatic
semantic merge of mutually incompatible relational transactions.

## Schema, scope and operational requirements

- PostgreSQL is authoritative. Route all replicated writes through PGCouch;
  external SQL writes are not captured automatically.
- Use matching client/server schemas. Foreign keys between replicated tables
  must be `DEFERRABLE` to support cyclic transactions and atomic snapshot
  replacement. Transactions explicitly defer constraints and validate them
  before success. Keep all affected tables in the same configured scope.
- Ordinary decisions contain only changed rows; checkpoints periodically include
  all configured rows. Increasing `checkpointInterval` reduces checkpoint storage
  at the cost of more deltas between checkpoints. History still grows over time;
  no pruning/compaction policy is implemented. Do not delete protocol documents
  or SQL receipts: ordering, recovery and idempotency depend on them.
- Initialization installs the `pgcouch_capture` function and an AFTER row trigger
  on each configured server table. The database account needs permission to
  create functions and triggers. Do not disable or replace these capture triggers.
  `TRUNCATE` and writes outside plugin transactions are not replicated.
- Local snapshot replacement writes explicit columns. Generated columns and
  non-idempotent local triggers are not supported; avoid them in the client
  schema. Use UUIDs for offline IDs, or an application-specific globally unique
  allocation scheme for integer IDs.
- Treat a CouchDB database as one shared trusted read scope. Every member can
  read every proposal and every snapshot. `authorize` controls SQL service reads
  and mutations, **not CouchDB replication reads**. Use separate databases and
  PostgreSQL scopes for different tenants. Do not rely on filtered replication
  for access control.
- Install `pgcouchValidationDocument()` using an admin connection before clients
  replicate. Give only the server account the `pgcouch-server` role; give normal
  clients database membership without that role. The validator prevents clients
  from writing decisions or mutating/deleting protocol documents. Configure
  authentication, `_security`, and CORS at the CouchDB/proxy layer.
- Authorization defaults to allowing operations, matching the Electric plugin.
  A replicated document carries no verified HTTP/socket identity. If the policy
  requires identity, provide `contextForDocument(doc)` from a trusted mapping
  (such as a database dedicated to an authenticated account), rather than trusting
  fields supplied inside the document. Direct service calls use Express-X's
  service invocation context.
- Query errors and initialization failures reject their promises; background
  replication failures and server rejections go to `onError`. `online` indicates
  the local loop/network status, not confirmed remote reachability.

## Tests

```sh
node --test test/pgcouch.test.mjs
# Isolated temporary schema, real PostgreSQL, PouchDB in-memory transport:
PGCOUCH_TEST_DATABASE_URL=postgresql://localhost/postgres \
  node --test test/pgcouch-postgres-couch.test.mjs
# Add real CouchDB and HTTP permission/validator checks (admin URL):
PGCOUCH_TEST_DATABASE_URL=postgresql://localhost/postgres \
PGCOUCH_TEST_COUCH_URL=http://admin:password@localhost:5984 \
  node --test test/pgcouch-postgres-couch.test.mjs
```

The integration test creates and removes a random PostgreSQL schema, CouchDB
database and test user. Its connection must have permission to manage those
resources. Protocol references: [PouchDB replication](https://pouchdb.apache.org/guides/replication.html),
[PouchDB conflicts](https://pouchdb.apache.org/guides/conflicts.html), and
[CouchDB validation](https://docs.couchdb.org/en/stable/ddocs/ddocs.html#validate-document-update-functions).


## Shared browser worker

`@jcbuisson/express-x-plugins/pgcouch-worker` exports `startPGCouchWorker`.
The plugin owns PGlite initialization, PouchDB's `indexeddb` adapter, replication,
transport-cursor upgrades, shared ownership, and the model request/subscription
protocol. Application schema, models, remote URL and presentation remain in the
application.

Keep a small bundler entry such as `sync.worker.js`:

```js
import { startPGCouchWorker } from '@jcbuisson/express-x-plugins/pgcouch-worker'
import schema from './local-schema.sql?raw' // Vite; otherwise pass SQL text

startPGCouchWorker({
  schema,
  models: ['user', 'group', 'user_group_relation'],
  remote: '/couchdb/my-database',
  storagePrefix: 'my-app',
})
```

Call this synchronously, before any top-level await. It registers the connection
handler immediately, then initializes asynchronously, preserving WebKit's first
connection. The returned `{ ready }` controller resolves after local setup.
Worker errors are reported to connected ports.

For Vite, import the entry as `./sync.worker.js?sharedworker&url` and construct a
module SharedWorker. Use `storageForRemote` from the `/pgcouch-storage` export on
both sides to derive the worker name from the remote URL and the same prefix:

```js
import workerUrl from './sync.worker.js?sharedworker&url'
import { storageForRemote } from '@jcbuisson/express-x-plugins/pgcouch-storage'

const storage = await storageForRemote('/couchdb/my-database', location.origin, 'my-app')
const worker = new SharedWorker(workerUrl, { type: 'module', name: storage.worker })
worker.port.start()
```

Install the optional browser peers: `@electric-sql/pglite`, `rxjs`,
`pouchdb-browser`, and `pouchdb-adapter-indexeddb`. Server-only users do not need
the browser peers.

Ports emit `{ type: 'sync-state', ready, syncing, stage }`. Queue application
messages until `ready` is true. Requests carry a unique `id` and use:

- `call`: `{ model, method, args }` for CRUD, `prepare`, `getStatus`,
  `flushQueue`, and `waitForIdle`;
- `subscribe`: `{ model, args: [where, queryOptions] }`, followed by `value`
  messages; cancel using `unsubscribe` with the same `id`;
- `transaction`: `{ args: [operations] }` for an operation-array transaction;
- `network`: `{ args: [enabled] }` to pause/resume replication for all tabs;
- `dispose`: release this port's subscriptions without disposing other tabs.

Responses are `{ type: 'result', id, value }` or include an `error` object.
Subscription errors use `subscription-error`; replication errors use
`sync-error`. `getStatus` also returns `initialized`, indicating whether a server
snapshot has been applied. UI loading/error labels and startup timeouts belong to
the application's port adapter. All connected tabs share the offline switch.

`storageForRemote(remote, origin, prefix = 'express-x')` returns `remote`,
`relational`, `transactions`, `lock`, and `worker`. Identity includes host and
path, excluding credentials, query and fragment. Preserve the application's
existing prefix when upgrading: `offline` retains the demo's previous names.
Old adapter stores are retained; relational rows and pending mutations are not
cleared. A transport change resets only its local change cursor.
