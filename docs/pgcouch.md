# PGCouch

PGCouch implements the Electric plugin's model CRUD/query API using PostgreSQL,
PGLite and CouchDB/PouchDB. It imports no Electric client or sync extensions.
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
`update(id, data)`, and `delete(id)` Express-X services. Server `create(data)`
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

Client methods: `create(data)`, `create(id, data)`, `update(id, data)`,
`remove(id)`, `findMany(where, { limit })`, `findUnique(where)`,
`getObservable(where, { initialValue, limit })`, `prepare()`, `start()`, `stop()`,
`dispose()`, `flushQueue()`, `waitForIdle()`, `subscribe(callback)`, and `getStatus()`.
Filters support equality, null, and `gt/gte/lt/lte` ranges. Observable queries emit
after complete local commits and reconciliation, never intermediate operations.
`pgcouchTransaction(operations)` returns `{ id, results }` after the durable local
commit. Completion means locally committed; `pending` drops once the server's
decision arrives. Server rejections are reported through `onError` and `failed`.
An update to a missing row rejects; deleting a missing row is a no-op.

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
`rejected`), a PostgreSQL-assigned `sequence`, an authoritative `snapshot`, and
`results` or `error`. Server service calls use the same receipt/outbox protocol.

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
5. Clients atomically replace their authoritative state from the newest decision
   and replay their own undecided local transactions in order. Failed rebases are
   retained as pending/blocked until the server decides them. Rejected optimistic
   work is removed during reconciliation. Newly installed clients bootstrap from
   `result:bootstrap` or the latest snapshot, without replaying history in arrival
   order.

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
- The initial implementation includes **all configured rows in every decision**.
  This captures defaults, cascades and server trigger effects reliably, but has
  substantial storage/bandwidth cost. It is intended for bounded datasets, not
  large tables or unrestricted transaction histories. No pruning/compaction
  policy is implemented. Do not delete protocol documents or SQL receipts:
  ordering, recovery and idempotency depend on them.
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
