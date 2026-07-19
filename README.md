# express-x-electric

The smallest useful ElectricSQL integration for Express-X. Express-X handles
authorized PostgreSQL mutations; Electric's Shape API streams those changes to
clients. It is analogous to `express-x-drizzle`, but deliberately has no
metadata table and no custom `sync.go`: Electric is the sync engine.

## Install

```sh
npm install @jcbuisson/express-x-electric pg
```

This server-only installation does not install the browser Electric client or
RxJS.

## Server

```js
import { Pool } from 'pg'
import { expressX } from '@jcbuisson/express-x'
import { electricOfflinePlugin } from '@jcbuisson/express-x-electric'

const app = expressX()
const db = new Pool({ connectionString: process.env.DATABASE_URL })

app.configure(electricOfflinePlugin, db, [
  'todos',
  { name: 'projects', table: 'projects', primaryKey: 'uid' },
], {
  electricUrl: process.env.ELECTRIC_URL, // ElectricSQL sync service, e.g. http://localhost:3000/v1/shape
  sourceId: process.env.ELECTRIC_SOURCE_ID,
  sourceSecret: process.env.ELECTRIC_SOURCE_SECRET,
  authorize: async (context, { modelName, action }) => {
    // `context.transport` is "http" for Shapes and "ws" for Express-X calls.
    return Boolean(context.request?.user || context.socket?.data?.user)
  },
})
```

This registers one Express-X service per model with the familiar API:

- `createWithMeta(uid, data, createdAt)`
- `updateWithMeta(uid, data, updatedAt)`
- `deleteWithMeta(uid, deletedAt)`

Synchronized reads are provided client-side by `getObservable(where)` below;
one-shot server reads would bypass Electric and are intentionally omitted.

Mutation results remain `[value, meta]` tuples for compatibility. `meta.txid`
contains `pg_current_xact_id()` and can be passed to an Electric-aware client to
wait for the matching transaction in its Shape stream.

## Client Shape

Install the optional client dependencies in the browser application:

```sh
npm install @jcbuisson/express-x-electric @electric-sql/client rxjs
```

Configure the client plugin and use the same `getObservable(where)` style as
`express-x-client`'s offline model:

```js
import { electricClientPlugin } from '@jcbuisson/express-x-electric/client'

app.configure(electricClientPlugin, {
  shapePath: '/electric/v1/shape',
})

const todo = app.createElectricModel('todos')
const subscription = todo.getObservable({ completed: false }).subscribe(rows => {
  console.log(rows)
})

// Mutations use the matching Express-X service and are reflected by Electric.
await todo.create({ title: 'Learn Shapes', completed: false })
await todo.update(uid, { completed: true })
await todo.remove(uid)

subscription.unsubscribe()
```

Object filters use parameterized Electric Shape predicates. Exact values,
`null`, and `gt`/`gte`/`lt`/`lte` ranges are supported.

All Electric cursor parameters are forwarded. The client cannot override the
configured table, and Electric source credentials stay server-side.

## Model configuration

Models may be strings (table, service name, and default `uid` key) or objects:

```js
{ name: 'todo', table: 'todos', primaryKey: 'id' }
```

Names are restricted to simple PostgreSQL identifiers. Values are always sent
as query parameters; range filters support `gt`, `gte`, `lt`, and `lte`.

Requires Node 18+ for the built-in Fetch API. The PostgreSQL client only needs a
`query(sql, values)` method; a `pg.Pool` is recommended so each mutation and its
transaction ID are captured in the same transaction.
# express-x-electric
