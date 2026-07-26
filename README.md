# express-x-plugins

Currently includes:

- a plugin which preserves room membership and socket data across page reloads
- a plugin integrating ElectricSQL sync engine into express-x, which greatly simplifies relational database
access and provides powerful local-first features


## Install

```sh
npm install @jcbuisson/express-x-plugins
```


## Reload plugin

### Server

```js
import { reloadPlugin } from '@jcbuisson/express-x-plugins/reload-server'
```


## Local-first Postgres plugin

The smallest useful ElectricSQL integration for Express-X. Express-X handles authorized PostgreSQL mutations;
Electric's Shape API streams those changes to clients: Electric is the sync engine.

### Server

```sh
npm install pg
```

```js
import { Pool } from 'pg'
import { expressX } from '@jcbuisson/express-x/server'
import { electricOfflinePlugin } from '@jcbuisson/express-x-plugins/electric-server'

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

- `create(uid, data)`
- `update(uid, data)`
- `delete(uid)`

Synchronized reads are provided client-side by `findMany(where)`, `findUnique(where)`, and `getObservable(where)` below;
one-shot server reads would bypass Electric and are intentionally omitted.

Mutation methods return the created, updated, or deleted row directly.

### Client

Install the optional client dependencies in the browser application:

```sh
npm install @electric-sql/client rxjs
```

```js
import { electricClientPlugin } from '@jcbuisson/express-x-plugins/electric-client'

app.configure(electricClientPlugin, {
  shapePath: '/electric/v1/shape',
})

const todo = app.createElectricModel('todos')

const incompleteTodos = await todo.findMany({ completed: false })
const selectedTodo = await todo.findUnique({ uid })

const subscription = todo.getObservable({ completed: false }).subscribe(rows => {
  console.log(rows)
})

// Mutations use the matching Express-X service and are reflected by Electric.
await todo.create({ title: 'Learn Shapes', completed: false })
await todo.update(uid, { completed: true })
await todo.remove(uid)

subscription.unsubscribe()
```

`findMany(where)` resolves with all matching rows from the first synchronized Shape emission.

`findUnique(where)` resolves with the first matching row, or `null` when there is no match.
Both unsubscribe after their first emission; if called within a Vue scope, they also unsubscribe
if that scope is disposed before a result arrives.

Object filters use parameterized Electric Shape predicates. Exact values,
`null`, and `gt`/`gte`/`lt`/`lte` ranges are supported.

All Electric cursor parameters are forwarded. The client cannot override the
configured table, and Electric source credentials stay server-side.

### Model configuration

Models may be strings (table, service name, and default `uid` key) or objects:

```js
{ name: 'todo', table: 'todos', primaryKey: 'id' }
```

Names are restricted to simple PostgreSQL identifiers. Values are always sent
as query parameters; range filters support `gt`, `gte`, `lt`, and `lte`.

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
    ports:
      - "3001:3000"
```

```
docker compose pull electric
docker compose up electric
```
