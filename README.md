
# express-x-plugins


IMPORTANT FOR ELECTRIC

# set lc_messages=C for the PostgreSQL role so Electric receives recognizable English errors
ALTER ROLE chris SET lc_messages = 'C';

# ALLOW POSTGRES LOGICAL REPLICATION
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER ROLE chris WITH REPLICATION;

(restart postgres: `sudo systemctl restart postgresql`)

## Use HTTP2
nginx: `listen 443 ssl http2;`


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

- `findUnique(where)`
- `findMany(where, queryOptions)`
- `create(id, data)` for a client-generated primary key
- `create(data)` for a database-generated primary key
- `update(id, data)`
- `delete(id)`

The client model provides synchronized reads through `findMany(where)` and
`getObservable(where)`. The service also exposes direct one-shot server reads
when synchronization is not required.

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

// Client-generated UUID stored in the default `uid` primary key:
const todo = app.createElectricModel('todos')

// Or, for a database-generated primary key such as SERIAL/IDENTITY:
const numberedTodo = app.createElectricModel('numberedTodos', {
  idGeneration: 'server',
})

const incompleteTodos = await todo.findMany({ completed: false })

const subscription = todo.getObservable({ completed: false }).subscribe(rows => {
  console.log(rows)
})

// Mutations use the matching Express-X service and are reflected by Electric.
await todo.create({ title: 'Learn Shapes', completed: false })
await todo.update(uid, { completed: true })
await todo.remove(uid)

const created = await numberedTodo.create({ title: 'Assigned by PostgreSQL' })
console.log(created.id)

subscription.unsubscribe()
```

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
