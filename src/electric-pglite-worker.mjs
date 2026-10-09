import { PGlite } from '@electric-sql/pglite'
import { worker } from '@electric-sql/pglite/worker'

await worker({ init: options => PGlite.create(options) })
