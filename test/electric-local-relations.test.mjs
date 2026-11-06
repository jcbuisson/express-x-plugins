import assert from 'node:assert/strict'
import test from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { firstValueFrom, filter, timeout } from 'rxjs'
import { electricClientPlugin } from '../src/electric-client-plugin.mjs'

test('local cascade visibility covers reads, observers, transitive cycles and restart', async () => {
   const db = new PGlite()
   try {
      const app = { service: () => ({}) }
      const relations = [
         { child: 'memberships', parent: 'groups', column: 'group_uid' },
         { child: 'details', parent: 'memberships', column: 'membership_uid' },
         { child: 'memberships', parent: 'details', column: 'detail_uid' },
      ]
      electricClientPlugin(app, { relations })
      const make = name => app.createElectricModel(name, { localDb: db, network: { onLine: false } })
      const groups = make('groups'), memberships = make('memberships'), details = make('details')
      await groups.prepare()
      const group = await groups.create({})
      const membership = await memberships.create({ group_uid: group.uid })
      const detail = await details.create({ membership_uid: membership.uid })
      await memberships.update(membership.uid, { detail_uid: detail.uid })
      const unrelated = await memberships.create({ group_uid: crypto.randomUUID() })
      const nullable = await memberships.create({ group_uid: null })
      const observed = firstValueFrom(memberships.getObservable().pipe(filter(rows => rows.length === 2), timeout(3000)))
      await groups.remove(group.uid)
      assert.deepEqual((await observed).map(row => row.uid).sort(), [unrelated.uid, nullable.uid].sort())
      assert.equal(await memberships.findUnique({ uid: membership.uid }), null)
      assert.deepEqual(await details.findMany(), [])
      assert.equal((await db.query("SELECT * FROM electric_local_rows WHERE model_name = 'memberships'")).rows.length, 3)
      assert.deepEqual((await db.query("SELECT DISTINCT model_name FROM electric_mutation_queue WHERE action = 'delete'")).rows, [{ model_name: 'groups' }])
      // Parent acknowledgement must not reveal children before their Shape arrives.
      await db.query("DELETE FROM electric_mutation_queue WHERE model_name = 'groups'")
      assert.equal(await make('memberships').findUnique({ uid: membership.uid }), null)
   } finally { await db.close() }
})
