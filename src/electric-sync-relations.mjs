
// PostgreSQL foreign keys remain satisfied by soft-deleted rows
// Mirror CASCADE relationships at the sync boundary, where deletions must produce tombstones.
export async function discoverSyncRelations(db, models) {
   const { rows } = await db.query(`SELECT child.table_name AS child_table, parent.table_name AS parent_table,
      ARRAY(SELECT attname::text FROM unnest(fk.conkey) WITH ORDINALITY AS key(attnum, position)
         JOIN pg_attribute ON attrelid = fk.conrelid AND pg_attribute.attnum = key.attnum ORDER BY position) AS child_columns,
      ARRAY(SELECT attname::text FROM unnest(fk.confkey) WITH ORDINALITY AS key(attnum, position)
         JOIN pg_attribute ON attrelid = fk.confrelid AND pg_attribute.attnum = key.attnum ORDER BY position) AS parent_columns
      FROM pg_constraint AS fk
      JOIN unnest($1::text[]) AS child(table_name) ON fk.conrelid = to_regclass(child.table_name)
      JOIN unnest($1::text[]) AS parent(table_name) ON fk.confrelid = to_regclass(parent.table_name)
      WHERE fk.contype = 'f' AND fk.confdeltype = 'c'`, [models.map(model => model.quotedTable)])
   const byTable = new Map(models.map(model => [model.quotedTable, model]))
   const relations = rows.map(row => {
      const child = byTable.get(row.child_table)
      const parent = byTable.get(row.parent_table)
      if (row.child_columns.length !== 1 || row.parent_columns.length !== 1 || row.parent_columns[0] !== parent.primaryKey) {
         throw new TypeError('sync CASCADE foreign keys must reference a single configured primary key')
      }
      // Names come from PostgreSQL, not request data; still quote embedded quotes.
      const column = row.child_columns[0]
      return { child, parent, column, quotedColumn: `"${column.replaceAll('"', '""')}"` }
   })
   const members = new Set(relations.flatMap(({ child, parent }) => [child, parent]))

   async function lock(tx, model) {
      if (!members.size || model && !members.has(model)) return
      // One transaction-scoped lock, acquired before any cursor/row locks. This
      // serializes relational mutations across processes and handles cyclic
      // graphs without lock-order inversions. Unrelated models remain parallel.
      await tx.query('SELECT pg_advisory_xact_lock(178693771, 1)')
   }

   async function deletedParent(tx, model, action, id, data, editedCreate) {
      const parents = relations.filter(relation => relation.child === model)
      if (action === 'delete' || !parents.length) return null
      const { rows } = await tx.query(`SELECT * FROM ${model.quotedTable} WHERE ${model.quotedPrimaryKey} = $1 FOR UPDATE`, [id])
      const current = rows[0]
      // A colliding create cannot overwrite or delete another client's row.
      if (action === 'create' && !editedCreate && current) return null
      if (action === 'update' || editedCreate) {
         if (!current || current.deleted) return null
      }
      const changes = Object.fromEntries(Object.entries(data).filter(([column, value]) => column !== model.primaryKey && value !== undefined))
      const proposed = { ...current, ...changes, [model.primaryKey]: id }
      for (const relation of parents) {
         const parentId = proposed[relation.column]
         if (parentId == null) continue
         const { parent } = relation
         // PostgreSQL permits a newly inserted row to reference itself.
         if (parent.table === model.table && String(parentId) === String(id)) continue
         const result = await tx.query(`SELECT deleted FROM ${parent.quotedTable} WHERE ${parent.quotedPrimaryKey} = $1 FOR UPDATE`, [parentId])
         if (!result.rows[0]) {
            // The parent may still be queued on another model. Retry rather than
            // discarding a legitimate offline parent+child creation.
            const error = new Error(`Waiting for '${parent.name}' parent '${parentId}' to synchronize`)
            error.code = 'sync-parent-missing'
            throw error
         }
         if (result.rows[0].deleted) return {
            code: 'sync-parent-deleted',
            message: `Change to '${model.name}' discarded because '${parent.name}' parent '${parentId}' was deleted`,
         }
      }
      return null
   }

   async function cascade(tx, model, id, writeTombstone, visited = new Set()) {
      const key = JSON.stringify([model.table, id])
      if (visited.has(key)) return
      visited.add(key)
      for (const relation of relations.filter(relation => relation.parent === model)) {
         const { child } = relation
         const { rows } = await tx.query(`SELECT ${child.quotedPrimaryKey} FROM ${child.quotedTable}
            WHERE ${relation.quotedColumn} = $1 AND NOT deleted ORDER BY ${child.quotedPrimaryKey} FOR UPDATE`, [id])
         for (const row of rows) {
            const childId = row[child.primaryKey]
            await writeTombstone(tx, child, childId)
            await cascade(tx, child, childId, writeTombstone, visited)
         }
      }
   }

   async function repair(tx, writeTombstone) {
      await lock(tx)
      for (const { child, parent, quotedColumn } of relations) {
         const { rows } = await tx.query(`SELECT child.${child.quotedPrimaryKey} FROM ${child.quotedTable} AS child
            JOIN ${parent.quotedTable} AS parent ON child.${quotedColumn} = parent.${parent.quotedPrimaryKey}
            WHERE NOT child.deleted AND parent.deleted ORDER BY child.${child.quotedPrimaryKey} FOR UPDATE OF child`)
         for (const row of rows) {
            const id = row[child.primaryKey]
            await writeTombstone(tx, child, id)
            await cascade(tx, child, id, writeTombstone)
         }
      }
   }

   return { lock, deletedParent, cascade, repair }
}
