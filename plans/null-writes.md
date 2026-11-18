# Null writes for nullable columns

- **Status:** implemented 2026-11-18, in the working tree (not committed)
- **Written:** 2026-11-17, against `faea38d` (line numbers below are from that commit)
- **Reviewed:** 2026-11-17, in discussion. Decisions 1 to 6 are settled. Decision 7 came out of the review.
- **Revised:** 2026-11-18, after a written second review of the implementation details (see *Review*). Step 6 is new, and steps 1, 5, 7 and 8 say more about edge cases. Decision 8 came out of it. Decisions 7 and 8 were settled the same day.
- **Modules:** rdb, rdb_lang, rdb_runtime, rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_adapter_test, rdb_adapter_test_gen

## Summary

rdb has no null. A nullable column is one a row may leave unwritten, and once a row has written it, no operation takes the value away again. An insert also cannot ask for an empty column where the schema gives a default. So a projection's `UPDATE … SET col = NULL` has no operation to become, and the device that made it stops agreeing with the database.

This plan adds a **null write**: an insert or an update can set a nullable column to `NULL`. It follows SQL. An explicit `NULL` wins over the column's default, and a column that is left out gets the default. The plan covers the operation format, how values are read, C-SQL (`NULL` in `INSERT` and `UPDATE`, and `IS [NOT] NULL` in `SELECT`), the projection of a `NULL` into the three targets, and the ingestion of a local write that sets one.

It also fixes three defects in how an operation's checks read the columns it carries (step 3). They are on the lines a null has to change, and each would otherwise give a null the wrong check.

One more fix has nothing to do with nulls: a SQLite target that captures local writes cannot project the drop of a column (step 6). It is here because the adapter tests of this plan need such a projection to work.

The project is unreleased. The operation format changes, and nothing here keeps existing stores or projected databases working.

## Background

The terms the rest of the plan uses.

- **Row operation.** An insert, an update or a delete of one row. An insert carries the columns it sets. An update carries only the columns it changes.
- **How a column is read.** Every insert and update is a *write* to each column it carries. The value of a column is the latest write to it in causal order. When two writes are concurrent, the one in the entry with the larger hash wins. This is last-writer-wins, per column.
- **Void.** An operation that breaks a rule, as seen from the reader's position, is void: readers ignore it. A void write does not hide the writes beneath it, so the value that was there before shows again. An operation can become void after it was applied, when a concurrent delete or deploy comes into view.
- **Never written.** A column no live write gives a value to. It reads as the column's default, or as nothing when the column has none.
- **Defaults do not change.** No schema rule changes a column's default or its nullability in place (`modules/rdb/src/rschema/payload.ts:365-371`). A column gets a new default only by being dropped and added again, which starts a new *incarnation* of the column and leaves its old writes behind. So "never written" reads the same default for the column's whole life. Dropping a table and adding it again starts a new incarnation of the table and of every column in it.
- **Checks.** An operation's allow rule and its foreign key references are checked twice. Once when it is appended, under the schema at its own position. And again by every reader, under a schema that also holds the deploys concurrent with the operation. The second check is what makes an operation void.
- **Bundle.** Several row operations in one entry. It is all-or-nothing: when one of them is void, the whole bundle is.
- **Delta.** The list of differences between two versions of a group. Its row channel names each row whose values changed, with the value of each changed column before and after.
- **Target.** The local database a group is projected into: SQLite, IndexedDB, or the in-memory target used in tests. The adapter turns a delta into *row actions* and a target applies them.
- **Capture and ingestion.** A target records the writes an app makes to it directly. Ingestion turns those captured changes into row operations. When rdb refuses one, the row in the target is put back to what rdb holds (a *revert*).

## What happens today

- **rdb.** A row operation's values are `json.Literal`, which has no null (`modules/json/src/literal.ts:2`), and every column type refuses one (`modules/rdb/src/rschema/validate.ts:99`).
- **C-SQL.** `NULL` parses, and then every use of it throws (`modules/rdb_lang/src/bind/values.ts:97`). There is no `IS NULL`.
- **SQLite target.** `UPDATE … SET memo = NULL` is captured, loses its `NULL` when the captured changes are read (`modules/rdb_adapter_sqlite/src/sqlite_target.ts:1155-1158`), and is then dropped as an update of no columns (`modules/rdb_adapter/src/ingest.ts:430`). The device shows `NULL`, rdb keeps the old value, and nothing is logged.
- **IndexedDB and memory targets.** The `null` reaches rdb, which refuses the operation. Recording the refusal then hashes a payload that holds a null, which throws (`modules/rdb_adapter/src/ingest_orchestrator.ts:387-406`, `modules/dag/src/dag.ts:65-69`). The captured change is never acknowledged, so every later ingestion pass fails the same way.
- **IndexedDB, a property removed by a `put`.** Not captured at all (`modules/rdb_adapter_idb/src/idb_facade.ts:245-259`).

## Behaviour

A column of a row is in one of three states: never written, holding a value, or holding a null.

| Write | Column has a default | No default |
|---|---|---|
| Insert, column left out | default | `NULL` |
| Insert, explicit null | `NULL` | `NULL` |
| Update to a value | the value | the value |
| Update to null | `NULL` | `NULL` |
| Row older than the column | default | `NULL` |

- **A null is a write.** It hides every write its writer had seen. A void null write is ignored, and what was there before shows again.
- **A value wins over a concurrent null.** Between two concurrent values, or two concurrent nulls, the larger entry hash decides, as today. So a null clears only what its writer saw, and a clear never wins a race against a write.
- **An insert's null where the column has no default** reads the same as leaving the column out. The one case found where the two could differ is two concurrent inserts of the same row, and there the other insert's value shows either way. This is by reasoning, and the read tests cover that case.
- **`NOT NULL` columns.** A null is refused, in an insert and in an update.
- **Readonly columns.** An update to null is refused, as any update of a readonly column is. An insert may set one to null.
- **Foreign key columns.** A null means no reference, and there is nothing to check. Where the column has a default, an explicit null also drops the reference the default would have made.
- **An operation answers for the references it makes.** An insert: each foreign key column it sets, and the default of each one it leaves out. An update: only the foreign key columns it sets.
- **A column or table added again.** What an operation carries for a column counts in its checks only while the column is the one it wrote to. An operation for a table that was dropped and added again is void (*Decisions* 7).
- **Reads.** A column that reads `NULL` is absent from a row's values, as an unwritten column without a default is today. Readers cannot tell the two apart, and do not need to.
- **Comparisons.** A `NULL` compares false with everything, in queries and in allow rules, as a missing value does today.
- **Indexed lookups.** A null adds nothing to the index of a `PUB` column, so an equality lookup never finds it.
- **No "back to default".** Writing the default's value gives the same result, because defaults do not change.

## Changes

In landing order. Each step leaves the tree working.

### 1. rdb: the operation

- **Format** (`modules/rdb/src/rtable/payload.ts:29-65`). Inserts and updates gain an optional `nulls`: the names of the columns set to null. `values` stays as it is, so no null enters the json layer or a hash.
- **One spelling** (`modules/rdb/src/rtable/validate.ts:25-56`). `nulls` is left out when empty. Its names are valid column names, in strictly ascending order, and none of them is also in `values`. An update must carry a value or a null.
  - The order is by UTF-16 code unit: the order of `<` on strings, and of `sort()` with no comparator. The validator checks that each name is greater than the one before it, which also refuses a repeated name.
  - `localeCompare` must not be used, by the writer or by the validator. Its order depends on the locale of the runtime, so two replicas could disagree on whether an operation is valid. Column names allow upper case and `_` (`modules/rdb/src/rschema/validate.ts:34`), where the two orders differ.
- **Schema rules** (`modules/rdb/src/rtable/validate_ops.ts:30-74`).
  - Insert: each nulled column exists and is nullable. Whether it has a default does not matter. See *Decisions* 1.
  - Update: each nulled column exists, is nullable, and is not readonly.
- **Write API** (`modules/rdb/src/rtable/interfaces.ts:15`, `:47-50`, `modules/rdb/src/rtable/rtable.ts:154-176`). `insert` and `update` take `RowWrite`, a map from column to a value or `null`, and write what they are given. One new exported helper, `rowWriteParts(write)`, splits a write into `values` and `nulls`, sorted in the order the validator checks. It needs no schema. `RTableImpl.insert` and `update` use it, and so does every caller that builds an operation for a bundle (steps 5 and 8).
- **Index tags** (`modules/rdb/src/rtable_group/scopes.ts:178-198`). `deriveRowOpInnerMeta` tags a nulled column in `cols`, the tag the per-column read uses, and exports nothing for it under `pub-<column>`.

### 2. rdb: reads

- **The column read** (`modules/rdb/src/rtable/view.ts:119`, `:504-541`). `ColumnValue.value` becomes a value, `null` when a null write wins, or `undefined` when no live write exists. In `columnValue`, an operation that nulls the column is a candidate like one that carries a value. Among the candidates, which are concurrent with each other, one with a value beats one with a null, and the larger entry hash decides between two of the same kind. This is the only place a winner is picked: the row read, the delta and the `EXISTS` check all go through it.
- **Default fallback.** Only `undefined` falls back to the default. Several sites use `??` for the fallback today, and `??` would also replace a `null`. Add one helper that maps a column read and the column's default to a value or nothing, and use it at each site:
  - `resolveRow` (`view.ts:545-563`);
  - the leaf check of an `EXISTS` read (`view.ts:922`);
  - the subject row of an allow rule, `opSubject` (`modules/rdb/src/rtable_group/predicates.ts:508`, `:519-521`). For an insert, a column the operation nulls has no value.
- **Delta** (`modules/rdb/src/rtable/interfaces.ts:28-32`, `modules/rdb/src/rtable/view.ts:601-611`, `modules/rdb/src/rtable/delta.ts:42-63`). `DeltaRowState.written` and a column change's `before` and `after` can be `null`. The three states stay distinct there: `undefined` means never written, and a consumer applies the default. `sameLiteral` compares a null without serializing it.
- **Unchanged.** `Row.values` (a `NULL` column is absent), the deploy that deletes rows not honoring a new foreign key (`modules/rdb/src/rtable_group/group.ts:964-965`), and the lookup of rows by indexed value (`view.ts:850-876`).

### 3. rdb: what an operation's checks read

The checks are `fkTargetsLive` at validation (`modules/rdb/src/rtable_group/validate_ops.ts:459-480`), and at use `opSubject` and `explainRowOpFKReach` (`modules/rdb/src/rtable_group/predicates.ts:495-524`, `:621-661`), called from `diagnoseEntry` (`modules/rdb/src/rtable_group/group.ts:1183-1255`). Three changes, each a defect today (*Found while reading* 2, 4 and 5).

- **Foreign key obligations.** Both functions take the column's default for every foreign key column the operation does not carry, in updates too (`validate_ops.ts:471`, `predicates.ts:636`). Change them so that:
  - an insert answers for each foreign key column it sets, and for the default of each one it leaves out;
  - an update answers only for the foreign key columns it sets;
  - a column the operation nulls carries no obligation.
- **The column must be the one the operation wrote to.** At use, the schema holds the deploys concurrent with the operation, so a column may have been dropped and added again since the operation was written. What the operation carries for a column, a value or a null, then belongs to the old column, and the row does not read it.
  - Compare the column's incarnation in the schema of the check with its incarnation in the schema at the operation's own position. That second schema is `resolveSchemaView(opPos)`, the one the operation's index tags were derived under.
  - When they differ, the column counts as not carried. An insert is checked as if it left the column out: the rule reads the default, and the default's reference is checked. An update is checked as if it did not write the column.
  - It applies to the insert's values and the update's overlay in `opSubject` (`predicates.ts:508`, `:519`), and to `explainRowOpFKReach` (`:636`).
  - At validation the two schemas are the same one, so validation does not change.
- **The table must be the one the operation wrote to.** A column's incarnation is seeded by its table's (`modules/rdb/src/rschema/incarnation.ts:38-46`), so after a table is dropped and added again, every column differs. The operation's row cannot live in the new table either. Make such an operation void, the way one for a dropped table already is (`predicates.ts:569-571`). Without this, a bundle that also writes to another table is half applied. See *Decisions* 7.
- **Cost.** One more schema resolution for each operation a reader checks. `diagnoseEntry` already resolves the schema and the parameters there. If it shows in a profile, the column incarnations can be read from the entry's own `cols` tags instead.

**What else this touches.** Checked by reading the code and the proof of the delta bound. The tests below check it by running.

- **The delta bound** (`modules/rdb/INCREMENTAL_DELTA_BOUND.md`, assumption A3 and Theorem 1). The proof lists what a verdict reads. The list already has the schema at the operation's position as the reader sees it, and the entries in the operation's history. The comparison reads those two and nothing else, so the proof stands as written and the walk does not change. A verdict this step changes belongs to an operation concurrent with a new deploy. Such an operation is above the walk's starting bound and is walked, as it is today when a concurrent deploy changes its rule.
- **The delta's row channel** (`modules/rdb/src/rtable/delta.ts:6-19`). It compares each row within the incarnations of the end version and leaves a re-add to the schema channel. That does not change. The step only changes which walked operations are void.
- **The verdict solve.** Which value a check uses depends on the schema and on the entry. A deploy is never void, so the choice never depends on a verdict, and the rules stay monotone.
- **Reads of another group while its observes are pending** (`modules/rdb/src/rtable/touch.ts`). An operation's own schema comes from deploys only, which are never pending. A default's target is read through the same calls as a carried value's target.
- **The rows a deploy deletes for a new foreign key** (`group.ts:925-979`). It reads rows, not operations, and only columns that keep their incarnation.
- **Lookups by indexed value** (`modules/rdb/src/rtable/view.ts:850-876`). A row is found by a value written to an old column too, since the index tags do not name the incarnation. Each row found is then checked through the column read, which does.
- **A value winning over a null** (step 2). The winner is still a function of the same live candidates, so no read is added.

### 4. rdb: `IS NULL` in queries

- **Filter** (`modules/rdb/src/rtable/query.ts:34-40`). `RowFilter` gains `{ p: 'isNull', col }`, true when the row has no value for the column. `IS NOT NULL` is the existing `not` around it. `col` may be `rowAuthor`, which finds anonymous rows.
- **Evaluation and validation** (`query.ts:59-85`, `:185-231`). The column must exist. Nothing is pushed down to an index, so a filter that is only `IS NULL` scans the table.

### 5. rdb_lang (C-SQL) and rdb_runtime

- **Writes.** `INSERT … VALUES (…, NULL)` and `UPDATE … SET col = NULL` produce a null write, on any nullable column.
  - Binding keeps a `NULL` literal as `null` at `bind.ts:812`, `:579` and `:623`, and the bound types allow it (`modules/rdb_lang/src/bind/bind.ts:150`, `:160`).
  - `canonicalEncodeRowValues` passes a null through (`modules/rdb_lang/src/bind/values.ts:77-94`).
  - `bindBundleWrite` builds its operations with `rowWriteParts` (`bind.ts:612-629`).
  - A `NULL` for `uuid`, a row id or a parameter stays an error.
- **Conditions** (`modules/rdb_lang/src/syntax/parser.ts:1555-1590`, `modules/rdb_lang/src/syntax/ast.ts:441-454`). `column IS [NOT] NULL` is parsed after the `LIKE` branch of `parseCmpExpr`. `IS` is already a reserved word that nothing uses (`modules/rdb_lang/src/syntax/tokens.ts:65`). The left side must be a column. The node has a `not` flag for `IS NOT NULL`, and no `not` node is put around it. The adapter's own `isNull` node is built the same way (`modules/rdb_adapter/src/types.ts:656`).
- **Lowering** (`modules/rdb_lang/src/compile/query.ts`).
  - `SELECT` (`lowerRowFilter`, `:47-77`): to the new filter atom, inside a `not` when the flag is set.
  - Allow rules (`lowerRestrictionPredicate`, `:135-180`): one error for both forms, which says `IS NULL` is not supported there. With a `not` node around it, `IS NOT NULL` would get the error for `NOT` (`:174`). That would send the author to `IS NULL`, which fails too. See *Not in this plan*.
  - `EXISTS` filters: already refused by the existing error (`:230`).
  - `= NULL` in a `SELECT`: an error that points to `IS NULL`, in place of today's message (`:130`).
- **Rendering** (`modules/rdb_lang/src/reverse/render.ts:342-360`). `renderRowOp` prints a nulled column as `NULL`, so a logged or dumped operation binds again to the same operation.
  - An insert's column list and value list, and an update's `SET` list, are each built from the values and the nulls together. `uuid` is one more item of an insert's lists.
  - So an insert of only nulls, an update of only nulls, and an insert that sets no column render without a stray comma or an empty `SET`. The last of them renders `(uuid, )` today (*Found while reading* 8).
- **rdb_runtime** (`modules/rdb_runtime/src/session/authz_suggest.ts:185-199`). `rowOpFromBound` builds its payload from the bound write with `rowWriteParts`.
- **Reference text** (`modules/rdb_lang/src/reference/commands.ts:129-175`). The `INSERT`, `UPDATE` and `SELECT` entries.

### 6. SQLite target: dropping a column while capture is on

A defect of today's code that has nothing to do with nulls (*Found while reading* 7). It comes ahead of the other adapter steps because their tests project a dropped column, and a column added again, into a target that captures local writes.

- **The defect** (`modules/rdb_adapter_sqlite/src/sqlite_target.ts:791-811`). `dropColumn` runs `ALTER TABLE … DROP COLUMN` and installs the capture triggers again after it. SQLite refuses to drop a column that a trigger names, and the capture triggers name every column of the table (`:500-535`). The whole apply rolls back, and every later projection of the group fails the same way.
- **What fails.** The drop of a plain column, a plain column that becomes a foreign key, and a column dropped and added again in one deploy. A foreign key or key column is dropped by rebuilding the table, which works: dropping the old table removes its triggers. Adding a column and dropping a table work too.
- **The fix.** With capture on, drop the table's capture triggers before the `ALTER`. They are installed again once the table's meta is written, as line 810 does today. Move the removal out of `installTriggers` (`:501-505`) into a helper that both use.
- **Nothing is missed in between.** An apply is one transaction, and the triggers record nothing during it (`:507`).

### 7. Adapter: projecting a null

- **Row actions** (`modules/rdb_adapter/src/types.ts:504-508`). An upsert's `values` can hold `null`, which means "write `NULL`".
- **From a delta** (`modules/rdb_adapter/src/row_actions.ts:62-80`). For each changed column:
  - `after` is a value: as today;
  - `after` is `null`: the action carries `null`;
  - `after` is `undefined`, so the column went back to never written: the action carries the column's default, or `null` when it has none. Today this case is skipped. See *Found while reading* 1.
  - The check for an identity provider's columns stays ahead of all three cases, where it is today (`:64-68`).
- **From a whole row** (`modules/rdb_adapter/src/project.ts:34-69`). `rowToUpsertAction` carries `null` for every column of the table the row has no value for. The first materialization of a row needs it, or a target would fill a nulled column with its default. A revert needs it too, to undo a local edit of a column that is `NULL` in rdb.
  - It walks the table's columns, where today it walks the row's values.
  - The provider check comes first for each column, before a projected name is computed. A provider's public key column is never projected, and `projectedColumnName` throws on it (`modules/rdb_adapter/src/names.ts:124-128`).
- **Targets.** A `null` skips foreign key id allocation, key interning and value conversion, each of which mishandles one today.
  - SQLite (`modules/rdb_adapter_sqlite/src/sqlite_target.ts:972-1045`): `col = NULL` for an existing row. For a new row the column is named in the `INSERT` with `NULL`, so the DDL default does not apply.
  - IndexedDB (`modules/rdb_adapter_idb/src/idb_target.ts:444-502`) and memory (`modules/rdb_adapter/src/memory_target.ts:777-833`): the key is removed from an existing row. For a new row it is left out. A `NULL` stays a missing key, as an unset column is today. See *Decisions* 3.
  - The default fill of a new row in those two targets (`idb_target.ts:495-497`, `memory_target.ts:826-828`) asks whether the action names the column: `column in action.values`. Today it asks whether the stored row has the key. A null leaves no key, so the fill would write the default over it. The fill itself stays: a new row that arrives in a delta leaves a never-written column out, and gets its default there.

### 8. Adapter: ingesting a null

- **Captured changes** (`modules/rdb_adapter/src/types.ts:139-144`). An insert's and an update's `values` can hold `null`.
- **SQLite** (`sqlite_target.ts:1155-1158`). Reading the captured changes keeps a `NULL` as `null`. An insert's captured row has a `NULL` for every empty column, and the planner sorts out which of them matter.
- **IndexedDB** (`modules/rdb_adapter_idb/src/idb_facade.ts:237-259`, `:484-507`).
  - The facade stores a `null` property as a missing key, as the memory target does. Today it stores the app's record as given, so a local `null` and a projected `NULL` would be two forms in one store.
  - An insert is captured with the `null`s the app's record names.
  - In the diff of a `put`, a missing property and a `null` are the same: `changedColumns` compares the two sides after `?? null`. A column that had a value and now has neither is captured as a change to `null`. A `null` put over a column the target left empty captures nothing.
- **Memory** (`memory_target.ts:262-278`). `localInsert` and `localUpdate` accept `null`, store it as a missing key, and record it.
- **Planner** (`modules/rdb_adapter/src/ingest.ts:263-460`).
  - `reverseCols` maps a `null` to a null of the rdb column for plain, foreign key and key columns. In each branch the readonly check runs first and the null is mapped next, before `lookup` (`:294-296`) or `keyLookup` (`:314-315`) is called. A null foreign key adds no dependency.
  - `translateNet` builds its operations with `rowWriteParts`. An update keeps every null. An insert keeps a null only where the column has a default. Elsewhere a target cannot tell an explicit `NULL` from an empty column, and the two read the same (*Behaviour*), so leaving it out loses nothing and saves an index tag for each empty column. This is the only writer that leaves a null out.
  - A null for a column that rdb's schema does not have follows the same two rules. An insert leaves it out, since the column has no default. An update keeps it, and rdb refuses the update as it refuses a value there. The insert's null is not an error to report: SQLite captures an insert as the whole row, and a deploy that drops a column can arrive between the local insert and the sync that ingests it. See *Decisions* 8.
  - An update that carries only nulls is no longer dropped as empty (`:430`, `:434`).
- **Prefetch** (`modules/rdb_adapter/src/ingest_orchestrator.ts:71-133`). A `null` foreign key is skipped before `prefetch` (`:88-93`), and a `null` key id before `keyHashForId` (`:117-125`). IndexedDB throws on such a lookup today.
- **Refusals.** No operation the planner builds holds a JS `null`, so recording a refusal no longer throws for one.

### 9. Docs

- **rdb README.** A new section, *Nulls and defaults*, with the three states, the table above, and what a null does under concurrency, voiding and a column's re-add. It is the one full explanation. *Column types* links to it. The foreign key section says which references an insert and an update answer for, and what happens to an operation concurrent with a re-add of its column or its table.
- **The foreign key notes in the code.** `rschema/payload.ts:135-140` and the header of `explainRowOpFKReach` (`predicates.ts:597-599`), which say a write answers for a default it inherits.
- **rdb_lang README.** The grammar (`README.md:217-256`), `NULL` in `INSERT` and `UPDATE` (`:99`), and `IS [NOT] NULL` in `SELECT`.
- **rdb_adapter README.** *Change capture* (`README.md:179-188`) says what a local `NULL` becomes, and links to the rdb section. The SQLite and IndexedDB READMEs link to it.
- **Comments.** The ones that say a nullable column "may be absent from a row", in `rschema/payload.ts:89` and the header of `rtable/view.ts`.

## Tests

Described by what they do. Labels continue each file's sequence.

**rdb**

- **Format** (`payload_format_tests.ts`). `nulls` empty, unsorted, repeated, or sharing a column with `values` is refused. An update with only `nulls` is accepted. The order is by code unit: `B`, `_x`, `a` is accepted, `rowWriteParts` produces it, and `a`, `B` is refused.
- **Schema rules** (`rtable_enforce_tests.ts`). A null is refused for a `NOT NULL` column, for an unknown column, and in an update of a readonly column. An insert with a null for a nullable column is accepted, with a default and without. An insert into an identity provider's table that nulls its key id or its public key column is refused.
- **Reads** (`rtable_lww_tests.ts`).
  - An update to null of a column that held a value reads nothing.
  - An insert with an explicit null for a column with a default reads nothing, and the same insert without the column reads the default.
  - A null concurrent with a value: the value shows, whichever entry has the larger hash. Run it over several rows so both orders occur.
  - Two concurrent inserts of the same row, one with an explicit null and one with a value: the value shows. With a default on the column, and one insert leaving the column out while the other nulls it: `NULL` shows.
  - A null written after a value, by a writer that had seen it: nothing shows.
  - A null write that becomes void: the earlier value shows again. With no earlier write, the default shows.
  - A column dropped and added again: a null written before the drop is gone, and the row reads the new default.
- **Indexed lookups.** A row whose `PUB` column is nulled is not found by its old value. A lookup by the default's value does not find a row that nulled the column.
- **Foreign keys.** A row nulls a foreign key column, the old target is deleted, and a later update of the row is accepted. A deploy that adopts a foreign key keeps a row whose column is null.
- **What an operation answers for** (`rtable_enforce_tests.ts`, `rtable_deploy_tests.ts`). Each case is checked on a second replica built by re-applying the entries, as `rtable_deploy_tests.ts` does. The first five are refused, void or half applied the wrong way today, so write them first and see them fail.
  - A foreign key column has a default, and the default's target is deleted. An update of another column of a row that points elsewhere is accepted.
  - An insert sets a column while a concurrent deploy drops the column and adds it again as a foreign key with a default whose target is live. The value it set is not a row of the target table. After the merge the insert is live and the row reads the default.
  - The same, where the insert set a live target and the default's target was deleted: after the merge the insert is void.
  - Both again with an explicit null in place of the value.
  - A bundle inserts into two tables while a concurrent deploy drops one of them and adds it again. After the merge the bundle is void, and neither row is live.
  - A row whose own target was deleted later is still updated. This holds today and must keep holding.
  - An update that sets a column, concurrent with a re-add of that column as a foreign key: the update answers for no reference, and the other columns it sets apply.
  - An insert rule reads a readonly column, and a concurrent deploy adds the column again with another default: the rule is evaluated on the new default.
- **Delta** (`op_delta_tests.ts`, `delta_bound_tests.ts` and the delta parity suite).
  - A change from a value to null, from null to a value, and from null back to never written, each reported with the right `before` and `after`.
  - The concurrent re-add cases above, as deltas from each branch to the merge: the bounded walk gives the full walk's result, and the insert's change of verdict is in the op channel.
  - The parity generator (`test/delta_parity/generate.ts`) already deploys column drops, defaults, foreign key changes and table resets. Add null writes, a column added again as a foreign key with a default, and bundles that write to a table that is reset. The bounded walk and the full walk must agree on the smoke and the extended profile.
- **Queries** (`rtable_query_tests.ts`). `isNull` on a column that is never written, nulled, and holding a value. `not isNull`. `rowAuthor` on an anonymous row.

**rdb_lang**

- `INSERT` and `UPDATE` with `NULL` run and read back as expected. The same inside a `BUNDLE`. An `INSERT` with `NULL` for a column without a default produces an operation that carries the null.
- A logged insert and update with a null render with `NULL`, and binding the rendered text gives the same operation. The same for an insert of only nulls, an update of only nulls, and an insert that sets no column. The last one fails today.
- `SELECT … WHERE col IS NULL` and `IS NOT NULL`. `= NULL` gives the error that names `IS NULL`.
- `IS NULL` and `IS NOT NULL` in an allow rule give the same error, which names `IS NULL`. Inside an `EXISTS` filter both are errors.

**Adapter**, in the shared suite, so each case runs on all three targets

- A fixture table with a nullable column that has a default. The fixtures have none today (`modules/rdb_adapter_test/src/group_fixture.ts:26-94`). Add a table and leave the existing ones as they are.
- **A column dropped while capture is on** (step 6, in the ingestion suite, whose targets capture). Project the drop of a plain column, a plain column that becomes a foreign key, and a column dropped and added again in one deploy. After each, a local insert is captured with the table's new columns and ingested. All three fail on SQLite today, so write them first and see them fail.
- **Projection.**
  - A row nulled in rdb reads `NULL` in the target, also where the column has a default.
  - A row first materialized with a nulled column reads `NULL`, not the default. Both ways a row can arrive: in the first projection, and as a new row of a later one.
  - A new row of a later projection that never wrote a column with a default reads the default.
  - A value set by an update that later becomes void: the target shows the default again, or `NULL`.
  - The row planners' own tests (`modules/rdb_adapter/test`): for a row of an identity provider's table, the action from a whole row and the action from a delta name no public key column, and neither throws.
- **Ingestion.**
  - A local update to `NULL` becomes a null write, and a second device reads `NULL`.
  - A local update to `NULL` of a foreign key column: no lookup is made, and a second device reads `NULL`. IndexedDB throws on that lookup today.
  - A local update to `NULL` on one device, concurrent with a local update to a value on another: both devices end with the value.
  - A local insert with an explicit `NULL` for a column with a default reads `NULL` everywhere.
  - A local insert that leaves empty a column without a default writes no null.
  - IndexedDB: a `put` that drops a property the row had. A `put` of `null` over a column the target left empty captures nothing. A `put` with a `null` property stores no key.
  - A local update to `NULL` of a `NOT NULL` column on memory and IndexedDB is refused and reverted, and the next ingestion pass runs.
  - SQLite: a local insert leaves a column empty, and a deploy that drops the column arrives before the next sync. The insert is accepted. With a value in the column it is refused and reverted, as today.
  - The planner (`ingest_tests.ts`): a `null` in a plain, a foreign key and a key column maps to a null with no lookup, and a `null` in a readonly column of an update is refused.
- **Revert.** A refused local write to a column that is `NULL` in rdb is undone.
- **Harnesses.** The local mutators accept `null` (`ingestion_suite.ts:137-142`, `sqlite_target_tests.ts:125-129`). The seeded history generator in rdb_adapter_test_gen writes nulls, so the planner parity run and the projection sweep cover them. The test-only action store handles a `null` as the targets do, with the same test in its default fill (`modules/rdb_adapter/test/planner_parity/action_store.ts:130-150`).

**Running them.** Downstream modules load each other's `dist`, so build a module before running the suites that depend on it. Type errors in a test file stop the suite from loading without a message, so typecheck the tests of each module touched.

## Effect on the HIP-20 draft

HIP-20 (a unified write API for targets) is a draft and not committed. Three of its rules assume rdb has no null, and change when this lands. A fourth point gains weight:

- **An insert's `null`.** The draft reads it as "not given". It becomes an explicit `NULL`, which differs only where the column has a default. The writer's inserts reach rdb through ingestion, so they get the planner's rule (step 8).
- **An update to `null`.** The draft has the writer throw and the planner refuse it. It becomes a null write.
- **Capturing only changed columns.** The draft already plans it. It matters more here: a value wins over a concurrent null, so a save of every field on one device would always undo a concurrent clear on another.
- **The author column.** The draft makes it the one column where `null` is a value. Null is then a value in every column, and that exception goes away. The draft's table for `autoAuthor` stays.

## Not in this plan

- **`IS NULL` in allow rules.** A rule can read only readonly columns of its row (`modules/rdb/src/rschema/validate.ts:458-463`), and a readonly column cannot be updated. So an update to null never changes a value a rule reads. An insert's null in a readonly column makes every comparison over it false, as a column left out does today. Letting a rule accept such a row needs a new predicate atom in the schema format. That gap exists today and is its own change.
- **`IS NULL` inside an `EXISTS` filter.** Rows are found through the index of the values they carry, and a null has no entry.
- **`NULL` in a catalog's `WITH ROWS`**, and `DEFAULT NULL` in a column declaration. Both stay errors.
- **The other fixes to local writes the HIP-20 draft lists.** Among them: a local insert on memory or IndexedDB does not show a column's default, and an unchanged column in a SQLite `UPDATE` is still captured. Until the second is fixed, an update to `NULL` of a column that is already `NULL` produces a null write that changes nothing, as an unchanged value produces a write today. And an update that names a column without changing it undoes a concurrent clear of that column.
- **Rows older than a foreign key column that is added with a default.** See *Found while reading* 6.
- **An update that re-checks the references its row already holds.** See *Decisions* 2.
- **Three-valued logic in `SELECT`.** `NOT (a = 1)` matches a row whose `a` is null, where SQL and the adapter's queries do not. The two also order nulls differently: last in rdb, first in the adapter.

## Found while reading

Each item says whether it was run on today's code.

1. **A value that goes back to never written stays in the target.** Not run. The row planner skips a column change with no after-value (`row_actions.ts:63`), and each target's upsert of an existing row writes only the columns it is given. So when an update that first set a column becomes void, the projected row keeps the value. Step 7 fixes it.
2. **An update re-checks a foreign key column's default.** Run. `fkTargetsLive` and `explainRowOpFKReach` take the default's reference for every foreign key column the operation does not carry (`validate_ops.ts:471`, `predicates.ts:636`). A task points at one project, the column's default is another project, and that other project is deleted. An update of the task's title is then refused. Without a default on the column the same update is accepted, also after the task's own project was deleted. With nulls the refusal would hit a row that nulled the column too. Step 3 fixes it.
3. **A refusal that cannot be recorded blocks ingestion.** Not run. Step 8 removes the case a `null` causes. Another payload rdb cannot hash, such as a json value with a null inside it written through IndexedDB, would still block it. The HIP-20 draft has the general fix.
4. **A check reads a value that was written to an earlier column.** Run. An insert sets a column while a concurrent deploy drops that column and adds it again as a foreign key with a default. After the merge the row reads the default, and the insert's check still uses the value it carried:
   - the insert carried a text that is no row of the target table, and the default's target is live: the insert is void, for a value its row does not hold;
   - the insert carried a live target, and the default's target was deleted: the insert is live, and its row reads a reference to a deleted row that nothing checked.

   The same lines read the subject row of an allow rule (`predicates.ts:508`, `:519`); that path was read, not run. Step 3 fixes both.
5. **A bundle concurrent with a table reset is half applied.** Run. A bundle inserts into `tasks` and into `notes` while a concurrent deploy drops `tasks` and adds it again. After the merge the bundle is live, the `notes` row is live and the `tasks` row is not, since it belongs to the old table. When the new `tasks` table allows no insert, the bundle is void instead, by the rule of a table it did not write to. Step 3 fixes it, under *Decisions* 7.
6. **A deploy that adds a foreign key column with a default does not check the rows already there.** Run. A row inserted before the deploy reads the default after it. When the default's target was deleted, the row stays live with that reference. A deploy deletes the rows that do not honor a new foreign key only for columns that keep their incarnation (`group.ts:939-941`), on the grounds that a new column holds no old values. A default gives every old row one. Not in this plan. After step 3, an insert concurrent with such a deploy is void, while a row from before it is kept.
7. **SQLite: projecting the drop of a column fails while capture is on.** Run. With capture off the drop of a plain column projects. With capture on it throws `error in trigger rdb_cap_ins_ledger after drop column: no such column: NEW.memo`. The same happens for a plain column that becomes a foreign key, and for a column dropped and added again in one deploy. With the table's capture triggers dropped by hand first, the drop projects, the triggers are installed again, and a later local insert is captured and ingested. The tests that drop a column all use a target without capture. Step 6 fixes it.
8. **An insert that sets no column renders to text that does not parse.** Run. `renderRowOp` prints it as `INSERT INTO items (uuid, ) VALUES ('u1', );` in the full profile (`modules/rdb_lang/src/reverse/render.ts:350`), and the parser stops at the comma. The schema profile prints `INSERT INTO items () VALUES ();`, which parses. Step 5 fixes it.

## Decisions

1 to 6 were settled in review on 2026-11-17, and 7 and 8 on 2026-11-18. The plan above follows each.

1. **An insert may name a null for any nullable column.** rdb accepts it and writes it as given, through `insert`, C-SQL and bundles. Only ingestion leaves it out, and only for a column without a default, where a target cannot tell it from an empty column. The first draft had validation refuse it. SQL accepts it, and the refusal bought nothing, since every writer removed the null first.
2. **An operation answers for the references it makes.** A default counts only for an insert that leaves the column out, and an update answers only for the foreign key columns it sets. The alternative was that an update reads what its row holds in each foreign key column and needs that target live. A row whose target was deleted after the reference was made stays live today, by design (`modules/rdb/src/rschema/payload.ts:141-146`). That alternative would leave it live and closed to updates. A reference that was bad when it was made is already covered: an update needs its row live, and a column reads only writes that are not void.
3. **Memory and IndexedDB keep a `NULL` as a missing key.** Nothing else in those targets changes. Storing an explicit `null` would make their rows look like SQLite's query results, which is a wider change the HIP-20 draft already raises for the memory target.
   - **This holds for the app's own writes too** (settled 2026-11-18, during implementation). The IndexedDB facade stores a `null` property as a missing key, so a record written with `memo: null` reads back without `memo`. That is one difference from native IndexedDB, which keeps the `null`, and the facade's differential test now compares documents without their null properties.
   - Keeping what the app wrote was the alternative. It would read `memo: null` after the write and no `memo` after the next sync, since the projection of the app's own null write removes the key. A row that reads one way before ingestion and another way after was judged worse than the difference from native.
   - Exposing "set to NULL" apart from "never written" in rdb's read API was also considered, so that a target could store each in its own form. rdb has one way to clear a column, so a property removed by a `put` would still come back as `null`. And SQLite cannot tell the two apart in an insert, so rows written through SQLite and through IndexedDB would look different in IndexedDB.
4. **`IS NULL` takes a column.** An expression would need a rule for arithmetic that overflows, which reads as "no value" today.
5. **A value wins over a concurrent null.** The entry hash decides only between two values or two nulls. It makes an insert's null on a column without a default read the same as leaving the column out, which is what lets ingestion leave it out. Its cost is that a clear never wins a race, also against a write that did not change the value (*Effect on the HIP-20 draft*).
6. **A column added again: values as well as nulls.** The comparison in step 3 applies to what an operation carries either way. Nulls need it, and values have the defect today (*Found while reading* 4). The condition was that nothing else in rdb breaks. Step 3 lists what was checked by reading, and the tests check it by running, the delta parity suite above all.
7. **An operation for a table that was dropped and added again is void.** Step 3 follows it. It matches what happens when the table is dropped and not added again, and it keeps a bundle all-or-nothing (*Found while reading* 5). The alternative leaves such an operation to the column rule alone. It is then checked as a write of nothing but defaults, and a bundle stays half applied.
   - The second review recommended the same, and added two reasons. A row is live only within the current incarnation of its table (`modules/rdb/src/rtable/view.ts:431-435`), so the operation's row cannot show in the new table anyway.
   - The delta bound also holds (`modules/rdb/INCREMENTAL_DELTA_BOUND.md`, A3 and Lemma 1). A deploy that resets the table between a delta's start and end is after the meet of the two. So every operation concurrent with it is above the walk's starting bound, and is walked.
8. **An insert's null for a column rdb does not have is left out.** The second review proposed to keep it, so that rdb refuses the insert and no unknown column is dropped without a word. The plan does not follow that, and step 8 keeps the rule it had: an insert keeps a null only where the column has a default, and an unknown column has none.
   - On SQLite such a null is not something the app wrote. An insert is captured as the whole row, with a `NULL` for every empty column. And a sync ingests before it projects, so a deploy that drops a column can arrive between a local insert and its ingestion. The planner then plans under a schema without the column, while the captured row still names it.
   - Run on today's code, on a table `ledger(ref, memo, amount)` with a nullable `memo`, and a deploy that drops `memo` arriving between the local insert and the sync:

     | Local insert | Captured row | Null left out (the plan) | Null kept (the review) |
     |---|---|---|---|
     | leaves `memo` empty | `{"ref":"r1","memo":null,"amount":"1.00"}` | accepted | refused, local row removed |
     | sets `memo` | `{"ref":"r1","memo":"hi","amount":"1.00"}` | refused, local row removed | the same |

     The last column is by reasoning: rdb refuses a null for a column that does not exist (step 1), and a refused insert is reverted, which removes the local row. So keeping the null would remove a row whose app never wrote to the dropped column.
   - An insert loses nothing: it asked for no value in the column, and rdb holds no value in a column it does not have.
   - The review's concern holds for an update, and is met there. An update names only the columns the app wrote, so it keeps every null, also for an unknown column, and rdb refuses it as it refuses a value there.

## Review

The first review, in discussion on 2026-11-17, settled decisions 1 to 6 and raised decision 7.

The second, a written review of the revised plan checked against `faea38d`, came on 2026-11-18. It found the plan sound: the cited lines and behaviours matched the code, the three column states keep nulls out of `json.Literal` and out of every hash, and the three defects step 3 fixes are real. It recommended decision 7 (see its two added reasons there) and listed eight implementation details. Seven were taken, and one became decision 8.

| Point | What the plan does |
|---|---|
| 1. Sort `nulls` by UTF-16 code unit, not with `localeCompare`, so the writer and the validator agree | Taken (step 1). The plan adds that a locale-aware order also differs between runtimes, so two replicas could disagree on whether an operation is valid. |
| 2. `renderRowOp` must not drop the columns of an insert of only nulls, nor leave a stray comma | Taken (step 5), for an update of only nulls too. The stray comma was already there for an insert that sets no column (*Found while reading* 8). |
| 3. `IS NOT NULL` in an allow rule would get the error for `NOT` | Taken another way (step 5). The node carries a `not` flag, so the allow-rule lowering has one case, and one error for both forms. |
| 4. The planners must check an identity provider's public key column before computing its projected name, which throws on it | Taken (step 7). |
| 5. The default fill of a new row must skip a column the action sets to NULL | Taken (step 7), by testing whether the action names the column (`column in action.values`). |
| 6. IndexedDB's `changedColumns` must treat a missing property and a `null` alike | Taken (step 8). It also showed that the facade stored a `null` as the app wrote it, so the plan now says it stores a missing key (*Decisions* 3). |
| 7. Keep an insert's null for a column rdb does not have, so that rdb refuses the insert | Not taken (*Decisions* 8). |
| 8. The prefetch and `reverseCols` must not look up a NULL foreign key or key id | Taken (step 8). |

Running point 7's case also found the SQLite defect that step 6 fixes (*Found while reading* 7).
