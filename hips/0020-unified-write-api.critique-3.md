# Third critique of HIP-20: Unified write API for projection targets

A review of [HIP-20](0020-unified-write-api.md) as revised on 2026-11-17, and of [the second response](0020-unified-write-api.response-2.md) to [the second critique](0020-unified-write-api.critique-2.md). Checked against the working tree on 2026-11-17. The revision takes the second critique in full, and the second response is right on its three points. I found nothing to change in the writer's rules. What is left is in the native write paths, where three writes still end differently by target after the fixes of *Changes* 1, and in how the SQLite writer runs its transaction.

Paths are relative to `modules/`. W22 to W27, Q1, S2 and T1 are the scenarios in §1. W1 to W21, V1, A1, M1, B1, K1, K2, S1 and O1 to O3 are the earlier critiques'.

## Verdict

**Does the revision resolve the second critique?** Yes. Every row of the second response's table is in the HIP where the table says.

**Is the second response right where it differs?** Yes, on all three points: no upgrade path for existing targets, a row spread into an update, and no op event for an insert skipped as already applied. §2 gives the check behind each.

**Is anything still wrong?** Not in the writer's rules. These are the items, numbered by the section that argues each:

| Item | Where in the HIP |
|---|---|
| 3. A native insert with a `null` still ends differently by target | *Changes* 1, "Updates to `NULL` are rejected the same way" |
| 4. An IndexedDB `put` that leaves a column out loses it without a trace | *Changes* 1, "Changes are compared by value", "The author column is captured" |
| 5. On SQLite, an explicit `NULL` for a column with a default reads `NULL` on the writing device only | *Changes* 1, "Local inserts show their defaults" |
| 6. The SQLite writer reads and then writes, so its transaction must be immediate | *Changes* 3 |
| 7. The test plan misses the cases above | *Tests* |

Items 3 to 5 are each one sentence in *Changes* 1. Item 6 is one word in *Changes* 3. §10 lists two defects of today's code that are outside the HIP.

## 1. How the claims were checked

I read the revised HIP and the second response against each other, and re-read the code behind the parts this revision added: the capture code of each target, the drain of the SQLite outbox, project-back's write of a row the target already has, the revert at the end of an ingest pass, and what each target caches when it opens.

Then I ran three scratch probes against the working tree, written the way the earlier ones were. The suite's group has the tables `ledger` (`ref` readonly, `memo` nullable, `amount`) and `tags` (`code`), and an identity provider.

**Native writes:**

| # | Scenario | Result |
|---|---|---|
| W22 | An insert with an explicit `null` in `memo`, a nullable column with no default, on each of the three targets | SQLite captures `ref` and `amount`, and the insert is accepted. Memory and IndexedDB capture `memo: null`, and the ingest pass throws. On IndexedDB a second pass throws again. |
| W23 | IndexedDB: a `put` over an ingested row. (a) The record holds only the id and a new `memo`. (b) The record holds every business column at its old value, and no author column | (a) The stored record loses `ref`, `amount` and the author. Only `memo` is captured, and it is accepted. After the sync the record has its author again, but not `ref` or `amount`, also after a later update of `memo` in rdb. No op event is logged. (b) Nothing is captured. After the sync the record still has no author. |
| W24 | A table with a nullable column `kind` whose default is `std`. SQLite: one insert that sets `kind` to `NULL`, and one that leaves `kind` out. Then a rejected update of the first row | The first is captured without `kind`, the second with `kind: 'std'`. Both are accepted. After the sync the first row reads `NULL` on the writing device, and `std` in rdb and on a second device. After the rejected update is reverted, it reads `std` on the writing device too. |
| W25 | SQLite: `UPDATE tags SET id = 99` on an ingested row. Then a native update of row 99, then an update of the row in rdb | Nothing is captured for the change of id. The native update of row 99 is rejected ("cannot update unknown local row") and logged as a failure, and the row is not reverted. After the update in rdb the target has two rows: one under the old id with rdb's value, and one under 99 with the local value. |
| W26 | An insert whose FK column holds a local id that the referenced table never had, on SQLite and memory | Stored on both. Ingestion rejects it at planning ("dangling FK"), removes the row and logs a failure op event. |
| W27 | SQLite: an insert whose author column holds another key's id. Today's behavior is the HIP's `autoAuthor` on | Accepted, and signed with the writer key. After the sync the column holds the writer key's id. No op event is logged. |
| T1 | SQLite: a `BEFORE UPDATE OF id` trigger that raises an error, with the same gate as the capture triggers | A native change of the id fails at the statement. An update that sets the id to itself passes. Ingestion and project-back work as before. |

**What the writer reads:**

| # | Scenario | Result |
|---|---|---|
| Q1 | A row returned by a query, on each target. The table has an FK column and two nullable columns the row never set | On SQLite and IndexedDB the row has the id column, the business columns, `post_id` and `author_key_id`, and an unset column is `null`. On memory an unset column is missing from the row. No target returns any other column. |
| S2 | Two SQLite connections on one file in WAL mode. The second begins a transaction and reads, the first commits a write, then the second writes. Once with a deferred transaction, and once with an immediate one, which takes the write lock before it reads | Deferred: the write throws `SQLITE_BUSY_SNAPSHOT`, although the connection has a busy timeout. Immediate: no commit can come in between, and the write succeeds. The second connection also sees `PRAGMA schema_version` change after the first runs `ALTER TABLE`. |

The probes are not in the repo.

## 2. Claims that hold up

**The revision.** Each row of the second response's table is in the HIP where the table says. The citations that are new or moved in this revision are right, among them `rdb_adapter/src/project.ts:80-82`, `rdb_adapter/src/ingest.ts:484-487`, `rdb_adapter/src/ingest_orchestrator.ts:411-412` and `rdb_adapter/src/types.ts:146-148`.

**The second response, where it differs:**

- **No upgrade path for existing targets.** The project is unreleased, and a projection can be rebuilt. One thing to know while developing: an old target fails silently in one case. Its old triggers capture no author, so with `autoAuthor` off every insert is ingested as anonymous.
- **A row spread into an update.** A query row holds the id column, the business columns, the FK columns and the author column, and nothing else (Q1). So the id column and the author column are the only two that a spread would trip on, and the unchanged rule now sets both aside.
- **No op event for an insert skipped as already applied.** When ingestion skips the insert, all it knows is that the row's id is already in the DAG (`ingest_orchestrator.ts:443-449`). A replaced row and a real crash replay look the same there.

**New claims of this revision:**

- **On SQLite, a changed schema version means the meta is already current.** A projection applies the schema actions, the table meta and the capture triggers in one immediate transaction (`rdb_adapter_sqlite/src/sqlite_target.ts:342-380`, `:782-783`). A writer that reloads its meta when `PRAGMA schema_version` changed therefore reads meta that matches the tables. The version is visible across connections (S2).
- **Memory and IndexedDB can write defaults.** Both already keep each table's defaults in their meta (`rdb_adapter/src/memory_target.ts:83`, `rdb_adapter_idb/src/idb_schema.ts:46`).
- **Comparing by value needs only `json.equals` and the `null` rule.** Every value a read returns is a `json.Literal` (`json/src/literal.ts:2`). Bigints, decimals and bytes are strings (`rdb_adapter_sqlite/src/sqlite_target.ts:108-111`).
- **With `autoAuthor` on, ingestion ignores a native insert's author column "as it does today".** The insert is signed with the writer key, and project-back corrects the column (W27).
- **The existing revert is enough for the fixes that say "revert to rdb's row".** It writes the whole row from rdb's view, defaults included (W24; `ingest_orchestrator.ts:581-586`).

## 3. A native insert with a `null` still ends differently by target

*Changes* 1 fixes an update to `NULL`: every target captures it, and the planner rejects it. It does not say what a `null` in a native insert means.

Today the three targets disagree (W22):

- **SQLite.** The insert trigger records every column, a `NULL` included (`sqlite_target.ts:512-519`). The drain then drops each `null` (`:1157`), so the captured insert leaves the column out and the insert is accepted.
- **Memory and IndexedDB.** The `null` is captured (`rdb_adapter_idb/src/idb_facade.ts:237-243`, `rdb_adapter/src/memory_target.ts:262-270`), rdb rejects the op, and recording the rejection throws.

After *Changes* 1 as written, memory and IndexedDB reject the insert and remove the row, by the planner or by rdb, and SQLite still accepts it. There is a second risk on SQLite. The drain has to start keeping a `null` for updates. If it keeps one for inserts as well, every SQLite insert that leaves a nullable column unset is rejected, because the insert image has a `null` for each of them.

The writer already has the rule: an insert's `null` means "not given".

**Change.** In *Changes* 1, say that a `null` in a captured insert is dropped on every target, and that only a captured update carries one.

- **Memory and IndexedDB.** `localInsert` and the facade's `businessValues` leave a `null` out of the captured insert. With "Local inserts show their defaults", a `null` for a column with a default then gets the default, as in the writer.
- **SQLite.** The drain keeps dropping a `null` from an insert, and keeps it in an update.

## 4. An IndexedDB `put` that leaves a column out loses it without a trace

An IndexedDB `put` replaces the stored record (`idb_facade.ts:489`). The facade's diff skips a column that the new record doesn't have (`:254-255`), so nothing is captured for it.

- **A business column** (W23a). After `put({ id, memo })` the device shows the row without `ref` and `amount`. rdb has both, no op event is logged, and a later change of another column in rdb does not bring them back.
- **The author column** (W23b). A record that an app builds from a form has no author column. After the `put`, the row has no author until an update of the row is projected back.

This revision rewrites that diff: it compares by value, a `null` equals a missing value, and the author column joins it. With those three, a column that the old record has a value for and the new record leaves out is an update to `null`. On SQLite the same write is `UPDATE … SET col = NULL`, which *Changes* 1 has rejected and reverted.

**Change.** Say in *Changes* 1 that a column missing from a `put` counts as `null`. When the old record has a value for it, the facade captures an update to `null`, the planner rejects it, and the row is reverted to rdb's row with a failure op event. The same holds for the author column, as an update of it. A column that neither record has captures nothing, as the HIP already says.

The alternative is a facade that merges, so that a `put` keeps the columns it leaves out, as SQL's `UPDATE` and the writer's `update` do. The row ends the same, and no events are logged. The cost is that `put` would no longer replace a record, which is what IndexedDB defines and what the README documents (`rdb_adapter_idb/README.md:51`, `:95`). The writer is not affected by the choice, because it reads, merges and writes.

## 5. On SQLite, an explicit `NULL` for a column with a default reads `NULL` on the writing device only

*One write, three meanings* lists this divergence for memory and IndexedDB, and "Local inserts show their defaults" fixes it there. SQLite has it too, through an explicit `NULL` (W24):

- **The insert.** `INSERT … (name, kind) VALUES ('a', NULL)` stores `NULL`, because SQLite applies a default only to a column the statement leaves out. The captured insert has no `kind`, so the op leaves it unwritten.
- **Everywhere else.** rdb and every other device read the default.
- **The writing device.** Project-back finds the row already there, and writes only the columns its row action carries (`sqlite_target.ts:1010-1025`). `kind` is not among them, so the row keeps reading `NULL`.

The writer does not hit this, because it treats the `null` as not given and leaves the column out of its statement.

**Change.** When ingestion accepts an insert whose captured values leave out a column that has a default, it marks the row for the revert that already exists. That revert writes rdb's row, with the default (W24). After §3, memory and IndexedDB never take this path, because their captured inserts carry the defaults.

## 6. The SQLite writer reads and then writes, so its transaction must be immediate

*Changes* 3 has each call run "in one `db.transaction`". Inside it the writer reads the schema version, the writer record and the capture switch, sometimes the row, and then writes. A better-sqlite3 transaction is deferred unless it is run with `.immediate()`.

A projection file is in WAL mode (`sqlite_target.ts:415`). There, a deferred transaction that has read cannot write once another connection has committed: the write fails with `SQLITE_BUSY_SNAPSHOT`, and a busy timeout does not retry it (S2). In a client process the other connection is the host's, which commits on every sync cycle. The writer would throw a storage error on a normal race.

The target's own transactions that read and then write are immediate, and say why (`sqlite_target.ts:374-380`). So is the node client's `registerKey` (`rhost_client_node/src/client.ts:203`).

**Change.** In *Changes* 3, say that each call runs in one immediate transaction.

## 7. The test plan

*Tests* covers the second critique's cases. These are missing:

- **A native insert with an explicit `null`** is accepted on all three targets, and its op leaves the column unwritten. For a column with a default, the row and the op carry the default (W22).
- **An IndexedDB `put` that leaves out a business column, and one that leaves out the author column.** Each is rejected and reverted, and logs a failure op event (W23).
- **SQLite: an explicit `NULL` for a nullable column with a default** reads the default on the writing device after a sync (W24).
- **A writer call that races a host commit** on a SQLite file succeeds (S2).

## 8. Smaller points

- **`openWriter()` is synchronous, and on IndexedDB the writer record is not.** The HIP has `openWriter` refuse a target where no projection has recorded a writer key. On IndexedDB that record can only be read asynchronously. A copy cached when the target opens, as the capture config is (`rdb_adapter_idb/src/idb_target.ts:142-157`), stays wrong for a tab that opened before the projection first did. Each write already reads the record in its transaction. So `openWriter` can refuse only a target without capture, and each write can refuse with `no-writer-key`. The case in *Tests* would then be a write, not an open.
- **A SQLite update that carries `{ pk }` has to read the row first.** Keys are interned after the row is found, and the statement needs the key's id. *Changes* 3 lists when an update reads the row: a `null`, the id column, the author column, or no columns. A `{ pk }` belongs in that list.
- **The shape holds FK target tables, and no check uses them.** A local id that the referenced table never had is certain to be rejected, at planning (W26; `rdb_adapter/src/ingest.ts:297-299`). That is the footing on which the writer refuses an unknown key id. Either the writer refuses such an id too, or the target tables come out of the shape.
- **The claim about concurrent edits is too wide.** *Write semantics* says a form that saves every field "does not overwrite concurrent edits to the fields the user left alone". The comparison is with the row in the target at the write. An edit that was projected after the form read the row is overwritten with the form's old value. The sentence holds for an edit that has not reached the device yet. A binding that wants more sends only the fields the user changed.
- **A native insert that names another key.** With `autoAuthor` on it is signed with the writer key, with no error (W27). With it off, the same insert is rejected, and a native update to another key is rejected under both settings. Capture now carries the column, so ingestion could reject the insert under both settings. Only `NULL` would then depend on the setting, as in the writer's table. The second critique proposed "ignores it when `autoAuthor` is on", so this is a second thought of mine, and optional.
- ***Changes* 4.** "Nullability is added to the table meta, and filled in when an existing projection opens" is an upgrade step. The second response decided against those.

## 9. Recommendations

1. **Say what a `null` in a captured insert means** (§3): dropped, on every target.
2. **Say what a column missing from a `put` means** (§4): an update to `null`.
3. **Write rdb's row back after an insert that left out a default** (§5).
4. **Run the SQLite writer in an immediate transaction** (§6).
5. **Add the cases of §7.**
6. **Move the `no-writer-key` refusal to the write, and read the row first for a `{ pk }`** (§8).

## 10. Outside this HIP

Two defects of today's code, found while checking. Neither is among the fixes of *Changes* 1.

1. **A native change of the id column on SQLite duplicates the row** (W25).
   - **What happens.** No trigger captures the change. The row sits under the new id with no rdb identity. A later native update of it is rejected, with a failure op event, but the row is not reverted, because ingestion finds no rdb row for that id. When the row next changes in rdb, project-back inserts it again under the old id.
   - **Why it matters here.** The writer refuses the id column. The native path stays supported, and this is the same class as a native update of the author column, which this revision now rejects.
   - **Fix.** A trigger, installed with the capture triggers and with their gate, that fails a native change of the id column at the statement. It does not get in the way of ingestion or project-back (T1).
2. **A query row on memory leaves out the columns the row never set** (Q1).
   - **What happens.** SQLite and IndexedDB return `null` for such a column, the author column included. Memory returns a row without the key (`rdb_adapter/src/memory_target.ts:327`).
   - **Why it matters here.** The HIP writes "the values a query returns". A row spread into an update works either way. A component tested on memory sees `undefined` where the app will see `null`.
   - **Fix.** Memory returns `null` for each column of the table that the row has no value for.
