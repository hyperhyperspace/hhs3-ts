# Rdb Adapter

## Intro

**[Rdb](../rdb)**'s data structures are designed for **self-verification** and **distributed state reconciliation**. To get fast querying over the last resolved & verified version, apps can use a **bi-directional projection** into a local database. **[SQLite](../rdb_adapter_sqlite/)**, **[IndexedDb](../rdb_adapter_idb/)** (for in-browser usage) and an ephemeral **memory-backed store** (packaged within this module) are available as projection targets.

This module (**rdb_adapter**) provides an abstract bi-directional projection mechanism, mapping an instance of Rdb's `RTableGroup` into a local database. The projection flattens all the history present in Rdb's DAG, omits validation metadata, and continuously exports the contents of the latest version. It includes an *outbox* construction that marshalls all the changes in the local database back into `RTableGroup` operations and the Rdb's DAG.

The export (rdb → relational) is powered by a planner, that generates abstract schema and row changes starting from an Rdb delta. These changes are then applied by a `MaterializationTarget`.

For ingesting changes (relational → rdb) an inverse planner is used, that maps each change in the outbox back into one Rdb operation. The job of reading the outbox, and of settling what ingestion did, is done by a `MaterializedChangeSource` instance. How the two directions keep the projection consistent is described in [Ingestion and checkpoints](#ingestion-and-checkpoints).

Hence to create a new a bi-directional projection, both `MaterializationTarget` and `MaterializedChangeSource` have to be implemented and injected into the generic adapter.

To project an entire database (`Rdb`), the module **[rdb_projection](../rdb_projection/)** acts as a reactive supervisor, creating instances of **rdb_adapter** as `RTableGroup` are added to the database, resolving group naming and managing cross-group interactions by mapping foreign key columns.

## Event monitoring

Co-transactional failures (Rdb changes reverted because of concurrency) and ingestion failures (changes in the local projection that cannot be ingested back into Rdb, either because of programming errors or concurrent Rdb changes) are reported using the `OpEvent` data structure. The app can **subscribe** to new events (`onOpEvents` at open, or `subscribeOpEvents` after) and **inspect** the durable log with `projection.opEvents({ afterId, beforeId, limit, order })`. Subscribe snapshots high-water and streams only later ids; catch-up is the app's own `afterId`. Delivery is at-least-once.

An event names the rows it is about. A row op's event names its row (`table`, `rowId`, `localId`). A bundle's event names each row the bundle writes, in `writes` (`table`, `action`, `rowId`, `localId`). A flip comes from rdb, which knows rows by hash only, so the target fills in each row's local id as it logs the event. The events about the rows a query shows also reach that query's snapshots, as [Events in a snapshot](#events-in-a-snapshot) describes.


## Status and limitations

While projection is a new concept in HHS, the test suite includes a synthetic test generator and an extensive reproducible stress test runner over the generic planner and the SQLite and IndexedDB adapters. The adapters have been extensively tested using this tooling.

The forward projection planner is universal, and should be usable without limitations. Change ingestion turns every local change into an operation of its own, so two writes the app made in one transaction reach rdb as two operations. If the app needs several writes to land or fail together, it should apply them directly at the [Rdb](../rdb/) level (or using [C-SQL](../rdb_lang/) for convenience).

Change ingestion works at the data level only. Schema changes can only be performed directly on the **Rdb** instance, since the schema change logic imposes limitations that make concurrent state reconciliation in the face of schema updates more straightforward. The forward planner then applies schema changes safely on any projections.

The projection replaces hash-based ids with database-native numeric ids, that also are used for foreign key dependencies when possible. The numeric ids are not inter-replica stable, and are meant for local usage. If the app needs to communicate off-replica, it sould use the hash-based row/key ids. They can be obtained by using the projection API (`projection.keyHashForId`, `projection.rowHashForLocalId`, etc.) or by directly inspecting the shadow tables that maintain sync state (not recommended).

## Ingestion and checkpoints

A projection keeps one promise: each group's tables hold rdb's rows at the group's checkpoint, with the local changes still in the outbox applied on top. The checkpoint is the version the target last projected, stored beside the tables, and the outbox holds what the app wrote since. Ingestion is built around keeping that promise. It is also why a projection never applies over a pending change: `apply` refuses with `PendingChangesError` while the outbox holds one, and the next cycle ingests the change first.

### What capture records

Each target records the app's writes in its outbox, in commit order. An insert is recorded as the row it stored, defaults included, and a delete by the row's local id. An update is recorded as the columns it names, whether their values changed or not. Naming a column is a write: rdb resolves concurrent updates column by column, so a column the app sets to the value it already has still takes part. The target READMEs say how each one captures ([SQLite](../rdb_adapter_sqlite#capture), [IndexedDB](../rdb_adapter_idb#capture-and-commit-events)).

Capture also fills in the author. When an insert leaves the author column empty, the target writes the writer key's id there before recording the row, so the row shows its author at once. It knows the key from the writer record, which [rdb_projection](../rdb_projection#cycles) keeps in the target: the writer key, and for each group whether an insert with an empty author column is signed with it (`autoAuthor`, the default) or anonymous. An insert into a signed group that still has an empty author column was written while no writer was recorded, and ingestion refuses it.

### A pass

Ingestion works from what the app saw. For each group it starts at the checkpoint, translates the group's changes with the schema there, and turns each change into one entry, in commit order: the plain row op the app wrote, the same entry a native write of it would make. The first entry of a group is appended at its checkpoint and each next one after the previous, so a pass builds a chain on top of the checkpoint. Nothing is bundled or merged. An insert that references a row inserted earlier in the pass finds it, since that row's entry comes first, and an update lands or is refused as a whole.

Groups that bind other groups need one more step. Before a change's entry, its group observes each group it binds whose chain grew in this pass, at that group's checkpoint plus its chain so far, joined with what it already observes (rdb only accepts an observe that extends the earlier ones). A permission written natively in one group and used right away in another reaches rdb in order this way: the permission's entry comes first, and the use reads it through the observe. An observe the gate would refuse, or one that would be void where it stands, is not written. While some observe of a gated binding is void, the group's entries are also checked through the live observes only, as their verdicts will read them, so a write that would be void from the start is refused rather than appended.

Remote changes that reached rdb after the checkpoint stay out of the pass. They are concurrent with the chain, and rdb resolves them against it when the next projection applies them.

### What becomes of each change

Before anything reaches rdb, ingestion writes into the change's outbox row what became of it. An entry is built, signed and validated, its hash is written into the row, and only then is it appended. A change that is refused gets its failure event there instead, and one with nothing to write gets a mark that says so, such as an update that names only readonly columns at their current values, or a change of a table no member projects. Every change a pass reaches ends with one of these records.

Some refusals come from the adapter rather than from rdb. An update may name a readonly column or the author column as long as it holds the row's own value, which is what a form that writes back the whole row does; any other value there is refused. An insert on a local id that already has an rdb identity is refused, as with an `INSERT OR REPLACE` over a row or an insert that reuses a deleted row's id, and so is any later change of a row whose insert was refused. An error while translating a change or building its entry counts as a refusal too, with the error's message as the reason.

An error from the target, or from an append, is different: it ends the pass where it happened, and the call throws ([rdb_projection](../rdb_projection#cycles) tries again later). Every record is written before its append, so what the pass stored is enough to finish it.

### The end of a pass

The end of a pass reads only what was stored: the outbox records, the checkpoints and rdb. It takes nothing from the pass's memory, so the process that ran the pass and the next cycle after a crash compute the same end.

The handled changes are the outbox's leading rows whose record says what became of them: a failure, nothing to write, or an entry rdb holds. Each group's chain ends at its checkpoint plus the recorded hashes rdb holds. If the pass's entries made an observe due that no later change of the pass called for, as when a permission is written and nothing uses it yet, the end writes it now, so that a use in the next pass reads the permission. It records that observe beside its group's checkpoint before appending it. Then, for each group whose chain holds an observe, the end asks rdb for the delta from the checkpoint to the chain's end. An observe can bring in an entry of the bound group that changes the verdict of something the group read through it: a revocation concurrent with the grant a row relied on voids that row, and an entry that voids the revocation brings it back. The delta's verdict changes become void and reinstate events, and the rows they name are rewritten.

The settle is one target transaction. It checks that each checkpoint is still the one the pass started from, removes the handled rows from the outbox, writes the inserts' mappings, rewrites the rows that need it, stores the failure events and then the delta's, and moves each checkpoint to the end of its chain. Nothing is projected back. Each entry of the chain was a change the target had already applied, so a local write that ingests cleanly is never written to the target again, and a screen that shows it doesn't change.

### Reverts

A refused change leaves the target showing something rdb doesn't have, and the settle puts the row back to what the promise says it holds: rdb's row at the end of the chain, with the row's changes still in the outbox applied on top. Say a ledger row reads memo `start` and amount `1.00`. The app sets the memo to a value rdb refuses, and while the pass runs it sets the amount to `2.00`. The settle writes rdb's row, memo `start`, and then the pending update, so the row reads `start` and `2.00` until the next pass ingests the amount.

A row rdb no longer has is removed, and its mapping marked `deleted`. A refused insert on a local id that had no identity yet is removed too, and the id keeps the refused identity, marked `ingestion_failure`, so it is never used again. One on a local id that already had an identity, such as an `INSERT OR REPLACE` over a row, puts that identity's row back. The rows an observe voided or reinstated are rewritten the same way, and a reinstated row that was never in the target gets a new local id from its table's counter.

### Crashes

A crash anywhere in a pass leaves a state the next cycle finishes. When the outbox's first row has a record, the cycle runs the end first and then a new pass over the rest. An entry whose hash was recorded but never appended is built again and recorded over the old hash, and an insert gets a new uuid then, which is safe because nothing referenced it yet. rdb's DAG writes nothing for an entry it already holds, so a cycle that appends what a crashed one had already appended stores it once, and so do two devices that make the same anonymous update.

Two things mean the store is not what a pass assumed: a checkpoint that moved before the settle, which only another projector on the same store does, and an entry the DAG appended under another hash than the one recorded. Both stop ingestion with an error, and [rdb_projection](../rdb_projection#cycles) halts on them.

The design, and why it holds, is [HIP-29](../../hips/0029-checkpoint-consistency.md).

## NULL values

An rdb column can be set to NULL (see [Nulls and defaults](../rdb#nulls-and-defaults) for the rules). This is how a NULL crosses the adapter in each direction.

**Projection.** A row action's `values` can hold `null`, which means "write NULL": the target clears the column and does not fill in its default.

- SQLite stores `NULL`. The memory and IndexedDB targets keep no key for the column, the same as for a column that was never set.
- A whole-row action (the first projection, a re-projection, a revert) names every column of the table, with `null` for the ones the row has no value in.
- When the write that set a column becomes void, the action carries what the row reads again: the earlier value, the column's default, or `null`.

**Ingestion.** A captured change's `values` can hold `null`.

- A local update to NULL becomes a null write. A value written concurrently on another replica wins over it, so every device ends with the value.
- A local insert keeps a NULL only where the column has a default, where it means "NULL, not the default". Elsewhere nothing is written: an explicit NULL and an empty column read the same in rdb, and a target cannot always tell them apart (SQLite captures an insert as the whole row, with a `NULL` for every empty column). That covers a column rdb's schema no longer has, when a deploy that drops it arrives between the local insert and its ingestion.
- A NULL foreign key or key id maps to a NULL of the rdb column, with no lookup.
- A NULL in a column rdb declares `NOT NULL` is refused by rdb. The local row is put back and the refusal is logged, like any ingestion failure.

## Usage

### REPL

Projection can be configured from the [CLI-based REPL](../rdb_tools/) using the `\project` meta-command:

```
\project start <db> as <id> to <path>
\project status [<db>]
\project stop|update <idx>
\project events <idx> [after <n>] [before <n>] [limit <m>] [order asc|desc]
\project indexes <idx> <spec.json | {inline json}> [dry-run]
```

The identity passed as `<id>` is used to sign the operations that are ingested back into Rdb. Writes to a group without an identity provider are ingested anonymously, and a ref advance is signed only when the binding's `ALLOW UPDATE REF` gate reads `$author`. A SQLite databse is created on `<path>`, and can be queried and modified using standard SQLite tooling.

If changes in the projection generate any ingestion failures, or Rdb concurrency generates op cancellations, those are reported live on the REPL console while the projection is running. `\project events` pages the durable log (default: newest 50, `order desc`).

`\project indexes` installs a projection index spec (see [Indexes](#indexes)) and prints the outcome, the index actions, and any pending declarations.

While `\project` is also supported in the [web REPL demo](../rdb_repl_web/), the only supported path is `:memory:` and the contents of the projection are not inspectable at the moment. The web REPL cannot read files, so `\project indexes` takes the spec inline there.

### Library

A table group can be projected using the library:

```typescript
import Database from 'better-sqlite3';
import { projectGroup } from '@hyper-hyper-space/hhs3_rdb_adapter';
import { SqliteTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_sqlite';

const target = new SqliteTarget(new Database('one-group.sqlite'));
await projectGroup(group, target);
```

For a whole `RDb`, inject the same target into [rdb_projection](../rdb_projection).

## Indexes

A projection can carry its own indexes to speed up local queries. They belong to the projection, not to the Rdb schema: other replicas never see them, and two projections of the same data can index differently. Indexes never enforce anything (there is no `UNIQUE` index); they only make reads faster.

### The index spec

An `IndexSpec` is written in **rdb names**, the names in the `RSchema`, not the projected ones:

```json
{
  "indexPub": true,
  "indexes": [
    { "name": "by_customer", "group": "orders", "table": "orders",
      "columns": ["customer", "placed_at"],
      "options": { "columns": { "placed_at": { "desc": true } } } },
    { "name": "by_title", "group": "catalog", "table": "products",
      "columns": ["title"], "options": { "columns": { "title": { "collate": "NOCASE" } } } },
    { "name": "mine", "group": "orders", "table": "orders", "columns": ["@author"] }
  ]
}
```

- `group` is the rdb group name and `table` the rdb table name. `name` must be unique within its group; the same name can be reused in another group.
- `columns` is the ordered list of columns in the index key, and it is the only part of the declaration the adapter core interprets: the index is dropped whenever one of these columns goes away. The pseudo-column `@author` stands for the row's author.
- The adapter maps rdb names to target names exactly as the schema mapper does: table renames and the group prefix (`<group>_<table>` in a replica-wide projection), column renames, foreign key companions (`<col>_id` or `<col>_row_hash`), identity columns (`<col>_key_id`), provider key-id columns (`key_id`), and `@author` to `author_key_id`.
- `options` holds everything else, per-column settings such as descending order included. Its format is defined and validated by each target (see [Targets](#targets)); the core never looks inside it. A spec is therefore written for one kind of target: an app that projects into both SQLite and IndexedDB ships one spec per target.
- `indexPub: true` adds one single-column index named `pub__<column>` for every `pub` column of every projected table. The `pub__` prefix is reserved for this.

### Pending declarations

A declaration whose group, table, or column is not projected (yet) is **pending**, not an error. It is built automatically when a later schema change adds what it needs. If something an index depends on is dropped by a schema change, the index is dropped first, in the same transaction, and the declaration goes back to pending. A provider's public-key column is never projected, so an index on it stays pending.

### Installing a spec

The target stores the installed spec, and syncing never changes it. The app installs a spec explicitly:

```typescript
// Replica-wide, with rdb_projection. For hand-wired members, this package's
// reconcileIndexes(members, target, spec) does the same.
const report = await projection.reconcileIndexes(spec);
console.log(report.status, report.actions, report.pending);
```

Reconcile compares the spec's fingerprint with the one stored on the target:

- nothing installed yet, or a different fingerprint: the spec is installed (`installed`);
- the same fingerprint: nothing happens (`unchanged`).

A different spec is then compared with the indexes actually built. Reconcile drops, builds, and records the new spec in one transaction. A declaration that changed in any way, `options` included, is a different index: the old one is dropped and the new one built. An index whose resolved form is unchanged is left in place. It holds the same per-database lock as sync, so the two never interleave. `{ dryRun: true }` returns the plan without changing anything. After that, every sync keeps the installed spec up to date across remote schema changes.

On first launch, open the projection (the initial backfill runs without indexes) and then call `reconcileIndexes`, which builds them. After a restart, the installed spec is already there and nothing needs to happen until the spec changes.

### Targets

- **SQLite** builds real indexes, named `<table>__<name>`, after the batch's rows are written. Its options are `{ "columns": { "<rdb column>": { "desc": true, "collate": "NOCASE" | "RTRIM" | "BINARY", "whereNotNull": true } } }`, every key optional. `whereNotNull` makes a partial index (on several columns, the conditions are ANDed). Validation is strict: any other key, at either level, and any column not listed in `columns`, is rejected before anything is installed. The installed spec and the index records live in the `rdb_index_spec` and `rdb_index_meta` tables.
- **Memory** keeps the bookkeeping only (the spec and index records, nothing physical) and takes no options: a declaration with `options` is rejected. It exists so the lifecycle can be tested without SQLite.
- **IndexedDB** builds shadow indexes that behave like native `IDBIndex`es, read through `store.index(name)` on `target.database` (see [rdb_adapter_idb](../rdb_adapter_idb#indexes)). It takes no options: a declaration with `options` is rejected. So there is no descending order, collation, or partial index, and, as natively, `null`, booleans, and objects are not keys (a row holding one is absent from that index).

### Known limits

- **Unmanaged indexes block remote column drops (SQLite).** Only indexes recorded in `rdb_index_meta` are dropped before a `drop-column`. If the app creates its own index on a projected column and a remote schema change drops that column, SQLite's `ALTER TABLE ... DROP COLUMN` fails, the whole apply rolls back, and that group's sync stalls until the index is removed. (A table rebuild, which restores NOT NULL after a required column is added, keeps hand-made indexes.) Declare indexes in the spec instead of creating them by hand.
- **A stale spec costs performance, never correctness.** A declaration whose table or column was dropped or renamed upstream (in rdb a rename is a drop plus an add) shows up in `report.pending` and stays inactive until the spec is updated. Check the `pending` list after each release.
- **Only reconcile applies spec changes.** A new spec does nothing until `reconcileIndexes` runs; tables that are already caught up keep their previous indexes until then.

## Reactive queries

UIs are expected to read state from a projection, not directly from [Rdb](../rdb). A fast reactivity engine is embedded into `rdb_adapter`. It enables UIs and other interactive components to attach their state directly to the projected database. Currently, a react binding is available in [rdb_react](../rdb_react).

### Building a query

Queries are written using the table and column names in the SQLite or IndexedDB projection (module_table, local numeric ids for rows, keys and fk references - `chat_messages`, `author_key_id`, `<col>_id`, `id`). Queries are created using a chained builder:

```typescript
import { QueryEngine } from '@hyper-hyper-space/hhs3_rdb_adapter';

const engine = new QueryEngine(target); // or use projection.selectFrom / projection.monitor

const messages = engine
  .selectFrom('chat_messages')
  .select('body', 'author_key_id', 'sent_at')
  .where('room_id = :roomId AND deleted = FALSE', { roomId })
  .orderBy('sent_at', 'desc')
  .before(anchorId)
  .limit(50);

const rows = await messages.results();          // one-shot read
const sub  = engine.monitor(messages);          // live QuerySubscription<ProjectedRow[]>
```

- `select(...columns)` chooses the delivered columns; `id` is always included. If `orderBy` or an exported `.col()` column is omitted from `select`, the engine still fetches it to maintain sort tuples and dependency sets, then strips it from the delivered rows.
- `where(expr, params)` parses a single-table predicate (`=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] LIKE`, `[NOT] IN :list`, `+`, `-`, `*`, `length(col)`) with `:named` parameters.
- `orderBy(column, direction?)` adds a sort key. When `limit`, `before`, or `after` is used, `id` is appended as a tiebreaker in the last key's direction (or `id ASC` when no `orderBy` was given) so every page has a total order.
- `before(id)` and `after(id)` keep the rows whose sort tuple is strictly below the anchor (`before`) or strictly above it (`after`). SQLite and the memory target load the anchor by primary key, including when the anchor itself fails the `WHERE`. IndexedDB finds the anchor among the rows that passed the `WHERE`, and otherwise uses the tuple the monitor cached from an earlier read.

### Query sets and `.col()`

A screen often renders a primary list and hydrates related rows from another table (the authors of the visible messages, the tags on the visible notes). Passing `messages.col('author_key_id')` into a second query's `IN` parameter links them into a DAG:

```typescript
const profiles = engine
  .selectFrom('user_profiles')
  .where('key_id IN :keys', { keys: messages.col('author_key_id') });

const sub = engine.monitor({ messages, profiles });
```

On each flush the engine evaluates upstream monitors before downstream ones, and every run of a monitor waits until each query its `.col()` parameters read has its first result. So `profiles` is never computed on the empty key set of a `messages` monitor that has not run yet: not on its first run or in `results()`, and not after its subscription is dropped and taken again, as React's Strict Mode does. If `messages` re-runs (say, a message body changed or a new message arrived from an author already on screen) and its distinct set of `author_key_id` values is unchanged, that dependency edge does not re-run `profiles`, and `snapshot.data.profiles` keeps the exact same array reference.

A UI reads the projected tables, not `RObject` views. What is on screen is a query over a projected table, and three different commits can change its result:

### Change capture

Changes come from three different sources:

1. **A local write.** The app inserts, updates, or deletes a row in the projection database. The row is already committed and readable by `SELECT`; the UI must paint it now, before the projection's 50 ms ingest debounce and before the replica accepts the op.
2. **A projected `apply()`.** Remote ops are written in one transaction with capture off, so they never enter the local change queue in `rdb_outbox`.
3. **An ingest `settle` (`commitIngest`).** Refused local writes are reverted, the rows an observe voided or reinstated are rewritten, and the events are logged to `rdb_op_events`, in one transaction, also with capture off. A local write that ingests cleanly is not written again.

An `RObject.subscribe` callback fires before `apply()` writes the target, and an outbox listener never hears `apply()` or `commitIngest()`. The query engine (`QueryEngine`) sits on the target itself, where all three commits land. Because it depends only on the target (`QueryableTarget & ReactiveChangeTarget`), a client process can run reactive queries over its local `SqliteTarget` or `IdbTarget` without opening `RdbProjection`. When `RdbProjection` is in the same process, `projection.selectFrom` and `projection.monitor` delegate to the same engine.

### Why relevance is asymmetric

Neither `rdb_outbox` nor a projected `delete-row` carries a before-image. A local update is recorded as the columns it names, not as what the rest of the row looked like before, and a delete carries only `localId`. Checking a `WHERE` clause against a partial change misses **departures** — a row that used to match the query and just moved out of it, or was deleted.

The engine handles the three commit streams differently:

- **`local` commits carry table names (`{ source: 'local', tables }`).** Local writes happen at human cadence, and the app needs its own edit on screen on the next microtask. Every active monitor on a named table re-runs its `SELECT`.
- **`apply` commits carry full post-images (`TargetRowChange`)** for inserts and updates, and `localId` for deletes. Remote traffic can be heavy while the user looks at one room or folder, so each monitor proves when a change cannot affect its page:
  - A delete re-runs the monitor only if `cachedRowIds.has(localId)` (or if it deletes a cursor anchor whose sort tuple was not cached yet; once cached, the anchor's tuple is kept so the page boundary stays stable even after the anchor row is deleted).
  - An insert or update on a row already in `cachedRowIds` (or on the cursor anchor) always re-runs — that covers in-place edits and departures.
  - A row not in `cachedRowIds` is tested in memory with `evalWhere`. If it fails the `WHERE`, or falls on the wrong side of the `before`/`after` anchor tuple, or sorts past the last row of a full `limit` page, the monitor ignores it without touching the database.
- **`settle` commits** re-run monitors on `revertedTables`: the tables of the rows the settle reverted, and of the rows its events name.

An `apply` or `settle` commit also carries the op-events it logged, which reach the monitors as [Events in a snapshot](#events-in-a-snapshot) describes.

Predicates agree on Memory, SQLite, and IndexedDB: SQL three-valued `NULL`, case-sensitive `LIKE` through `likeMatch` from `hhs3_rdb`, and Unicode code-point order for text-stored values (`string`, `bigint`, `decimal`, `bytes`, `json`). Sort order places `NULL` below every non-null value. Memory and IndexedDB use that order for the `before` / `after` bound. SQLite's bound is a SQL tuple comparison, `(columns) < (anchor)` or `>`, so a `NULL` in the tuple matches neither side. SQLite `ORDER BY` still sorts `NULL` first.

### At-least-once local wake: the drain rule

When a local write commits, `RdbProjection` drains `rdb_outbox` after its debounce and `commitIngest()` deletes the consumed outbox rows. If a target only looked for local writes by polling or watching `rdb_outbox` after the fact, a fast drain could delete an outbox row before the watcher ever saw it.

Every target therefore enforces the **drain rule**: `drainChanges()` reads its outbox batch once and, before returning that batch for ingestion, emits `{ source: 'local', tables }` for any outbox id in the batch that has not been announced yet. IndexedDB's `oncomplete` and `MemoryTarget.record` announce during the commit and record those outbox ids in `emittedOutboxIds`, so the later drain does not announce them again. On SQLite that same early announcement happens when the app wraps the handle with `wrapDatabase(db, target)`. A write on the raw handle is announced by the coalesced WAL watcher, by the memory poll, or by `drainChanges` when its read is the first to see the row.

### Subscriptions and snapshot identity

`monitor()` returns a `QuerySubscription<T>` (`getSnapshot()`, `subscribe(listener)`, `whenReady()`), shaped directly for React's `useSyncExternalStore` or any signal/store wrapper.

Monitors are deduplicated and reference-counted by a canonical fingerprint of the query plan (table, `select`, `where` template, sorted literal parameters, `orderBy`, cursor `side`+`id`, `limit`). A subscription holds its monitor, and a held monitor holds the monitors of the queries its `.col()` parameters read. A monitor nobody holds leaves the engine and stops running. `results()` holds its monitor only while it reads, so a one-shot read leaves nothing running, and it never drops a monitor a subscription holds. Dirty monitors coalesce onto a microtask. When a re-query finishes:

- Any row whose columns equal the previous snapshot's row with the same `id` reuses the previous `ProjectedRow` object reference.
- If every row in the new result matches and no relevant `OpEvent` arrived, `getSnapshot()` keeps returning the exact same `QuerySnapshot` object and listeners are not called.

### Events in a snapshot

A snapshot's `events` are the op-events about rows the query shows: local writes that ingestion refused, and concurrent ops that were voided or reinstated (see [Event monitoring](#event-monitoring)). Each `apply` or `settle` commit carries the events it logged, and each monitor takes the ones that concern it:

- **The rows an event names.** A row op's event names its row by `table` and `localId`. A bundle's event names each row the bundle writes, in `writes`: a voided bundle removes or brings back every one of them, in any table. rdb knows rows by hash only, so the target fills in the local ids when it logs the event, from its sync table, in the transaction that writes the rows: a row the transaction inserts has its new id, and a row it removes keeps the id its mapping holds. A row the target never had has no local id, and no query shows it.
- **Which queries it reaches.** An event reaches a query when a row it names is in the query's table and in its result just before the event's commit, or just after it. The table counts, because local ids are per table: the refusal of row 1 of `chat_messages` never reaches a query that shows row 1 of `user_profiles`. Refusals and flips follow the same rule. A refused insert or update, and a void, name a row that was on screen and is now reverted or gone. A refused delete, and a reinstate, name a row that comes back, and the event arrives with the result that shows it again.
- **When it is delivered.** In the delivery that follows its commit, even when the rows did not change, as when the voided op was an update a later update had already overwritten.
- **Never later.** A monitor starts at the op-event log's high-water mark and moves past every event it reads, whether the event concerned it or not. An event for a row off the page is not delivered when that row later scrolls into the page, and a monitor opened after an event never sees it. The log keeps every event for `opEvents({ afterId })`.
- **Query sets.** A query set delivers once per pass, after every member that had to re-run has, and its snapshot carries the events of its members' deliveries in log order, each once even when two members show the same row.

## Layout

- `types.ts` — vocabulary + `MaterializationTarget` / `MaterializedChangeSource` / `ReactiveChangeTarget` / `QueryableTarget` / `ChangeSignalSource` / `AdapterConfig`.
- `names.ts` — target/column naming + FK resolution (`resolveFk`).
- `schema_actions.ts`, `row_actions.ts` — pure project-side planners.
- `ingest.ts` — pure inverse planner: one change to one op, a refusal, or nothing to write.
- `ingest_chain.ts` — the ingestion pass and the end of a pass (see [Ingestion and checkpoints](#ingestion-and-checkpoints)).
- `project.ts`, `ingest_orchestrator.ts` — single-group and database-level orchestrators.
- `testing.ts` — the `/testing` entry point the suites use: `leaveEndsToNewCycles`, a process-wide switch that leaves every end of a pass to a new cycle, and two helpers of the pass.
- `index_actions.ts` — pure index planner (spec validation, rdb → target name resolution, diff against the built indexes).
- `index_reconcile.ts` — `reconcileIndexes`: the fingerprint check and the atomic install of an index spec.
- `where_parser.ts`, `query_eval.ts`, `query_builder.ts`, `query_monitor.ts` — predicate parser, in-memory `WHERE` and sort-tuple evaluator, fluent `TableQuery` builder, and `QueryEngine`.
- `memory_target.ts` — a self-contained in-memory backend implementing `MaterializationTarget`, `MaterializedChangeSource`, `ReactiveChangeTarget`, and `QueryableTarget` (used in tests).

## Implementation details

- **Project** (rdb → relational): pure mappers turn an `RTableGroup`'s resolved schema and row deltas into ordered `SchemaAction` / `RowAction` lists (`schema_actions.ts`, `row_actions.ts`), which a `MaterializationTarget` applies transactionally per group checkpoint (`project.ts`). A same-shape **reincarnation** (a drop+re-add whose resolved def is unchanged, see [Rdb incarnations](../rdb#schema-evolution-and-incarnations)) is a reset, not an in-place diff: a table reincarnation projects as `drop-table` + `create-table` + a live-row backfill (of the table and of every table with a local FK into it, whose `<col>_id` companions referenced the dropped serial ids), and a column reincarnation as `drop-column` + `add-column`, so stale cells cannot survive an incremental apply. FK columns are reshaped to a companion form: a local (or co-projected cross-group) FK becomes an integer `<col>_id` holding the referenced row's serial id, a plain column with no database foreign key behind it (see the SQLite target's [Foreign keys](../rdb_adapter_sqlite#foreign-keys)); a non-co-projected cross-group FK becomes a text `<col>_row_hash` passthrough. Authorship projects as integer `author_key_id` into a shared `rdb_keys(id, key_hash, public_key)` side table (duplicates of a key hash collapse to one id). An identity-provider table's keyId column projects as `key_id` (same side table); its publicKey column is **not** projected — crypto material lives only in `rdb_keys`. A first-class `identity` column type likewise projects as `<col>_key_id`.
- **Ingest** (relational → rdb): the inverse planner (`ingest.ts`) plans each captured change on its own, in **commit order**: it mints an insert's uuid and rowId, reverse-maps names, rewrites FK and key-ref values (including reconstructing a provider's `keyId`+`publicKey` from `rdb_keys`), and sets aside a readonly or author column that holds the row's own value. The pass (`ingest_chain.ts`) builds each entry with `prepareRowOp`, so it is the plain row op the app wrote, records its hash in the outbox row, and appends it with `appendPrepared`, signed by the configured `writer` when the group has an identity provider and anonymous otherwise (rowIds are then derived without an author). Commit order is already FK-respecting (a local FK can only be written against an already-local row), so nothing is reordered. New keys are introduced with `KeyIndex.registerKey(domain, keyHash, publicKey)` (public key mandatory). See [Ingestion and checkpoints](#ingestion-and-checkpoints).
- **Replica-wide**: `projectDatabase` / `ingestDatabaseChanges` / `syncDatabase` materialize several groups of one `RDb` into **one shared target** so cross-group FKs resolve to serial ids; group-qualified names keep tables from colliding. Before a group's entry, ingestion observes the co-projected groups it binds whose chain grew in the pass, in gate-graph order (a group that an `ALLOW UPDATE REF` gate reads comes before the binding the gate guards, so the gate sees its new version), so cross-group FKs **and** `exists` / restriction reads validate against freshly ingested rows. The end of a pass writes the observes that are still due, transitively. Calls that read and then write a target's outbox, checkpoints or index spec take one lock per target (`withTargetLock`), so they never interleave in a process.
- **Reactive commits**: a `ReactiveChangeTarget` emits `local`, `apply`, `settle`, and `reconnect` commit events so both `RdbProjection` (which schedules ingestion on `local`) and `QueryEngine` monitors can react without polling. (`ChangeSignalSource` remains as a fallback for custom targets that only signal outbox growth.)

## Test

The suite is driven by a small custom runner, `test/run_tests.ts`, which registers eight groups and applies positional name filters plus `--profile` / `--seeds` / `--ops` / `--max-pairs` flags before running.

```
npm test                  # full suite; planner parity runs under the default `smoke` profile
npm run test:parity:fast  # only the planner-parity fuzzer, `fast` profile (~1.5 min)
npm run test:parity:full  # only the planner-parity fuzzer, `full` profile (deep, tens of minutes)
npm run debug             # same as `npm test` under node --inspect-brk
```

### Unit suites

Deterministic, fast checks on the pure planners (each prints under its own console tag):

- `[ADPT] rdb_adapter schema actions` (`schema_actions_tests.ts`) — project-side schema-action planner.
- `[ADPTR] rdb_adapter row planner` (`row_actions_tests.ts`) — project-side row-action planner.
- `[ADPTI] rdb_adapter inverse planner` (`ingest_tests.ts`) — ingest planner (mint, reverse-map, the set-aside of readonly and author columns, refusals).
- `[ADPTV] rdb_adapter concurrency op-events (verdict flips)` (`verdict_events_tests.ts`).
- `[IDX] rdb_adapter projection-local indexes` (`index_actions_tests.ts`) — index planner, plus reconcile and apply-time index maintenance end to end on the memory target.
- `[ADPTQ] Query evaluation` and `Query monitors` (`query_eval_tests.ts`, `query_monitor_tests.ts`) — `evalWhere`, `compareSortTuples`, `MemoryTarget` commit events and keyset seeks, and `QueryEngine` relevance filtering and [events in a snapshot](#events-in-a-snapshot).

### Planner-parity fuzzer

`[PLANNER] rdb_adapter planner-parity fuzzer` (`test/planner_parity/`) is a seeded, generative cross-check with two tests:

- **project** (`project_parity.ts`, generator `@hyper-hyper-space/hhs3_rdb_adapter_test_gen`): for every extending checkpoint pair in a random schema+row history, the `full` re-projection, the `incremental` projection, and the live `rdb` projection must all agree (in-process `ActionStore`). A generated index spec covering every column of both versions rides along, and both paths must end with the same index set, equal to the spec resolved at the end version. The same generator feeds the real-target suite in [rdb_adapter_test](../rdb_adapter_test), which walks a linear checkpoint chain on a concrete backend instead of all pairs.
- **ingest** (`ingest_parity.ts`, generator `ingest_generate.ts` in this package): for a random captured outbox, every change must become exactly one op, a refusal or a mark of nothing to write, an insert on a local id that already has an identity must be refused, and the planned ops must apply to rdb.

Both run against a mock context with `selfValidate: true` (every mutation and view build is re-validated), which is what makes this suite heavy compared to the unit tests.

### Profiles

The fuzzer size is set by `PARITY_PROFILES` in [rdb_adapter_test_gen](../rdb_adapter_test_gen) (`seeds / ops / maxPairs / ingestBatches / ingestChanges`). Projection-vs-rdb sweeps use a separate `PROJECTION_PROFILES` table there (longer histories, linear in ops). Planner knobs:

- `smoke` — `seeds [1, 42]`, `ops 18`, `maxPairs 32`, `ingestBatches 4`, `ingestChanges 24`. Tiny; the default used by `npm test`.
- `fast` — `seeds [1, 42, 9001]`, `ops 30`, `maxPairs 60`, `ingestBatches 6`, `ingestChanges 32`. ~1.5 min; a broader pre-push sweep.
- `full` — `seeds [1, 7, 42, 93, 1771, 9001, 31415]`, `ops 60`, `maxPairs 160`, `ingestBatches 8`, `ingestChanges 48`. Deep/nightly (tens of minutes).

The project sweep dominates runtime: cost scales roughly with `seeds x maxPairs`, each pair doing full + incremental + rdb re-projections over a history of `ops` mutations.

### Filtering and overrides

Positional arguments are name filters (e.g. `PLANNER` runs only the parity tests; any substring runs a single named test), and the profile/knobs can be set explicitly:

```
node --import ../../register.mjs ./test/run_tests.ts PLANNER --profile fast --seeds 1,42
node --import ../../register.mjs ./test/run_tests.ts --ops 40 --max-pairs 80
```

`--profile`, `--seeds`, `--ops`, and `--max-pairs` override the profile's values; the profile itself can also be selected with the `PARITY_PROFILE` env var.

**HIPs:** [3](../../hips/0003-schema-composability.md), [5](../../hips/0005-reactive-queries.md), [14](../../hips/0014-observer-frame-ref.md), [29](../../hips/0029-checkpoint-consistency.md)
