# Reactive Query Engine & UI Bindings Plan (C1)

## 1. Problem Statement

`REACT_BINDINGS.md` established the fundamental shift from v2 to v3: UI components do not observe live `RObject`s; they render rows from a **projected relational target** (`SqliteTarget`, `IdbTarget`, `MemoryTarget`) kept in sync by `RdbProjection` (either in-process or in a separate `rhost` host process/tab).

While the baseline idea in `REACT_BINDINGS.md`—waking on any database change, re-running every mounted query, and diffing result snapshots before re-rendering React—is simple and correct, it is **too chatty at the database layer for busy replicated RDbs**:

1. **Unnecessary Query Re-execution:** In a high-traffic RDb where remote peers continuously stream ops into various tables or partitions (e.g., other chat channels, other folders, background presence/telemetry updates), waking every mounted monitor and re-executing dozens of SQLite or IndexedDB queries on every sync cycle wastes CPU and I/O—even when 99% of those changes cannot possibly affect the rows on screen.
2. **Three Separate Commit Streams with Different Vocabularies & Wake Latencies:**
   - **Local writes (Outbox):** Captured synchronously by DB triggers / facade into `rdb_outbox`. Uses **target vocabulary** (`table`, integer `localId`, full post-image on `insert`, one outbox row per changed column on SQLite `update`, and no column values on `delete`). Must wake the writing process's UI immediately (<1 frame) *before* `RdbProjection`'s 50ms ingest debounce—which requires an explicit post-commit poke after the outermost commit on same-process SQLite and a payload-carrying poke on same-tab/cross-tab IndexedDB.
   - **Remote / project-back writes (`MaterializationTarget.apply`):** Written with capture gated off. Incoming `RowAction`s use **RDb vocabulary** (`rowId` hashes, FK `rowId` hashes, `KeyId` hashes), which the target translates *during `apply()`* into target vocabulary (integer `id`, integer `<fk>_id` companions, integer `rdb_keys` ids).
   - **Failure & concurrency backchannel (`commitIngest` & `OpEvent`s):** Reverts rejected local writes and logs ingestion failures or concurrency void/reinstate flips with capture gated off.
3. **Multi-Process & Multi-Observer Topologies (`rhost` vs. `rhost_client`):**
   - An application may run `RdbProjection` in the same process as the UI, **or** run `rhost` in one daemon process / serving browser tab while multiple separate observer processes/tabs (a React UI, a headless alerts/monitoring worker, a secondary tab) read/write the projected database and connect via `rhost_client_node` (`run/rhost.sock`) or `rhost_client_browser` (`BroadcastChannel`).
   - Because `apply()` and `commitIngest()` gate `rdb_outbox` off, external observer processes cannot discover remote changes from `rdb_outbox`. And because `drainChanges()` deletes rows from `rdb_outbox`, another process's local write must be broadcast by the host's pre-drain peek rather than relying on client WAL watches that may wake after the outbox is already emptied.
4. **The Missing Before-Image Problem:** Neither `rdb_outbox` nor `RowAction` records the prior state of a mutated or deleted row. A naïve change filter that only checks whether the *new* row values match a `WHERE` predicate misses **departures** (an update that moves a row *out* of the `WHERE`, or a delete that carries only `localId` / `rowId`).
5. **Pagination & Cross-Table Hydration ("Joins") Without SQL False Friends:** Real screens paginate tables (where numeric `OFFSET` degrades both DB scan cost and reactive filtering on head inserts) and hydrate related records from other tables (e.g., `user_profiles` for the authors on screen). Offering raw SQL strings invites "SQL false friends" (`JOIN`, `GROUP BY`, `OFFSET`, subqueries) that cannot be cheaply filtered in memory or executed on `IdbTarget`.

---

## 2. Core Design Overview (Compromise 1)

We solve these problems with a **single-table reactive query engine and DAG composer (C1)** built on five pillars:

1. **Fluent Single-Table Query Builder + `.before(id)` / `.after(id)` Keyset Pagination + Column-Set Dependencies:**
   - Queries are constructed via a fluent builder on any `QuerySource` (`RdbProjection`, `SqliteTarget`, `IdbTarget`, `MemoryTarget`):
     ```ts
     // Newest page (live tail):
     const head = db
       .selectFrom("chat_messages")
       .where("channel_id = :id", { id: 33 })
       .orderBy("id", "desc")
       .limit(50);

     // Next older page (lesser side of the order tuple):
     const olderPage = db
       .selectFrom("chat_messages")
       .where("channel_id = :id", { id: 33 })
       .orderBy("id", "desc")
       .before(lastId)
       .limit(50);

     // Next page of an ascending list (greater side of the order tuple):
     const nextAscPage = db
       .selectFrom("chat_messages")
       .where("channel_id = :id", { id: 33 })
       .orderBy("id", "asc")
       .after(lastId)
       .limit(50);
     ```
   - `.where(...)` accepts a **boolean row predicate** lowered to a bound AST (`BoundWhereExpr`) in `rdb_adapter` (keeping `rdb_adapter` free of any `rdb_lang` dependency), eliminating SQL false friends by construction.
   - Cross-table dependencies (joins) are expressed by passing a column reference from an upstream query (`messages.col("author_key_id")`) as a parameter to a downstream query (`profiles.where("key_id IN :keys", { keys: messages.col("author_key_id") })`), forming an explicit, acyclic **Query-Set DAG**.
2. **Narrow, Sound In-Memory Relevance Filtering:**
   - An ignore is a **proof** that a commit cannot change the query result; anything short of a proof re-runs the query and lets snapshot comparison drop no-op wakes.
   - Every active single-table monitor remembers `cachedRowIds: Set<number>` (the `id`s in its last delivered snapshot), plus the sort tuple of its cursor (`cursorTuple`) and last cached row (`lastRowTuple`) when `.before(id)` / `.after(id)` / `.limit(n)` are used.
   - **Busy path (`apply` upserts/deletes + local `insert`/`delete`):** Full post-images are tested with `evalWhere(whereAst, values)` (using `likeMatch` from `@hyper-hyper-space/hhs3_rdb` and SQL `NULL` semantics across all targets) combined with `cachedRowIds.has(localId)`, anchor-row tracking (`localId === anchorId`), and keyset bounds ordered by SQLite's UTF-8 binary / Unicode code-point order.
   - **Quiet path (local `update`, `settle` reverts, schema actions, IPC `reconnect`):** Any local `update` or `settle` revert or schema change on `table` (or a client `reconnect` event) simply marks affected monitors dirty without needing a three-valued partial evaluator.
3. **Post-Commit Local Poke + Duck-Typed Target Capabilities:**
   - Targets opt in one by one (`MemoryTarget` $\to$ `SqliteTarget` $\to$ `IdbTarget`) via duck-typed interfaces in `rdb_adapter`: **`QueryableTarget`** and **`ReactiveChangeTarget`**.
   - On `IdbTarget`, `FacadeTransaction` accumulates its outbox records in memory and delivers them directly in `onOutboxCommitted` (and over `BroadcastChannel`) so an async `drainChanges()` can never race and delete outbox rows before a monitor sees them.
   - On `SqliteTarget`, a wrapped `Database` handle calls `target.noteLocalCommit()` **once, after the outermost successful commit** (never mid-transaction on nested `Statement#run()` or `exec()`), reading `rdb_outbox` synchronously via `toLogical`.
4. **Multi-Process Commit Streaming over `rhost_client` (`run/rhost.sock` & `BroadcastChannel`):**
   - Because the query builder and monitor engine depend only on `QueryableTarget & ReactiveChangeTarget` (in `rdb_adapter`, not `RdbProjection`), any process can host reactive monitors over its local target.
   - When `rhost` runs in a separate process/tab, `serveClient` streams `TargetCommitEvent`s (`local`, `apply`, `settle`) over the existing `rhost_client` IPC bus (`watchCommits` on `run/rhost.sock` or `BroadcastChannel`) to all connected observer processes, deduplicating per-change `outboxId`s so interleaved local writes across multiple processes are never dropped or applied twice.
5. **Refcounted Monitors & Cached-Snapshot Subscription Contract:**
   - Monitors are deduplicated and refcounted by structural value fingerprint (including static bound params and cursor side+id, excluding live `ColumnSetRef` sets).
   - `getSnapshot()` returns a stable cached snapshot reference, preserving unchanged row objects and unchanged sub-query arrays, built directly for React's `useSyncExternalStore` and easily wrapped for Svelte, Signals, Vanilla JS, and Headless Node.

---

## 3. Changes to the Change Ingestion, Emission & Cross-Process Pipeline

### 3.1. Duck-Typed Interface 1: `ReactiveChangeTarget` (`@hyper-hyper-space/hhs3_rdb_adapter`)

During `target.apply(...)` and `target.commitIngest(...)`, only the target knows the resolved integer `localId`, translated FK `<col>_id` integers, interned `rdb_keys` integers, and the durable monotonic log `id` of newly appended `StoredOpEvent`s. And for `local` writes, each change carries its monotonic `outboxId` so multiple processes can deduplicate individual outbox rows cleanly:

```ts
// Normalized, TARGET-VOCABULARY row mutation emitted after a transaction commits.
// All table/column names are target names; localId is the table's integer PK (`id`);
// FK companions (`<col>_id`) and key-refs (`author_key_id`, `key_id`, `<col>_key_id`)
// are already resolved to their target integer ids.
// Missing keys in `values` (e.g. NULL columns omitted by toLogical) represent SQL NULL;
// the id column itself is carried in `localId`.
// `outboxId` is present on `source: 'local'` changes so observers can deduplicate
// individual outbox rows across local pokes and host broadcasts.
export type TargetRowChange =
  | {
      kind: 'insert';
      table: string;
      localId: number;
      outboxId?: number;
      // Full post-image in logical target values (missing keys = NULL; id = localId).
      values: { [column: string]: json.Literal | null };
    }
  | {
      kind: 'update';
      table: string;
      localId: number;
      outboxId?: number;
      // Full post-image when emitted by apply(); omitted/ignored for local outbox updates.
      values?: { [column: string]: json.Literal | null };
    }
  | {
      kind: 'delete';
      table: string;
      localId: number;
      outboxId?: number;
    };

export type TargetCommitEvent =
  | {
      // Fired when local app writes commit into rdb_outbox (BEFORE ingest).
      source: 'local';
      changes: TargetRowChange[];
    }
  | {
      // Fired at the end of MaterializationTarget.apply() after commit.
      source: 'apply';
      groupId: B64Hash;
      schemaTables: string[];       // tables touched by SchemaAction
      changes: TargetRowChange[];   // resolved from RowAction[] during apply (always full post-image on upsert)
      events: StoredOpEvent[];      // newly logged concurrency void/reinstate events (with log id and localId populated)
    }
  | {
      // Fired at the end of MaterializedChangeSource.commitIngest() when reverts
      // or ingestion failure events are committed. (Pure outbox-ack settles with
      // no reverts and no events are silent.)
      source: 'settle';
      revertedTables: string[];     // tables touched by settle.reverts
      events: StoredOpEvent[];      // newly logged ingestion failure events (with log id and localId populated)
    }
  | {
      // Fired when an out-of-process client (rhost_client_node / rhost_client_browser)
      // connects or reconnects to the host's commit stream, forcing a row catch-up re-run.
      source: 'reconnect';
    };

export type TargetCommitListener = (commit: TargetCommitEvent) => void;

export interface ReactiveChangeTarget {
  addCommitListener(listener: TargetCommitListener): void;
  removeCommitListener(listener: TargetCommitListener): void;
}

export function isReactiveChangeTarget(t: object): t is ReactiveChangeTarget {
  return typeof (t as Partial<ReactiveChangeTarget>).addCommitListener === 'function';
}
```

### 3.2. How Each Pipeline Stage Produces `TargetCommitEvent` (In-Process & Multi-Process)

1. **Stage A — Local Write (`source: 'local'`), Per-Change `outboxId` Dedup, & Post-Commit Poke:**
   - Every local `TargetRowChange` carries its `outboxId: number` (the `rdb_outbox.id` serial).
   - Each target instance maintains `lastNotifiedOutboxId` (and/or a bounded seen-set for out-of-order IPC delivery): when a `local` batch arrives, **each change is filtered individually by its `outboxId`** (keeping only changes not yet seen by this target instance, and advancing `lastNotifiedOutboxId` to the highest `outboxId` applied). This ensures that if Process B writes outbox row 10 and Process C writes outbox row 12, Process C's local poke and the Host's broadcast of `[10, 12]` never cause Process C to drop Process B's row 10!
   - **`MemoryTarget`:** `record()` assigns `outboxId`, appends the outbox entry, and invokes commit listeners synchronously before returning.
   - **`IdbTarget` (poke carries the commit payload):**
     - An async `getAll` on `OUTBOX` after commit would race with `syncDatabase` (`drainChanges` + `commitIngest`), which can delete those outbox rows before the monitor's read returns.
     - Instead, since `queueOutbox` in `idb_facade.ts` *already* builds the `OutboxRecord`s in memory during the app transaction, `FacadeTransaction` accumulates that list and passes it to `IdbTarget.onOutboxCommitted(records)` from `tx.oncomplete`.
     - `onOutboxCommitted` immediately converts those records to `TargetRowChange[]` (with `outboxId: record.id`), notifies local commit listeners synchronously in that turn, and posts the same payload over `BroadcastChannel` so other tabs also receive the changes without racing the writer's `drainChanges`.
   - **`SqliteTarget` (synchronous peek once after outermost commit + host pre-drain peek):**
     - When `noteLocalCommit()` runs on `SqliteTarget`, it synchronously reads `rdb_outbox WHERE id > lastNotifiedOutboxId ORDER BY id` (converting values with `toLogical`, setting `outboxId: row.id`, and taking the row's PK from `change.localId`), advances `lastNotifiedOutboxId`, and emits `{ source: 'local', changes }`.
     - **Outermost-commit wrapper (for the writing process):** Provide a thin wrapper around the shared `better-sqlite3` `Database` handle that tracks transaction nesting depth and calls `target.noteLocalCommit()` **once, immediately after the outermost successful commit** (never after inner `Statement#run()` calls inside `db.transaction()` or mid-`exec()`, so a rolled-back transaction never wakes monitors with phantom rows). `apply()` can safely share the wrapped handle because `apply()` gates capture off and does not advance `MAX(rdb_outbox.id)`.
     - **Host pre-drain peek in `drainChanges()` (for other processes):** Right before the Host's `SqliteTarget.drainChanges()` reads the outbox for ingestion (after the projection's 50ms debounce), it calls `this.noteLocalCommit()` **before** `commitIngest` deletes the consumed rows. While the writing process already painted immediately from its own wrapper, every *other* observer process receives the writer's `local` changes from this Host pre-drain broadcast over `watchCommits`. (A client WAL watch that wakes after `commitIngest` and sees an empty `rdb_outbox` has missed those rows and is not used as a catch-up mechanism; the IPC payload from the Host's pre-drain peek is what carries another process's local write.)

2. **Stage B — Remote / Project-Back Apply (`source: 'apply'`):**
   - Inside `target.apply(...)`, as the target processes `rowActions`:
     - On `upsert-row`: resolves `id = allocateId(syncTable, action.rowId)`, checks `exists`, translates every column to its target logical value (FK `rowId` $\to$ integer `id`, `keyHash` $\to$ integer `key_id`, `author` $\to$ `author_key_id`), and records `{ kind: exists ? 'update' : 'insert', table: action.table, localId: id, values: fullPostImage }`.
     - On `delete-row`: resolves `id` from `syncTable` for `action.rowId` before deleting the app row, and records `{ kind: 'delete', table: action.table, localId: id }`.
     - For `events` (concurrency void/reinstate): populates `event.localId` from `syncTable` and captures the `{ id, event } satisfies StoredOpEvent` assigned when logging each new event.
   - Immediately **after** the `apply()` transaction commits, the target fires `{ source: 'apply', groupId, schemaTables, changes, events }`.

3. **Stage C — Ingest Settle (`source: 'settle'`):**
   - Inside `target.commitIngest(settle)`:
     - If `(settle.reverts?.length ?? 0) === 0 && (settle.events?.length ?? 0) === 0`, emit nothing.
     - Otherwise, collect `revertedTables` from `settle.reverts` and `settle.events`, capture the `StoredOpEvent[]` (with their assigned log `id`s), and after the settle transaction commits, fire `{ source: 'settle', revertedTables, events }`.

### 3.3. Multi-Process & Multi-Tab Channel (`rhost` $\leftrightarrow$ `rhost_client`)

When the UI or a background observer (e.g., an alerts/monitoring service) runs in a separate process (`rhost_client_node`) or a non-serving browser tab (`rhost_client_browser`):

1. **`watchCommits` on the `rhost_client` Protocol & Push Typing:**
   - Extend `ClientRequest` in `@hyper-hyper-space/hhs3_rhost_client/src/protocol.ts` with `{ id, method: 'watchCommits' }` (unwatched via `{ id, method: 'unwatch', watch }`).
   - Update `ServerMessage` and `decodeServerMessage` in `protocol.ts` (which currently types every push as `{ id, event: HostStatus }`) so push messages discriminate or type both `HostStatus` pushes and `TargetCommitEvent` pushes cleanly. The channel stays strictly read-only and carries nothing that signs.
   - **Send-failure cleanup in `serveClient`:** In `modules/rhost/src/serve.ts`, `send()` currently swallows exceptions when writing to a broken connection. Update `serveClient` so a failed `send` on a watch cleans up and closes that watch/connection. That way, a transient write failure never leaves a client watching a silent socket; the client detects the disconnect, reconnects, and runs the `{ source: 'reconnect' }` re-query.
2. **Client-Side Notification Fan-Out (`notifyExternalCommit` — Never Writes Rows):**
   - A client process opens its local `SqliteTarget` (over `projectionPath(dir)`) or `IdbTarget` (over `browserProjectionDatabase`) to execute queries and perform local writes, and connects `client.commits.watch(...)` to `target.notifyExternalCommit(commit)`.
   - **`notifyExternalCommit(commit)` does not write rows to the database** (the Host or writing process has already committed the rows into the shared SQLite file / IDB store). It only filters out already-seen `outboxId`s on `source: 'local'` and fans the remaining `TargetCommitEvent` out to the target's `ReactiveChangeTarget` listeners so monitors can filter and re-query.
3. **Connect / Reconnect Row Refresh (Leaving `lastSeenEventId` Intact):**
   - Whenever `rhost_client_node` or `rhost_client_browser` establishes or re-establishes its `watchCommits` stream after the host starts/restarts, it emits `{ source: 'reconnect' }`.
   - On `reconnect`, every active monitor is marked dirty to re-query current database rows, while **`lastSeenEventId` is left intact** (or initialized to the current high-water mark on first open—never reset backward). Events from a disconnected gap remain in the durable log (inspectable via `opEvents({ afterId: lastSeenEventId })`) and are never replayed as if they were fresh snapshot events or attached later when a row scrolls into a page.

---

## 4. Narrow Relevance Filter & Keyset Pagination Rules

### 4.1. Keyset Pagination (`.before(id)` and `.after(id)`)

Numeric `OFFSET` is out of the language: it forces linear scans in SQLite/IndexedDB and causes every head insert to dirty every deeper page.

Instead, a page is either `.before(id)` or `.after(id)` (never both on the same query) plus `.limit(n)`:
- **`BEFORE id` (`.before(id)`):** seeks to rows on the **lesser** side of the anchor row's order tuple (`tuple < anchorTuple`).
- **`AFTER id` (`.after(id)`):** seeks to rows on the **greater** side of the anchor row's order tuple (`tuple > anchorTuple`).
- **Pairing with `orderBy` and `LIMIT`:** `LIMIT` keeps the rows **adjacent to the anchor** only when the bound faces the sort direction:
  - A descending page (`ORDER BY ... DESC`) uses **`.before(lastId)`** to get the greatest tuples below the anchor (the next older page).
  - An ascending page (`ORDER BY ... ASC`) uses **`.after(lastId)`** to get the least tuples above the anchor (the next newer page).
  - Pairing `DESC` with `AFTER` (or `ASC` with `BEFORE`) is legal, but `LIMIT` then takes the far end of that side (e.g., the highest keys in the table above the anchor), not the anchor's immediate neighbors.
- The anchor `id` is **exclusive**. The live tail is the query with neither `before` nor `after`.
- **Total Order & Tiebreaker:** Every query that uses `LIMIT`, `BEFORE`, or `AFTER` has a total order. When the caller specifies no `orderBy`, the default order is `id ASC`. When `orderBy` ends with a column other than `id` (e.g., `ORDER BY created_at DESC`), `id` is appended as the final tiebreaker **in the same direction** as the column it breaks (`ORDER BY created_at DESC, id DESC`).
- **Anchor Lookup & Deletion-Safe Caching:** The caller passes only the row `id`. Execution loads the anchor row's sort columns with one primary-key lookup and builds `cursorTuple`. The monitor caches `cursorTuple`:
  - If the anchor row is **updated**, the monitor re-runs and refreshes `cursorTuple` with the anchor's new sort key.
  - If the anchor row is **deleted**, the monitor **retains the previous `cursorTuple`** (never clearing it to `undefined`) and passes it via `CursorBound.cachedValues` so subsequent runs and rule-3 checks still have the exact bound.

### 4.2. Commit Relevance Rules

Every active single-table monitor holds:
- `table`: target table name
- `whereAst`: bound predicate AST (`BoundWhereExpr | undefined`)
- `orderBy`: `OrderBySpec[]` (normalized to end with `id` tiebreaker)
- `cursor`: `{ side: 'before' | 'after'; id: number } | undefined`
- `limit`: `number | undefined`
- `cachedRowIds`: `Set<number>` of `id`s in the last delivered result snapshot
- `cursorTuple`: cached sort tuple of the `cursor.id` anchor row (`undefined` until resolved)
- `lastRowTuple`: cached sort tuple of the last row in the snapshot when `rows.length === limit` (`undefined` when the page is not full or before the first run)

A commit batch is evaluated as a unit: **if any change in the commit is dirty for the monitor, the monitor is marked dirty and re-queries once against the post-commit database** (so an anchor change and a sibling insert in the same commit never ignore against a stale `cursorTuple`). Both the cursor-side check (step 3) and the far-edge check (step 4) below run **only when `cursorTuple` / `lastRowTuple` is already known**—a missing tuple is never an ignore.

| Commit Kind | Relevance Rule |
| :--- | :--- |
| **`source: 'reconnect'`** | **Re-run** every monitor to refresh rows; leave `lastSeenEventId` unchanged. |
| **Any change where `change.table !== table`** | **Ignore.** |
| **`settle` or `apply` schema on `table`** | **Re-run** every monitor on `table`. |
| **Local `update` on `table`** | **Re-run** every monitor on `table`. (SQLite emits one outbox row per updated column; avoids three-valued partial predicate evaluation on the user's own keystrokes.) |
| **Update to the cursor anchor row (`cursor !== undefined && change.localId === cursor.id && change.kind !== 'delete'`)** | **Re-run** and refresh `cursorTuple` from the updated anchor row. (Because `BEFORE` / `AFTER` is exclusive, `cursor.id` is not in `cachedRowIds`; if an `apply` update moves the anchor's sort key, the page window shifts.) |
| **Delete of the cursor anchor row (`cursor !== undefined && change.localId === cursor.id && change.kind === 'delete'`)** | **Keep the cached `cursorTuple` unchanged.** If `cursorTuple` was not yet known, **Re-run**; otherwise **Ignore** (the anchor is already excluded from the page, and its cached sort tuple stays in place as `cachedValues`). |
| **`apply` or local `delete` on `table`** | **Re-run only when `cachedRowIds.has(localId)`**; otherwise **Ignore**. |
| **`apply` insert, `apply` update (full post-image), or local `insert`** | Build row view with `id = change.localId` and missing keys treated as `NULL`:<br>1. If `cachedRowIds.has(localId)` (or `localId === cursor?.id`): **Re-run** (covers in-place changes, departures where `evalWhere` is now `false`, and anchor updates).<br>2. If `!evalWhere(whereAst, row)`: **Ignore**.<br>3. If `cursor !== undefined && cursorTuple !== undefined` and `row`'s sort tuple is not on the `cursor.side` of `cursorTuple` (for `BEFORE`, not `< cursorTuple`; for `AFTER`, not `> cursorTuple` under the `SELECT`'s comparator): **Ignore** (head inserts do not wake older history pages).<br>4. **Far-edge rule:** If the page is full (`rows.length === limit && lastRowTuple !== undefined`) and `row`'s sort tuple sorts strictly past `lastRowTuple` in the query's `orderBy` direction: **Ignore** (backfill past a full window cannot enter the page).<br>5. Otherwise: **Re-run**. |

---

## 5. `SELECT`-Order Comparator, `evalWhere`, and `QueryableTarget`

### 5.1. Module Placement & Unified Semantics (`rdb_adapter` vs `rdb_projection`)

Because multi-process clients (`rhost_client_node`, `rhost_client_browser`) run queries and monitors over a target without instantiating `RdbProjection`, the query builder, predicate parser, evaluator, and C1 monitor engine all live in **`@hyper-hyper-space/hhs3_rdb_adapter`** (which already depends on `@hyper-hyper-space/hhs3_rdb` for `likeMatch`, and has zero dependency on `rdb_lang`), and **`@hyper-hyper-space/hhs3_rdb_projection`** delegates `projection.selectFrom()` and `projection.monitor()` to the same engine:
- **`BoundWhereExpr` & `where_parser.ts`:** Parses `.where("channel_id = :id AND ...", params)` into `BoundWhereExpr` templates (supporting `=`, `!=`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`, `IN`, `NOT IN`, `LIKE`, `AND`, `OR`, `NOT`, `+`, `-`, `*`, `length`).
- **`evalWhere(expr: BoundWhereExpr | undefined, row: Record<string, json.Literal | null>, localId?: number): boolean`:**
  - Treats any missing key on `row` as `null` (since `toLogical` in SQLite omits SQL `NULL` keys), and resolves the table's `id` column from `localId` when not present on `row`.
  - Enforces **SQL `NULL` semantics**: any comparison with `null` other than `IS NULL` / `IS NOT NULL` produces `unknown`, which `WHERE` treats as `false`.
  - Calls **`likeMatch` from `@hyper-hyper-space/hhs3_rdb`** (`modules/rdb/src/rschema/expr.ts`) for `LIKE`: case-sensitive, Unicode code-point semantics, `%` any run, `_` one code point, `\` escape.
- **The `SELECT` Order Comparator (`compareProjectedValues` / `compareSortTuples`):**
  - The far-edge and cursor-side ignore rules are sound **only when they use the exact order `executeQuery` returns** (not `orderRows` / `compareNumericStr` from `rdb`, which sorts nulls last and orders `bigint`/`decimal` numerically).
  - In projected storage (`sqliteType` in `sqlite_target.ts` and `idb_index_keys.ts`), `integer` and `float` are numbers, `boolean` is `0`/`1`, and `string`, `bigint`, `decimal`, `bytes`, and `json` are stored as `TEXT` (ordered lexically so native B-tree indexes serve `ORDER BY`).
  - **Text Order is SQLite's UTF-8 Binary (Unicode Code-Point) Order:** SQLite's default `BINARY` collation compares UTF-8 bytes, which follows Unicode **code-point** order (`codePointAt`), *not* JavaScript's `<` / `>` UTF-16 code-unit order (which disagrees on supplementary-plane characters above `U+FFFF`). `compareProjectedValues` compares strings by Unicode code points so `compareSortTuples`, `MemoryTarget`, `IdbTarget`, and SQLite's native index order agree on every string.
  - **`NULL` is smaller than every non-null value** (`ASC` puts `NULL` first; `DESC` puts `NULL` last).

### 5.2. Duck-Typed Interface 2: `QueryableTarget` (`@hyper-hyper-space/hhs3_rdb_adapter`)

```ts
export type OrderDirection = 'asc' | 'desc';
export type OrderBySpec = { column: string; direction: OrderDirection };

export type CursorBound = {
  side: 'before' | 'after';     // 'before' = lesser (<), 'after' = greater (>)
  id: number;                   // exclusive anchor row id
  // Fallback sort-column values from the monitor's cached cursorTuple in case row `id` was deleted.
  cachedValues?: { [column: string]: json.Literal | null };
};

export type SingleTableQueryPlan = {
  table: string;
  select?: string[];            // omit for all projected columns (`id` is always included)
  where?: BoundWhereExpr;       // bound predicate AST with literal / set values
  orderBy?: OrderBySpec[];      // normalized: defaults to [{ column: idCol, direction: 'asc' }] when limit/cursor present, and appends idCol tiebreaker in the last key's direction
  cursor?: CursorBound;         // keyset seek bound (BEFORE or AFTER)
  limit?: number;
};

export type ProjectedRow = {
  id: number;
  [column: string]: json.Literal | null;
};

export interface QueryableTarget {
  executeQuery(plan: SingleTableQueryPlan): Promise<ProjectedRow[]>;
}

export function isQueryableTarget(t: object): t is QueryableTarget {
  return typeof (t as Partial<QueryableTarget>).executeQuery === 'function';
}
```

### 5.3. Target Implementations of `QueryableTarget`
- **`MemoryTarget`:** Short-circuits `id = X` / `id IN (...)` via direct `Map.get(id)` lookups; otherwise scans `state.rows`, filters via `evalWhere`, applies the `cursor` tuple bound (`<` for `before`, `>` for `after` using `compareSortTuples`), sorts by `orderBy`, and slices `limit`.
- **`SqliteTarget`:**
  - Resolves `cursor` (reading the anchor row by PK if needed, falling back to `cachedValues`), compiles `where` + the lexicographic keyset bound into a parameterized `SELECT ... FROM "table" WHERE ... ORDER BY ... LIMIT ...`, and converts stored values via `toLogical(...)`.
  - **`LIKE` is `likeMatch`:** Do **not** set `PRAGMA case_sensitive_like` (which only flips ASCII case, lacks C-SQL's `\` escape semantics, and mutates global connection behavior for the app's own SQL). Instead, register a deterministic scalar function on the connection (e.g., `rdb_like_match(value, pattern)`) backed by `likeMatch` from `@hyper-hyper-space/hhs3_rdb`, and compile `LIKE` predicates in `executeQuery` to call it. Then `evalWhere` and the SQL `SELECT` call the exact same function.
- **`IdbTarget`:**
  - Direct PK lookups on `ROWS` (`[table, id]`) when `where` is `id = X` or `id IN (...)`.
  - **Index prefix + `id` cursor range:** Because `idb_index_keys.ts` stores every index entry as `[table, indexName, ...keyValues, id]`, an equality filter on the indexed columns combined with an order/cursor on `id` (e.g., `WHERE channel_id = 33 ORDER BY id DESC BEFORE 1000 LIMIT 50`) executes as a **single bounded range scan** inside the `[table, indexName, 33, ...]` prefix (using `indexEntryRange`), never scanning rows from other channels.
  - Primary key range on `ROWS` (`[table, id]` via `rowRange`) when unfiltered and ordered/bounded by `id`.
  - Otherwise scans `ROWS` for `table`, filters with `evalWhere`, applies the `cursor` tuple bound, sorts with `compareSortTuples`, and slices `limit`.

---

## 6. Fluent Query Builder, Query-Set DAG, and Subscription Contract

### 6.1. Fluent Query Builder & `QuerySource`

Any `QueryableTarget & ReactiveChangeTarget` (or `RdbProjection`) can be wrapped in (or expose) a `QuerySource`:

```ts
const messages = source
  .selectFrom("chat_messages")
  .where("channel_id = :id AND deleted = false", { id: channelId })
  .orderBy("id", "desc")
  .before(cursorId)
  .limit(50);

const profiles = source
  .selectFrom("user_profiles")
  .where("key_id IN :keys", { keys: messages.col("author_key_id") });
```

**Builder Methods (callable in any order; shallow-copy-on-write so partial queries can be forked safely):**
- `source.selectFrom(table: string): TableQuery`
- `.select(...columns: string[]): TableQuery`
- `.where(expr: string, params?: Record<string, ParamValue | ColumnSetRef>): TableQuery`
- `.orderBy(column: string, direction?: 'asc' | 'desc'): TableQuery`
- `.before(id: number): TableQuery` (lesser side of order tuple; throws if `.after()` already set)
- `.after(id: number): TableQuery` (greater side of order tuple; throws if `.before()` already set)
- `.limit(count: number): TableQuery`
- `.col(column: string): ColumnSetRef` (also exposed via property proxy `query.$.<column>`)
- `.results(): Promise<ProjectedRow[]>` (one-shot execution)

### 6.2. Fingerprinting, Refcounting, Arbitrary Acyclic DAG Execution, and `OpEvent` Rules

1. **Refcounted Monitors by Value Fingerprint:**
   - Each monitor is keyed by a canonical fingerprint that **includes** `table`, `select`, `where` template, bound literal parameter values (canonicalized by value, so a fresh array `[1, 2]` produces the same fingerprint), `orderBy`, `cursor` (`side` + `id`), and `limit`, and **excludes** the live key set behind a `ColumnSetRef`.
   - Monitors are **reference-counted**: React StrictMode double-mounts or multiple components sharing the same query increment/decrement the refcount; the monitor is only torn down when its refcount reaches zero.
   - Changing a literal parameter or cursor `side`/`id` produces a new fingerprint $\to$ attaches to a fresh monitor with an empty `cachedRowIds` and releases the previous monitor.
2. **Arbitrary Acyclic DAG Evaluation & Edge-Scoped Key-Set Stability:**
   - A query set can form any acyclic DAG (chains $A \to B \to C$, trees, diamonds where $C$ references both $A.\text{col}$ and $B.\text{col}$).
   - Dirty monitors coalesce over a short frame window (~16ms, or microtask when configured) and evaluate in topological order.
   - Upstream queries (`messages`) execute first and extract the deduplicated, sorted key set for each exported `ColumnSetRef` (`author_key_id`).
   - **Key-set stability governs only the dependency edge:** If the extracted key set is identical to the previous run, the `messages -> profiles` dependency edge does **not** mark `profiles` dirty. However, if `profiles` was **already** marked dirty on its own by a `user_profiles` change in the same commit window, `profiles` still keeps its dirty bit and re-runs!
   - If the extracted key set changed, `profiles` updates its bound `:keys` set, is marked dirty via the dependency edge, and executes in the **same pass** before listeners are notified.
3. **`StoredOpEvent` Relevance & Log-Id Cursor Advancement:**
   - Each monitor initializes its event cursor (`lastSeenEventId`) to the current high-water mark of the target's op-event log and **always advances `lastSeenEventId` past `storedEvent.id` for every inspected `StoredOpEvent`**, including events that were dropped because they did not match this monitor (so an off-screen void from an earlier commit never attaches later if that row eventually scrolls into the page).
   - On `{ source: 'reconnect' }`, the monitor re-queries rows and **leaves `lastSeenEventId` unchanged** (gap events remain in the durable log for explicit `opEvents({ afterId })` inspection and are never re-delivered as live snapshot events).
   - Only events with `storedEvent.id > lastSeenEventId` that concern this monitor's rows are attached to the snapshot:
     - **Ingestion failure (`origin: 'ingestion'`):** attached iff `event.localId !== undefined && cachedRowIds.has(event.localId)`. (The optimistic row was in the previous snapshot and is now reverted. `event.op` is in RDb vocabulary and is not passed to `evalWhere`.)
     - **Concurrency flip (`origin: 'concurrency'`, `void` / `reinstate`):** attached iff `event.localId !== undefined` and `event.localId` is in the previous result (`cachedRowIds`) **or** in the new result.

### 6.3. Subscription Contract

```ts
export type QuerySnapshot<T> = {
  data: T;              // ProjectedRow[] for a single query, or { [K]: ProjectedRow[] } for a query set
  events: OpEvent[];    // Only relevant events from this delivery (see §6.2)
  loading: boolean;     // true only before initial query settles
  error?: unknown;
};

export interface QuerySubscription<T> {
  // Returns the CACHED snapshot reference. Never allocates on read.
  // Replaced only when a delivery changes `data`, `events`, `loading`, or `error`.
  getSnapshot(): QuerySnapshot<T>;

  // Registers a zero-arg change listener and returns an unsubscribe function
  // (decrements monitor refcounts when the last subscriber leaves).
  // Does NOT call `listener` synchronously during `subscribe()`.
  subscribe(listener: () => void): () => void;

  // Resolves with the snapshot once `loading` transitions to false (including when
  // the initial query fails, which sets `loading: false` and populates `error`).
  whenReady(): Promise<QuerySnapshot<T>>;
}
```

- **Snapshot & Row Identity Invariants:**
  - `getSnapshot()` always returns the exact same `QuerySnapshot<T>` reference until a delivery actually changes rows, attaches relevant `OpEvent`s, or transitions `loading`/`error`.
  - If the initial query throws, the monitor transitions to `{ data: empty, events: [], loading: false, error }`, notifies listeners, and resolves `whenReady()` with that error snapshot so callers never hang.
  - A project-back (`source: 'apply'`) whose row values match the optimistic local row preserves the existing row object references, produces no new snapshot object, and **does not call listeners**.
  - Within a `ProjectedRow[]`, any row whose columns are shallow-equal to the previous snapshot's row with the same `id` reuses the previous `ProjectedRow` object reference.
  - Within a query-set `{ messages, profiles }`, if `messages` updates while `profiles` did not re-run (or its rows did not change), `snapshot.data.profiles` preserves the exact same `Array` reference.
- **Framework Adapters:**
  - **React (`@hyper-hyper-space/hhs3_react`):** `useQuery` computes the canonical value fingerprint of `queryOrQuerySet`, retains the `QuerySubscription` across renders as long as the fingerprint is unchanged (even when the caller passes freshly constructed `TableQuery` instances or parameter arrays each render), and binds `sub.subscribe` and `sub.getSnapshot` to `useSyncExternalStore`.
  - **Svelte (`svelte/store`):** A 5-line helper `{ subscribe(run) { run(sub.getSnapshot()); return sub.subscribe(() => run(sub.getSnapshot())); } }` adapts `QuerySubscription` to the Svelte store contract.
  - **Signals / Vanilla JS / Headless Node:** Subscribe via `sub.subscribe(() => { const s = sub.getSnapshot(); ... })` or `await sub.whenReady()`.

---

## 7. Layered Implementation Plan

### Layer 1: Target Capability Interfaces, `SELECT` Comparator, `evalWhere`, & `MemoryTarget` (`@hyper-hyper-space/hhs3_rdb_adapter`)
- **Files:**
  - `modules/rdb_adapter/src/types.ts`: Define `TargetRowChange` (with `outboxId?: number`), `TargetCommitEvent` (`local`, `apply`, `settle` with `StoredOpEvent[]`, and `'reconnect'`), `ReactiveChangeTarget`, `BoundWhereExpr`, `CursorBound` (`side: 'before' | 'after'`), `SingleTableQueryPlan`, `ProjectedRow`, `QueryableTarget`.
  - `modules/rdb_adapter/src/query_eval.ts`: Implement `evalWhere` (missing keys as `null`, `id` from `localId`, SQL `NULL` semantics, `likeMatch` from `@hyper-hyper-space/hhs3_rdb`) and `compareSortTuples` (matching projected storage `SELECT` order: `NULL` smallest, UTF-8 binary / Unicode code-point comparison for `TEXT`-stored `string`/`bigint`/`decimal`/`bytes`, numeric for `integer`/`float`/`boolean`, default `id ASC`, `id` tiebreaker in last key's direction).
  - `modules/rdb_adapter/src/memory_target.ts`: Implement `ReactiveChangeTarget` (`local` with `outboxId`, `apply`, `settle` with `StoredOpEvent[]`, `notifyExternalCommit`) and `QueryableTarget` (`executeQuery` with `.before(id)` and `.after(id)` keyset seek).
- **Tests (`modules/rdb_adapter/test`):**
  - Unit tests for `evalWhere` (`likeMatch`, `NULL` / missing-key edge cases) and `compareSortTuples` (including Unicode code-point vs. UTF-16 surrogate-pair ordering, lexical `TEXT` ordering for `bigint`/`decimal` strings, and `NULL`-smallest ordering).
  - Unit tests verifying `MemoryTarget` emits target-vocabulary `TargetCommitEvent`s (with `outboxId` and `StoredOpEvent` log ids) and executes `SingleTableQueryPlan` with `before` and `after` bounds on both `id` and non-`id` sort orders.

### Layer 2: Predicate Parser, Fluent Builder, C1 Monitor & Query-Set DAG (`@hyper-hyper-space/hhs3_rdb_adapter` & `@hyper-hyper-space/hhs3_rdb_projection`)
- **Files:**
  - `modules/rdb_adapter/src/where_parser.ts`: Parse predicate strings + parameter maps (`ParamValue | ColumnSetRef`) into `BoundWhereExpr` templates.
  - `modules/rdb_adapter/src/query_builder.ts`: `TableQuery`, `ColumnSetRef`, `.selectFrom()`, `.select()`, `.where()`, `.orderBy()`, `.before()`, `.after()`, `.limit()`, `.col()`, canonical value fingerprinting.
  - `modules/rdb_adapter/src/query_monitor.ts`: `QueryEngine` / `QuerySource` over any `QueryableTarget & ReactiveChangeTarget`, refcounted single-table C1 monitor (narrow relevance filter from §4.2, anchor-row update vs. delete `cursorTuple` preservation, keyset far-edge rule, row & array identity preservation, `StoredOpEvent` log-id cursor advancement + `localId` matching from §6.2), and topological Query-Set DAG coordinator.
  - `modules/rdb_projection/src/projection.ts`: Expose `projection.selectFrom(table)` and `projection.monitor(queryOrSet, options)` delegating to the `QueryEngine` on `this.target`.
- **Tests (`modules/rdb_projection/test` & `modules/rdb_adapter/test`):**
  - Writes/deletes outside `WHERE` or on other tables run **zero** `executeQuery` calls.
  - Departure detection: updating or deleting a rendered row re-runs the query and removes the row.
  - Optimistic project-back convergence: a local insert followed by a matching `apply()` project-back does **not** replace the snapshot object and does **not** call listeners.
  - Parameter & cursor changes: changing `:id`, `.before(id)`, or `.after(id)` attaches a new monitor, clears `cachedRowIds`, and preserves refcounts under double-mount/unmount; passing a new array literal with equal elements reuses the same monitor.
  - Anchor mutation vs. deletion: updating the `.before(anchorId)` / `.after(anchorId)` row on a non-`id` sort order re-runs the page and refreshes `cursorTuple`; deleting the anchor row preserves the cached `cursorTuple` so subsequent commits still filter against the bound.
  - DAG independence, multi-hop/diamond DAGs, & simultaneous dirtying:
    - Updating a hydrated profile re-runs `profiles` only (`messages` array reference preserved).
    - A new message from an already-visible author re-runs `messages` only (`profiles` is not re-queried and its array reference is preserved).
    - A commit that both adds a message from an existing author **and** updates a visible profile re-runs **both** `messages` and `profiles` (unchanged key-set on the edge does not clear `profiles`'s own dirty bit).
  - Keyset pagination (`.before(id)` / `.after(id)`): a head insert does **not** re-query an older page; a backfill insert past a full page's last row is ignored; a delete inside a full page re-queries and pulls the next row in.
  - `StoredOpEvent` filtering & log-id cursor advance: a rejected local insert reverts the row and delivers the failure event matched via `localId`; an off-screen concurrency void advances `lastSeenEventId` and does **not** attach later if that row subsequently enters the page; `{ source: 'reconnect' }` refreshes rows while leaving `lastSeenEventId` intact.
  - Error handling: a failed initial query sets `loading: false` and `error`, resolving `whenReady()`.

### Layer 3: `SqliteTarget` Implementation & Outermost-Commit Poke (`@hyper-hyper-space/hhs3_rdb_adapter_sqlite`)
- **Files:**
  - `modules/rdb_adapter_sqlite/src/sqlite_target.ts`:
    - Implement `QueryableTarget.executeQuery` (compiling `SingleTableQueryPlan` + `before`/`after` lexicographic tuple seek to parameterized SQLite `SELECT`, registering `likeMatch` from `@hyper-hyper-space/hhs3_rdb` as a custom SQLite function instead of touching `PRAGMA case_sensitive_like`, and mapping rows via `toLogical`).
    - Implement `ReactiveChangeTarget` (`local` peek via `lastNotifiedOutboxId` with per-change `outboxId` and `change.localId` for `id`, pre-drain peek inside `drainChanges()`, `apply` post-images + `StoredOpEvent[]`, `settle` reverts + `StoredOpEvent[]`, and `notifyExternalCommit()` that deduplicates per-change `outboxId`s and fans out to listeners without writing rows).
    - Add `noteLocalCommit()` and a transaction-depth-aware `Database` wrapper that pokes **once after the outermost successful commit** (staying silent inside nested `db.transaction()` statements, on rollbacks, and during `apply()`).
- **Tests (`modules/rdb_adapter_sqlite/test`):**
  - Run the full Layer 2 reactive query & DAG test suite against `SqliteTarget`.
  - Verify that a wrapped SQLite write paints before `syncDatabase`, that a rolled-back `db.transaction()` emits no local wake, that interleaved `outboxId`s from multiple writers are deduplicated per change without dropping another writer's lower `outboxId`, and that `evalWhere` and `compareSortTuples` agree with SQLite `SELECT` on `NULL`, `likeMatch`, supplementary-plane Unicode strings, and `TEXT`-stored `bigint`/`decimal` ordering.

### Layer 4: `IdbTarget` Implementation & Payload-Carrying Poke (`@hyper-hyper-space/hhs3_rdb_adapter_idb`)
- **Files:**
  - `modules/rdb_adapter_idb/src/idb_target.ts` & `idb_facade.ts`:
    - Implement `QueryableTarget.executeQuery`: PK lookup on `ROWS`, composite index prefix + `id` range scan on `INDEX_ENTRIES` (`[table, indexName, ...key, id]` via `idb_index_keys.ts`), PK range scan on `ROWS`, and `ROWS` table scan fallback.
    - Implement `ReactiveChangeTarget` and update `FacadeTransaction` / `onOutboxCommitted` to pass the accumulated outbox records (with `outboxId`) directly to local commit listeners and over `BroadcastChannel` (preventing any race with `drainChanges`), plus `notifyExternalCommit()` for `BroadcastChannel` forwarding of `local`, `apply`, and `settle` commit events across tabs.
- **Tests (`modules/rdb_adapter_idb/test`):**
  - Run the full Layer 2 reactive query & DAG test suite against `IdbTarget`.
  - Verify that a same-tab `IdbFacade` write paints before `syncDatabase` even if `sync()` runs immediately, that a second tab receives `local`, `apply`, and `settle` commit events over `BroadcastChannel` without duplicating rows, and that an indexed equality + `id` cursor seek scans only the matching index prefix.

### Layer 5: Multi-Process Commit Streaming (`@hyper-hyper-space/hhs3_rhost` & `@hyper-hyper-space/hhs3_rhost_client*`)
- **Files:**
  - `modules/rhost_client/src/types.ts` & `protocol.ts`: Add `watchCommits` request, update `ServerMessage` push typing / `decodeServerMessage` so `watchCommits` pushes are typed separately from `HostStatus` pushes, and add `commits.watch(listener)` on `RhostClient`.
  - `modules/rhost/src/serve.ts` & `host.ts`: Subscribe `watchCommits` connections to the host projection target's `ReactiveChangeTarget` so `local` (pre-drain), `apply`, and `settle` `TargetCommitEvent`s stream over `run/rhost.sock` and `BroadcastChannel`, and clean up / end a watch when `connection.send()` throws so the client reconnects and catches up.
  - `modules/rhost_client_node/src/client.ts` & `modules/rhost_client_browser/src/client.ts`: Implement `commits.watch(listener)` over the socket / channel, emitting `{ source: 'reconnect' }` on connect/reconnect, and provide a helper to wire `client.commits` into a client-side `SqliteTarget.notifyExternalCommit` / `IdbTarget.notifyExternalCommit`.
- **Tests (`modules/rhost_client_node/test` & `modules/rhost_client_browser/test`):**
  - Multi-observer & interleaved-writer test: run `rhost` in one context and attach two separate observer clients (Process B and Process C). Verify:
    - When Process B writes id 10 and Process C writes id 12 before the host drains, each sees its own write in <1 frame, and when the host's pre-drain peek broadcasts `[10, 12]`, Process C applies 10 (without re-applying 12) and Process B applies 12 (without re-applying 10).
    - Both clients receive remote `apply` and `settle` commits over `watchCommits`.
    - Dropping the socket or restarting the host closes the watch cleanly, triggers `{ source: 'reconnect' }` on reconnect, refreshes active client monitors, and leaves `lastSeenEventId` intact.

### Layer 6: React Bindings Package (`@hyper-hyper-space/hhs3_react`)
- **Files:**
  - `modules/react/package.json`, `tsconfig.json`
  - `modules/react/src/context.ts`: `ProjectionProvider` (accepting any `QuerySource`—an in-process `RdbProjection` or a client-side target) and `useProjection()`.
  - `modules/react/src/use_query.ts`: `useQuery(queryOrQuerySet)` keyed by canonical value fingerprint over `useSyncExternalStore(sub.subscribe, sub.getSnapshot)`.
- **Tests (`modules/react/test`):**
  - React StrictMode double-mount/unmount refcount safety and re-render stability when inline query builders / param arrays are recreated with identical values.
  - Optimistic write $\to$ immediate render $\to$ zero extra render on matching project-back $\to$ revert + `OpEvent` delivery on rejected write.
  - Zero re-renders and zero DB queries under heavy background RDb traffic to unrelated rows/tables.
  - Single-render atomic updates and preserved child array references for `{ messages, profiles }` Query-Set DAGs.
