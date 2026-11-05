# Critique of the Reactive Query Engine & UI Bindings Plan

A review of [REACT_BINDINGS_PLAN.md](./REACT_BINDINGS_PLAN.md) against the projection, the three targets, and [REACT_BINDINGS.md](./REACT_BINDINGS.md). The architecture stands. Five changes belong in the plan before implementation.

## Verdict

Build it in the order the plan gives: memory target, monitor and query-set DAG, SQLite parity, IndexedDB parity, then a thin React hook. Keep the three commit sources in target vocabulary, `cachedRowIds` for departures, single-table queries composed by a DAG, and `useSyncExternalStore` over a cached snapshot.

Change the relevance filter, the page cursor, the local wake, the subscription contract, and where the predicate parser lives.

| Item | Where in the plan |
| --- | --- |
| 1. Narrow the relevance filter | §4 |
| 2. A page seeks from a row id | §5, §6 |
| 3. A post-commit poke for the local wake | §3 |
| 4. The subscription contract | §6.2, §6.3, layer 5 |
| 5. Parser and evaluator placement | §5, layer 1, layer 4 |

The delivered rows stay whatever `executeQuery` returns, compared with the last delivery. An ignore is a proof that this commit cannot change the result. Anything short of a proof re-runs the query.

---

## 1. Narrow the Relevance Filter

§4 decides dirtiness with a partial `WHERE` evaluator, a `fullRow` flag, and a special case for `LIMIT` plus `orderColumns`. That machinery is aimed at the quiet stream.

Remote traffic arrives as `source: 'apply'`. An upsert there is a full post-image (`fullRow: true`); a delete carries a `localId`. Membership is `evalWhere` on the whole row, or `cachedRowIds.has(localId)`. That is the busy path in the problem statement, and it is sound: a row the screen is showing that fails the new predicate is a departure, and a row the screen is not showing that fails the predicate cannot enter.

Local updates are a different record. SQLite writes one outbox row per changed column:

```sql
-- sqlite_target.ts, installTriggers
AFTER UPDATE OF "col" ON "table" BEGIN
  INSERT INTO rdb_outbox ("table", local_id, op, changed)
  SELECT ..., 'update', json_object('col', NEW."col")
  WHERE <capture gate>;
END
```

§4 is written as if one `update` carried every changed column. A partial evaluator then has to be three-valued: only a proven `false` may ignore the change, and `unknown` must dirty the monitor. `OR`, `NULL`, `LIMIT`, and an order column missing from the fragment all fall out of that, which is what the note under the table is already hedging. Those writes are the user's own keystrokes.

Use this filter:

| Commit | Rule |
| --- | --- |
| `apply` insert, or `apply` update with a full post-image | Ignore when `evalWhere` is false and the id is not cached. Otherwise re-run. A cached id whose new image fails the predicate re-runs. |
| `apply` or local delete | Re-run only when the id is cached. |
| Local insert | Same as a full-row insert. The outbox insert stores the full new row. |
| Local update | Re-run every monitor of that table. |
| `settle`, or `apply` schema | Re-run the named tables, as §3 and §4 already say. |

A successful local edit still queries twice: once on the outbox wake, once on project-back. The second delivery is swallowed when the canonical row matches the optimistic one. That is the success criterion. Holding the optimistic row past the second read is how the screen drifts, which [REACT_BINDINGS.md](./REACT_BINDINGS.md) already states.

Folding per-column outbox rows into one `TargetRowChange` is optional under this rule. It becomes required only if local updates later grow a precise filter. If they do, `unknown` is dirty, and an ignore of one column must not drop a sibling column of the same commit.

---

## 2. A Page Seeks from a Row Id

§6.1 has `limit` and no cursor. A numeric `OFFSET` is the wrong addition, for the query and for the filter.

SQLite still walks the skipped rows. IndexedDB `cursor.advance(n)` does the same, and every deeper page starts again at the beginning of the range. The filter is worse: a page written as `WHERE channel_id = 33` plus a later slice still sees a new head insert as a matching row that might displace `LIMIT`, so §4 marks every open page dirty.

A page is a seek to a row in the ordered result, compiled into the predicate the filter evaluates. The builder method is `.before(id)`. It never becomes SQL `OFFSET`.

- The id is exclusive. The direction is the query's `orderBy`. `ORDER BY id DESC` seeks to rows before that id; `ASC` seeks to rows after it. The next page starts after the last row of the previous one, so two mounted pages do not repeat a row.
- `id` alone is the position only when the order is `id`. `id` is the projection-local serial, stable for a `rowId`, and not reused. For `ORDER BY created_at` the position of a row is `(created_at, id)`. `id` is the tiebreak that makes the seek unique. The method still takes a row id: execution loads that row with one primary-key get and builds the tuple. If that row has been deleted, an `id` order can still compare against the bare id. Any other order needs the sort values from the last snapshot, which the page already has.
- The live tail is the query with no cursor. History pages are the ones with `.before(id)`.

`SingleTableQueryPlan` carries that bound. Each target turns it into a range. IndexedDB already stores `id` as the last component of every index entry (`[table, name, key, id]` in `idb_index_keys.ts`), so "the next 50 rows of channel 33 after id 1000" is one range on the channel index. SQLite uses the primary key: `WHERE channel_id = 33 AND id < 1000 ORDER BY id DESC LIMIT 50`, and stops after 50 hits. `MemoryTarget` seeks from that id.

When the page is full (`result.length === limit`), ignore a matching insert that sorts strictly past the last cached row. A delete inside the page still re-queries and pulls the next row in. Without that far-edge rule, backfill below the window matches `id < cursor` and wakes the page anyway.

---

## 3. A Post-Commit Poke for the Local Wake

§3.3 says a local commit wakes monitors on the next frame, before the projection's 50ms ingest debounce. The outbox already meets the capture half of that. The wake does not, except on `MemoryTarget`.

The triggers run inside the app's statement. When that statement returns, `rdb_outbox` has the new rows and a `SELECT` sees the new table state. `SqliteTarget` never sees the call. The app writes the shared `better-sqlite3` handle directly, that library exposes no commit hook, and the target notices afterward:

- a file-backed database watches the WAL (`fs.watch` on `${dbPath}-wal` in `sqlite_target.ts`);
- `:memory:`, a missing `dbPath`, or an unreliable watch polls `MAX(id)` every `pollMs`, default 250.

The poll is slower than the ingest debounce the UI is trying to beat. The WAL watch is usually a few milliseconds, and it stays a hint: it coalesces, and a write that lands before the watcher is registered can miss it. The code comments document that window.

IndexedDB has the same split. `queueOutbox` inserts the outbox row in the app transaction. `FacadeTransaction`'s `oncomplete` already runs in the writing tab after commit, and it calls `IdbTarget.onOutboxCommitted`. That method only `postMessage`s a `BroadcastChannel`. The sending tab does not receive its own message, so the tab that just wrote is the one that waits.

`MemoryTarget.record` is the shape the others want: it appends the outbox entry and calls listeners before returning.

Keep the triggers. They are what captures a raw write, including one from a connection that forgot to notify. Add a post-commit poke of the check that already exists (`fireIfOutboxGrew` on SQLite, `notifyLocal` on IndexedDB):

- **IndexedDB, same tab.** Call `notifyLocal()` from `onOutboxCommitted`, and still post the channel for other tabs. The listener peeks `id > lastNotifiedOutboxId`. The rows are durable. The peek has to run in that turn, before `drainChanges` deletes the rows. A `sync()` that drains first loses the optimistic frame until `apply`.
- **SQLite, same process.** Wrap the shared `Database`. After a successful `run` or `transaction`, call `target.noteLocalCommit()`, which runs the existing `MAX(id)` check. Better-sqlite3 commits synchronously, so this is the commit hook the library does not expose. A rolled-back transaction leaves `MAX(id)` unchanged, and the poke stays quiet.
- **Any other writer** — a second connection, another process — keeps the WAL watch or the poll as the backstop. The outbox cursor makes a duplicate wake harmless.

A user-defined function inside the trigger is the wrong notifier. It runs before commit, so a rollback would announce a write that no longer exists, and every other connection would have to register the same function or its writes would fail. The trigger stays pure SQL.

The screen still paints by re-running the query after the poke. The outbox is the record of what changed. The poke only shortens the time until a monitor reads it.

---

## 4. The Subscription Contract

§6.2 and §6.3 under-specify the object React actually subscribes to.

**`getSnapshot` returns the cached snapshot.** `useSyncExternalStore` loops when every call allocates a new object. Replace the snapshot only when a delivery happens. Keep the previous row objects when a row is shallow-equal, and keep the previous array when a query in a set did not re-run. A messages update that leaves the author key set unchanged delivers a new messages array and the same profiles array.

**Monitors are refcounted by fingerprint.** React strict mode mounts twice. `destroy()` on one consumer must leave a monitor another component still holds. The fingerprint includes bound literal parameters and excludes the live key set behind a `ColumnSetRef`. Otherwise two channels share a monitor, or a profile query changes identity every time the message page changes. A parameter change is a new monitor, an immediate run, and a cleared `cachedRowIds`.

**`subscribe` has one job.** It registers a listener and returns an unsubscribe. React's listener is a zero-arg "the store changed"; React reads `getSnapshot` itself. A Svelte store calls the listener with the current value during `subscribe`. Pass an adapter into `useSyncExternalStore`. The other frameworks wrap the same pair later.

**Events on the snapshot are the ones that concern this result.** Take the rule already in [REACT_BINDINGS.md](./REACT_BINDINGS.md). Attach an ingestion failure when the reverted row was in the previous result, or the attempted values in `op` match this predicate. Attach a void or reinstate when `localId` is in the previous or the next result. Other events for the same table stay off the snapshot. "Relevant events" in §6.2 needs that sentence, or a busy table delivers a void for an off-screen row and the listener runs with unchanged rows.

---

## 5. Parser and Evaluator Placement

Layer 1 puts the C-SQL predicate parser in `rdb_adapter` and says it extends `rdb_lang`. The adapter does not depend on `rdb_lang` today, and it should stay that way. The adapter is the storage port. The language package is a frontend.

`rdb_lang`'s `PredicateExpr` has comparisons, `LIKE`, `AND`, `OR`, `NOT`, arithmetic, and `length`. It has no `IN` and no `IS NULL`, and it does have `EXISTS`, which this builder refuses. "Reuse the grammar plus `IN :param`" is a new predicate parser either way.

Put the bound predicate AST and `evalWhere` next to the targets that execute them (`rdb_adapter`). Put the string parser next to the builder (`rdb_projection`). `evalWhere` on a full row agrees with the target's `WHERE`: SQL drops unknown, so a `NULL` comparison does not match. The filter's ignore path uses that same function on a full post-image. There is no `evalWherePartial` until a local update grows a precise filter (section 1).

On IndexedDB, primary-key lookup and a table scan are enough to land the engine. A keyset bound (section 2) is a range on `ROWS` or on `INDEX_ENTRIES`, using the bounds `idb_index_keys.ts` already defines. Compiling an arbitrary predicate into an index range is a separate planner. A wrong range is a wrong screen, and it can wait.

---

## Tests the plan should add

Layer 2 already proves "writes outside the `WHERE` run zero queries," departures, the DAG key-set check, and a rejected insert. Add:

- A project-back whose values match the optimistic row does not replace the snapshot object and does not call listeners.
- Changing `:id` builds a new monitor and drops `cachedRowIds`.
- An update to a hydrated profile re-runs profiles only. A new message from an author already on screen re-runs messages only.
- A same-tab IndexedDB write paints before `syncDatabase`. A SQLite write through the wrapped handle does the same.
- A head insert does not re-query an older `.before(id)` page. A delete inside a full page does, and the next row appears.
- `evalWhere` treats `NULL` the way the target's `WHERE` does.
