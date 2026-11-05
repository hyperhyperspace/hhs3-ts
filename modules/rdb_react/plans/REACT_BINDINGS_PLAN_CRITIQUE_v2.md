# Critique of the Reactive Query Engine & UI Bindings Plan, v2

A review of [REACT_BINDINGS_PLAN.md](./REACT_BINDINGS_PLAN.md) after it absorbed [REACT_BINDINGS_PLAN_CRITIQUE.md](./REACT_BINDINGS_PLAN_CRITIQUE.md). The architecture and the layer order still stand. What is left is the page language, and soundness inside the rules that critique already put in.

## Verdict

Keep the three commit sources, `cachedRowIds`, the query-set DAG, the cached snapshot, and the split that puts `evalWhere` in `rdb_adapter` and the string parser in `rdb_projection`.

Six changes belong in the plan before implementation.

| Item | Where in the plan |
| --- | --- |
| 1. `BEFORE` and `AFTER` replace `OFFSET` | §4.2, §5.2, §6.1 |
| 2. A change to the anchor re-runs the page | §4.1 |
| 3. Failure events use `localId`, and the cursor moves past dropped events | §6.2 |
| 4. The poke carries the commit, and SQLite pokes once | §3.2 |
| 5. One comparator, and `evalWhere` false means the target would exclude the row | §4.1, §5.1, layer 3 |
| 6. Key-set stability and `useQuery` | §6.2, layer 5 |

An ignore is still a proof that this commit cannot change the result. The query result is still the snapshot.

---

## 1. `BEFORE` and `AFTER` Replace `OFFSET`

§4.2 has one method, `.before(id)`, and the comparison flips with `orderBy`. `ORDER BY id DESC` seeks `id < 1000`. `ORDER BY id ASC` seeks `id > 1000`. That second spelling is `AFTER`.

`OFFSET` is out of the language. A page is one of these, plus `LIMIT`:

- `BEFORE id` is the lesser side of the order tuple.
- `AFTER id` is the greater side of the order tuple.

The id is exclusive. A descending page uses `BEFORE`. An ascending page uses `AFTER`. Those two are the replacements for `OFFSET`: `limit` then keeps the rows next to the anchor.

```ts
// newest page
.orderBy("id", "desc").limit(50)

// the next older page
.orderBy("id", "desc").before(lastId).limit(50)   // id < lastId

// the next page of an ascending list
.orderBy("id", "asc").after(lastId).limit(50)     // id > lastId
```

`DESC` with `AFTER`, and `ASC` with `BEFORE`, are the other side of the same order. A query carries one bound. Both bounds at once would be a window between two anchors; the builder rejects that until something needs it. The live tail is the query with neither.

The cursor argument stays a row `id`. It is stable and it is not reused. `ORDER BY` may name other columns. `BEFORE 1000` on `ORDER BY created_at DESC` means rows whose `(created_at, id)` is lesser than row 1000's tuple. The engine reads `created_at` with one primary-key lookup. The caller does not hold `created_at`.

`id` is appended as the last key, in the same direction as the column it breaks, so two rows with the same `created_at` have one order. An `id` order can compare the number after that row is deleted. Any other order keeps the anchor's values on the monitor (`cursorTuple`) for that case.

`CursorBound` records which side, not only an id. The fingerprint includes that side and the id.

---

## 2. A Change to the Anchor Re-Runs the Page

§4.1 rules 3 and 4 compare a change with `cursorTuple` and `lastRowTuple` from the last delivery. Those tuples are the window, and they stay true only while the anchor's sort key stays put.

`BEFORE` / `AFTER` do not put the anchor id in `cachedRowIds`. A remote `apply` of that row misses the cached-id rule, and a delete of it misses the delete rule. For `ORDER BY id` the bound is the id itself, so this is harmless. For `ORDER BY created_at, id` it is a wrong page.

If the anchor's `created_at` moves toward the head, the window expands over rows that are already stored. The only event is the anchor write. Rule 3 ignores it, `cursorTuple` stays at the old key, and a later insert that sits between the old key and the new one is ignored too: against the stale tuple it still looks like the head side.

A change whose `localId` is the anchor re-runs the monitor and refreshes `cursorTuple`. When that commit also contains other changes, those changes do not get to ignore against the old tuple. One query after the commit sees the new bound. A commit is dirty if any change in it is dirty, and the query runs once, against the database after the commit.

---

## 3. Failure Events Use `localId`

§6.2 attaches an ingestion failure when the attempted values in `event.op` pass `evalWhere`. `event.op` is the bundle payload from `recordFailure`:

```ts
{ action: 'bundle', writes: [{ table: rdbTable, op: write.op }] }
```

That image uses RDb names, `rowId`s, and foreign keys as hashes. `evalWhere` wants target columns and integer ids. A hash does not equal `channel_id = 33`.

The screen's case does not need that image. The optimistic row was in the previous snapshot, the settle re-runs the table, and the failure attaches because `event.localId` was in `cachedRowIds`. Keep that clause. Drop the `event.op` clause. A target-vocabulary image can be added later, at the moment the failure is recorded, if a failure must surface for a row that was never delivered.

The per-monitor event cursor advances past events that were not attached. An off-screen void from an earlier commit does not attach when that row later enters the page.

---

## 4. The Poke Carries the Commit

§3.2 tells `onOutboxCommitted` to call `notifyLocal()` and then read `rdb_outbox`. IndexedDB `getAll` yields, and `drainChanges` can delete those rows before the read returns. The writing transaction already has the records: `queueOutbox` in `idb_facade.ts` built them. Accumulate that list on the facade transaction and hand it to the commit listeners from `oncomplete`. Put the same list on the `BroadcastChannel` message. The other tab has the same race, and a payload survives the writer draining the outbox. The query is still the snapshot. The payload is only the wake.

On SQLite the peek stays synchronous, and it uses the logical values `drainChanges` already builds (`toLogical`). The wrapper calls `noteLocalCommit()` once, after the outermost successful commit. A `Statement#run` inside `db.transaction()`, and each statement inside `exec`, are not that moment. A poke per statement runs listeners while the transaction is open. A rollback then leaves the screen on rows that are gone, and no settle follows, because the outbox insert rolled back with them. `apply()` may share the wrapped handle. It stays quiet because it does not grow the outbox, which is the check `noteLocalCommit` already runs.

The insert trigger's JSON image is `columnTypes`, and the id column is not one of them. The sort tuple's `id` component is `change.localId`.

---

## 5. One Comparator, and a False `evalWhere` Is a Proof

The same comparison orders `MemoryTarget`, the SQLite `ORDER BY`, the IndexedDB range, and rules 3 and 4. `NULL` sorts the way SQLite sorts it: smaller than every value. A query that uses `LIMIT`, `BEFORE`, or `AFTER` has that total order even when the caller wrote no `orderBy`. The default is `id ASC`.

`toLogical` drops SQL `NULL`, so a local insert's image omits those columns. `evalWhere` treats a missing key as `NULL`.

Memory and IndexedDB execute `evalWhere`, so they agree with the filter. SQLite executes real SQL. An ignore is sound only when `evalWhere` false means SQLite would exclude the row too. `LIKE` is the sharp edge: SQLite's default is ASCII case-insensitive. The layer 3 comparison covers `LIKE` and ordinary comparisons, along with `NULL`.

On IndexedDB, an equality on the indexed columns plus a bound on `id` is one range. Every index entry is already `[table, name, key, id]`. A channel page ordered by `id` seeks inside the `channel_id` prefix. A scan of every `id` below the cursor, filtered by `channel_id` afterwards, is the slow plan `BEFORE` / `AFTER` exist to avoid. Any order that does not match an index prefix still scans, as §5.3 says.

---

## 6. Key-Set Stability and `useQuery`

§6.2 says a downstream query is not marked dirty when the extracted key set is unchanged. That suppresses the dependency edge only. A profile update in the same commit still dirties `profiles` on its own. Clearing the dirty bit because the author set was stable drops that update.

`useQuery` subscribes by fingerprint, not by the `TableQuery` object the component built during render. Literal parameters are in the fingerprint by value. A new array with the same ids is the same monitor. Switching fingerprint releases the previous monitor. Strict-mode double-mount still only refcounts.
