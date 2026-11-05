# Critique of the Reactive Query Engine & UI Bindings Plan, v3

A review of [REACT_BINDINGS_PLAN.md](./REACT_BINDINGS_PLAN.md) after it absorbed [REACT_BINDINGS_PLAN_CRITIQUE_v2.md](./REACT_BINDINGS_PLAN_CRITIQUE_v2.md). The plan is ready to implement. What is left is where the new rules meet code that already has an opinion: `LIKE`, sort order, the event log id, and one sentence about which bound `LIMIT` sits next to.

## Verdict

Keep the plan. Lock these five so an ignore stays a proof.

| Item | Where in the plan |
| --- | --- |
| 1. `LIKE` is `likeMatch` | §5.3 |
| 2. The comparator is the `SELECT`'s order | §5.1, §4.2 |
| 3. A deleted anchor keeps `cursorTuple` | §4.1, §4.2 |
| 4. The event cursor needs a log id | §6.2, §3.1 |
| 5. `LIMIT` sits next to the anchor only when the bound faces the sort | §4.1 |

A failed first query sets `loading: false` and `error`. Otherwise `whenReady` never resolves.

---

## 1. `LIKE` Is `likeMatch`

§5.3 says `executeQuery` may compile `LIKE` with a registered helper, or with `PRAGMA case_sensitive_like`. The pragma is the wrong one.

C-SQL's matcher is `likeMatch` in `modules/rdb/src/rschema/expr.ts`. It is case-sensitive, it matches code points, `%` is any run, `_` is one code point, and `\` makes the next code point literal. SQLite's `LIKE` is ASCII case-insensitive, and it has no escape unless the statement names one. `PRAGMA case_sensitive_like` only flips that ASCII case, and it flips it for every statement on the shared handle, including the app's own SQL.

`evalWhere` and the generated `SELECT` both call `likeMatch`. A false result is then a proof that the select would exclude the row. The function is registered on the connection for the statement the planner emits. The pragma stays unset.

---

## 2. The Comparator Is the `SELECT`'s Order

§5.1 asks for one comparison shared by the monitor, `MemoryTarget`, SQLite `ORDER BY`, and the IndexedDB scan. Two existing orders will not do that job.

`orderRows` in `modules/rdb/src/rtable/query.ts` sorts a missing value last in both directions, and `compareNumericStr` orders `bigint` and `decimal` by value. The projection does neither. SQLite stores `bigint`, `decimal`, and `bytes` as `TEXT` (`sqliteType` in `sqlite_target.ts`), so a native `ORDER BY` on those columns is lexical: `"9"` sorts after `"10"`. SQLite orders `NULL` as smaller than every other value, so `ASC` puts nulls first and `DESC` puts them last.

The far-edge ignore is sound only when it uses the order `executeQuery` actually returns. Native `ORDER BY` keeps the index. A JavaScript `<`, or a copy of `orderRows`, will ignore a row the select would return. If a screen needs numeric order for a `decimal` column, both the SQL and the monitor compare numerically, and the text index does not serve that order.

The side test and the far-edge test in §4.2 run only when `cursorTuple` or `lastRowTuple` is already known. A missing tuple is not an ignore.

---

## 3. A Deleted Anchor Keeps `cursorTuple`

§4.1 caches `cursorTuple` so a deleted anchor still has a bound. §4.2 tells the anchor rule to re-run and refresh that tuple. A refresh that reads the row and finds it gone will clear the cache, and the next commit has nothing to test rule 3 against.

On a delete of `cursor.id`, keep the previous tuple. On an update that leaves the row in place, replace the tuple with the new sort key. The re-query uses `cachedValues` when the primary-key lookup misses, which §5.2 already says.

---

## 4. The Event Cursor Needs a Log Id

§6.2 advances `lastSeenEventId` past every inspected event, including ones that were not attached. `TargetCommitEvent.events` is `OpEvent[]`. That type has no log id. `StoredOpEvent` is the stored pair, `{ id, event }`, and the id is the cursor `opEvents` already pages on.

Emit `StoredOpEvent[]` from `apply` and `settle`. The monitor's cursor is that id. Without it, "advance past" has nothing ordered to advance, and an off-screen void can attach later when the row enters the page. `(opHash, direction)` works as a seen-set if a commit ever carries an event before it has a log id. The log id is the cursor once the row is stored.

---

## 5. `LIMIT` Sits Next to the Anchor When the Bound Faces the Sort

§4.1 defines `BEFORE` as `tuple < anchor` and `AFTER` as `tuple > anchor`. `LIMIT` then keeps the rows next to the anchor only for the pair that faces the sort:

- `ORDER BY id DESC` with `BEFORE` keeps the greatest ids below the anchor.
- `ORDER BY id ASC` with `AFTER` keeps the least ids above the anchor.

`ORDER BY id DESC` with `AFTER` is the greater side, and `LIMIT` keeps the far end of that side: the largest ids above the anchor, not the neighbors of the anchor. The same is true of `ASC` with `BEFORE`. That query is legal. It is not the next page. The plan should say so next to the two inequalities, so a descending history page stays `BEFORE`.
