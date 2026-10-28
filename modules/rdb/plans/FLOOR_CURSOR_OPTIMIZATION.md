# Making the floor cheap

**Status:** landscape. The floor is the one in [`FLOOR_CURSOR.md`](./FLOOR_CURSOR.md). Nothing here changes its definition or the cursor rules. A faster query that returns a different cut is a bug.

The level index ([`dag/src/idx/level/level_idx.ts`](../dag/src/idx/level/level_idx.ts)) already makes one `findForkPosition` jump long chains. The default `levelFactor` is 64 ([`level_idx_mem_store.ts`](../dag/src/idx/level/level_idx_mem_store.ts)): an entry's level is how many times its distance to the nearest root is divisible by that factor, and level `i+1` has an edge wherever level `≤ i` has a path. Fork finding projects both sides onto the next level, solves there, and stitches back ([`findForkPositionAtLevel`](../dag/src/idx/level/level_idx.ts)). The floor is slow when it does that once per barrier, and once per member of `fork.common`.

Two costs survive any index. The ingest of the final walked region loads every entry the delta reports; if the floor sits at the root, that is the full delta, which is the bound in §6 of [`RDB_OBSERVE_FIX_PLAN.md`](./RDB_OBSERVE_FIX_PLAN.md). And each observed object runs the same floor between its own two versions. Skipping the foreign accumulator (`computeRevisionBound` in that plan) removes the foreign ingest, not the foreign geometry.

---

## 1. Where the time goes

[`FLOOR_CURSOR.md`](./FLOOR_CURSOR.md) §2, against the primitives that exist today:

| Step | Primitive | Scales with |
| :--- | :--- | :--- |
| Fork of `(start, end)` | `findForkPosition`, level-indexed | one fork |
| Meet of `fork.common` | [`computeForkMeet`](../mvt/src/delta.ts) folds [`computeMeet`](../dag/src/dag_meet.ts) | one fork per member of `common` |
| Unstable observes | [`projectForeignBound`](../mvt/src/refs.ts), and `refVersionAtOrBelow` per observe it touches | one foreign fork per tested observe, then a foreign floor |
| Each barrier in the walked region | `findForkPosition(floor, preds(Z))` | one fork per barrier per round |
| Lowering | another `computeMeet` over the violators' predecessors | one fork per violator |
| Listing the region | [`walkEntriesBackwardsToBound`](../mvt/src/delta.ts), level-0 predecessors | every entry from `end` down to the current floor |

`commonFrontier` of `(start, end)` is the shared frontier, not the meet of `common`. In the local history the frontier is `{s1, T}` and the meet is `{s1}`. Using the frontier as the floor leaves `T` unwalked.

`computeMeet` already notes that a native k-ary meet would remove the fold. Dominated members of `common` (entries after some other member) do not move the result, but the fold still pays for them. Reducing `common` with `findMinimalCover` first keeps the latest members and drops the earliest, which is the wrong end: the earliest members are the ones that pull the meet toward the origin.

`findCoverWithMetaUsingLevelIndex` does not use the higher levels. It loads every entry and walks `getPreds(0)`. The barrier tag is not a key in the level index. Higher-level edges are reachability shortcuts, and a barrier can sit on the path a shortcut jumps.

`reachabilityAtLevel(start, target)` walks predecessors. It returns the targets that lie in the past of `start`. It does not answer "which targets are after every head of `start`."

---

## 2. With the primitives that exist

### One walk per round, one meet at the end of it

The plan's batching note (§5) is the right use of today's API. A round has a fixed floor. Walk back from `end` to that floor once, and record the successor lists the walk already discovers. That is the forward map in [`concurrent_cover.ts`](../dag/src/idx/concurrent_cover.ts): while scanning predecessors of the region, each predecessor remembers the children that pointed at it, and a count from each floor head marks the entries reached by every head.

A barrier in the region is a violator when that count is short of the number of floor heads. `preds` of every violator go into one `computeMeet` with the current floor. The next round walks only the entries that left the floor. A barrier that was after every head stays after every lower floor, so it is not tested again.

The last region is the one the accumulator ingests. Entries below the final floor are never loaded. Entries above it are loaded once for the round that settled, and that load is the ingest.

This removes the per-barrier fork. The remaining fork work inside a round is the pairwise fold of the violators, and the pairwise fold that built the meet.

### Shrink the fold before paying it

Before `computeMeet`, drop any input position that is after some other input position. One `reachabilityAtLevel` from the whole set, with the set as the target, names the members that are in the past of other members. Keep those, drop the rest, then fold. The local history's `common` is already a singleton, so this saves nothing there. It pays when a wide `end` hangs off many attachments that are totally ordered.

### The cursor path is already a small round

A warm `(cursor, floor)` pair lowers only for the new entry and for the entries the lowering exposes ([`FLOOR_CURSOR.md`](./FLOOR_CURSOR.md) §3). That exposed set is the round above. No new primitive is required to make the warm path cheaper than a cold `computeDelta`. The cold path, a cursor that has fallen far behind the head, is where the folds and the barrier tests dominate.

### Foreign stability

`projectForeignBound` already stops at the first stable observe on each branch, so it does not test every observe in the past of the meet. Each test is one `findForkPosition` on the foreign DAG (`refVersionAtOrBelow`). The expensive call is the foreign floor itself, `o.computeDelta(refAtMeet, refAtEnd).revisionBound`, which repeats this whole procedure and then ingests. A `revisionBound` that skips the accumulator, as the fix plan describes, is the cut available without any new geometry.

---

## 3. New queries on the same index

Both queries are the k-sided form of a recursion the level index already runs for two sides. Neither stores new edges. `addToLevelIndex` already records predecessors and successors at every level.

### `meetMany(positions) → Position`

The greatest lower bound of k positions: the frontier of the intersection of their histories. `computeMeet` becomes one call. `computeForkMeet` passes the members of `common`. A round's lowering passes the current floor and `preds` of each violator.

Shape, following `findForkPositionAtLevel`:

- Project every position into the next level (`projectIntoNextLevel`).
- Recurse. The result is the common frontier one level up.
- At this level, a node is in the intersection when every input position reaches it. Count positions, not heads: two heads of one position are one side.
- The frontier is the intersection members with no successor in the intersection. Stitching them down is the same stitch the two-sided fork already does (`common`, `commonFrontier`, `forkSiblings`).

Disconnected inputs meet to the empty position, and the empty position stays empty under further meets, matching `computeMeet`.

A chain of length n is a recursion of depth about `log_64 n`, touching the highway nodes rather than the chain. A wide frontier still costs its width at the top level. The index removes depth, not branching.

### `afterAll(heads, candidates) → candidates`

The candidates that are strictly after every head. A barrier `Z` violates the current floor iff `Z` is absent from `afterAll(floor, candidates)`.

This is the forward dual of `reachabilityAtLevel`, with an intersection across heads instead of a union:

- Project the heads forward (`projectForwardIntoNextLevel`, `getSuccs`).
- Project the candidates up through their predecessors, so a candidate that sits mid-chain is represented by the highway nodes that can reach it. Traversing only the level-`i+1` edges, and testing only those endpoints, misses a barrier on the skipped path.
- Recurse. At this level, walk `getSuccs` from the heads along nodes of this level, inside the topo window the reachability code already uses (`topoIndex` increases as entries are indexed after their predecessors).
- A candidate is returned when every head reaches it.

`topoIndex` is an insertion order consistent with causality, not a causal interval. The window from the earliest floor head to the latest `end` head over-includes other branches indexed in between. It is a safe superset filter, and `afterAll` still has to test reachability.

One `afterAll` plus one `meetMany` is a round. The local history is one round: `afterAll({s1}, {T, N})` misses `T`, and `meetMany({s1}, preds(T))` is `{x}`. History B is the observe lowering, then one round for `Y`.

---

## 4. A stored barrier list

A side structure of `(topoIndex, hash)` for entries carrying the barrier tag would name the candidates in the topo window without loading non-barriers. It is worth building only after the rounds above are in place, and only if a profile shows the probe loads matter.

On the settled round the walked entries are ingested anyway, so the list does not reduce the loads of a delta whose floor equals the meet: that walk is the answer. It saves loads only for entries that a probe visits and the final floor then puts back below the cut. Batching makes that probe the same walk as the ingest, stopped at the floor. Entries below the final floor are not visited.

The list does not change `afterAll`. It only feeds it a shorter `candidates` set.

---

## 5. Keeping the region small

Index work cannot shrink a floor the definition puts at the root. §6 of the fix plan is the lever that changes the size of the region: condition (2) applied only to barriers that actually changed, rather than to every barrier outside the floor. An old side branch that this cursor does not touch stays inside the floor, and the walk never enters it. That is a smaller `candidates` set for `afterAll` and a smaller ingest. It is independent of the level index, and it is the optimization that matters once a single round is cheap and the floor is still low.

Relevance (a delete only pulls entries that read its table) shrinks `candidates` further. It needs the schema at the entry, which the fix plan defers. It composes with `afterAll`: the candidate set gets smaller, the query does not change.

---

## 6. Order

1. The batch round of §2 on the existing fork and the existing forward map. The cursor path uses that round on its exposed entries. This is the procedure to measure.
2. `revisionBound` without the foreign accumulator, so condition (3) stops paying for a foreign ingest.
3. `meetMany`, when the fold over `common` or over violators shows up in that measurement.
4. `afterAll`, when the forward map's level-0 walk of a deep region shows up. It replaces the map, not the round.
5. The exact change set from the fix plan's §6, when the floor is cheap to compute and still sits near the root on histories that should have stayed high.
6. A barrier posting list only if, after (1)–(4), probe loads of entries the delta does not ingest are still visible.

The cursor cache stays a pair beside the checkpoint. These queries compute the same floor the batch procedure computes. They do not give it a second definition.
