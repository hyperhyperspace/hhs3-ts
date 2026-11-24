# Response to the fifth critique of HIP-29

A reply to [the fifth critique](0029-checkpoint-consistency.critique-5.md) of [HIP-29](0029-checkpoint-consistency.md) and of its plan, `plans/checkpoint-consistency.md`. HIP-29 and the plan were revised on 2026-11-22, and so was [HIP-20](0020-unified-write-api.md) where HIP-29 changes what it describes. Paths are relative to `modules/`.

Decisions 21 to 23 are taken as they were made. Decision 20 was revised in discussion, and the four open points were decided (below). The code the critique cites was read again, and every claim checked holds (*Checked*).

| Critique | In the revised documents |
|---|---|
| §4. An IndexedDB `put` names nothing | Taken in part: `patch` as revised below, and `put` keeps capturing every column (*Capture*, *What changes for apps*, *Changes* 5, *Tests*, *Considered and not taken*). The plan's decision 2, detail 4, P3 and the flip of ADPTI-06 |
| §5. Every change ends with a record | Taken (*What became of each change*, *Reverts*, the settle's steps, *HIP-20*, *Changes* 3, *Tests*, *Why it is sound*). HIP-20's step 6 lands in the plan's P5. The plan's details 2, 6 and 7, IN71 to IN75, and the flips of ADPTI14 to ADPTI18 |
| §6. Every gated binding is compared again | Taken (*Writes void where they stand*, *The algorithm*, *Tests*). The plan's decision 8, detail 7, ENF24 and IN76 |
| §7. A projection without a writer key | Taken, with the refusal at open (*No projection over a pending change*, *What changes for apps*, *Changes* 6). The plan's decision 11, detail 8 and RDBPROJ19 |
| §8. What the records cost | The records stay in the outbox, and P4 measures them. The alternative is in HIP-29's second open question |
| §9. Smaller points | All taken. The pseudo-code's loop has its stop, the chain's end is a union, the deltas' condition reads the chain, a reinstated row is written by its row hash, and the end halts on a delta with a schema action |
| §10, §11 | Taken, as revised by the decisions below |
| *Outside this HIP* | Taken into HIP-29: dag joins its modules (*Changes* 1, the plan's decision 12, P1 and ENTRY_04) |

## Decisions taken in discussion

**Decision 20, revised.** `patch` takes the fields to write and, separately, a list of fields to clear. It leaves every other field as stored, and captures exactly the fields it names, changed or not. It compares nothing: comparing is for the libraries above it, as `compare` is for HIP-20's writer. So `patch` is IndexedDB's counterpart of a partial `UPDATE`, and "a named column is a write" holds for it as for SQL statements, memory's `localUpdate` and HIP-20's writer. As decided in the critique, `patch` compared the fields it was given, so a field it named at its current value wasn't a write.

**`put` doesn't compare.** A `put` keeps IndexedDB's meaning: it replaces the record, so it writes every column, and capture records every column. `patch` is the write for code that cares about distributed updates. The three costs of §4 stay with `put`, and *Capture* states the first two. The critique's recommendation is in *Considered and not taken*.

**The refusal at open.** Taken, with the cycle's error for a change captured later. Writers are set per group (`rdb_projection/src/scope.ts:126`), so the refusal is for a pending change of a group without a writer key. A reconfigure keeps the options given at open and only adds groups, which have no tables yet (`rdb_projection/src/projection.ts:496-503`), so a check at open is enough.

**The records stay in the outbox rows.** P4 measures a pass of 1,000 inserts on IndexedDB with and without them. If they are a large share of it, the plan reports before P5, since moving them into rdb's store is a design change.

**`Dag.append` writes nothing for an entry it already holds.** An entry's hash is the hash of its header, which holds the payload's hash and the parents' hashes (`dag/src/dag.ts:65-79`), so a second append with the same hash is an exact duplicate. The entry's meta isn't hashed, but rdb derives it from the payload. The check goes inside the append's transaction. Both of rdb's scoped DAGs append through `Dag.append` (`mvt/src/dag/dag_nesting.ts:60`, `:188`), so the stores don't change. HIP-29's ingestion then appends each recorded entry without looking first.

**HIP-20's *Changes* 1** is committed by hand before P0, once the plan settles.

## What follows from them

- **HIP-20's step 6 moves into HIP-29.** The native insert over an existing local id lands in P5 with the planner's drops (§5). After HIP-29 come HIP-20's Phases C to E. HIP-20's header, its *Changes* 1 item and its tests say so, and the change-source method that told a stale replay from a live insert goes.
- **A replayed insert whose local row is gone** was a HIP-20 test case. With no replays, the case goes with them.

## Checked

Read again on 2026-11-22. Nothing was run.
- **The planner's drops** are where §5 says (`rdb_adapter/src/ingest.ts:251-268`, `:461-467`, `:581`, `:592-596`). The refusal for a missing rdb table carries no kind (`:451-453`), so the orchestrator only warns (`ingest_orchestrator.ts:442-444`).
- **An observe gate reads other bindings and the group's tables** (`rdb/src/rtable_group/group.ts:1285-1286`).
- **`put` compares today** (`rdb_adapter_idb/src/idb_facade.ts:274-286`). ADPTI-06 relies on it, and joins the plan's flips in P3.
- **Every store puts a second append back into the frontier**: memory (`dag/src/store/mem_dag_store.ts:24-33`), SQL (`dag_sql/src/sql_dag_store.ts:42-62`, `INSERT OR IGNORE` and then the frontier insert) and IndexedDB (`dag_idb/src/idb_dag_store.ts:45-59`). What the index does with a second `index()` call wasn't checked. The fix doesn't reach it.
- **rhost and rdb_repl always open with a writer key** (`rhost/src/host.ts:913`, `rdb_repl/src/projection/command.ts:187`).

## Corrected in writing

- **The pseudo-code's end** looped for good on an observe it declined, and took the chain's end as "the last hash". Both are fixed as §9 has them.
- **A reinstated row**: the critique says it "has never been in the target". It may have been, when a projection removed it once. It is written by its row hash, which reuses the local id its mapping kept (`rdb_adapter_sqlite/src/sqlite_target.ts:1170-1175`), or takes a new one.
- **The schema check** is made by the end, before the settle, since it reads only the delta.
- **The foreign-key point** in *Considered and not taken* said that nobody edits a row whose referenced row is gone. rdb keeps such rows, so the target shows them. The text now says so, and points to the writes that name fewer columns.

## Added to the plan's *To confirm*

- A `patch` of a key with no record writes and captures nothing, as an `UPDATE` that matches no row does.
- `patch`'s shape: `store.patch(fields, { delete?: string[] })`, with the key in `fields`. A column both given and listed is a `DataError`.

The end's own observes recorded on the last outbox row with a record stays there as well. HIP-29's *The algorithm* uses it.
