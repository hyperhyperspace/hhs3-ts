# Response to the sixth critique of HIP-29

A reply to [the sixth critique](0029-checkpoint-consistency.critique-6.md) of [HIP-29](0029-checkpoint-consistency.md) and of its plan, `plans/checkpoint-consistency.md`. HIP-29 and the plan were revised on 2026-11-23. HIP-20 changed in one place (below). Paths are relative to `modules/`.

Decisions 24 to 29 are taken as they were made. The point left undecided and the one to confirm were decided on 2026-11-23 (below). The code the critique cites was read again, and every claim checked holds.

| Critique | In the revised documents |
|---|---|
| §4. The memory DAG store | Taken (*Changes* 1, *Tests*). The plan's decision 12, detail 1 and ENTRY_04 |
| §5. An error that isn't a refusal | Taken (*The algorithm*, *What became of each change*, *Crashes*, *Why it is sound*, *Changes* 3, *Tests*). The plan's decision 13, detail 7, IN80 and IN81 |
| §6. The IndexedDB measurement | Taken. HIP-29's second open question moves it to a browser, in HIP-20's trial app, and says when a move would be cheapest. The plan's P4 loses it, and *Deferred* keeps the move |
| §7. Explicit ids on memory | Taken (*Tests*). The plan's IN72 and IN73 |
| §8. Starting without a writer key | Taken, with `onWarning` (*The algorithm*, *No projection over a pending change*, *What changes for apps*, *Why it is sound*, *Changes* 3, 6 and 10, *Tests*). The plan's decision 11, detail 8, RDBPROJ19 and RDBPROJ24 |
| §9. Smaller points | Both taken. Anonymous groups are decided below. The settle's step 3 and *The algorithm* write the inserts' mappings before the deletes, and so does the plan's detail 6 |
| §10, §11 | Taken, with the cases of the two decisions below |

## Decided on 2026-11-23

**Anonymous groups need no writer key.** A group without an identity provider signs nothing: its entries and observes are anonymous, and an insert's row hash comes from its uuid alone (`writerKeyId: authorFor(groupId)?.keyId`, `rdb_adapter/src/ingest_orchestrator.ts:390`). So ingestion takes its changes with no writer key. Three places change:
- `authorFor` checks the identity provider before it asks for the writer (`:362-373`);
- the observes' author does the same (`:537`);
- `syncDatabase` ingests the members that have a writer key or no identity provider (`:646`).

Decision 29's warning and stop then apply only to a group with an identity provider and no writer key. Without this, an app whose groups are all anonymous and that passes no key would have its first pending change hold remote changes back for good. Today such an app never pushes its local edits, but remote changes still reach it.

**The warning's channel.** A new `onWarning` option of `RdbProjection`, with `console.warn` when it isn't given, as §8 proposed. It fires once for each group: at open when a change of the group is pending, or else when a cycle first stops at one. A field in each group's `IngestResult`, through `onResult`, was the alternative. rdb_repl already prints rejections from there (`formatRejectWarning`). It needed no new option, but it would have come every cycle.

## Checked

- **The memory store's transactions** run with no queue (`dag/src/store/mem_dag_store.ts:16-22`). Only `Dag.append` calls `withTransaction` in any module's sources, and the memory store calls its growth listeners without awaiting them (`:70-74`). So a queue can't deadlock on a nested call.
- **`attempt` turns every error into a failure** today (`rdb_adapter/src/ingest_orchestrator.ts:456-465`).
- **Memory throws on an explicit id** (`rdb_adapter/src/memory_target.ts:274-278`).
- **The SQLite and IndexedDB stores queue their transactions** (`dag_sqlite/src/sqlite_connection.ts:41-45`, `dag_idb/src/idb_env.ts:108-117`).
- **No rdb_projection test writes through capture without a writer.** Every case that captures opens with one, so decision 29 flips none of them.

## Decided for HIP-20

HIP-20's writer refused every write with `no-writer-key` on a projection that has recorded no writer key, because such a write would never reach rdb. With anonymous groups ingested without a key, a write to one now does. Decided on 2026-11-23: `no-writer-key` covers only groups with an identity provider, so the writer refuses exactly what ingestion would. The writer tells the two apart by the group's recorded mode, `anonymous` for a group without an identity provider. HIP-20's *What the projection records in the target*, *Checks* and *Tests* say so, and HIP-29's *HIP-20* section lists it among the things HIP-29 changes there. It lands with the writer, in HIP-20's Phase C.
