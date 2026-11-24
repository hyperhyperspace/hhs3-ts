# Response to the third critique of HIP-29

A reply to [the third critique](0029-checkpoint-consistency.critique-3.md) of [HIP-29](0029-checkpoint-consistency.md) and of its plan, `plans/checkpoint-consistency.md`. HIP-29 and the plan were revised on 2026-11-21, and so was [HIP-20](0020-unified-write-api.md) where HIP-29 changes what it describes. Paths are relative to `modules/`.

The revision takes the decisions of §3 as they were made. Discussion on 2026-11-21 settled the points §3 left open, and found three things the design of §6 and §11 needed. Each claim below was checked against the code.

| Critique | In the revised documents |
|---|---|
| §4. Held rows aren't needed | Taken (*Reverts*). A revert writes rdb's row at C ∪ I with the row's pending changes on top, in the settle. The list of held rows, its storage on the three targets, `heldRows()`, `RowAction.release` and the masks of the consistency check go. Confirmed in discussion. |
| §5. An update is one change, refused whole | Taken (*Capture*, *Ingestion at the checkpoint*), with the two open points decided below. |
| §6. Entries are found by a recorded hash | Taken (*Ingestion at the checkpoint*, *Crash replay*), with the refinements below. *Reserved identities* goes. |
| §7. A moved checkpoint halts in both places | Taken (*One projector per target*). |
| §8. An accepted entry can be void where it stands | Taken (*Writes void where they stand*, and *Changes* 1 in rdb). The cause the user isn't told is left to HIP-30. |
| §9. Skew between groups | Taken (*What changes for apps*, with HIP-30). |
| §10. The wait stays | Taken: pointers to HIP-31 from the first open question and from *Considered and not taken*. The second ingestion in a cycle stays. |
| §11. A pass after these changes | Taken, with the replay made linear (below). |
| §12. The plan | Taken. The plan has a phase in rdb (P1), capture in a phase of its own (P3), and the target side of the settle (P4) before the ingestion core (P5). |
| §13. HIP-20's text | Taken. The comparison leaves capture and becomes a `compare` option of the writer's `update`, decided on 2026-11-21. |
| §14. Tests | Taken (*Tests*, and the plan's phases). |
| §15. Smaller points | All four taken. The summary is rewritten. *Validity* gains the other direction. The thrown-away delta joins the first open question. The recorded hash wakes nothing on memory and IndexedDB either: memory signals only from `record()` (`rdb_adapter/src/memory_target.ts:206-212`), and IndexedDB only for facade transactions that captured new outbox rows (`rdb_adapter_idb/src/idb_target.ts:124-129`). |

## The open points of §3

- **Held rows.** They go.
- **Readonly and author columns.** Ingestion sets aside a named readonly or author column that holds rdb's value for the row, and refuses the update when it holds another. On its own this fails a common case. A native insert leaves the author column empty until the settle, so an app that reads the row and writes it back before then names the author as `NULL`, which is not rdb's signer. So capture now fills in the author at insert (*Capture*), from HIP-20's writer record. On SQLite an `AFTER INSERT` trigger does it, suspending capture as the `applying` flag does, since a `BEFORE INSERT` trigger can't change the row. This was checked on 2026-11-21: the fill works inside a transaction, keeps an author the insert named, and leaves the column empty when the mode is `off`. It also removes the settle's author write, one of the two exceptions of *Settlement parity*. Two consequences are written down: `INSERT … RETURNING` shows `NULL` for the column, and an app can no longer tell a pending row by its empty author (memoblocks' chat does, `mb-chat/src/chat_server.tsx:96`).
- **IndexedDB.** A `put` carries the whole record and no list of columns, so a `put` over an existing record captures every column. Keeping the columns that a pending change writes, as §5 suggested, is not taken: a `put` repeats what the app read, without naming it. A facade `patch` that names fields can come later.
- **The second ingestion in a cycle.** It stays.

## Refinements to §6 and §11

1. **Observes are looked up by hash too.** Ingestion signs an observe only when the binding's gate reads `$author` (`rdb_adapter/src/ingest_orchestrator.ts:550`). Most of its observes are anonymous, so a replay rebuilds them exactly. The DAG appends an entry without checking whether it holds it (`dag/src/dag.ts:98-112`), which puts it back into the frontier of rdb's in-memory DAG store. §11 checks only changes against rdb. The revision checks every entry before its append.
2. **The replay is linear.** In §11's order, a change first gets the observes due before it, and only then is its recorded entry looked up. A replay at a change whose entry rdb holds would write an observe that the entry's past already holds, on every replay that passes an observe. The revised replay takes every entry up to the last change whose recorded entry rdb holds, and carries on after it. A second signed observe then comes only from one written after the last recorded entry, the case accepted in decision 6.
3. **Durable commits.** The case of a power failure that loses a recorded hash and keeps the append is dropped. The replay relies on durable target commits, as the settle already does.

## Added in writing

These came up while the documents were written. The plan lists the first under *To confirm*. The second was confirmed on 2026-11-21.
- **The writer record lands with HIP-29**, ahead of the rest of HIP-20's writer, since capture reads it.
- **An empty author column where the group's mode is `on`** means the insert was written while no writer key was recorded. Ingestion refuses it, as HIP-20's writer refuses a write then.
- **A writer update on IndexedDB** captures the columns it writes, not the whole record, though it shares the facade's write code, where a `put` now captures every column.
- **A revert is two writes on each target**: rdb's row as projection writes it, then the row's pending changes as capture recorded them. The pending changes are in the target's form and are read inside the target's transaction, so the shared core doesn't compute the row.

## HIP-20's open question 4

Rewritten as discussed. A function waits for a new row's ingestion and returns the row's hash. When it can't wait, it mints the uuid and saves it in the insert's outbox row, and ingestion, or a replay, uses that uuid. With the author filled in at capture, the row hash follows from the uuid.
