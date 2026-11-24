# Response to the fourth critique of HIP-29

A reply to [the fourth critique](0029-checkpoint-consistency.critique-4.md) of [HIP-29](0029-checkpoint-consistency.md) and of its plan, `plans/checkpoint-consistency.md`. HIP-29 and the plan were revised on 2026-11-22, and so was [HIP-20](0020-unified-write-api.md) where HIP-29 changes what it describes. Paths are relative to `modules/`.

The revision takes decisions 14 to 19 as they were made. Discussion on 2026-11-22 settled the points of §12, and found one gap in §7, closed below. HIP-29's design now starts with the whole cycle as pseudo-code (*The algorithm*), as asked in discussion.

| Critique | In the revised documents |
|---|---|
| §4. One delta gives the observes' effects | Taken (*The settle moves the checkpoint*, *Why it is sound*, *Changes* 2, *Tests*, *Considered and not taken*). The probes S1 to S4 are described in the settle's section. The plan's P6 computes one delta per group at the end (decision 10). |
| §5. The end of a pass reads only what is stored | Taken, in a new section, *The end of a pass*. The three records are in *Ingestion at the checkpoint*. The plan's detail 7 now records the hash before it looks the entry up. |
| §6. A refusal is recorded when it happens | Taken. *Refused before the crash* goes, with its example. The plan's P4 writes the records on the three targets. |
| §7. A crashed pass is finished, not resumed | Taken (*Crashes*, which replaces *Crash replay*). The replay that resumes goes to *Considered and not taken*. |
| §8. A named column stays a write | Taken: one entry in *Considered and not taken*, and the plan's decision 2. |
| §9. A pass after these changes | Taken, as the pseudo-code of *The algorithm*, with the change below. |
| §10. The plan | Taken, row by row. |
| §11. Tests | Taken, with one case added for the gap below. |
| §12. Smaller points | All five taken. The first and the last are decided below. |

## A gap in §7: an observe written for a refused change

Under §7, a group's chain ends at its last accepted entry. But a pass writes an observe before the entry that needs it, and rdb can still refuse that entry. Say `items` binds `perm` behind a gate that reads `$author`, so ingestion signs its observes, and the outbox holds a grant in `perm`, then an insert in `items`:
1. The pass appends the grant.
2. Before the insert, it writes a signed observe O1 of `perm` from `items`.
3. rdb refuses the insert, and the failure goes in its outbox row.
4. The end finds no accepted entry in `items`. So the chain ends at the checkpoint, which doesn't observe the grant, and the end writes a second signed observe.

This happens in a normal pass, with no crash. The third critique accepted a second signed observe only after a crash.

**Decided on 2026-11-22.** Each change's record also holds the hashes of the observes written for it, each written before its observe is appended. A group's chain then ends at the last hash recorded for it that rdb holds, an entry or an observe. The end's own observes are recorded on the last outbox row that has a record. The plan lists this last point under *To confirm*.

What follows:
- Every entry a pass appends is recorded before it is appended. So no signed observe is written twice, after a crash either, and the case accepted in the third critique's decision 6 goes.
- The records say which chains hold an observe. §4's rule for it, which read an entry's parents, isn't needed.
- It costs a target write per observe as well. One write per change isn't possible: an entry's hash is known only once its observes are in rdb, since validating the entry reads them.

## The points of §12 that needed a decision

- **The mode `off`.** `autoAuthor` lands with HIP-29, in the plan's P3, together with the writer record (decided 2026-11-22). This covers the setting in `AdapterConfig`, ingestion's signing of each insert as its captured author column says, `RdbProjection`'s option and its check, and rhost's config field (`rhost/src/config.ts:33`, `:195`). rhost joins HIP-29's modules. The writer's own refusals stay with the writer, in HIP-20's Phase C. Nothing found stood in the way. One entry per change already takes its author, which is what signing per insert needs.
- **Durable commits.** Decided on 2026-11-22: the design assumes that a commit that returned is kept, in the target and in rdb's store. *Crashes* now says what a lost commit does. Checked:
  - **Two stores.** On Node, the replica and the projection are separate files, `rdb/replica.rdb` and `db/data.sqlite` (`rhost_node/src/platform.ts:9-10`). In the browser they are separate IndexedDB databases. A power failure can cut each at a different point.
  - **SQLite.** Both stores run in WAL mode (`rdb_adapter_sqlite/src/sqlite_target.ts:453`, `dag_sqlite/src/sqlite_connection.ts:11`). better-sqlite3 builds SQLite with `SQLITE_DEFAULT_WAL_SYNCHRONOUS=1`, which is `NORMAL`. A crash of the process keeps every commit, and a power failure can lose the last ones.
  - **IndexedDB.** No transaction sets `durability`.
  - **The target loses an insert's record while rdb keeps the entry.** The next pass mints a new uuid, so the row shows twice once the first copy is projected. An anonymous insert is affected too, since its uuid is random.
  - **rdb loses entries that a settle already put in a checkpoint.** The checkpoint then names entries rdb doesn't have.

  Looked at and declined for now:
  - one write to disk in rdb's store before each settle;
  - keeping settled rows in the outbox until rdb's store writes to disk now and then, and redoing what rdb lost. Redoing a settle has to undo its effects, such as the inserts' mappings and a row that an observe's void removed;
  - storing each entry in its outbox row, so that it can be appended again;
  - minting an insert's uuid at capture.

  A later design that stores the changed rows in the outbox from the triggers would make recovery simpler.

## Looked at in discussion

- **A lock on the outbox from the last drain to the apply**, so that steady local writes can't hold the projection back. No native write path can wait for it:
  - On SQLite it would be a write transaction held across ingestion's async work. An app writing through `wrapDatabase` would write inside it. One on another connection in the same process would block the event loop in better-sqlite3's busy wait, so the pass couldn't finish.
  - An IndexedDB transaction closes when it has no pending request, and the facade's `transaction()` is synchronous (`rdb_adapter_idb/src/idb_facade.ts:1014`).
  - Memory's local writes are synchronous (`rdb_adapter/src/memory_target.ts:274`, `:316`, `:345`).

  It goes to *Considered and not taken*, with a pointer from the first open question.

## Corrected in writing

- **Signed entries found by hash.** HIP-29 said only an anonymous entry made again can be found by its hash, since the signing suites sign with randomness. That holds for ML-DSA (`crypto/src/signing.ts:54`, randomized by default in `@noble/post-quantum`), but Ed25519 signs deterministically (`:31`). The text now says both.
- **HIP-20, line 212** and the other mentions of a crash replay now describe the end of a pass and the recorded hashes.
