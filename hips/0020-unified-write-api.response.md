# Response to the critique of HIP-20

A reply to [the critique](0020-unified-write-api.critique.md) of [HIP-20](0020-unified-write-api.md). The HIP was revised in place on 2026-11-16, after a discussion on 2026-11-15 and 2026-11-16 whose decisions are listed at the end. Paths are relative to `modules/`. The labels W1 to W14, O1, O2 and `[IN12]` are the critique's.

## What the revision takes

| Critique | In the revised HIP |
|---|---|
| §3. Write one binding before the interface lands | *Changes*, first paragraph: a landing condition, which also settles open questions 2, 4 and 6 |
| §4. `null` | *Write semantics*: an insert's `null` is "not given", and an update to `null` throws. The two defects behind W2 and W3 are in *Changes* 1. |
| §5. Defaults and required columns | *Write semantics*: an omitted column with a default is written in the row and the op, and an omitted required column throws. `TableShape` gains defaults and nullability. Local inserts on memory and IndexedDB write defaults (*Changes* 1). |
| §6. Which columns an update writes | Compared by value on every target (*Changes* 1: `json.equals` in the facade and in `localUpdate`, `WHEN NEW.<col> IS NOT OLD.<col>` on SQLite) |
| §6. What fails together | The first option, `[IN12]`'s rule: each column lands or fails alone, and the planner splits memory's and IndexedDB's changes per column. See below for why the choice follows from dropping batches. |
| §7. Batch atomicity | Not followed, see below |
| §8. Project-back is the authority for the author column | Taken: *Authorship*, *Changes* 1 |
| §8. Drop the `author` parameter | Taken. `openWriter()` reads the key id the projection records. The author is now governed by a per-group `autoAuthor` setting, see the decisions at the end. |
| §8. Don't project an author column for anonymous groups | Not followed, see below |
| §9. Refresh table meta in observer targets | Taken: *Checks*, *Changes* 1. The writer also reads the projection's recorded values inside each write's transaction. |
| §10. `registerKey` off the writer | Taken. It stays on the rhost clients and `RdbProjection`. With `{ pk }` key values, the writer interns keys without it. |
| §10. The hash suite | Taken further: the projection records the replica's hash suite in the target, and the writer and both rhost clients read it there (*What the projection records in the target*) |
| §10. `rowHash` and `localId` beside the reads | Taken: they move to `QuerySource` |
| §10. Decide open question 4 with them | Kept open, narrowed, see below |
| §11. The test plan | Taken. Every case asserts the captured change, the ingest result, the row after project-back, and rdb's row. A new fixture table. The *One write, three meanings* cases go in first as tests of today's behavior. Some ingestion cases keep the raw paths. |
| §12. Unknown ids | Taken for updates too: an update or delete of a missing row does nothing and resolves `false`. Memory's `localUpdate` stops throwing and `localDelete` stops capturing. See below. |
| §12. A writer that cannot replicate | Taken: `openWriter` refuses on a target without capture, or without a recorded writer key |
| §12. `ProjectionProvider` takes `source` and `writer` separately | Taken: it takes only `source`, and `useWriter()` opens the writer from it |
| §12. The summary sentence | Rewritten. The writer normalizes, then writes. |
| §12. Projected names | Deferred to open question 6, see below |
| §13. Recommendations 1 to 8 | As above. Recommendation 1 (fix the existing paths first) is *Changes* 1. |
| §14. Outside this HIP | All six are in *Changes* 1, with the fifth merged into the meta refresh |

## §7: no batches, instead of an atomic batch

The critique asked for replica atomicity in HIP-20, or, if it was deferred, for a batch id in the outbox from the first version and a contract worded so that a batch may be rejected as a whole. The revision does neither. It drops `batch` and says that the writer offers no multi-row atomicity (*Atomic writes*).

- **The deferral risk was a batch that applies in part.** The critique's concern was that "a batch that used to apply in part would start to fail as a whole, under bindings written against the first behavior". With no batch, there is no first behavior. A later atomic `batch` adds a method and changes no existing contract.
- **A batch id with no batch is a column nobody writes.** The outbox is bookkeeping in each target, so the later HIP can add the column together with the API that sets it.
- **Atomicity takes more than the critique's four steps.** The discussion checked rdb's bundle validation and found four more constraints, each a design decision of its own:
  - **A bundle writes each row once**, and the rows it updates or deletes must exist before it (`rdb/src/rtable_group/validate_ops.ts:535`, `:550`). The planner has to collapse each row's changes first.
  - **`ALLOW` rules read only the state before the bundle**, while FKs see earlier ops in it (`validate_ops.ts:554-555`). A batch that grants a permission and then uses it is rejected whole.
  - **No op in a bundle can touch a foreign group** (`validate_ops.ts:507-508`). The critique's "one entry per group" leaves a batch over two groups non-atomic, which is the misreading the critique warned about. The revision's sketch has the writer refuse such a batch.
  - **A bundle has one signer** (`rdb/src/rtable_group/validate.ts:158-172`), which interacts with `autoAuthor`.
- **The batch callback no longer needs a placeholder type.** Real ids can be handed out inside the synchronous callback on all three targets. On SQLite the callback runs inside the transaction. On IndexedDB, ids already come from the table meta read inside the transaction (`rdb_adapter_idb/src/idb_target.ts:414-424`, `idb_facade.ts:511`). That removes `RowRef`.

All of this is recorded as open question 1, for its own HIP.

The choice for §6's failures follows from this. With no unit larger than a column that the outbox can mark, per-column failure is the rule all three targets can keep today. Making one call all or nothing would need the same mark as a batch.

## §8: the author column stays in anonymous groups

The critique called not projecting an author column for anonymous groups "cleaner still". The revision keeps the column (*Considered and not taken*):

- **The column is already correct.** Once project-back writes `NULL` for a row with no author, every device shows `NULL`, so the W11 discrepancy is gone.
- **Dropping it costs every existing projection a column.** On a capture-provisioned SQLite target that is the `drop-column` path of §14.6, which fails today.
- **It would vary by group.** A binding that reads tables from several groups would have to check whether each group has the column.

## §10: open question 4 stays open

The critique asked to decide "following a write" together with the identity helpers. The revision moves the helpers but keeps the question open, because the binding of §3 should show whether apps need a row hash before ingestion. It does record a concrete option. Ingestion already reuses a reserved identity for a local row on crash replay (`rdb_adapter/src/ingest.ts:219-236`). If the writer minted the identity at insert and recorded it the same way, the row hash would be known at once, even while the host is stopped.

## §12: missing rows, reported by a boolean

The critique says a delete of a missing row should be a no-op, because the click can race with a remote delete. The revision makes updates no-ops too, and has both resolve `false`:

- **Neither can be ingested.** rdb accepts an update or a delete only of a row that is live at the op's parent frontier (`rdb/src/rtable_group/validate_ops.ts:429-433`, `:550` in a bundle). Recording one would only produce a rejection, as memory's `localDelete` does today.
- **Throwing was considered and dropped.** The case for it was that the delete can still be voided and the row come back, so the user's edit is lost. But it is lost under every rule, since no op can carry it, and the row's return arrives as a `reinstate` op event (`rdb_adapter/src/project.ts:72-82`). What remains is telling the app at write time, which a boolean does without a try/catch around a normal race.
- **No special case for an id that never existed.** It would need more bookkeeping, for an error a binding's own tests find.

The check costs nothing. IndexedDB's facade already reads the row before it deletes it (`rdb_adapter_idb/src/idb_facade.ts:452-455`), and the writer's update reads it to merge. SQLite reports the changed rows, and memory checks its map.

## §12: projected names, deferred

Bindings build `${group}_${table}` by hand, while the projection sanitizes the prefix and lets the host rename it (`rdb_projection/src/scope.ts:45`, `:72`). The same is true of every query a binding makes, so the fix belongs where a binding gets its tables, not in the writer. It is open question 6. It may land together with typed rows (open question 2).

## Decisions taken in discussion, 2026-11-15 and 2026-11-16

These change the HIP beyond the critique:

- **No batches** (above).
- **Key columns take a number or `{ pk }`.**
  - A number is a key id.
  - `{ pk }` is a serialized public key, which the writer hashes with the replica's suite and interns in the write's transaction. The serialized key names its signing suite, so no suite field is needed.
  - A bare string throws, so that a JavaScript string from a form or a URL is never mistaken for a key. This matters because `deserializePublicKey` checks nothing (`crypto/src/identity.ts:28-35`): a key hash passed by mistake parses into a garbage suite and an empty key. The writer checks the suite and the key size (`crypto/src/signing.ts:13`).
  - This closes the old open question 3.
- **The hash suite is recorded in the target by the projection.** The writer can reach only the target, and the projection already reads the suite from its replica. Both rhost clients hard-code SHA-256 today (`rhost_client_node/src/client.ts:31`, `rhost_client_browser/src/client.ts:28`). On a BLAKE3 replica, an identity-provider row they write would fail rdb's self-certification check (`validate_ops.ts:406`, `:529`).
- **Authorship is a per-group `autoAuthor` setting, on by default.**
  - On: today's behavior. Every op in a group with an identity provider is signed with the writer key, and the writer fills the author column.
  - Off: an insert's author column is the app's to set. The writer key's id signs the insert, and `null` makes it anonymous. Ingestion respects it, for native writes too.
  - Updates and deletes stay signed with the writer key, because nothing in a change can carry their signer. Under rdb's default rules, this makes an anonymous row in an authenticated group permanent (`rdb/src/rschema/payload.ts:312-315`, `rdb/src/rtable_group/predicates.ts:186`), and the HIP says so.
- **Missing rows** are no-ops that resolve `false`, for updates as well as deletes (above).
- **Refusals have a type.** Every check throws a `WriteRefusedError` with a reason code, so a binding can tell a refused write from a storage error.
- **Capture is checked on each write**, not only at open, because `setCaptureEnabled(false)` would otherwise leave a writer storing rows that never reach rdb.
- **`autoAuthor: false` without an author column** is refused by `RdbProjection`, at open and on reconfigure, before it writes to the target.
- **`{ pk }` is a key only in a key column.** In a json column it is json.
- **A native insert that reuses a deleted row's local id** is reverted and logged as an op event. A probe on SQLite showed that today ingestion drops it as a stale crash replay, and the row stays in the target with no rdb row and no op event.
- **Only the host key.** Signing as several identities, and anonymous updates and deletes, need a signer per captured change and a key list on the host. Both are left to a later HIP (open question 3).
