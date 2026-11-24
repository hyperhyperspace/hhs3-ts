# Response to the second critique of HIP-20

A reply to [the second critique](0020-unified-write-api.critique-2.md) of [HIP-20](0020-unified-write-api.md). The HIP was revised in place on 2026-11-17. Paths are relative to `modules/`. The labels W15 to W21, V1, A1, M1, B1, K1, K2, S1 and O3 are the critique's.

## What the revision takes

| Critique | In the revised HIP |
|---|---|
| §2. One reason for keeping the author column in anonymous groups doesn't count | Dropped from *Considered and not taken*. The first response is left as it was, since this critique reviewed it. |
| §3. Record the author column in captured inserts | Taken: *Changes* 1 ("The author column is captured"), *Authorship* (the signer comes from the captured change), *What stays*. Existing targets are not handled, see below. |
| §4. One table for the author column | Taken as proposed: *Authorship*. `RowWriter` gains `writerKeyId()`. The first row of *Write semantics* names the author column as the exception. |
| §5. "Unchanged" before "`null`" | Taken, in the writer (*Write semantics*, *Checks*) and in the native comparison (*Changes* 1). A row spread into an update: decided, see below. |
| §6. A voided delete is a `void` event | Taken: *Write semantics*, "Missing rows". |
| §7. Read the writer's meta where the write happens | Taken: *Checks*, *Changes* 3 and 4, *Why it is sound* 6. The commit-driven drop stays, for reads. |
| §8. The missing test cases | All taken: *Tests*. |
| §9. A native update to `null` takes two paths | Taken: a captured change can carry a `null`, and the planner rejects it from any target (*Changes* 1). |
| §9. A key id the key table doesn't have | Taken: `invalid-key` (*Key columns*, *Checks*). |
| §9. `id-column` | Taken: "the table's id column". |
| §9. An update that changes nothing | Taken: no commit event (*RowWriter*). |
| §9. Refused writes and interned keys | Taken: keys are interned after the checks and the row read (*Key columns*, *Checks*). |
| §9. *Changes* | Taken: `rowHash` and `localId` on `RdbProjection` (*Changes* 6), the `registerWriterKey` call sites (*Changes* 7), `useWriter` only (*Changes* 9). |
| §9. Docs, citations and wording | Taken: the three READMEs in *Changes* 10, both citations, and the wording, in the rewritten author table. |
| §10.8. The trial binding | Taken: *Changes*, first paragraph. |
| §11.1. A native update of the author column | Taken: captured, rejected by the planner and reverted, under both settings (*Changes* 1). |
| §11.2. `INSERT OR REPLACE` over a live row | Taken: the fix is a revert to rdb's row (*Changes* 1), with one addition, see below. |

## §3: existing SQLite targets

The critique notes that existing targets would need their capture triggers installed again. It asks the HIP either to reinstall them when a projection opens or to say that existing projections are rebuilt. The revision does neither. The project is unreleased, so no target needs an upgrade path, and new targets get the new triggers.

A general way to handle a target format change is left for when there are targets to keep. The direction is to store a format version in the target, compare it when the projection opens, and act only on a mismatch, probably with a rebuild. The outbox and the op event log are not derived data, so such a rebuild would have to ingest the outbox first, or refuse while changes are pending.

## §5: a row spread into an update

The critique leaves a choice to the trial binding. Either the unchanged rule also covers the id column and the author column, or the README tells bindings not to spread a row into an update. The revision takes the first. Those two columns, at the row's own values, are not written, and a different value is still refused.

A form that saves every field of a row it read is the case the unchanged rule exists for. A README rule would trip exactly that code. On SQLite, an update that carries either column reads the row first, as it does for a `null`.

## §11.2: an insert skipped as already applied logs no event

The critique proposes marking such a row for the revert. The revision adds that this revert logs no op event. A real crash replay of an insert looks the same, so an event would report a failure for an insert that landed. A dropped insert whose row the target still has is logged as a failure.

## Decisions taken in discussion, 2026-11-17

- **No upgrade path for existing targets** (above).
- **The author table of §4**, and `writerKeyId()` on `RowWriter`.
- **A row spread into an update works** (above).
