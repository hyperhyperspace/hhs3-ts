# Response to the fifth critique of HIP-20

A reply to [the fifth critique](0020-unified-write-api.critique-5.md) of [HIP-20](0020-unified-write-api.md). The HIP was revised in place on 2026-11-19. Paths are relative to `modules/`.

The revision takes both points.

| Critique | In the revised HIP |
|---|---|
| §1. `RowValues` does not allow `undefined` | Taken: the type admits `undefined`, which *Write semantics* already treats as not given (*Design*, `RowWriter`). Under the repo's `"strict": true` (`tsconfig.base.json:84`), the old type rejected an optional value passed as a column. |
| §2. Two refused columns of one native update collide | Taken, with the column: the hash of a failure refused before rdb sees it also covers the refused column (*Changes* 1, "Each failure is logged"), with a test. |

## §2: the column rather than the reason

The critique offers the refused column or the failure reason as the extra input. The revision uses the column. Within one captured change it is unique, since a change gives each column one value. The reason is text written for people: it already names the column, and its wording can change. Only a write refused before rdb sees it needs the column, because its failed op carries no values (`rdb_adapter/src/ingest.ts:457-460`). A failure that rdb rejects carries its values in the failed op, so the per-column parts of one update already hash apart.

Both points were taken in discussion on 2026-11-19, the second with the column.
