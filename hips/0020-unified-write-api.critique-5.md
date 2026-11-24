# Fifth critique of HIP-20: Unified write API for projection targets

A brief follow-up on [HIP-20](0020-unified-write-api.md) as revised on 2026-11-19 and [the fourth response](0020-unified-write-api.response-4.md).

## Verdict

The revision resolves the fourth critique, and the three alternate solutions in the fourth response (refusing non-literal JSON before rdb, hashing captured change ids into failure events, and recording authorship modes per group in the writer record) are cleaner than what the fourth critique proposed. The fourth response is also right to reject the fourth critique's citation corrections.

Two minor items remain, neither of which affects the design:

## 1. `RowValues` does not allow `undefined`

*Write semantics* states that *"A column given `undefined` counts as not given"*, but `RowValues` in *Design* (`hips/0020-unified-write-api.md:69`) is:

```ts
export type RowValues = { [column: string]: json.Literal | KeyValue | null };
```

Because `tsconfig.base.json:84` sets `"strict": true` (enabling `strictNullChecks`), passing an optional value such as `{ title, memo: maybeMemo }` where `maybeMemo: string | undefined` will not type-check.

**Change.** Include `| undefined` in `RowValues`:

```ts
export type RowValues = { [column: string]: json.Literal | KeyValue | null | undefined };
```

## 2. Two translate-time rejects from one multi-column native update still collide

*Changes* 1 has a failure's hash cover the ids of the captured changes it came from, and splits a multi-column native update from memory or IndexedDB into one change per column before merging.

Because the split changes originate from a single outbox row (one `CapturedChange.id`), two columns rejected at translate time in the same native `put` or `localUpdate` (for example, two readonly columns, or a readonly column together with `author_key_id`) share both the same `CapturedChange.id` and the same empty `failWrite` (`values: {}`, `modules/rdb_adapter/src/ingest.ts:457-460`). Their failure hashes will match, so the target's `UNIQUE (op_hash, direction)` constraint will still drop the second event.

**Change.** Include the offending column (or failure reason) alongside the captured change ids in the failure hash.
