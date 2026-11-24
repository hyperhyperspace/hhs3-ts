# Second critique of HIP-29: Checkpoint consistency

A brief follow-up on [HIP-29](0029-checkpoint-consistency.md), the coupled revisions to [HIP-20](0020-unified-write-api.md), and [the response](0029-checkpoint-consistency.response.md) of 2026-11-19.

## Verdict

The revisions resolve all ten points of the first critique, and the refinements in the response (canonicalizing non-canonical SQLite JSON at the settle, keeping reserved identities on the batch record until the settle, and writing settle columns one at a time) leave both HIPs ready to implement.

One subtle edge case remains in applying an `observe`'s delta inside the settle:

## 1. A mid-chain `observe` delta can overwrite a later accepted write in the same pass

HIP-29 (*The settle moves the checkpoint*, *Changes* 1) specifies:

> *"For each observe the pass appended, the settle applies the delta from the tip before the observe to the observe: its op events, and its row actions."*

Suppose one ingest pass for group `A` appends:
1. `obs = observe(B, ...)` at `prevTip`, which voids an earlier write in `C_A` to column `x` of row `r`. `computeDelta(prevTip, {obs})` emits `upsert-row(r, { x: 'old' })`.
2. Later in the same pass (at `{obs}`), an accepted local update in the batch writes `r.x = 'new'`.

The target already holds `r.x = 'new'`, and rdb at the chain's final tip `C ∪ I` holds `r.x = 'new'`. Applying the mid-chain `computeDelta(prevTip, {obs})` row action verbatim in `commitIngest` would overwrite `r.x` back to `'old'`.

**Change.** Keep `opVerdictEvents` from each `computeDelta(prevTip, {obs})`, but materialize every row touched by those observe deltas from rdb at the chain's final tip **`C ∪ I`** (as reverts already read at `C ∪ I`), emitting `delete-row` (without setting `'ingestion_failure'`) when the row is absent at `C ∪ I` and holding back via the revert flag when a newer change to the row is pending. That also covers multiple observes in the same pass without ever clobbering later accepted ops in the chain.
