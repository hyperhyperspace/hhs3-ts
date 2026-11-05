# HIP-17: Verdict cache across computations

- **Type:** implementation
- **Status:** deferred
- **Modules:** mvt, rdb
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

Keep completed verdicts across computations, keyed by `(group, entry, from)`, so that repeated reads of one group, such as the REPL's `LOG` or a per-entry sweep, stop re-diagnosing the same entries. It is sound because a completed verdict is a pure function of its key. It is deferred because the per-computation memo already bounds the acyclic path, and because the instance cache that existed before June 2026 kept conditional answers, which is the bug the current design was built to remove. Only completed components may ever be stored.

## Why

Within one computation the evaluation memoizes the verdicts of completed components, and the memo dies with the computation ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §4, "The per-computation memo"). The public `isEntryVoided` and `explainEntryVoided` mint a fresh evaluation per call, so a per-entry sweep such as the REPL's `LOG` pays $O(n)$ per line, $O(n^2)$ overall. Gates over the observer's own tables ([HIP-14](0014-observer-frame-ref.md) §7, item 4) add reads to an observe's verdict, so the cost grows with them.

## Design

An instance-level map of completed verdicts, keyed by `(group, entry, from)` and kept across `getView` / `LOG` lines. A completed verdict never changes, so an entry needs no invalidation; only memory does.

## Why it is sound

A completed verdict is a pure function of `(group, entry, from)`: it depends on the histories below those positions, which an append-only DAG never changes. What must never be cached is anything uncompleted: a conditional answer, or a member's value inside a solve. Those depend on the traversal, and so on query order, which can differ across replicas (`VOID_SEMANTICS.md` §4; [`WELL_FOUNDED_VERDICTS.md`](../modules/mvt/WELL_FOUNDED_VERDICTS.md) §3.2 and §6.4 on the instance cache that kept them).

## Open questions

- Where the map lives: the group instance, the replica, or the evaluation factory.
- Its bound. Nothing in it expires, so it needs a size limit or a scope, such as one REPL session.
- Whether the touch memo of HIP-14, which dies with the computation for the same reason, could share it.

## History

Extracted on 2026-11-04 by [HIP-1](0001-hips.md) from `modules/rdb/VOID_SEMANTICS.md` §4 ("The per-computation memo") and `modules/mvt/WELL_FOUNDED_VERDICTS.md` §6.3 and §6.5, where it was a cache that "would be sound, but it is not implemented". Both are readable at commit `f45f1e3`.
