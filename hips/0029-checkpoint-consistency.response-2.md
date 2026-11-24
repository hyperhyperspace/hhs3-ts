# Response to the second critique of HIP-29

A reply to [the second critique](0029-checkpoint-consistency.critique-2.md) of [HIP-29](0029-checkpoint-consistency.md). The HIP was revised in place on 2026-11-19. Paths are relative to `modules/`.

The revision takes the point, with the proposed change.

| Critique | In the revised HIP |
|---|---|
| §1. A mid-chain `observe` delta can overwrite a later accepted write in the same pass | Taken (*The settle moves the checkpoint*, *Held-back reverts*, *Why it is sound*, *Changes* 1 and 2, *Tests*). The settle keeps each observe's op events. It doesn't apply the deltas' row actions as they are: it rewrites every row any of the pass's observes touched, from rdb at C ∪ I. A row rdb no longer has there is removed, with its mapping marked deleted rather than `ingestion_failure`. A row with a pending change is flagged. |

## §1: why this is the common case

An observe in the middle of a chain is not rare. Ingestion makes a group's observations current before each of its entries (`ingest_orchestrator.ts:564-569`), so an observe is often followed by the pass's own writes. Each observe's delta gives a row as rdb has it at that observe. rdb at C ∪ I has the row as it ends up after every later write and every observe of the pass, so that is where the rewrite reads.

The same rewrite covers a case the critique doesn't name. An observe can also void one of the pass's own earlier entries, not only an entry of C. That entry's row is one the observe touched, so the rewrite from C ∪ I removes its effect.

The rewrite stays within settlement parity. It reaches only the rows that observes touched, which no write of the target could have settled the same way, and not the pass's other rows.

Op events still come from each observe's own delta. When a later observe in the same pass reverses an earlier one's flip, the events hold a void and then a reinstate of the same op. That is what happened, and the rewritten row is right either way, so the revision leaves it.

The point was taken in discussion on 2026-11-19.
