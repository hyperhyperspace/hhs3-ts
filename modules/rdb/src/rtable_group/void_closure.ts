// Per-computation cycle guard for the void-recursion fixpoint (see group.ts
// and VOID_SEMANTICS.md). One VoidClosure is minted at each top-level entry
// (isEntryVoided / explainEntryVoided / getView / resolveForeignTableView /
// evaluateObserveGate) and threaded — MANDATORY — through every `*Closure`
// helper and every RTableViewImpl those helpers build. Concurrent top-level
// computations get distinct closures, so their transient cycle marks never
// cross-talk. (The bug this fixes: a single per-group `Set` let one async
// evaluation observe another interleaved evaluation's visiting mark and
// falsely conclude a cycle, voiding a live witness row.)

export type VoidClosure = { visiting: Set<string> };

export function freshVoidClosure(): VoidClosure {
    return { visiting: new Set() };
}

// Fail-safe only, never a verdict. A dropped closure does not corrupt a
// result; on cyclic data it recurses forever through fresh closures as an
// unbounded microtask chain that never yields to timers (the memory store and
// better-sqlite3 reads are both microtask-only), so a setTimeout-based test
// timeout alone would never fire. The in-flight counter in RTableGroupImpl
// throws past this bound so a lost closure surfaces as an immediate error
// instead of an OOM hang. Legit recursion depth times the number of concurrent
// computations stays far below this, and the counter is never a verdict, so it
// cannot reintroduce cross-talk.
export const VOID_MAX_INFLIGHT = 10_000;
