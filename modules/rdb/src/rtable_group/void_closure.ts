// Per-computation cycle guard for the void-recursion fixpoint (see group.ts
// and VOID_SEMANTICS.md). One VoidClosure is minted at each top-level entry
// (isEntryVoided / explainEntryVoided / getView / resolveForeignTableView /
// evaluateObserveGate) and threaded — MANDATORY — through every `*Closure`
// helper and every RTableViewImpl those helpers build. Concurrent top-level
// computations get distinct closures, so their transient cycle marks never
// cross-talk. (The bug this fixes: a single per-group `Set` let one async
// evaluation observe another interleaved evaluation's visiting mark and
// falsely conclude a cycle, voiding a live witness row.)
//
// The closure also carries the PER-COMPUTATION MEMO (VOID_SEMANTICS.md §4,
// "The per-computation memo"). Without it the engine is exponential in the
// number of updates to a row: diagnosing update U_k getRow's the subject at
// version(U_k), which void-checks U_1..U_k, each of which getRow's again —
// T(k) ~ 2^(k-1), with recursion depth only k, so the in-flight fail-safe never
// trips and it presents as a hang. `visiting` alone cannot help: it is a DFS
// stack popped in `finally`, so a finished subtree is forgotten at once.
//
// Soundness in one line: the ONLY way the stack shape can influence a verdict
// is a descendant receiving the transient deny for a key that is currently
// visiting. A deny on the frame's own key (stack.top === key: getRow at
// version(U_k) seeing U_k itself) happens at the same structural point in every
// evaluation of that key, so the verdict stays stack-independent and is stored.
// A deny on any lower ancestor (a FOREIGN hit) makes every open frame from that
// ancestor to the top root-dependent — the deny-the-whole-cycle collapse is
// per root — so none of them may be stored; caching one side of a mutual revoke
// and reusing it from the other side yields a single survivor (PERM12).
//
// An INSTANCE-level (entry, from) cache remains forbidden (§4): this map dies
// with the computation, exactly like `visiting`.

import type { OpVoidDetail } from "./op_void.js";

export type VoidClosure = {
    // Keys (`createOpId|entryHash|fromKey`) of the frames currently open.
    visiting: Set<string>;
    // The same keys in DFS order; invariant: visiting === new Set(stack).
    // `stack[stack.length - 1]` is the frame currently diagnosing, which is
    // what tells a self hit (top === asked key) from a foreign one.
    stack: string[];
    // Finished verdicts for this computation (`undefined` = live). `has(key)`
    // vs `get(key) === undefined` distinguishes uncached from diagnosed-live.
    // Never holds the in-stack deny, and never holds a frame that was open
    // during a foreign hit.
    completed: Map<string, OpVoidDetail | undefined>;
    // Monotonic count of foreign hits. A frame records it on push and stores
    // its verdict only if it is unchanged on completion.
    foreignCycleHits: number;
};

// Requires ONE sequential traversal per closure: `stack.top` identifies the
// asker only if no other traversal interleaves on the same closure. Every void
// check in view.ts is sequentially awaited; if per-row work is ever run in
// parallel on one view, mint one closure per branch.

export function freshVoidClosure(): VoidClosure {
    return { visiting: new Set(), stack: [], completed: new Map(), foreignCycleHits: 0 };
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
