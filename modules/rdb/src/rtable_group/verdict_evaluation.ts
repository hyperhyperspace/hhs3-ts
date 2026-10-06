// rdb's binding of the well-founded verdict engine (VerdictEvaluation in
// hhs3_mvt; VOID_SEMANTICS.md section 4). One evaluation is minted at each
// top-level entry (isEntryVoided / explainEntryVoided / getView /
// resolveForeignTableView / evaluateObserveGate) and threaded — MANDATORY —
// through every helper that receives it and every RTableViewImpl those helpers
// build. Concurrent top-level computations get distinct evaluations, so they
// never see each other's open frames. (The bug this prevents: a single
// per-group `Set` let one async evaluation observe another interleaved
// evaluation's visiting mark and falsely conclude a cycle, voiding a live
// witness row.)
//
// The evaluation's answers, which hold completed components only, are the
// per-computation memo: without it the engine is exponential in the number of
// updates to a row (diagnosing update U_k reads the subject at version(U_k),
// which void-checks U_1..U_k, each of which reads it again). An INSTANCE-level
// (entry, from) cache remains forbidden: the memo dies with the computation.
//
// Requires ONE sequential traversal per evaluation. Every void check in
// view.ts is sequentially awaited; if per-row work is ever run in parallel on
// one view, mint one evaluation per branch.

import { VerdictEvaluation } from "@hyper-hyper-space/hhs3_mvt";

import type { OpVoidDetail } from "./op_void.js";

export function freshVerdictEvaluation(): VerdictEvaluation<OpVoidDetail> {
    return new VerdictEvaluation<OpVoidDetail>({ kind: 'undecided-cycle' });
}

// Fail-safe only, never a verdict. A dropped evaluation does not corrupt a
// result; on cyclic data it recurses forever through fresh evaluations as an
// unbounded microtask chain that never yields to timers (the memory store and
// better-sqlite3 reads are both microtask-only), so a setTimeout-based test
// timeout alone would never fire. The in-flight counter in RTableGroupImpl
// throws past this bound so a lost evaluation surfaces as an immediate error
// instead of an OOM hang. Legit recursion depth times the number of concurrent
// computations stays far below this, and the counter is never a verdict, so it
// cannot reintroduce cross-talk.
export const VERDICT_MAX_INFLIGHT = 10_000;
