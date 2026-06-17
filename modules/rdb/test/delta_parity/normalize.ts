import { set } from "@hyper-hyper-space/hhs3_util";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RTableGroupDelta } from "../../src/rtable_group/delta.js";
import type { RTableChanges } from "../../src/rtable/delta.js";

// Normalize a group delta for byte-comparison: the accumulator already sorts
// rowChanges by rowId and columnChanges by column and drops empties, so the
// only ordering left to pin is the per-table list (the nested map iteration
// order). schemaChanges are computed identically under both strategies, so
// they need no further sorting.
export function normalizeGroupDelta(delta: RTableGroupDelta) {
    const tables = [...delta.nested.entries()]
        .map(([id, child]) => ({ tableId: id, changes: child.changes as RTableChanges }))
        .sort((a, b) => a.tableId.localeCompare(b.tableId));
    return { schemaChanges: delta.schemaChanges, tables };
}

function versionLabel(v: Version): string {
    return [...v].sort().join(",");
}

export type DeltaParityContext = {
    seed: number;
    opIndex?: number;
    start: Version;
    end: Version;
};

function assertVersionParity(label: string, bounded: Version, full: Version, ctx: DeltaParityContext): void {
    if (!set.eq(bounded, full)) {
        throw new Error(
            `${label} mismatch (seed=${ctx.seed}, opIndex=${ctx.opIndex ?? "?"}, `
            + `start=${versionLabel(ctx.start)}, end=${versionLabel(ctx.end)}): `
            + `bounded=${versionLabel(bounded)} full=${versionLabel(full)}`,
        );
    }
}

export function assertGroupDeltaParity(
    bounded: RTableGroupDelta, full: RTableGroupDelta, ctx: DeltaParityContext,
): void {
    assertVersionParity("start", bounded.start, full.start, ctx);
    assertVersionParity("end", bounded.end, full.end, ctx);

    const b = JSON.stringify(normalizeGroupDelta(bounded));
    const f = JSON.stringify(normalizeGroupDelta(full));
    if (b !== f) {
        throw new Error(
            `RTableGroup changes mismatch (seed=${ctx.seed}, opIndex=${ctx.opIndex ?? "?"}, `
            + `start=${versionLabel(ctx.start)}, end=${versionLabel(ctx.end)}):\n`
            + `  bounded=${b}\n  full=${f}`,
        );
    }
}
