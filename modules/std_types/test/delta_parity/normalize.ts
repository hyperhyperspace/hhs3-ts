import { set } from "@hyper-hyper-space/hhs3_util";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RCapDelta } from "../../src/types/rcap/rcap.js";
import type { RSetDelta } from "../../src/types/rset/rset.js";

export function normalizeRCapDelta(delta: RCapDelta) {
    const identityChanges = [...delta.identityChanges].sort((a, b) => a.keyId.localeCompare(b.keyId));
    const capabilityChanges = [...delta.capabilityChanges].sort((a, b) => a.capName.localeCompare(b.capName));
    const grantChanges = [...delta.grantChanges].sort((a, b) => {
        const capCmp = a.capName.localeCompare(b.capName);
        if (capCmp !== 0) return capCmp;
        return a.keyId.localeCompare(b.keyId);
    });
    return { identityChanges, capabilityChanges, grantChanges };
}

export function normalizeRSetDelta(delta: RSetDelta) {
    const added = [...delta.added].sort();
    const removed = [...delta.removed].sort();
    const validityChanges = [...delta.validityChanges].sort((a, b) => {
        const entryCmp = a.entryHash.localeCompare(b.entryHash);
        if (entryCmp !== 0) return entryCmp;
        return a.elementHash.localeCompare(b.elementHash);
    });
    return { added, removed, validityChanges };
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

function assertVersionParity(
    label: string,
    bounded: Version,
    full: Version,
    ctx: DeltaParityContext,
): void {
    if (!set.eq(bounded, full)) {
        throw new Error(
            `${label} mismatch (seed=${ctx.seed}, opIndex=${ctx.opIndex ?? "?"}, `
            + `start=${versionLabel(ctx.start)}, end=${versionLabel(ctx.end)}): `
            + `bounded=${versionLabel(bounded)} full=${versionLabel(full)}`,
        );
    }
}

export function assertRCapDeltaParity(
    bounded: RCapDelta,
    full: RCapDelta,
    ctx: DeltaParityContext,
): void {
    assertVersionParity("start", bounded.start, full.start, ctx);
    assertVersionParity("end", bounded.end, full.end, ctx);

    const b = JSON.stringify(normalizeRCapDelta(bounded));
    const f = JSON.stringify(normalizeRCapDelta(full));
    if (b !== f) {
        throw new Error(
            `RCap changes mismatch (seed=${ctx.seed}, opIndex=${ctx.opIndex ?? "?"}, `
            + `start=${versionLabel(ctx.start)}, end=${versionLabel(ctx.end)}): `
            + `bounded=${b} full=${f}`,
        );
    }
}

export function assertRSetDeltaParity(
    bounded: RSetDelta,
    full: RSetDelta,
    ctx: DeltaParityContext,
): void {
    assertVersionParity("start", bounded.start, full.start, ctx);
    assertVersionParity("end", bounded.end, full.end, ctx);

    const b = JSON.stringify(normalizeRSetDelta(bounded));
    const f = JSON.stringify(normalizeRSetDelta(full));
    if (b !== f) {
        throw new Error(
            `RSet changes mismatch (seed=${ctx.seed}, opIndex=${ctx.opIndex ?? "?"}, `
            + `start=${versionLabel(ctx.start)}, end=${versionLabel(ctx.end)}): `
            + `bounded=${b} full=${f}`,
        );
    }

    if (bounded.nested.size !== full.nested.size) {
        throw new Error(
            `RSet nested size mismatch (seed=${ctx.seed}, opIndex=${ctx.opIndex ?? "?"}, `
            + `start=${versionLabel(ctx.start)}, end=${versionLabel(ctx.end)}): `
            + `bounded=${bounded.nested.size} full=${full.nested.size}`,
        );
    }

    for (const [key, nestedBounded] of bounded.nested) {
        const nestedFull = full.nested.get(key as B64Hash);
        if (nestedFull === undefined) {
            throw new Error(
                `RSet nested key missing in full delta (seed=${ctx.seed}, key=${key})`,
            );
        }
        const nb = JSON.stringify(nestedBounded);
        const nf = JSON.stringify(nestedFull);
        if (nb !== nf) {
            throw new Error(
                `RSet nested changes mismatch (seed=${ctx.seed}, key=${key}): bounded=${nb} full=${nf}`,
            );
        }
    }
}
