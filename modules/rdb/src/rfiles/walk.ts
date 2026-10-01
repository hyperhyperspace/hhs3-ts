// The entries in past(end) \ past(start), each loaded once.
//
// The walk goes back from end and stops at the fork's `common` entries (the
// entries of the intersection with a successor on one side only) and at start
// itself. With both positions reduced to minimal covers, every path from a new
// entry into the intersection enters it at a `common` entry, and every entry
// of end that is in the intersection is in start or in `common`, so nothing
// older is ever loaded. Returns the fork's common frontier.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

export async function walkNewEntries(
    rawDag: dag.Dag, start: Version, end: Version, visit: (entry: dag.Entry) => Promise<void>,
): Promise<Position> {
    const from = await rawDag.findMinimalCover(start);
    const to = await rawDag.findMinimalCover(end);
    const fork = await rawDag.findForkPosition(from, to);

    const stop = new Set<B64Hash>([...fork.common, ...from]);
    const visited = new Set<B64Hash>();
    const pending: B64Hash[] = [...to];
    while (pending.length > 0) {
        const hash = pending.pop()!;
        if (visited.has(hash) || stop.has(hash)) continue;
        visited.add(hash);
        const entry = await rawDag.loadEntry(hash);
        if (entry === undefined) continue;
        await visit(entry);
        for (const prev of json.fromSet(entry.header.prevEntryHashes)) {
            if (!visited.has(prev) && !stop.has(prev)) pending.push(prev);
        }
    }
    return fork.commonFrontier;
}

export function sameVersion(a: Version, b: Version): boolean {
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}
