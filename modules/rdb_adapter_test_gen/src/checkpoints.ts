import { dag, Position } from "@hyper-hyper-space/hhs3_dag";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { PRNG } from "./prng.js";

// `end` extends `start` iff start has no entries that end lacks (forkA empty).
export async function endExtendsStart(rawDag: dag.Dag, start: Version, end: Version): Promise<boolean> {
    const fork = await rawDag.findForkPosition(start, end);
    return fork.forkA.size === 0;
}

// Occasionally branch a write off an older checkpoint (concurrency injection).
export function pickConcurrentAt(prng: PRNG, checkpoints: Version[], rate = 0.3): Version | undefined {
    if (checkpoints.length === 0 || prng.next() >= rate) return undefined;
    return checkpoints[prng.nextInt(0, checkpoints.length - 1)];
}

export function cloneVersion(v: Version): Version {
    return new Set(v) as Version;
}

export async function recordCheckpoint(checkpoints: Version[], frontier: Position): Promise<void> {
    checkpoints.push(cloneVersion(frontier as Version));
}

// All (start, end) checkpoint pairs where end extends start — the delta inputs.
export async function collectExtendingPairs(
    rawDag: dag.Dag, checkpoints: Version[],
): Promise<Array<[number, number, Version, Version]>> {
    const pairs: Array<[number, number, Version, Version]> = [];
    for (let i = 0; i < checkpoints.length; i++) {
        for (let j = i + 1; j < checkpoints.length; j++) {
            if (await endExtendsStart(rawDag, checkpoints[i], checkpoints[j])) {
                pairs.push([i, j, checkpoints[i], checkpoints[j]]);
            }
        }
    }
    return pairs;
}
