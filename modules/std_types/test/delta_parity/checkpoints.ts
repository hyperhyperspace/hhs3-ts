import { dag, Position } from "@hyper-hyper-space/hhs3_dag";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { PRNG } from "./prng.js";

export async function endExtendsStart(
    rawDag: dag.Dag,
    start: Version,
    end: Version,
): Promise<boolean> {
    const fork = await rawDag.findForkPosition(start, end);
    return fork.forkA.size === 0;
}

export function pickConcurrentAtRate(
    prng: PRNG,
    checkpoints: Version[],
    rate: number,
): Version | undefined {
    if (checkpoints.length === 0 || prng.next() >= rate) {
        return undefined;
    }
    return checkpoints[prng.nextInt(0, checkpoints.length - 1)];
}

export function pickConcurrentAt(prng: PRNG, checkpoints: Version[]): Version | undefined {
    return pickConcurrentAtRate(prng, checkpoints, 0.3);
}

export function cloneVersion(v: Version): Version {
    return new Set(v) as Version;
}

export async function collectExtendingPairs(
    rawDag: dag.Dag,
    checkpoints: Version[],
): Promise<Array<[number, number, Version, Version]>> {
    const pairs: Array<[number, number, Version, Version]> = [];

    for (let i = 0; i < checkpoints.length; i++) {
        for (let j = i + 1; j < checkpoints.length; j++) {
            const start = checkpoints[i];
            const end = checkpoints[j];
            if (await endExtendsStart(rawDag, start, end)) {
                pairs.push([i, j, start, end]);
            }
        }
    }

    return pairs;
}

export async function recordCheckpoint(
    checkpoints: Version[],
    frontier: Position,
): Promise<void> {
    checkpoints.push(cloneVersion(frontier as Version));
}

export function isExtendedParityProfile(): boolean {
    for (let i = 0; i < process.argv.length; i++) {
        if (process.argv[i] === "--profile" && process.argv[i + 1] === "extended") {
            return true;
        }
    }
    return false;
}
