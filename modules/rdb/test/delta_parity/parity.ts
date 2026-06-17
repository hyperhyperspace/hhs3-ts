import { dag } from "@hyper-hyper-space/hhs3_dag";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RTableGroupImpl } from "../../src/rtable_group/group.js";
import type { RTableGroupDelta } from "../../src/rtable_group/delta.js";

import { collectExtendingPairs } from "./checkpoints.js";
import { PRNG } from "./prng.js";
import { assertGroupDeltaParity, type DeltaParityContext } from "./normalize.js";

export type FuzzProfileName = "smoke" | "extended";

export type FuzzProfile = { seeds: number[]; ops: number; maxPairs: number };

// Smoke: quick parity check on every `npm test`. Extended: heavier sweep for
// `npm run test:parity`.
export const PARITY_PROFILES: Record<FuzzProfileName, FuzzProfile> = {
    smoke: { seeds: [1, 42], ops: 18, maxPairs: 32 },
    extended: { seeds: [1, 7, 42, 93, 1771, 9001, 31415], ops: 60, maxPairs: 160 },
};

const DEFAULT_PARITY_PROFILE: FuzzProfileName = "smoke";

export type ResolvedFuzzSweepOptions = FuzzProfile & { profile: FuzzProfileName };

export function parseTestFilters(argv: string[]): string[] {
    const filters: string[] = [];
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === "--seeds" || argv[i] === "--ops" || argv[i] === "--max-pairs" || argv[i] === "--profile") {
            i++;
            continue;
        }
        if (!argv[i].startsWith("--")) filters.push(argv[i]);
    }
    return filters;
}

function parseProfileName(value: string): FuzzProfileName {
    if (value === "smoke" || value === "extended") return value;
    throw new Error(`Unknown fuzz profile '${value}' (expected smoke or extended)`);
}

export function resolveFuzzSweepOptions(argv: string[]): ResolvedFuzzSweepOptions {
    let profileName: FuzzProfileName | undefined;
    let seeds: number[] | undefined;
    let ops: number | undefined;
    let maxPairs: number | undefined;

    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === "--profile" && argv[i + 1] !== undefined) { profileName = parseProfileName(argv[++i]); }
        else if (argv[i] === "--seeds" && argv[i + 1] !== undefined) { seeds = argv[++i].split(",").map((s) => parseInt(s, 10)); }
        else if (argv[i] === "--ops" && argv[i + 1] !== undefined) { ops = parseInt(argv[++i], 10); }
        else if (argv[i] === "--max-pairs" && argv[i + 1] !== undefined) { maxPairs = parseInt(argv[++i], 10); }
    }

    const name = profileName
        ?? (process.env.PARITY_PROFILE !== undefined ? parseProfileName(process.env.PARITY_PROFILE) : DEFAULT_PARITY_PROFILE);
    const profile = PARITY_PROFILES[name];

    return {
        profile: name,
        seeds: seeds ?? profile.seeds,
        ops: ops ?? profile.ops,
        maxPairs: maxPairs ?? profile.maxPairs,
    };
}

function subsamplePairs<T>(pairs: T[], seed: number, maxPairs: number): T[] {
    if (pairs.length <= maxPairs) return pairs;
    const prng = new PRNG(seed ^ 0x9e3779b9);
    const chosen = new Set<number>();
    while (chosen.size < maxPairs) chosen.add(prng.nextInt(0, pairs.length - 1));
    return [...chosen].sort((a, b) => a - b).map((index) => pairs[index]);
}

async function computeGroupDelta(
    group: RTableGroupImpl, strategy: "full" | "bounded", start: Version, end: Version,
): Promise<RTableGroupDelta> {
    group.setDeltaStrategy(strategy);
    return group.computeDelta(start, end);
}

export async function compareGroupDeltaStrategies(
    group: RTableGroupImpl, start: Version, end: Version, ctx: DeltaParityContext,
): Promise<void> {
    const bounded = await computeGroupDelta(group, "bounded", start, end);
    const full = await computeGroupDelta(group, "full", start, end);
    assertGroupDeltaParity(bounded, full, ctx);
}

export type GroupHistory = {
    group: RTableGroupImpl;
    rawDag: dag.Dag;
    checkpoints: Version[];
    seed: number;
};

export async function runGroupSeedSweep(
    generate: (seed: number, ops: number) => Promise<GroupHistory>,
    options: ResolvedFuzzSweepOptions,
): Promise<void> {
    for (const seed of options.seeds) {
        const history = await generate(seed, options.ops);
        const pairs = subsamplePairs(
            await collectExtendingPairs(history.rawDag, history.checkpoints),
            seed, options.maxPairs,
        );

        for (const [, endIdx, start, end] of pairs) {
            await compareGroupDeltaStrategies(history.group, start, end, { seed, opIndex: endIdx, start, end });
            process.stdout.write(".");
        }

        process.stdout.write(`\n  seed=${seed} pairs=${pairs.length} (max ${options.maxPairs})\n`);
    }
}
