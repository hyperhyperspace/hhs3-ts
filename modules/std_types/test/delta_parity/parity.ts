import { dag } from "@hyper-hyper-space/hhs3_dag";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RCap } from "../../src/types/rcap/rcap.js";
import type { RCapDelta } from "../../src/types/rcap/rcap.js";
import type { RSet } from "../../src/types/rset/rset.js";
import type { RSetDelta } from "../../src/types/rset/rset.js";

import { collectExtendingPairs } from "./checkpoints.js";
import { PRNG } from "./prng.js";
import {
    assertRCapDeltaParity,
    assertRSetDeltaParity,
    type DeltaParityContext,
} from "./normalize.js";

export type FuzzProfileName = "smoke" | "extended";

export type FuzzProfile = {
    seeds: number[];
    ops: number;
    maxPairs: number;
};

// Smoke: quick parity check on every `npm run test` (few seconds).
// Extended: heavier sweep for `npm run test:parity` (more seeds, longer histories, more pairs).
export const PARITY_PROFILES: Record<FuzzProfileName, FuzzProfile> = {
    smoke: { seeds: [1, 42], ops: 20, maxPairs: 40 },
    extended: {
        seeds: [1, 7, 42, 93, 1771, 9001, 31415, 271828],
        ops: 80,
        maxPairs: 200,
    },
};

const DEFAULT_PARITY_PROFILE: FuzzProfileName = "smoke";

export type ResolvedFuzzSweepOptions = FuzzProfile & {
    profile: FuzzProfileName;
};

export type FuzzSweepOptions = {
    profile?: FuzzProfileName;
    seeds?: number[];
    ops?: number;
    maxPairs?: number;
};

export function parseTestFilters(argv: string[]): string[] {
    const filters: string[] = [];

    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === "--seeds" || argv[i] === "--ops" || argv[i] === "--max-pairs" || argv[i] === "--profile") {
            i++;
            continue;
        }
        if (!argv[i].startsWith("--")) {
            filters.push(argv[i]);
        }
    }

    return filters;
}

function parseProfileName(value: string): FuzzProfileName {
    if (value === "smoke" || value === "extended") {
        return value;
    }
    throw new Error(`Unknown fuzz profile '${value}' (expected smoke or extended)`);
}

export function parseFuzzSweepOptions(argv: string[]): FuzzSweepOptions {
    const options: FuzzSweepOptions = {};

    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === "--profile" && argv[i + 1] !== undefined) {
            options.profile = parseProfileName(argv[i + 1]);
            i++;
        } else if (argv[i] === "--seeds" && argv[i + 1] !== undefined) {
            options.seeds = argv[i + 1].split(",").map((s) => parseInt(s, 10));
            i++;
        } else if (argv[i] === "--ops" && argv[i + 1] !== undefined) {
            options.ops = parseInt(argv[i + 1], 10);
            i++;
        } else if (argv[i] === "--max-pairs" && argv[i + 1] !== undefined) {
            options.maxPairs = parseInt(argv[i + 1], 10);
            i++;
        }
    }

    return options;
}

export function resolveFuzzSweepOptions(argv: string[]): ResolvedFuzzSweepOptions {
    const parsed = parseFuzzSweepOptions(argv);
    const profileName = parsed.profile
        ?? (process.env.PARITY_PROFILE !== undefined ? parseProfileName(process.env.PARITY_PROFILE) : DEFAULT_PARITY_PROFILE);
    const profile = PARITY_PROFILES[profileName];

    return {
        profile: profileName,
        seeds: parsed.seeds ?? profile.seeds,
        ops: parsed.ops ?? profile.ops,
        maxPairs: parsed.maxPairs ?? profile.maxPairs,
    };
}

function subsamplePairs<T>(
    pairs: T[],
    seed: number,
    maxPairs: number,
): T[] {
    if (pairs.length <= maxPairs) {
        return pairs;
    }

    const prng = new PRNG(seed ^ 0x9e3779b9);
    const chosen = new Set<number>();
    while (chosen.size < maxPairs) {
        chosen.add(prng.nextInt(0, pairs.length - 1));
    }

    return [...chosen].sort((a, b) => a - b).map((index) => pairs[index]);
}

async function computeRCapDelta(
    cap: RCap,
    strategy: "full" | "bounded",
    start: Version,
    end: Version,
): Promise<RCapDelta> {
    cap.setDeltaStrategy(strategy);
    return await cap.computeDelta(start, end) as RCapDelta;
}

async function computeRSetDelta(
    rset: RSet,
    strategy: "full" | "bounded",
    start: Version,
    end: Version,
): Promise<RSetDelta> {
    rset.setDeltaStrategy(strategy);
    return await rset.computeDelta(start, end) as RSetDelta;
}

export async function compareRCapDeltaStrategies(
    cap: RCap,
    start: Version,
    end: Version,
    ctx: DeltaParityContext,
): Promise<void> {
    const bounded = await computeRCapDelta(cap, "bounded", start, end);
    const full = await computeRCapDelta(cap, "full", start, end);
    assertRCapDeltaParity(bounded, full, ctx);
}

export async function compareRSetDeltaStrategies(
    rset: RSet,
    start: Version,
    end: Version,
    ctx: DeltaParityContext,
): Promise<void> {
    const bounded = await computeRSetDelta(rset, "bounded", start, end);
    const full = await computeRSetDelta(rset, "full", start, end);
    assertRSetDeltaParity(bounded, full, ctx);
}

export type RCapHistory = {
    cap: RCap;
    rawDag: dag.Dag;
    checkpoints: Version[];
    seed: number;
};

export type RSetHistory = {
    rset: RSet;
    rawDag: dag.Dag;
    checkpoints: Version[];
    seed: number;
};

export async function runRCapSeedSweep(
    generate: (seed: number, ops: number) => Promise<RCapHistory>,
    options: ResolvedFuzzSweepOptions,
): Promise<void> {
    for (const seed of options.seeds) {
        const history = await generate(seed, options.ops);
        const pairs = subsamplePairs(
            await collectExtendingPairs(history.rawDag, history.checkpoints),
            seed,
            options.maxPairs,
        );

        for (const [startIdx, endIdx, start, end] of pairs) {
            await compareRCapDeltaStrategies(history.cap, start, end, {
                seed,
                opIndex: endIdx,
                start,
                end,
            });
            process.stdout.write(".");
        }

        process.stdout.write(`\n  seed=${seed} pairs=${pairs.length} (max ${options.maxPairs})\n`);
    }
}

export async function runRSetSeedSweep(
    generate: (seed: number, ops: number) => Promise<RSetHistory>,
    options: ResolvedFuzzSweepOptions,
): Promise<void> {
    for (const seed of options.seeds) {
        const history = await generate(seed, options.ops);
        const pairs = subsamplePairs(
            await collectExtendingPairs(history.rawDag, history.checkpoints),
            seed,
            options.maxPairs,
        );

        for (const [startIdx, endIdx, start, end] of pairs) {
            await compareRSetDeltaStrategies(history.rset, start, end, {
                seed,
                opIndex: endIdx,
                start,
                end,
            });
            process.stdout.write(".");
        }

        process.stdout.write(`\n  seed=${seed} pairs=${pairs.length} (max ${options.maxPairs})\n`);
    }
}
