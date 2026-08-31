import { PRNG } from "../../../rdb/test/delta_parity/prng.js";

export type FuzzProfileName = "smoke" | "fast" | "full";

export type FuzzProfile = {
    seeds: number[];
    ops: number;
    maxPairs: number;
    ingestBatches: number;
    ingestChanges: number;
};

export const PARITY_PROFILES: Record<FuzzProfileName, FuzzProfile> = {
    smoke: { seeds: [1, 42], ops: 18, maxPairs: 32, ingestBatches: 4, ingestChanges: 24 },
    fast: { seeds: [1, 42, 9001], ops: 30, maxPairs: 60, ingestBatches: 6, ingestChanges: 32 },
    full: {
        seeds: [1, 7, 42, 93, 1771, 9001, 31415],
        ops: 60, maxPairs: 160, ingestBatches: 8, ingestChanges: 48,
    },
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
    if (value === "smoke" || value === "fast" || value === "full") return value;
    throw new Error(`Unknown fuzz profile '${value}' (expected smoke, fast or full)`);
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
        ingestBatches: profile.ingestBatches,
        ingestChanges: profile.ingestChanges,
    };
}

export function subsamplePairs<T>(pairs: T[], seed: number, maxPairs: number): T[] {
    if (pairs.length <= maxPairs) return pairs;
    const prng = new PRNG(seed ^ 0x9e3779b9);
    const chosen = new Set<number>();
    while (chosen.size < maxPairs) chosen.add(prng.nextInt(0, pairs.length - 1));
    return [...chosen].sort((a, b) => a - b).map((index) => pairs[index]);
}
