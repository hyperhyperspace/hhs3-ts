// Generative projection-parity suite: drive a real MaterializationTarget
// through generateProjectHistory and assert ProjectionReader read-back equals
// the rdb live projected view. rdb is the oracle; the target is not privileged.

import { writeSync } from "node:fs";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    KeyIndex, RowIdentityIndex, projectGroupTo,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    generateProjectHistory, endExtendsStart, subsamplePairs, mergeTallies,
    assertPathologicalCoverage, resolveFuzzSweepOptions, PROJECTION_PROFILES,
    type ResolvedFuzzSweepOptions, type Tallies,
} from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";

import { TargetFactory } from "./projection_reader.js";
import { fingerprintRdbProjectedRows, readerProjectedRows } from "./rdb_projection_oracle.js";

type NamedTest = { name: string; invoke: () => Promise<void> };

function tick(text: string): void {
    try {
        writeSync(process.stdout.fd, text);
    } catch {
        process.stdout.write(text);
    }
}

function asKeyIndex(target: object): KeyIndex | undefined {
    return typeof (target as KeyIndex).keyHashForId === 'function' ? target as KeyIndex : undefined;
}

function asRowIndex(target: object): RowIdentityIndex | undefined {
    return typeof (target as RowIdentityIndex).rowHashForLocalId === 'function'
        ? target as RowIdentityIndex : undefined;
}

async function extendingChain(
    rawDag: Parameters<typeof endExtendsStart>[0], checkpoints: Version[],
): Promise<Array<[number, Version]>> {
    const chain: Array<[number, Version]> = [];
    let last: Version | undefined;
    for (let i = 0; i < checkpoints.length; i++) {
        const cp = checkpoints[i];
        if (last === undefined || await endExtendsStart(rawDag, last, cp)) {
            chain.push([i, cp]);
            last = cp;
        }
    }
    return chain;
}

export function createProjectionParitySuite(
    label: string,
    factory: TargetFactory,
    options: ResolvedFuzzSweepOptions = resolveFuzzSweepOptions(process.argv.slice(2), PROJECTION_PROFILES),
): { title: string; tests: NamedTest[] } {
    return {
        title: `[${label}] rdb_adapter projection`,
        tests: [
            {
                name: `[${label}-PROJECTION] projectGroupTo vs rdb view (${options.profile})`,
                invoke: async () => {
                    const combined: Tallies = {};
                    for (const seed of options.seeds) {
                        tick(`  [${label}] seed=${seed} gen `);
                        const history = await generateProjectHistory(seed, options.ops, () => tick('.'));
                        mergeTallies(combined, history.tallies);
                        const chain = subsamplePairs(
                            await extendingChain(history.rawDag, history.checkpoints),
                            seed, options.maxPairs,
                        );
                        const { target, read, cleanup } = await factory();
                        try {
                            tick(' proj ');
                            for (const [idx, cp] of chain) {
                                await projectGroupTo(history.group, target, cp);
                                const view = await history.group.getView(cp, cp);
                                const rdbFp = await fingerprintRdbProjectedRows(view);
                                const readerFp = await readerProjectedRows(read, {
                                    schemaView: view.getSchemaView(),
                                    keyIndex: asKeyIndex(target),
                                    rowIndex: asRowIndex(target),
                                });
                                if (rdbFp !== readerFp) {
                                    tick('\n');
                                    throw new Error(
                                        `kind=projection seed=${seed} checkpoint=${idx} `
                                        + `profile=${options.profile}\n`
                                        + `rdb=${rdbFp}\nreader=${readerFp}\n`
                                        + `tallies=${JSON.stringify(history.tallies)}\nlog:\n`
                                        + history.opLog.join('\n'),
                                    );
                                }
                                tick('.');
                            }
                            tick(
                                ` checkpoints=${chain.length} `
                                + `tallies=${JSON.stringify(history.tallies)}\n`,
                            );
                        } finally {
                            await cleanup?.();
                        }
                    }
                    assertPathologicalCoverage(
                        combined,
                        `kind=projection label=${label} profile=${options.profile}`,
                    );
                },
            },
        ],
    };
}
