import { dag, Position } from "../src/index";

import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test";
import { set } from "@hyper-hyper-space/hhs3_util";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";

import { createRandomBranchingDags, createRandomDags } from "./utils/dag_create";
import { draw, label } from "./utils/dag_diagram";

const pp = (ns: Set<Hash>) => Array.from(ns).map(label).sort();

function stats(values: number[]): { avg: number; p85: number } {
    if (values.length === 0) {
        throw new Error("Array must not be empty");
    }

    const avg = values.reduce((sum, v) => sum + v, 0) / values.length;

    const sorted = [...values].sort((a, b) => a - b);
    const rank = 0.85 * (sorted.length - 1);
    const lower = Math.floor(rank);
    const upper = Math.ceil(rank);

    const p85 =
        lower === upper
            ? sorted[lower]
            : sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower);

    return { avg, p85 };
}

export const testMinimalCoverDags = async (
    cases: { dags: Array<[dag.Dag, dag.Dag]>; branches: Array<[Position, Position]> },
    options?: { timings: boolean; timingLabels?: [string, string] }
) => {
    const timings1 = new Array<number>();
    const timings2 = new Array<number>();

    for (let i = 0; i < cases.dags.length; i++) {
        const [d1, d2] = cases.dags[i];
        const [b1, b2] = cases.branches[i];

        const positions: Position[] = [
            b1,
            b2,
            new Set<Hash>([...b1, ...b2]),
            await d1.getFrontier()
        ];

        for (let j = 0; j < positions.length; j++) {
            const p = positions[j];

            //console.log('testing case', i, 'position', j, 'with position', pp(p as Set<Hash>));

            //if (i!==7 || j!==0) {
            //    continue;
            //}

            //if (i==1 && j==2) {
            //    await draw(d1, "d1_" + i + "_" + j);
            //}

            const start1 = performance.now();
            const cover1 = await d1.findMinimalCover(p);
            const end1 = performance.now();

            const start2 = performance.now();
            const cover2 = await d2.findMinimalCover(p);
            const end2 = performance.now();

            timings1.push(end1 - start1);
            timings2.push(end2 - start2);

            const coversMatch = set.eq(cover1, cover2);
            if (!coversMatch) {
                console.log("--- minimal cover mismatch ---");
                console.log("case idx:", i, "position idx:", j);
                console.log("position:", pp(p as Set<Hash>));
                console.log("cover1:", pp(cover1 as Set<Hash>));
                console.log("cover2:", pp(cover2 as Set<Hash>));
                await draw(d1, "d1_" + i + "_" + j);
            }

            assertTrue(coversMatch, `minimal cover mismatch for case ${i} position ${j}`);
            process.stdout.write(".");
        }
    }

    const stats1 = stats(timings1);
    const stats2 = stats(timings2);

    if (options?.timings) {
        console.log();
        console.log(
            (options?.timingLabels !== undefined ? options?.timingLabels[0] : "Baseline") +
                " timings: avg=" +
                stats1.avg.toFixed(1) +
                ", p85=" +
                stats1.p85.toFixed(1)
        );
        console.log(
            (options?.timingLabels !== undefined ? options?.timingLabels[1] : "Tested") +
                " timings:   avg=" +
                stats2.avg.toFixed(1) +
                ", p85=" +
                stats2.p85.toFixed(1)
        );
        console.log("avg speedup ", (stats1.avg / stats2.avg).toFixed(1) + "X");
    }

    process.stdout.write("\n");
};

export const flatTopoPairConstr: [() => dag.Dag, () => dag.Dag] = [
    () => {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.flat.createFlatIndex(
            store,
            new dag.idx.flat.mem.MemFlatIndexStore()
        );
        return dag.create(store, index);
    },
    () => {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.topo.createDagTopoIndex(
            store,
            new dag.idx.topo.mem.MemTopoIndexStore()
        );
        return dag.create(store, index);
    }
];

export const topoLevelPairConstr: [() => dag.Dag, () => dag.Dag] = [
    () => {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.topo.createDagTopoIndex(
            store,
            new dag.idx.topo.mem.MemTopoIndexStore()
        );
        return dag.create(store, index);
    },
    () => {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.level.createDagLevelIndex(
            store,
            new dag.idx.level.mem.MemLevelIndexStore({ levelFactor: 8 })
        );
        return dag.create(store, index);
    }
];

const coverTestSuite = {
    title: "[COVER] Minimal cover parity tests",

    tests: [
        {
            name: "[COVER_00] Minimal cover parity flat vs topo (small random DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover test cases");
                const cases = await createRandomDags(flatTopoPairConstr, seed, 1000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_01] Minimal cover parity flat vs topo (medium random DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover test cases");
                const cases = await createRandomDags(flatTopoPairConstr, seed, 5000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_02] Minimal cover parity flat vs topo (large random DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover test cases");
                const cases = await createRandomDags(flatTopoPairConstr, seed, 10000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_03] Minimal cover parity flat vs topo (small random branching DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover test cases");
                const cases = await createRandomBranchingDags(flatTopoPairConstr, seed, 1000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_04] Minimal cover parity flat vs topo (medium random branching DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover test cases");
                const cases = await createRandomBranchingDags(flatTopoPairConstr, seed, 5000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_05] Minimal cover parity flat vs topo (large random branching DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover test cases");
                const cases = await createRandomBranchingDags(flatTopoPairConstr, seed, 10000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_06] Minimal cover parity topo vs level (small random DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover test cases");
                const cases = await createRandomDags(topoLevelPairConstr, seed, 1000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_07] Minimal cover parity topo vs level (medium random DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover test cases");
                const cases = await createRandomDags(topoLevelPairConstr, seed, 5000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_08] Minimal cover parity topo vs level (large random DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover test cases");
                const cases = await createRandomDags(topoLevelPairConstr, seed, 10000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_09] Minimal cover parity topo vs level (small random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover test cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 1000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_10] Minimal cover parity topo vs level (medium random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover test cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 5000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
        {
            name: "[COVER_11] Minimal cover parity topo vs level (large random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover test cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 10000, {
                    progressBar: true
                });
                console.log("Testing minimal cover parity");
                await testMinimalCoverDags(cases);
            }
        },
    ]
};

export { coverTestSuite };
