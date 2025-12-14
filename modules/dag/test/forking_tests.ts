import { dag, ForkPosition, Position } from "../src/index";

import { assertTrue, run } from "@hyper-hyper-space/hhs3_util/dist/test";

import {
    appendNodesToDag,
    createBranchingDag,
    createD1,
    createD3,
    createRandomBranchingDags,
    createRandomDag,
    createRandomDags,
    createRandomDisconnectedDags
} from "./utils/dag_create";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { set } from "@hyper-hyper-space/hhs3_util";
import { json } from "@hyper-hyper-space/hhs3_json";

import { draw, graph, label } from "./utils/dag_diagram";
import { MemLevelIndexStore } from "../src/idx/level/level_idx_mem_store";

const pp = (ns: Set<Hash>) => Array.from(ns).map(label).sort();

const showForkPosition = (fp: ForkPosition) => {
    console.log("common:        ", pp(fp.common));
    console.log("commonFrontier:", pp(fp.commonFrontier));
    console.log("forkA:         ", pp(fp.forkA));
    console.log("forkB:         ", pp(fp.forkB));
};

const showBranches = (a: Position, b: Position) => {
    console.log("branch a", pp(a));
    console.log("branch b", pp(b));
};

const forkPositionsMatch = (f1: ForkPosition, f2: ForkPosition) => {
    if (!set.eq(f1.common, f2.common)) {
        return false;
    }

    if (!set.eq(f1.commonFrontier, f2.commonFrontier)) {
        return false;
    }

    if (!set.eq(f1.forkA, f2.forkA)) {
        return false;
    }

    if (!set.eq(f1.forkB, f2.forkB)) {
        return false;
    }

    return true;
};

const topoDagPairConstr: [() => dag.Dag, () => dag.Dag] = [
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

export const levelDagPairConstr: [() => dag.Dag, () => dag.Dag] = [
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
            new dag.idx.level.mem.MemLevelIndexStore({ levelFactor: 4 })
        );
        return dag.create(store, index);
    }
];

// const mediumForkingDags = createRandomForkingDags(seed+1, 1000);
// const largeForkingDags = createRandomForkingDags(seed+2, 5000);

function stats(values: number[]): { avg: number; p85: number } {
    if (values.length === 0) {
        throw new Error("Array must not be empty");
    }

    // average
    const avg = values.reduce((sum, v) => sum + v, 0) / values.length;

    // percentile
    const sorted = [...values].sort((a, b) => a - b);
    const rank = 0.85 * (sorted.length - 1);
    const lower = Math.floor(rank);
    const upper = Math.ceil(rank);

    // linear interpolation if needed
    const p85 =
        lower === upper
            ? sorted[lower]
            : sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower);

    return { avg, p85 };
}

export const testForkingDags = async (
    cases: { dags: Array<[dag.Dag, dag.Dag]>; branches: Array<[Position, Position]> },
    options?: { timings: boolean; timingLabels?: [string, string] }
) => {
    const timings1 = new Array<number>();
    const timings2 = new Array<number>();
    for (let i = 0; i < cases.dags.length; i++) {
        const [d1, d2] = cases.dags[i];
        const [a, b] = cases.branches[i];
        const start1 = performance.now();
        const f1 = await d1.findForkPosition(a, b);
        const end1 = performance.now();
        const start2 = performance.now();
        const f2 = await d2.findForkPosition(a, b);
        const end2 = performance.now();

        timings1.push(end1 - start1);
        timings2.push(end2 - start2);

        const commonMatches = set.eq(f1.common, f2.common);
        const commonFrontierMatches = set.eq(f1.commonFrontier, f2.commonFrontier);
        const forkAMatches = set.eq(f1.forkA, f2.forkA);
        const forkBMatches = set.eq(f1.forkB, f2.forkB);

        const match = commonMatches && commonFrontierMatches && forkAMatches && forkBMatches;

        if (!match) {
            console.log("branches:");
            showBranches(a, b);
            console.log("fork position for d1:");
            showForkPosition(f1);
            console.log("fork position for d2:");
            showForkPosition(f2);
            console.log("saving failing graph to g.dot, g.png");

            const levelTags = new Map<Hash, string>();
            const levels = new Set<number>();

            const idxStore = d2.getIndex().getIndexStore();

            if (idxStore instanceof MemLevelIndexStore) {
                for await (const e of d2.loadAllEntries()) {
                    const level = (await idxStore.getEntryInfo(e.hash)).level;
                    levelTags.set(e.hash, "L" + level);
                    if (level < Number.MAX_SAFE_INTEGER) {
                        levels.add(level);
                    }
                }

                for (const level of levels) {
                    const filter = async (h: Hash) =>
                        (await idxStore.getEntryInfo(h)).level >= level;
                    const prev = async (h: Hash) => idxStore.getPreds(level, h);

                    await draw(d1, "d_" + level, { filter: filter, prev: prev });
                }
            }

            await draw(d1, "d", { namedSets: [["a", a], ["b", b]], tags: levelTags });
        }

        assertTrue(commonMatches, "common mismatch for i=" + i);
        assertTrue(commonFrontierMatches, "commonFrontier mismatch for i=" + i);
        assertTrue(forkAMatches, "forkA mismatch for i=" + i);
        assertTrue(forkBMatches, "forkB mismatch for i=" + i);

        process.stdout.write(".");
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

const createSuite = (tag: string, title: string, constrs: [() => dag.Dag, () => dag.Dag]) => ({
    title: "[" + tag + "] " + title,
    tests: [
        {
            name: "[" + tag + "00] Testing forking on small random branching DAGs",
            invoke: async () => {
                const seed = 93;
                console.log("Generating test cases");
                const cases = await createRandomBranchingDags(constrs, seed, 1000, {
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "01] Testing forking on medium random branching DAGs",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 666;
                const cases = await createRandomBranchingDags(constrs, seed, 2000, {
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "02] Testing forking on large random branching DAGs",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 999;
                const cases = await createRandomBranchingDags(constrs, seed, 20000, {
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "03] Testing forking on small random DAGs",
            invoke: async () => {
                const seed = 33;
                console.log("Generating test cases");
                const cases = await createRandomDags(constrs, seed, 1000, {
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "04] Testing forking on medium random DAGs",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 66;
                const cases = await createRandomDags(constrs, seed, 5000, {
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "05] Testing forking on large random DAGs",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 99;
                const cases = await createRandomDags(constrs, seed, 20000, {
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "06] Testing forking on small disconnected DAGs w/connected branches",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 31;
                const cases = await createRandomDisconnectedDags(constrs, seed, 1000, {
                    connectedBranches: true,
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "07] Testing forking on medium disconnected DAGs w/connected branches",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 61;
                const cases = await createRandomDisconnectedDags(constrs, seed, 5000, {
                    connectedBranches: true,
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "08] Testing forking on large disconnected DAGs w/connected branches",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 91;
                const cases = await createRandomDisconnectedDags(constrs, seed, 20000, {
                    connectedBranches: true,
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "09] Testing forking on small disconnected DAGs w/disconnected branches",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 31;
                const cases = await createRandomDisconnectedDags(constrs, seed, 1000, {
                    connectedBranches: false,
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "10] Testing forking on medium disconnected DAGs w/disconnected branches",
            invoke: async () => {
                console.log("Generating test cases");
                const seed = 61;
                const cases = await createRandomDisconnectedDags(constrs, seed, 5000, {
                    connectedBranches: false,
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        },
        {
            name: "[" + tag + "11] Testing forking on large disconnected DAGs w/disconnected branches",
            invoke: async () => {
                console.log("Generating test cases");
                const d1 = topoDagPairConstr[0]();

                const seed = 91;
                const cases = await createRandomDisconnectedDags(constrs, seed, 20000, {
                    connectedBranches: false,
                    progressBar: true
                });
                console.log("Testing");
                await testForkingDags(cases);
            }
        }
    ]
});

const topoSuite = createSuite(
    "FORK_TOPO_",
    "Test Topological Fork Analysis Solution",
    topoDagPairConstr
);
const levelSuite = createSuite(
    "FORK_LEVEL_",
    "Test Level-index Fork Analysis Solution",
    levelDagPairConstr
);

export { topoSuite, levelSuite };

