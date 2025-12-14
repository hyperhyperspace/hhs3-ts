import { testMinimalCoverDags, flatTopoPairConstr, topoLevelPairConstr } from "./cover_tests";
import { createRandomBranchingDags } from "./utils/dag_create";

const coverBenchmarkSuite = {
    title: "[COVER_BENCH] Minimal Cover finding benchmarks for long branching random DAGs",

    tests: [
        {
            name: "[COVER_BENCH_00] Minimal cover: flat vs topo (1,000 node random branching DAGs)",
            invoke: async () => {
                const seed = 123;
                console.log("Generating flat vs topo cover benchmark cases");
                const cases = await createRandomBranchingDags(flatTopoPairConstr, seed, 1000, {
                    progressBar: true
                });
                console.log("Benchmarking minimal cover parity");
                await testMinimalCoverDags(cases, {
                    timings: true,
                    timingLabels: ["Flat index", "Topological index"]
                });
            }
        },
        {
            name: "[COVER_BENCH_01] Minimal cover: flat vs topo (5,000 node random branching DAGs)",
            invoke: async () => {
                const seed = 124;
                console.log("Generating flat vs topo cover benchmark cases");
                const cases = await createRandomBranchingDags(flatTopoPairConstr, seed, 5000, {
                    progressBar: true,
                    instanceCount: 10
                });
                console.log("Benchmarking minimal cover parity");
                await testMinimalCoverDags(cases, {
                    timings: true,
                    timingLabels: ["Flat index", "Topological index"]
                });
            }
        },
        {
            name: "[COVER_BENCH_02] Minimal cover: topo vs level (10,000 node random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover benchmark cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 10000, {
                    progressBar: true
                });
                console.log("Benchmarking minimal cover parity");
                await testMinimalCoverDags(cases, {
                    timings: true,
                    timingLabels: ["Topological index", "Level index"]
                });
            }
        },
        {
            name: "[COVER_BENCH_03] Minimal cover: topo vs level (20,000 node random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover benchmark cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 20000, {
                    progressBar: true,
                    instanceCount: 5
                });
                console.log("Benchmarking minimal cover parity");
                await testMinimalCoverDags(cases, {
                    timings: true,
                    timingLabels: ["Topological index", "Level index"]
                });
            }
        },
        {
            name: "[COVER_BENCH_04] Minimal cover: topo vs level (50,000 node random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover benchmark cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 50000, {
                    progressBar: true,
                    instanceCount: 4
                });
                console.log("Benchmarking minimal cover parity");
                await testMinimalCoverDags(cases, {
                    timings: true,
                    timingLabels: ["Topological index", "Level index"]
                });
            }
        },
        {
            name: "[COVER_BENCH_05] Minimal cover: topo vs level (100,000 node random branching DAGs)",
            invoke: async () => {
                const seed = 456;
                console.log("Generating topo vs level cover benchmark cases");
                const cases = await createRandomBranchingDags(topoLevelPairConstr, seed, 100000, {
                    progressBar: true,
                    instanceCount: 3
                });
                console.log("Benchmarking minimal cover parity");
                await testMinimalCoverDags(cases, {
                    timings: true,
                    timingLabels: ["Topological index", "Level index"]
                });
            }
        }
    ]
};

export { coverBenchmarkSuite };
