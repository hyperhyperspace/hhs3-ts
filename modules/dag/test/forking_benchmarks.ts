import { levelDagPairConstr, testForkingDags } from "./forking_tests";
import { createRandomBranchingDags } from "./utils/dag_create";

const benchmarkSuite = {
    title: "[FORK_BENCH] Fork Analysis Timings for long branching random DAGs",

    tests: [
        {
            name: "[FORK_BENCH_00] Testing on 10,000 node DAGs",
            invoke: async () => {
                const seed = 93;
                console.log("Generating benchmark cases");
                const cases = await createRandomBranchingDags(levelDagPairConstr, seed, 10000, {
                    progressBar: true
                });
                console.log("Running");
                await testForkingDags(cases, {
                    timings: true,
                    timingLabels: ["Topological search", "Multi-level indexed search"]
                });
            }
        },
        {
            name: "[FORK_BENCH_01] Testing on 20,000 node DAGs",
            invoke: async () => {
                const seed = 94;
                console.log("Generating benchmark cases");
                const cases = await createRandomBranchingDags(levelDagPairConstr, seed, 20000, {
                    progressBar: true,
                    instanceCount: 10
                });
                console.log("Running");
                await testForkingDags(cases, {
                    timings: true,
                    timingLabels: ["Topological search", "Multi-level indexed search"]
                });
            }
        },
        {
            name: "[FORK_BENCH_02] Testing on 50,000 node DAGs",
            invoke: async () => {
                const seed = 95;
                console.log("Generating benchmark cases");
                const cases = await createRandomBranchingDags(levelDagPairConstr, seed, 50000, {
                    progressBar: true,
                    instanceCount: 5
                });
                console.log("Running");
                await testForkingDags(cases, {
                    timings: true,
                    timingLabels: ["Topological search", "Multi-level indexed search"]
                });
            }
        },
        {
            name: "[FORK_BENCH_03] Testing on 100,000 node DAGs",
            invoke: async () => {
                const seed = 93;
                console.log("Generating benchmark cases");
                const cases = await createRandomBranchingDags(levelDagPairConstr, seed, 100000, {
                    progressBar: true,
                    instanceCount: 4
                });
                console.log("Running");
                await testForkingDags(cases, {
                    timings: true,
                    timingLabels: ["Topological search", "Multi-level indexed search"]
                });
            }
        }
    ]
};

export { benchmarkSuite };
