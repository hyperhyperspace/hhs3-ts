import { runIngestPlannerFromArgv } from "./ingest_parity.js";
import { runProjectPlannerFromArgv } from "./project_parity.js";

export const plannerParityTests = {
    title: '[PLANNER] rdb_adapter planner-parity fuzzer',
    tests: [
        {
            name: '[PLANNER] project planner full vs incremental vs rdb view',
            invoke: async () => {
                await runProjectPlannerFromArgv();
            },
        },
        {
            name: '[PLANNER] ingest planner naive vs optimized',
            invoke: async () => {
                await runIngestPlannerFromArgv();
            },
        },
    ],
};
