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
            name: '[PLANNER] ingest planner: one op, refusal or mark per change, and the plan applies',
            invoke: async () => {
                await runIngestPlannerFromArgv();
            },
        },
    ],
};
