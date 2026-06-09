import {
    generateBranchyPermissionedRSetHistory,
    generateBranchyPlainRSetHistory,
    generateBranchyRCapHistory,
} from "./branchy_generate.js";
import { resolveFuzzSweepOptions, runRCapSeedSweep, runRSetSeedSweep } from "./parity.js";

export const branchyDeltaParityTests = {
    title: "[DELTA_PARITY] Branchy computeDelta parity (extended profile only)",
    tests: [
        {
            name: "[DELTA_PARITY_BRANCHY_RCAP] bounded and full agree on branchy grant/revoke/cap histories",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                if (options.profile !== "extended") {
                    console.log("  (skipped — requires --profile extended)");
                    return;
                }
                await runRCapSeedSweep(generateBranchyRCapHistory, options);
            },
        },
        {
            name: "[DELTA_PARITY_BRANCHY_RSET_PLAIN] bounded and full agree on branchy add/delete histories",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                if (options.profile !== "extended") {
                    console.log("  (skipped — requires --profile extended)");
                    return;
                }
                await runRSetSeedSweep(generateBranchyPlainRSetHistory, options);
            },
        },
        {
            name: "[DELTA_PARITY_BRANCHY_RSET_PERM] bounded and full agree on branchy cap/set episode histories",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                if (options.profile !== "extended") {
                    console.log("  (skipped — requires --profile extended)");
                    return;
                }
                await runRSetSeedSweep(generateBranchyPermissionedRSetHistory, options);
            },
        },
    ],
};
