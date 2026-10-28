import { resolveFuzzSweepOptions, runRCapSeedSweep } from "./parity.js";
import { generateRCapHistory } from "./rcap_generate.js";
import { generateShapedRCapHistories } from "./rcap_shaped_generate.js";

export const rcapDeltaParityTests = {
    title: "[DELTA_PARITY] RCap computeDelta parity (bounded vs full)",
    tests: [
        {
            name: "[DELTA_PARITY_RCAP] bounded and full computeDelta agree on random grant/revoke/cap histories",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runRCapSeedSweep(generateRCapHistory, options);
            },
        },
        {
            name: "[DELTA_PARITY_RCAP_SHAPED] bounded and full computeDelta agree on the revoke-chain skeleton with randomized barriers",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runRCapSeedSweep(generateShapedRCapHistories, options);
            },
        },
    ],
};
