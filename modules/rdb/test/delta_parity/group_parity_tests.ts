import { resolveFuzzSweepOptions, runGroupSeedSweep } from "./parity.js";
import { generateSingleGroupHistory, generateCrossGroupHistory } from "./generate.js";

export const groupDeltaParityTests = {
    title: "[DELTA_PARITY] RTableGroup computeDelta parity (bounded vs full)",
    tests: [
        {
            name: "[DELTA_PARITY_GROUP] bounded and full computeDelta agree on random rows/bundles/deploys (single group)",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runGroupSeedSweep(generateSingleGroupHistory, options);
            },
        },
        {
            name: "[DELTA_PARITY_XGROUP] bounded and full computeDelta agree with a co-evolving bound foreign group (multi-observer bound)",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runGroupSeedSweep(generateCrossGroupHistory, options);
            },
        },
    ],
};
