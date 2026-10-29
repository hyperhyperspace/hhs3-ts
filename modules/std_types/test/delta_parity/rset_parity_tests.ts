import { resolveFuzzSweepOptions, runRSetSeedSweep } from "./parity.js";
import { generatePermissionedRSetHistory, generatePlainRSetHistory } from "./rset_generate.js";
import { generateShapedRSetHistories } from "./rset_shaped_generate.js";

export const rsetDeltaParityTests = {
    title: "[DELTA_PARITY] RSet computeDelta parity (bounded vs full)",
    tests: [
        {
            name: "[DELTA_PARITY_RSET_PLAIN] bounded and full computeDelta agree on random add/delete histories",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runRSetSeedSweep(generatePlainRSetHistory, options);
            },
        },
        {
            name: "[DELTA_PARITY_RSET_PERM] bounded and full computeDelta agree with co-evolving RCap (ref-advance, signed ops)",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runRSetSeedSweep(generatePermissionedRSetHistory, options);
            },
        },
        {
            name: "[DELTA_PARITY_RSET_SHAPED] bounded and full computeDelta agree on branching sets over an RCap whose verdicts depend on the horizon",
            invoke: async () => {
                const options = resolveFuzzSweepOptions(process.argv.slice(2));
                await runRSetSeedSweep(generateShapedRSetHistories, options);
            },
        },
    ],
};
