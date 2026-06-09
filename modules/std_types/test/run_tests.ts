import { simpleSetTests } from "./simple_set_tests.js";
import { nestedSetTests } from "./nested_set_tests.js";
import { authorshipTests } from "./authorship_tests.js";
import { rcapTests } from "./rcap_tests.js";
import { permissionedSetTests } from "./permissioned_set_tests.js";
import { deltaTests } from "./delta_set_tests.js";
import { deltaCapTests } from "./delta_cap_tests.js";
import { branchyDeltaParityTests } from "./delta_parity/branchy_parity_tests.js";
import { rcapDeltaParityTests } from "./delta_parity/rcap_parity_tests.js";
import { rsetDeltaParityTests } from "./delta_parity/rset_parity_tests.js";
import { parseTestFilters } from "./delta_parity/parity.js";
import { testing } from "@hyper-hyper-space/hhs3_util";

async function main() {

    const allTests = new Map<string, Array<{name: string, invoke: () => Promise<void>}>>();

    const filters = parseTestFilters(process.argv.slice(2));

    allTests.set(simpleSetTests.title, simpleSetTests.tests);
    allTests.set(nestedSetTests.title, nestedSetTests.tests);
    allTests.set(authorshipTests.title, authorshipTests.tests);
    allTests.set(rcapTests.title, rcapTests.tests);
    allTests.set(permissionedSetTests.title, permissionedSetTests.tests);
    allTests.set(deltaTests.title, deltaTests.tests);
    allTests.set(deltaCapTests.title, deltaCapTests.tests);
    allTests.set(rcapDeltaParityTests.title, rcapDeltaParityTests.tests);
    allTests.set(rsetDeltaParityTests.title, rsetDeltaParityTests.tests);
    allTests.set(branchyDeltaParityTests.title, branchyDeltaParityTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 std_types module' + (filters.length > 0? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');    

    for (const [title, tests] of allTests.entries()) {
        console.log(title);

        for (const test of tests) {

            let match = true;
            for (const filter of filters) {
                match = match && (test.name.indexOf(filter) >= 0)
            }

            if (match) {
                testing.exitIfFailed(await testing.run(test.name, test.invoke));
            } else {
                await testing.skip(test.name);
            }
        }

        console.log();
    }
}

main();
