import { testing } from "@hyper-hyper-space/hhs3_util";

import { createIngestionSuite, createKeysSuite, createProjectionSuite } from "@hyper-hyper-space/hhs3_rdb_adapter_test";
import { idbHarness, idbIngestionHarness, idbSpecificTests } from "./idb_target_tests.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = process.argv.slice(2);

    const sharedSuite = createProjectionSuite('IDB', idbHarness);
    allTests.set(sharedSuite.title, sharedSuite.tests);
    const ingestionSuite = createIngestionSuite('IDB', idbIngestionHarness);
    allTests.set(ingestionSuite.title, ingestionSuite.tests);
    const keysSuite = createKeysSuite('IDB', idbHarness);
    allTests.set(keysSuite.title, keysSuite.tests);
    allTests.set(idbSpecificTests.title, idbSpecificTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 rdb_adapter_idb module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    for (const [title, tests] of allTests.entries()) {
        console.log(title);

        for (const test of tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
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
