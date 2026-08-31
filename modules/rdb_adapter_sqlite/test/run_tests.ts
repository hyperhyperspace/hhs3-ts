import { testing } from "@hyper-hyper-space/hhs3_util";

import { createIngestionSuite, createKeysSuite, createProjectionSuite } from "@hyper-hyper-space/hhs3_rdb_adapter_test";
import { sqliteHarness, sqliteIngestionHarness, sqliteSpecificTests } from "./sqlite_target_tests.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = process.argv.slice(2);

    const sharedSuite = createProjectionSuite('SQLITE', sqliteHarness);
    allTests.set(sharedSuite.title, sharedSuite.tests);
    const ingestionSuite = createIngestionSuite('SQLITE', sqliteIngestionHarness);
    allTests.set(ingestionSuite.title, ingestionSuite.tests);
    const keysSuite = createKeysSuite('SQLITE', sqliteHarness);
    allTests.set(keysSuite.title, keysSuite.tests);
    allTests.set(sqliteSpecificTests.title, sqliteSpecificTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 rdb_adapter_sqlite module'
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
