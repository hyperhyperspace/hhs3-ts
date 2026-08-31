import { testing } from "@hyper-hyper-space/hhs3_util";

import {
    createIngestionSuite, createKeysSuite, createProjectionSuite, memoryAuthorIdTest,
    memoryHarness, memoryIngestionHarness,
} from "../src/index.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = process.argv.slice(2);

    // The reference-target self-check: run the shared suites against MemoryTarget.
    const memorySuite = createProjectionSuite('MEMORY', memoryHarness);
    allTests.set(memorySuite.title, memorySuite.tests);
    const memoryIngestion = createIngestionSuite('MEMORY', memoryIngestionHarness);
    allTests.set(memoryIngestion.title, memoryIngestion.tests);
    const memoryKeys = createKeysSuite('MEMORY', memoryHarness);
    allTests.set(memoryKeys.title, [memoryAuthorIdTest, ...memoryKeys.tests]);

    console.log('Running tests for Hyper Hyper Space v3 rdb_adapter_test module'
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
