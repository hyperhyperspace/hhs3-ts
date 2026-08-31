import { testing } from "@hyper-hyper-space/hhs3_util";

import { projectionTests } from "./projection_tests.js";
import { ingestionTests } from "./ingestion_tests.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = process.argv.slice(2);

    allTests.set(projectionTests.title, projectionTests.tests);
    allTests.set(ingestionTests.title, ingestionTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 rdb_projection module'
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
