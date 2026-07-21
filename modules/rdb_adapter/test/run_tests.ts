import { testing } from "@hyper-hyper-space/hhs3_util";

import { schemaActionsTests } from "./schema_actions_tests.js";
import { rowActionsTests } from "./row_actions_tests.js";
import { sqliteTargetTests } from "./sqlite_target_tests.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = process.argv.slice(2);

    allTests.set(schemaActionsTests.title, schemaActionsTests.tests);
    allTests.set(rowActionsTests.title, rowActionsTests.tests);
    allTests.set(sqliteTargetTests.title, sqliteTargetTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 rdb_adapter module'
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
