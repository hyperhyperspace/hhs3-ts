import { testing } from "@hyper-hyper-space/hhs3_util";

import { schemaActionsTests } from "./schema_actions_tests.js";
import { rowActionsTests } from "./row_actions_tests.js";
import { ingestTests } from "./ingest_tests.js";
import { verdictEventsTests } from "./verdict_events_tests.js";
import { plannerParityTests } from "./planner_parity/planner_parity_tests.js";
import { parseTestFilters } from "./planner_parity/profiles.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = parseTestFilters(process.argv.slice(2));

    allTests.set(schemaActionsTests.title, schemaActionsTests.tests);
    allTests.set(rowActionsTests.title, rowActionsTests.tests);
    allTests.set(ingestTests.title, ingestTests.tests);
    allTests.set(verdictEventsTests.title, verdictEventsTests.tests);
    allTests.set(plannerParityTests.title, plannerParityTests.tests);

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
