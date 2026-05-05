import { replicaBasicTests } from "./replica_basic_tests.js";
import { replicaNestedTests } from "./replica_nested_tests.js";
import { replicaSyncTests } from "./replica_sync_tests.js";
import { replicaFullSyncTests } from "./replica_full_sync_tests.js";
import { testing } from "@hyper-hyper-space/hhs3_util";

async function main() {

    const allTests = new Map<string, Array<{name: string, invoke: () => Promise<void>}>>();

    const filters = process.argv.slice(2);

    allTests.set(replicaBasicTests.title, replicaBasicTests.tests);
    allTests.set(replicaNestedTests.title, replicaNestedTests.tests);
    allTests.set(replicaSyncTests.title, replicaSyncTests.tests);
    allTests.set(replicaFullSyncTests.title, replicaFullSyncTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 Replica module' + (filters.length > 0? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');    

    for (const [title, tests] of allTests.entries()) {
        console.log(title);

        for (const test of tests) {

            let match = true;
            for (const filter of filters) {
                match = match && (test.name.indexOf(filter) >= 0)
            }

            if (match) {
                const result = await testing.run(test.name, test.invoke);

                if (!result) {
                    return;
                }
            } else {
                await testing.skip(test.name);
            }
        }

        console.log();
    }
}

main();
