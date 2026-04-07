import { schemaSuite, levelBackendSuite, topoBackendSuite, levelParitySuite, topoParitySuite } from "./sql_dag_tests.js";
import { testing } from "@hyper-hyper-space/hhs3_util";

async function main() {

    const allTests = new Map<string, Array<{name: string, invoke: () => Promise<void>}>>();

    const filters = process.argv.slice(2);

    allTests.set(schemaSuite.title, schemaSuite.tests);
    allTests.set(levelBackendSuite.title, levelBackendSuite.tests);
    allTests.set(topoBackendSuite.title, topoBackendSuite.tests);
    allTests.set(levelParitySuite.title, levelParitySuite.tests);
    allTests.set(topoParitySuite.title, topoParitySuite.tests);

    console.log('Running tests for HHSv3 SQL DAG module' + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    for (const [title, tests] of allTests.entries()) {
        console.log(title);

        for (const test of tests) {

            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
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
