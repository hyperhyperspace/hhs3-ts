import { testing } from "@hyper-hyper-space/hhs3_util";
import { dagNestingSuite } from "./dag_nesting_tests.js";
import { refsSuite } from "./refs_tests.js";
import { deltaSuite } from "./delta_tests.js";

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for Hyper Hyper Space v3 MVT module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    const allSuites = [dagNestingSuite, refsSuite, deltaSuite];

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                const result = await testing.run(test.name, test.invoke);
                if (!result) return;
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();
