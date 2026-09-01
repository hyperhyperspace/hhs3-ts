import { testing } from '@hyper-hyper-space/hhs3_util';
import { browserWsTests } from './browser_ws_tests.js';

const allSuites = [browserWsTests];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_ws_browser module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
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
