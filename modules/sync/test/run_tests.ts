import { testing } from '@hyper-hyper-space/hhs3_util';
import { syncSuite } from './sync_test.js';

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 sync module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    const allSuites = [syncSuite];

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                const result = await testing.run(test.name, test.invoke);
                if (!result) process.exit(1);
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();
