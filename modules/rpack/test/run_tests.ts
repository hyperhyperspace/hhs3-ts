import { testing } from "@hyper-hyper-space/hhs3_util";

import * as rpack from "../src/index.js";

const tests = [
    {
        name: '[RPACK01] stub module loads',
        invoke: async () => {
            if (rpack === undefined) throw new Error('rpack module did not load');
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);

    console.log('Running tests for Hyper Hyper Space v3 rpack module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    console.log('rpack');

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

main();
