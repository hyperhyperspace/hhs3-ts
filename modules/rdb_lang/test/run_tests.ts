import { testing } from "@hyper-hyper-space/hhs3_util";

import { lexerTests } from "./lexer_tests.js";
import { parserTests } from "./parser_tests.js";
import { scannerTests } from "./scanner_tests.js";
import { phase1VerticalTests } from "./phase1_vertical_tests.js";
import { diagnosticTests } from "./diagnostic_tests.js";
import { restPhaseTests } from "./rest_phase_tests.js";
import { fkHashValuesTests } from "./fk_hash_values_tests.js";
import { creatorResolutionTests } from "./creator_resolution_tests.js";
import { usersPermissionScriptTests } from "./users_permission_script_tests.js";

async function main() {
    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();
    const filters = process.argv.slice(2);

    allTests.set(lexerTests.title, lexerTests.tests);
    allTests.set(parserTests.title, parserTests.tests);
    allTests.set(scannerTests.title, scannerTests.tests);
    allTests.set(phase1VerticalTests.title, phase1VerticalTests.tests);
    allTests.set(diagnosticTests.title, diagnosticTests.tests);
    allTests.set(restPhaseTests.title, restPhaseTests.tests);
    allTests.set(fkHashValuesTests.title, fkHashValuesTests.tests);
    allTests.set(creatorResolutionTests.title, creatorResolutionTests.tests);
    allTests.set(usersPermissionScriptTests.title, usersPermissionScriptTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 rdb_lang module' + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

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
