import { testing } from "@hyper-hyper-space/hhs3_util";

import { schemaModelTests } from "./schema_model_tests.js";
import { payloadFormatTests } from "./payload_format_tests.js";
import { rowIdTests } from "./row_id_tests.js";
import { rschemaTests } from "./rschema_tests.js";
import { rtableGroupTests } from "./rtable_group_tests.js";
import { rtableLwwTests } from "./rtable_lww_tests.js";
import { rtableBundleTests } from "./rtable_bundle_tests.js";
import { rtableEnforceTests } from "./rtable_enforce_tests.js";
import { rtableXGroupTests } from "./rtable_xgroup_tests.js";
import { rtablePermTests } from "./rtable_perm_tests.js";
import { rtableQueryTests } from "./rtable_query_tests.js";
import { rtableDeployTests } from "./rtable_deploy_tests.js";
import { rdbSyncTests } from "./rdb_sync_tests.js";
import { rdbFullSyncTests } from "./rdb_full_sync_tests.js";
import { groupDeltaParityTests } from "./delta_parity/group_parity_tests.js";
import { parseTestFilters } from "./delta_parity/parity.js";

async function main() {

    const allTests = new Map<string, Array<{ name: string, invoke: () => Promise<void> }>>();

    const filters = parseTestFilters(process.argv.slice(2));

    allTests.set(schemaModelTests.title, schemaModelTests.tests);
    allTests.set(payloadFormatTests.title, payloadFormatTests.tests);
    allTests.set(rowIdTests.title, rowIdTests.tests);
    allTests.set(rschemaTests.title, rschemaTests.tests);
    allTests.set(rtableGroupTests.title, rtableGroupTests.tests);
    allTests.set(rtableLwwTests.title, rtableLwwTests.tests);
    allTests.set(rtableBundleTests.title, rtableBundleTests.tests);
    allTests.set(rtableEnforceTests.title, rtableEnforceTests.tests);
    allTests.set(rtableXGroupTests.title, rtableXGroupTests.tests);
    allTests.set(rtablePermTests.title, rtablePermTests.tests);
    allTests.set(rtableQueryTests.title, rtableQueryTests.tests);
    allTests.set(rtableDeployTests.title, rtableDeployTests.tests);
    allTests.set(rdbSyncTests.title, rdbSyncTests.tests);
    allTests.set(rdbFullSyncTests.title, rdbFullSyncTests.tests);
    allTests.set(groupDeltaParityTests.title, groupDeltaParityTests.tests);

    console.log('Running tests for Hyper Hyper Space v3 rdb module' + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    for (const [title, tests] of allTests.entries()) {
        console.log(title);

        for (const test of tests) {

            let match = true;
            for (const filter of filters) {
                match = match && (test.name.indexOf(filter) >= 0);
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
