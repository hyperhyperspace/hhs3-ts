import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { bind } from "../src/bind/bind.js";
import { compileParamDecls } from "../src/compile/catalog.js";
import { compileSchemaParamDecls } from "../src/compile/create.js";
import { SpannedError } from "../src/diagnostics.js";
import { parseStatement } from "../src/syntax/parser.js";
import { createTestBindContext } from "./mock_bind_context.js";

const span = { start: 0, end: 1, line: 1, column: 1 };

export const diagnosticTests = {
    title: '[RDB_LANG:DIAG] Diagnostics',
    tests: [
        {
            name: '[DIAG01] parse diagnostics include stable spans',
            invoke: async () => {
                const parsed = parseStatement("SELECT FROM;");
                assertTrue(!parsed.ok, 'bad SELECT should fail');
                if (parsed.ok) return;
                assertTrue(parsed.diagnostics[0].span !== undefined, 'diagnostic has span');
                assertEquals(parsed.diagnostics[0].severity, 'error', 'diagnostic severity');
            },
        },
        {
            name: '[DIAG02] bind diagnostics report missing variables',
            invoke: async () => {
                const parsed = parseStatement("CREATE SCHEMA shop CREATORS ($admin) AS (TABLE t (name string));");
                assertTrue(parsed.ok, 'parse should succeed');
                if (!parsed.ok) return;
                const bound = await bind(parsed.value, createTestBindContext(createMockRContext()));
                assertTrue(!bound.ok, 'bind should fail on missing variable');
                if (bound.ok) return;
                assertEquals(bound.diagnostics[0].code, 'BIND_UNKNOWN_NAME', 'bind diagnostic code');
            },
        },
        {
            name: '[DIAG03] decimal params are rejected at compile',
            invoke: async () => {
                let schema = '';
                try {
                    compileSchemaParamDecls([{ name: 'rate', type: 'decimal', span }], false);
                } catch (err) {
                    schema = err instanceof SpannedError ? err.message : String(err);
                }
                assertTrue(schema.includes('no scale'), `schema param: ${schema}`);
                let catalog = '';
                try {
                    compileParamDecls([{ name: 'rate', type: 'decimal', span }]);
                } catch (err) {
                    catalog = err instanceof SpannedError ? err.message : String(err);
                }
                assertTrue(catalog.includes('no scale'), `catalog param: ${catalog}`);
            },
        },
    ],
};
