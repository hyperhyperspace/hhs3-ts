import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { SchemaUpdatePayload } from "@hyper-hyper-space/hhs3_rdb";

import { firstNonCommentLine, renderLogOpLine } from "../src/reverse/log_line.js";
import { renderSchemaUpdate } from "../src/reverse/render.js";

export const logLineTests = {
    title: '[RDB_LANG:LOG_LINE] Reverse render log line',
    tests: [
        {
            name: '[LOGLINE01] firstNonCommentLine skips leading comments',
            invoke: async () => {
                assertEquals(
                    firstNonCommentLine('-- shop\nALTER SCHEMA #abc AS (\n  ADD TABLE t\n);'),
                    'ALTER SCHEMA #abc AS (',
                );
                assertEquals(firstNonCommentLine('INSERT INTO t (a) VALUES (1);'), 'INSERT INTO t (a) VALUES (1);');
            },
        },
        {
            name: '[LOGLINE02] renderLogOpLine renders row insert as one line',
            invoke: async () => {
                const payload = {
                    action: 'row',
                    table: 'products',
                    op: {
                        action: 'insert',
                        uuid: 'u-1',
                        values: { sku: 'A', name: 'Widget' },
                    },
                } as json.Literal;
                const line = renderLogOpLine(payload, [], {});
                assertTrue(line.startsWith('INSERT INTO products'), line);
                assertTrue(!line.includes('\n'), 'row op should be single line');
            },
        },
        {
            name: '[LOGLINE03] comments false omits schema name prefix on ALTER SCHEMA',
            invoke: async () => {
                const payload: SchemaUpdatePayload = {
                    action: 'schema-update',
                    migration: [{ rule: 'add-column', table: 'products', column: 'tag', def: { type: 'string' } }],
                    author: 'author-key-id',
                    signature: 'sig',
                };
                const withComments = renderSchemaUpdate(payload, {
                    schemaRef: 'schema-id',
                    schemaName: 'shop',
                });
                assertTrue(withComments.startsWith('-- shop\n'), 'default keeps comment');

                const line = renderLogOpLine(payload as json.Literal, [], {
                    schemaRef: 'schema-id',
                    schemaName: 'shop',
                });
                assertTrue(line.startsWith('ALTER SCHEMA'), line);
                assertTrue(!line.startsWith('--'), line);
            },
        },
        {
            name: '[LOGLINE04] renderLogOpLine sets causal AT from prev',
            invoke: async () => {
                const payload = {
                    action: 'row',
                    table: 'products',
                    op: { action: 'delete', rowId: 'row-id-hash' },
                } as json.Literal;
                const prev = ['prev-hash'];
                const line = renderLogOpLine(payload, prev, {});
                assertTrue(line.includes(' AT {'), line);
                assertTrue(line.includes('#prev-hash'), line);
            },
        },
    ],
};
