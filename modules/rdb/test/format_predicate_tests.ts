import { assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { formatPredicate, formatRestrictionFailureReason } from "../src/rschema/format_predicate.js";
import type { Predicate } from "../src/rschema/payload.js";
import type { RowOpPayload } from "../src/rtable/payload.js";

export const formatPredicateTests = {
    title: '[FORMAT] Predicate formatting tests',
    tests: [
        {
            name: '[FORMAT01] formatPredicate renders cmp, exists, and, or, str atoms',
            invoke: async () => {
                assertEquals(formatPredicate({ p: 'true' }), 'true');
                assertEquals(formatPredicate({ p: 'false' }), 'false');
                assertEquals(
                    formatPredicate({ p: 'cmp', cmp: 'eq', left: { col: 'keyId' }, right: { lit: '$author' } }),
                    'keyId = $author',
                );
                assertEquals(
                    formatPredicate({ p: 'exists', table: 'caps', where: { label: 'grant' } }),
                    "EXISTS caps WHERE caps.label = 'grant'",
                );
                assertEquals(
                    formatPredicate(
                        { p: 'exists', table: 'profiles', where: { keyId: '$row.keyId' } },
                        { gatedTable: 'profiles' },
                    ),
                    'EXISTS profiles AS p WHERE p.keyId = profiles.keyId',
                );
                assertEquals(
                    formatPredicate({
                        p: 'and',
                        args: [
                            { p: 'cmp', cmp: 'eq', left: { col: 'keyId' }, right: { lit: '$author' } },
                            { p: 'exists', table: 'caps', where: { label: 'manager' } },
                        ],
                    }),
                    "keyId = $author AND EXISTS caps WHERE caps.label = 'manager'",
                );
                assertEquals(
                    formatPredicate({
                        p: 'or',
                        args: [
                            { p: 'cmp', cmp: 'eq', left: { col: 'grantee' }, right: { lit: '$author' } },
                            { p: 'false' },
                        ],
                    }),
                    'grantee = $author OR false',
                );
                assertEquals(
                    formatPredicate({
                        p: 'str', str: 'prefix', value: { col: 'name' }, sub: { lit: 'admin' },
                    }),
                    "name LIKE 'admin%'",
                );
            },
        },
        {
            name: '[FORMAT02] formatRestrictionFailureReason includes op, rowId, and predicate',
            invoke: async () => {
                const op: RowOpPayload = {
                    action: 'update',
                    rowId: 'abc123=',
                    values: { name: 'x' },
                };
                const rule: Predicate = {
                    p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' },
                };
                assertEquals(
                    formatRestrictionFailureReason('docs', op, rule),
                    "docs update on row 'abc123=' does not satisfy ALLOW update IF docs.rowAuthor = $author",
                );
            },
        },
        {
            name: '[FORMAT03] formatOpVoidDetail renders restriction and observe-gate reasons',
            invoke: async () => {
                const { formatOpVoidDetail } = await import("../src/rtable_group/op_void.js");
                assertEquals(
                    formatOpVoidDetail({
                        kind: 'restriction',
                        table: 'items',
                        action: 'insert',
                        rowId: 'abc123=',
                        rule: { p: 'exists', table: 'caps', where: { label: 'grant' } },
                    }),
                    "items insert on row 'abc123=' does not satisfy ALLOW insert IF EXISTS caps WHERE caps.label = 'grant'",
                );
                assertEquals(
                    formatOpVoidDetail({
                        kind: 'observe-gate',
                        binding: 'users',
                        rule: { p: 'exists', table: 'caps', where: { label: 'manager', grantee: '$author' } },
                    }),
                    "canObserve predicate rejected observation of 'users': EXISTS caps WHERE caps.label = 'manager' AND caps.grantee = $author",
                );
            },
        },
    ],
};

async function main() {
    const { testing } = await import("@hyper-hyper-space/hhs3_util");
    console.log('Running format_predicate tests\n');
    for (const test of formatPredicateTests.tests) {
        testing.exitIfFailed(await testing.run(test.name, test.invoke));
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    main();
}
