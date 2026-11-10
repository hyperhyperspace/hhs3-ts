import { assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { formatPredicate, formatRestrictionFailureReason, renderStringLiteral } from "../src/rschema/format_predicate.js";
import type { Operand, Predicate, Where } from "../src/rschema/payload.js";
import type { RowOpPayload } from "../src/rtable/payload.js";

export const formatPredicateTests = {
    title: '[FORMAT] Predicate formatting tests',
    tests: [
        {
            name: '[FORMAT01] formatPredicate renders cmp, exists, and, or, like atoms',
            invoke: async () => {
                assertEquals(formatPredicate({ p: 'true' }), 'true');
                assertEquals(formatPredicate({ p: 'false' }), 'false');
                assertEquals(
                    formatPredicate({ p: 'cmp', cmp: 'eq', left: { col: 'keyId' }, right: { lit: '$author' } }),
                    'keyId = $author',
                );
                assertEquals(
                    formatPredicate({ p: 'exists', table: 'caps', where: { label: 'grant' } }),
                    "EXISTS (caps WHERE caps.label = 'grant')",
                );
                assertEquals(
                    formatPredicate(
                        { p: 'exists', table: 'profiles', where: { keyId: '$row.keyId' } },
                        { gatedTable: 'profiles' },
                    ),
                    'EXISTS (profiles AS p WHERE p.keyId = profiles.keyId)',
                );
                assertEquals(
                    formatPredicate({
                        p: 'and',
                        args: [
                            { p: 'cmp', cmp: 'eq', left: { col: 'keyId' }, right: { lit: '$author' } },
                            { p: 'exists', table: 'caps', where: { label: 'manager' } },
                        ],
                    }),
                    "keyId = $author AND EXISTS (caps WHERE caps.label = 'manager')",
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
                        p: 'like', value: { col: 'name' }, pattern: { lit: 'admin%' },
                    }),
                    "name LIKE 'admin%'",
                );
                assertEquals(
                    formatPredicate({
                        p: 'like', value: { col: 'name' }, pattern: { lit: "it's 100\\%" },
                    }),
                    "name LIKE 'it''s 100\\%'",
                );
            },
        },
        {
            name: '[FORMAT01b] formatPredicate parenthesizes groups only where re-parsing needs it',
            invoke: async () => {
                const a: Predicate = { p: 'cmp', cmp: 'eq', left: { col: 'a' }, right: { lit: 1 } };
                const b: Predicate = { p: 'cmp', cmp: 'eq', left: { col: 'b' }, right: { lit: 2 } };
                const c: Predicate = { p: 'cmp', cmp: 'eq', left: { col: 'c' }, right: { lit: 3 } };
                const ex: Predicate = { p: 'exists', table: 'caps', where: { grantee: '$author' } };
                assertEquals(formatPredicate({ p: 'and', args: [{ p: 'or', args: [a, b] }, c] }), '(a = 1 OR b = 2) AND c = 3', 'OR inside AND');
                assertEquals(formatPredicate({ p: 'or', args: [{ p: 'and', args: [a, b] }, c] }), 'a = 1 AND b = 2 OR c = 3', 'AND inside OR binds tighter anyway');
                assertEquals(formatPredicate({ p: 'and', args: [{ p: 'and', args: [a, b] }, c] }), '(a = 1 AND b = 2) AND c = 3', 'a nested AND is kept apart from its parent');
                assertEquals(formatPredicate({ p: 'or', args: [a, { p: 'or', args: [b, c] }] }), 'a = 1 OR (b = 2 OR c = 3)', 'a nested OR is kept apart from its parent');
                assertEquals(formatPredicate({ p: 'or', args: [ex, a] }), 'EXISTS (caps WHERE caps.grantee = $author) OR a = 1', 'EXISTS inside a group brings its own parentheses');
                assertEquals(formatPredicate({ p: 'and', args: [a, ex, { p: 'or', args: [ex, b] }] }),
                    'a = 1 AND EXISTS (caps WHERE caps.grantee = $author) AND (EXISTS (caps WHERE caps.grantee = $author) OR b = 2)',
                    'no parentheses around an EXISTS member, only around the nested OR');
                assertEquals(formatPredicate(ex), 'EXISTS (caps WHERE caps.grantee = $author)', 'top-level EXISTS');
            },
        },
        {
            name: '[FORMAT01g] formatPredicate renders a where tree: a leaf as a conjunction, IN for an $or of one-field leaves on one column, and the parentheses the filter needs',
            invoke: async () => {
                const exists = (where: Where, gatedTable?: string) => formatPredicate({ p: 'exists', table: 'grants', where }, { gatedTable });
                const a = { a: 1 };
                const b = { b: 2 };
                const c = { c: 3 };

                // leaves and IN
                assertEquals(exists({ a: 1, b: 'x' }), "EXISTS (grants WHERE grants.a = 1 AND grants.b = 'x')", 'a leaf is its equalities joined by AND');
                assertEquals(exists({ $or: [{ label: 'writer' }, { label: 'manager' }] }),
                    "EXISTS (grants WHERE grants.label IN ('writer', 'manager'))", 'an $or of one-field leaves on one column is IN');
                assertEquals(exists({ $or: [{ label: 'writer' }, { label: 'writer' }] }),
                    "EXISTS (grants WHERE grants.label IN ('writer', 'writer'))", 'equal members are repeated');
                assertEquals(exists({ $or: [{ grantee: '$author' }, { grantee: 'public' }, { grantee: '$param.guest' }, { grantee: '$row.owner' }] }, 'docs'),
                    "EXISTS (grants WHERE grants.grantee IN ($author, 'public', :guest, docs.owner))", 'a list holds every kind of value');
                assertEquals(exists({ $or: [a, b] }), 'EXISTS (grants WHERE grants.a = 1 OR grants.b = 2)', 'leaves on different columns stay an OR');
                assertEquals(exists({ $or: [{ a: 1 }, { a: 2, b: 3 }] }), 'EXISTS (grants WHERE grants.a = 1 OR grants.a = 2 AND grants.b = 3)',
                    'a leaf of two fields keeps the group an OR, and needs no parentheses inside it');

                // the example rule of HIP-21
                assertEquals(exists({ $and: [
                    { grantee: '$author' },
                    { $or: [{ label: 'writer' }, { label: 'manager' }] },
                    { $or: [{ resource: '$row.resource' }, { resource: '*' }] },
                ] }, 'docs'),
                    "EXISTS (grants WHERE grants.grantee = $author AND grants.label IN ('writer', 'manager') AND grants.resource IN (docs.resource, '*'))",
                    'IN lists beside a leaf, under an $and');

                // parentheses
                assertEquals(exists({ $and: [a, { $or: [b, c] }] }), 'EXISTS (grants WHERE grants.a = 1 AND (grants.b = 2 OR grants.c = 3))', 'an $or inside an $and');
                assertEquals(exists({ $or: [a, { $and: [b, { $or: [c, a] }] }] }),
                    'EXISTS (grants WHERE grants.a = 1 OR grants.b = 2 AND (grants.c = 3 OR grants.a = 1))', 'an $and inside an $or binds tighter anyway');
                assertEquals(exists({ $or: [a, { $or: [b, c] }] }), 'EXISTS (grants WHERE grants.a = 1 OR (grants.b = 2 OR grants.c = 3))', 'a nested $or is kept apart from its parent');
                assertEquals(exists({ $and: [{ $and: [a, { $or: [b, c] }] }, { $or: [c, a] }] }),
                    'EXISTS (grants WHERE (grants.a = 1 AND (grants.b = 2 OR grants.c = 3)) AND (grants.c = 3 OR grants.a = 1))', 'a nested $and is kept apart from its parent');
                assertEquals(exists({ $or: [{ $or: [{ a: 1 }, { a: 2 }] }, c] }), 'EXISTS (grants WHERE grants.a IN (1, 2) OR grants.c = 3)',
                    'an IN list is one term: no parentheses inside a group of its kind');
                assertEquals(exists({ $and: [{ $or: [{ a: 1 }, { a: 2 }] }, { $or: [{ b: 1 }, { b: 2 }] }] }), 'EXISTS (grants WHERE grants.a IN (1, 2) AND grants.b IN (1, 2))',
                    'nor inside an $and');
                assertEquals(exists({ $and: [{ $or: [b, c] }, { a: 1, b: 2 }] }), 'EXISTS (grants WHERE (grants.b = 2 OR grants.c = 3) AND grants.a = 1 AND grants.b = 2)',
                    'a leaf after a group keeps its place');

                // the alias of a self-referential EXISTS qualifies every column of the tree
                assertEquals(
                    formatPredicate({ p: 'exists', table: 'docs', where: { $or: [{ owner: '$author' }, { owner: '$row.owner', kind: 'shared' }] } }, { gatedTable: 'docs' }),
                    "EXISTS (docs AS d WHERE d.owner = $author OR d.owner = docs.owner AND d.kind = 'shared')", 'the alias qualifies every column');
            },
        },
        {
            name: '[FORMAT01c] formatPredicate renders fn operands infix, with length() and negative literals',
            invoke: async () => {
                const cmp = (left: Operand, right: Operand): Predicate => ({ p: 'cmp', cmp: 'le', left, right });
                const col = (c: string): Operand => ({ col: c });
                const lit = (v: number): Operand => ({ lit: v });
                const add = (l: Operand, r: Operand): Operand => ({ fn: 'add', args: [l, r] });
                const sub = (l: Operand, r: Operand): Operand => ({ fn: 'sub', args: [l, r] });
                const mul = (l: Operand, r: Operand): Operand => ({ fn: 'mul', args: [l, r] });
                assertEquals(formatPredicate(cmp(add(col('a'), mul(col('b'), lit(2))), lit(10))), 'a + b * 2 <= 10', '* binds tighter');
                assertEquals(formatPredicate(cmp(mul(add(col('a'), col('b')), lit(2)), lit(10))), '(a + b) * 2 <= 10', 'sum inside product');
                assertEquals(formatPredicate(cmp(sub(sub(col('a'), col('b')), col('c')), lit(0))), 'a - b - c <= 0', 'left-nested sub');
                assertEquals(formatPredicate(cmp(sub(col('a'), sub(col('b'), col('c'))), lit(0))), 'a - (b - c) <= 0', 'right-nested sub');
                assertEquals(formatPredicate(cmp(mul(col('a'), mul(col('b'), col('c'))), lit(0))), 'a * (b * c) <= 0', 'right-nested mul');
                assertEquals(formatPredicate(cmp(sub(col('a'), lit(-1)), lit(-2))), 'a - -1 <= -2', 'negative literals never form --');
                assertEquals(formatPredicate(cmp(mul(lit(-3), col('a')), lit(1e-7))), '-3 * a <= 1e-7', 'negative factor');
                assertEquals(formatPredicate(cmp({ fn: 'len', args: [col('name')] }, add(lit(1), lit(2)))), 'length(name) <= 1 + 2', 'length()');
                assertEquals(
                    formatPredicate(cmp(add(col('a'), lit(1)), col('b')), { gatedTable: 'items' }),
                    'items.a + 1 <= items.b', 'gated columns are qualified');
            },
        },
        {
            name: '[FORMAT01d] formatPredicate routes table and column names through quoteIdent',
            invoke: async () => {
                const quoteIdent = (name: string) => (name === 'identity' || name === 'table' ? `"${name}"` : name);
                assertEquals(
                    formatPredicate({ p: 'cmp', cmp: 'eq', left: { col: 'identity' }, right: { lit: '$author' } }, { gatedTable: 'table', quoteIdent }),
                    '"table"."identity" = $author');
                assertEquals(
                    formatPredicate({ p: 'exists', table: 'users.identity', where: { identity: '$row.identity' } }, { gatedTable: 'endpoints', quoteIdent }),
                    'EXISTS (users."identity" WHERE users."identity"."identity" = endpoints."identity")');
            },
        },
        {
            name: '[FORMAT01e] formatPredicate renders array and object literals as JSON text',
            invoke: async () => {
                const long = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
                assertEquals(
                    formatPredicate({ p: 'exists', table: 'docs', where: { meta: { tags: ["it's", 'a\nb'], n: -0 }, seq: long } }),
                    `EXISTS (docs WHERE docs.meta = JSON '{"n":0,"tags":["it''s","a\\\\nb"]}' AND docs.seq = JSON '[0,1,2,3,4,5,6,7,8,9,10,11]')`,
                    'objects and arrays render as JSON literals, arrays in order');
            },
        },
        {
            name: '[FORMAT01f] renderStringLiteral doubles a backslash only where the lexer would read it differently',
            invoke: async () => {
                const cases: [string, string, string][] = [
                    ['100\\%', "'100\\%'", 'a lone backslash before % is kept'],
                    ['a\\"b', "'a\\\"b'", 'a lone backslash before " is kept'],
                    ['dir\\new', "'dir\\\\new'", 'a backslash before n is doubled'],
                    ['a\\rb\\tc', "'a\\\\rb\\\\tc'", 'backslashes before r and t are doubled'],
                    ['x\\\\y', "'x\\\\\\\\y'", 'a run of two becomes four'],
                    ['end\\', "'end\\\\'", 'a trailing backslash is doubled'],
                    ["a\\'b", "'a\\\\''b'", 'a backslash before a quote is doubled'],
                    ['a\\\nb', "'a\\\\\\nb'", 'a backslash before a line feed is doubled'],
                    ['l1\nl2\rl3\tl4', "'l1\\nl2\\rl3\\tl4'", 'control characters render as escapes'],
                    ["it's", "'it''s'", 'a quote is doubled'],
                ];
                for (const [value, rendered, what] of cases) assertEquals(renderStringLiteral(value), rendered, what);
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
                    "items insert on row 'abc123=' does not satisfy ALLOW insert IF EXISTS (caps WHERE caps.label = 'grant')",
                );
                assertEquals(
                    formatOpVoidDetail({
                        kind: 'observe-gate',
                        binding: 'users',
                        rule: { p: 'exists', table: 'caps', where: { label: 'manager', grantee: '$author' } },
                    }),
                    "canObserve predicate rejected observation of 'users': EXISTS (caps WHERE caps.label = 'manager' AND caps.grantee = $author)",
                );
            },
        },
        {
            name: '[FORMAT04] formatOpVoidDetail renders row-not-live reasons for update and delete',
            invoke: async () => {
                const { formatOpVoidDetail } = await import("../src/rtable_group/op_void.js");
                assertEquals(
                    formatOpVoidDetail({
                        kind: 'row-not-live',
                        table: 'docs',
                        action: 'update',
                        rowId: 'abc123=',
                    }),
                    "docs update on row 'abc123=': rowId is not live in table 'docs'",
                );
                assertEquals(
                    formatOpVoidDetail({
                        kind: 'row-not-live',
                        table: 'docs',
                        action: 'delete',
                        rowId: 'abc123=',
                    }),
                    "docs delete on row 'abc123=': rowId is not live in table 'docs'",
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
