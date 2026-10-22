import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { MemoryTarget } from "../src/memory_target.js";
import { compareCodePoints, compareProjectedValues, evalWhere } from "../src/query_eval.js";
import type { BoundWhereExpr, TargetCommitEvent } from "../src/types.js";

const cmp = (op: '=' | '!=' | '<' | '<=' | '>' | '>=', column: string, value: string | number | boolean | null): BoundWhereExpr => ({
    kind: 'cmp', op,
    left: { kind: 'col', name: column },
    right: { kind: 'lit', value },
});

export const queryEvalTests = {
    title: 'Query evaluation',
    tests: [
        {
            name: '[ADPTQ01] NULL comparisons are unknown and WHERE drops them',
            invoke: async () => {
                const row = { name: null, n: 1 };
                assertEquals(evalWhere(cmp('=', 'name', 'a'), row), false, 'null = a');
                assertEquals(evalWhere(cmp('!=', 'name', 'a'), row), false, 'null != a');
                assertEquals(evalWhere({ kind: 'isNull', arg: { kind: 'col', name: 'name' }, not: false }, row), true, 'is null');
                assertEquals(evalWhere({ kind: 'isNull', arg: { kind: 'col', name: 'name' }, not: true }, row), false, 'is not null');
                assertEquals(evalWhere({
                    kind: 'and',
                    args: [cmp('=', 'n', 1), cmp('=', 'name', 'a')],
                }, row), false, 'true AND unknown');
                assertEquals(evalWhere({
                    kind: 'or',
                    args: [cmp('=', 'name', 'a'), cmp('=', 'n', 1)],
                }, row), true, 'unknown OR true');
            },
        },
        {
            name: '[ADPTQ02] a missing key is NULL and id comes from localId',
            invoke: async () => {
                assertEquals(evalWhere(cmp('=', 'id', 4), {}, 4), true, 'local id');
                assertEquals(evalWhere({ kind: 'isNull', arg: { kind: 'col', name: 'gone' }, not: false }, {}), true, 'missing');
            },
        },
        {
            name: '[ADPTQ03] LIKE is case-sensitive and uses backslash escapes',
            invoke: async () => {
                const like = (pattern: string): BoundWhereExpr => ({
                    kind: 'like',
                    arg: { kind: 'col', name: 'name' },
                    pattern: { kind: 'lit', value: pattern },
                });
                assertEquals(evalWhere(like('A%'), { name: 'Abc' }), true, 'prefix');
                assertEquals(evalWhere(like('a%'), { name: 'Abc' }), false, 'case');
                assertEquals(evalWhere(like('100\\%'), { name: '100%' }), true, 'escaped percent');
                assertEquals(evalWhere(like('100\\%'), { name: '100x' }), false, 'escaped percent is literal');
            },
        },
        {
            name: '[ADPTQ04] IN, arithmetic, and length',
            invoke: async () => {
                assertEquals(evalWhere({
                    kind: 'in', not: false, arg: { kind: 'col', name: 'n' }, values: [1, 2],
                }, { n: 2 }), true, 'in');
                assertEquals(evalWhere({
                    kind: 'in', not: true, arg: { kind: 'col', name: 'n' }, values: [1, null],
                }, { n: 2 }), false, 'not in with null is unknown');
                assertEquals(evalWhere({
                    kind: 'cmp', op: '=',
                    left: { kind: 'length', arg: { kind: 'col', name: 'name' } },
                    right: { kind: 'lit', value: 2 },
                }, { name: 'é👍' }), true, 'length counts two code points');
                assertEquals(evalWhere({
                    kind: 'cmp', op: '=',
                    left: { kind: 'arith', op: '+', left: { kind: 'col', name: 'n' }, right: { kind: 'lit', value: 1 } },
                    right: { kind: 'lit', value: 3 },
                }, { n: 2 }), true, 'add');
            },
        },
        {
            name: '[ADPTQ05] NULL sorts first and supplementary-plane text uses code points',
            invoke: async () => {
                assertEquals(compareProjectedValues(null, 'a'), -1, 'null is smaller');
                assertEquals(compareProjectedValues('a', null), 1, 'value is larger');
                const emoji = String.fromCodePoint(0x1f600);
                const replacement = '\ufffd';
                assertTrue(emoji < replacement, 'javascript utf-16 order disagrees');
                assertTrue(compareCodePoints(emoji, replacement) > 0, 'code point order matches sqlite utf-8');
                assertEquals(compareProjectedValues('10', '9', 'decimal'), -1, 'decimal text is lexical');
                assertEquals(compareProjectedValues(false, true, 'boolean'), -1, 'false before true');
            },
        },
        {
            name: '[ADPTQ06] MemoryTarget announces local, apply, and settle, and seeks by before/after',
            invoke: async () => {
                const target = new MemoryTarget({ captureChanges: true });
                const seen: TargetCommitEvent[] = [];
                target.addCommitListener((commit) => { seen.push(commit); });
                await target.apply('g', [{
                    kind: 'create-table', table: 'notes', syncTable: 'notes_sync', primaryKey: 'id',
                    columns: [{ name: 'title', def: { type: 'string', nullable: true, pub: true, readonly: false } }],
                }], [], new Set(['c']));
                target.localInsert('notes', { title: 'a' });
                target.localInsert('notes', { title: 'b' });
                const rolled = target.localInsert('notes', { title: 'gone' });
                target.localDelete('notes', rolled);
                assertEquals(seen.map((event) => event.source).join(','), 'apply,local,local,local,local', 'schema then one event per write');
                const locals = seen.filter((event) => event.source === 'local');
                for (const event of locals) {
                    if (event.source === 'local') assertEquals(event.tables.join(','), 'notes', 'table once');
                }

                const asc = await target.executeQuery({
                    table: 'notes', orderBy: [{ column: 'id', direction: 'asc' }], limit: 10,
                });
                assertEquals(asc.map((row) => row.title).join(','), 'a,b', 'deleted row is gone');
                const older = await target.executeQuery({
                    table: 'notes',
                    where: cmp('=', 'title', 'b'),
                    orderBy: [{ column: 'id', direction: 'desc' }],
                    cursor: { side: 'before', id: asc[1]!.id },
                    limit: 10,
                });
                assertEquals(older.map((row) => row.title).join(','), '', 'before the second row, title b does not match');
                const page = await target.executeQuery({
                    table: 'notes',
                    orderBy: [{ column: 'id', direction: 'desc' }],
                    cursor: { side: 'before', id: asc[1]!.id },
                    limit: 10,
                });
                assertEquals(page.map((row) => row.title).join(','), 'a', 'desc before the later id');

                const applied = seen.filter((event) => event.source === 'apply');
                assertEquals(applied.length, 1, 'schema apply is announced');
            },
        },
    ],
};
