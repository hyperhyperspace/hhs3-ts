import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";
import type { Row } from "../src/rtable/interfaces.js";
import type { ColumnTypes, RowFilter, RowQuery } from "../src/rtable/query.js";
import { evalRowFilter, orderRows, projectRow, validateRowQuery } from "../src/rtable/query.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// `kind` is pub (drives index pushdown); the rest are non-pub (scan). All ops
// permitted so these read-path tests aren't gated by restrictions.
function itemsTable(): TableDef {
    return {
        name: 'items',
        columns: {
            kind: { type: 'string', pub: true },
            qty: { type: 'integer', nullable: true },
            price: { type: 'float', nullable: true },
            label: { type: 'string', default: '' },
        },
        concurrentDeletes: false,
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

// A ledger with a pub bigint key, an exact decimal amount, and a bounded
// integer, for typed comparison / ordering / write-path tests.
function ledgerTable(): TableDef {
    return {
        name: 'ledger',
        columns: {
            seq: { type: 'bigint', pub: true },
            amount: { type: 'decimal', constraints: { scale: 2 } },
            qty: { type: 'integer', nullable: true, constraints: { min: '0', max: '100' } },
        },
        concurrentDeletes: false,
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

async function createLedgerGroup() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'finance',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [ledgerTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'ledger-test', seed: 'ledger-test', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { ctx, group, admin };
}

async function createItemsGroup() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'inventory',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [itemsTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'query-test', seed: 'query-test', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, group, admin };
}

const ITEM_COLUMNS: ColumnTypes = { kind: 'string', qty: 'integer', price: 'float', label: 'string' };

function row(rowId: string, values: Row['values'], author?: string): Row {
    const r: Row = { rowId, uuid: rowId, values };
    if (author !== undefined) r.author = author;
    return r;
}

// The uuids of a result set, sorted — order-independent identity for set
// comparisons (queries without orderBy are rowId-ordered, which is uuid-ish but
// not value-ordered).
function uuids(rows: Row[]): string {
    return rows.map((r) => r.uuid).sort().join(',');
}

function expectThrow(fn: () => void, why: string) {
    let threw = false;
    try { fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

export const rtableQueryTests = {
    title: '[QRY] RTableView single-table query',
    tests: [
        {
            name: '[QRY01] evalRowFilter: cmp / str / and / or / not, two-valued on missing',
            invoke: async () => {
                const r = row('r', { kind: 'apple', qty: 3, price: 1.5 });

                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'qty' }, right: { lit: 3 } }, r), 'eq int');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'gt', left: { col: 'qty' }, right: { lit: 2 } }, r), 'gt int');
                assertFalse(evalRowFilter({ p: 'cmp', cmp: 'lt', left: { col: 'qty' }, right: { lit: 2 } }, r), 'lt int');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'le', left: { col: 'price' }, right: { lit: 1.5 } }, r), 'le float');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'apple' } }, r), 'eq str');

                // arithmetic + len operands
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { fn: 'add', args: [{ col: 'qty' }, { lit: 1 }] }, right: { lit: 4 } }, r), 'add');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { fn: 'len', args: [{ col: 'kind' }] }, right: { lit: 5 } }, r), 'len');

                assertTrue(evalRowFilter({ p: 'str', str: 'prefix', value: { col: 'kind' }, sub: { lit: 'app' } }, r), 'prefix');
                assertTrue(evalRowFilter({ p: 'str', str: 'suffix', value: { col: 'kind' }, sub: { lit: 'ple' } }, r), 'suffix');
                assertTrue(evalRowFilter({ p: 'str', str: 'contains', value: { col: 'kind' }, sub: { lit: 'ppl' } }, r), 'contains');

                assertTrue(evalRowFilter({ p: 'and', args: [
                    { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'apple' } },
                    { p: 'cmp', cmp: 'ge', left: { col: 'qty' }, right: { lit: 3 } },
                ] }, r), 'and');
                assertFalse(evalRowFilter({ p: 'and', args: [
                    { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'apple' } },
                    { p: 'cmp', cmp: 'gt', left: { col: 'qty' }, right: { lit: 3 } },
                ] }, r), 'and short-circuits false');
                assertTrue(evalRowFilter({ p: 'or', args: [
                    { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'pear' } },
                    { p: 'cmp', cmp: 'eq', left: { col: 'qty' }, right: { lit: 3 } },
                ] }, r), 'or');
                assertTrue(evalRowFilter({ p: 'not', arg: { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'pear' } } }, r), 'not');

                // two-valued: a missing column makes the atom false, and `not` of
                // a false atom is true (no three-valued NULL).
                const missing = row('m', { kind: 'apple' });
                assertFalse(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'qty' }, right: { lit: 3 } }, missing), 'missing -> false');
                assertTrue(evalRowFilter({ p: 'not', arg: { p: 'cmp', cmp: 'eq', left: { col: 'qty' }, right: { lit: 3 } } }, missing), 'not missing -> true');

                // implicit author system column
                const authored = row('o', { kind: 'apple' }, 'key-1');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: 'key-1' } }, authored), 'author match');
                assertFalse(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: 'key-2' } }, authored), 'author mismatch');
                assertFalse(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: 'key-1' } }, r), 'missing author does not match');
            }
        },
        {
            name: '[QRY02] validateRowQuery throws on user mistakes',
            invoke: async () => {
                // valid queries do not throw
                validateRowQuery({ where: { p: 'cmp', cmp: 'eq', left: { col: 'qty' }, right: { lit: 3 } } }, ITEM_COLUMNS);
                validateRowQuery({ select: ['kind', 'qty'], orderBy: [{ column: 'qty', dir: 'desc' }], limit: 5, offset: 2 }, ITEM_COLUMNS);

                expectThrow(() => validateRowQuery({ where: { p: 'cmp', cmp: 'eq', left: { col: 'nope' }, right: { lit: 1 } } }, ITEM_COLUMNS), 'unknown column in filter');
                expectThrow(() => validateRowQuery({ select: ['nope'] }, ITEM_COLUMNS), 'unknown column in select');
                expectThrow(() => validateRowQuery({ orderBy: [{ column: 'nope' }] }, ITEM_COLUMNS), 'unknown column in orderBy');
                expectThrow(() => validateRowQuery({ where: { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 3 } } }, ITEM_COLUMNS), 'type-incoherent cmp (string vs int)');
                expectThrow(() => validateRowQuery({ where: { p: 'str', str: 'prefix', value: { col: 'qty' }, sub: { lit: 'x' } } }, ITEM_COLUMNS), 'non-string str operand');
                expectThrow(() => validateRowQuery({ limit: -1 }, ITEM_COLUMNS), 'negative limit');
                expectThrow(() => validateRowQuery({ limit: 1.5 }, ITEM_COLUMNS), 'non-integer limit');
                expectThrow(() => validateRowQuery({ offset: -2 }, ITEM_COLUMNS), 'negative offset');
                expectThrow(() => validateRowQuery({ orderBy: [{ column: 'qty', dir: 'down' as 'asc' }] }, ITEM_COLUMNS), 'bad orderBy dir');
                expectThrow(() => validateRowQuery({ where: { p: 'bogus' } as unknown as RowFilter }, ITEM_COLUMNS), 'unknown filter tag');
                validateRowQuery({ where: { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: 'key-1' } } }, ITEM_COLUMNS);
            }
        },
        {
            name: '[QRY03] orderRows nulls-last + tiebreak, projectRow keeps identity',
            invoke: async () => {
                const rows = [
                    row('b', { qty: 2 }),
                    row('a', { qty: 2 }),
                    row('c', {}),            // missing qty -> sorts last
                    row('d', { qty: 1 }),
                ];

                const asc = orderRows(rows, [{ column: 'qty', dir: 'asc' }]);
                assertEquals(asc.map((r) => r.uuid).join(','), 'd,a,b,c', 'asc: 1, then 2 (rowId tiebreak a<b), then missing last');

                const desc = orderRows(rows, [{ column: 'qty', dir: 'desc' }]);
                assertEquals(desc.map((r) => r.uuid).join(','), 'a,b,d,c', 'desc: 2 (a<b tiebreak), 1, missing still last');

                const projected = projectRow(row('x', { kind: 'apple', qty: 3, label: 'hi' }, 'author-key'), ['kind']);
                assertEquals(projected.uuid, 'x', 'projection keeps uuid');
                assertEquals(projected.author, 'author-key', 'projection keeps author');
                assertEquals(JSON.stringify(projected.values), JSON.stringify({ kind: 'apple' }), 'projection restricts values to select');
            }
        },
        {
            name: '[QRY04] engine: pub-eq pushdown agrees with full scan',
            invoke: async () => {
                const { group } = await createItemsGroup();
                const items = await group.getTable('items');
                await items.insert('i1', { kind: 'fruit', qty: 5, price: 1.0 });
                await items.insert('i2', { kind: 'veg', qty: 2, price: 2.0 });
                await items.insert('i3', { kind: 'fruit', qty: 9, price: 3.0 });
                await items.insert('i4', { kind: 'grain', qty: 1, price: 0.5 });

                const view = await items.getView();

                // pushdown: cmp eq on the pub column `kind`
                const pushdown = await view.query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'fruit' } } });
                assertEquals(uuids(pushdown), 'i1,i3', 'pushdown returns the two fruit rows');

                // scan-equivalent (str prefix on `kind` does not push down): same rows
                const scan = await view.query({ where: { p: 'str', str: 'prefix', value: { col: 'kind' }, sub: { lit: 'fruit' } } });
                assertEquals(uuids(scan), uuids(pushdown), 'scan path matches pushdown path');

                // filter on a NON-pub column (forces residual scan over resolved rows)
                const byQty = await view.query({ where: { p: 'cmp', cmp: 'ge', left: { col: 'qty' }, right: { lit: 5 } } });
                assertEquals(uuids(byQty), 'i1,i3', 'qty>=5 returns the two high-qty rows');

                // empty query returns all live rows
                const all = await view.query({});
                assertEquals(uuids(all), 'i1,i2,i3,i4', 'empty query returns all live rows');

                // compound: pushable conjunct + residual conjunct
                const compound = await view.query({ where: { p: 'and', args: [
                    { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'fruit' } },
                    { p: 'cmp', cmp: 'gt', left: { col: 'qty' }, right: { lit: 5 } },
                ] } });
                assertEquals(uuids(compound), 'i3', 'pushdown candidate further filtered by residual qty>5');
            }
        },
        {
            name: '[QRY05] engine: author system-column filters',
            invoke: async () => {
                const { group } = await createItemsGroup();
                const items = await group.getTable('items');
                const alice = await makeIdentity();

                await items.insert('anon1', { kind: 'fruit', qty: 1 });
                await items.insert('authored1', { kind: 'fruit', qty: 2 }, alice);
                await items.insert('anon2', { kind: 'veg', qty: 3 });

                const view = await items.getView();

                const authored = await view.query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: alice.keyId } } });
                assertEquals(uuids(authored), 'authored1', 'author filter returns only alice rows');

                const authoredFruit = await view.query({ where: { p: 'and', args: [
                    { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: alice.keyId } },
                    { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'fruit' } },
                ] } });
                assertEquals(uuids(authoredFruit), 'authored1', 'author pushdown + residual kind filter');
            }
        },
        {
            name: '[QRY06] engine: orderBy / limit / offset / select',
            invoke: async () => {
                const { group } = await createItemsGroup();
                const items = await group.getTable('items');
                await items.insert('i1', { kind: 'a', qty: 3, price: 1.0 });
                await items.insert('i2', { kind: 'b', qty: 1, price: 2.0 });
                await items.insert('i3', { kind: 'c', qty: 2, price: 3.0 });

                const view = await items.getView();

                const ordered = await view.query({ orderBy: [{ column: 'qty', dir: 'asc' }] });
                assertEquals(ordered.map((r) => r.uuid).join(','), 'i2,i3,i1', 'orderBy qty asc');

                const desc = await view.query({ orderBy: [{ column: 'qty', dir: 'desc' }] });
                assertEquals(desc.map((r) => r.uuid).join(','), 'i1,i3,i2', 'orderBy qty desc');

                const page = await view.query({ orderBy: [{ column: 'qty', dir: 'asc' }], offset: 1, limit: 1 });
                assertEquals(page.map((r) => r.uuid).join(','), 'i3', 'offset+limit page');

                const projected = await view.query({ select: ['kind'], orderBy: [{ column: 'qty', dir: 'asc' }], limit: 1 });
                assertEquals(projected.length, 1, 'one row');
                assertEquals(projected[0].uuid, 'i2', 'projection keeps identity');
                assertEquals(JSON.stringify(projected[0].values), JSON.stringify({ kind: 'b' }), 'projection restricts values');
            }
        },
        {
            name: '[QRY07] engine: liveness + LWW (deletes excluded, resolved not stale values)',
            invoke: async () => {
                const { group } = await createItemsGroup();
                const items = await group.getTable('items');
                await items.insert('i1', { kind: 'fruit', qty: 5 });
                await items.insert('i2', { kind: 'fruit', qty: 6 });
                await items.insert('i3', { kind: 'veg', qty: 7 });

                // delete one fruit row; update the other's non-pub and pub fields
                await items.delete(deriveRowId('i2'));
                await items.update(deriveRowId('i1'), { qty: 50 });          // non-pub LWW
                await items.update(deriveRowId('i3'), { kind: 'fruit' });    // pub LWW: veg -> fruit

                const view = await items.getView();

                const fruit = await view.query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'fruit' } } });
                assertEquals(uuids(fruit), 'i1,i3', 'deleted i2 excluded; i3 now matches via resolved pub value');

                // pushdown on stale pub meta must not surface i3 under its OLD value
                const stillVeg = await view.query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'veg' } } });
                assertEquals(uuids(stillVeg), '', 'no row resolves to the stale veg value');

                const i1 = (await view.query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'fruit' } }, select: ['qty'] }))
                    .find((r) => r.uuid === 'i1')!;
                assertEquals(i1.values['qty'], 50, 'non-pub LWW value reflected (not the stale 5)');
            }
        },
        {
            name: '[QRY08] typed comparison + ordering: bigint / decimal are numeric, not lexical',
            invoke: async () => {
                const typeOf = (c: string): 'bigint' | 'decimal' | undefined =>
                    c === 'seq' ? 'bigint' : c === 'amount' ? 'decimal' : undefined;

                // Without type context, string carriers would compare lexically
                // ('9' > '10'); with type context they compare by value.
                const nine = row('n', { seq: '9', amount: '9.99' });
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'lt', left: { col: 'seq' }, right: { lit: '10' } }, nine, typeOf),
                    'bigint 9 < 10 with type context');
                assertFalse(evalRowFilter({ p: 'cmp', cmp: 'lt', left: { col: 'seq' }, right: { lit: '10' } }, nine),
                    'bigint 9 vs 10 would compare lexically (9 > 10) with no type context');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'lt', left: { col: 'amount' }, right: { lit: '10.00' } }, nine, typeOf),
                    'decimal 9.99 < 10.00 with type context');
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq', left: { col: 'amount' }, right: { lit: '9.99' } }, nine, typeOf),
                    'decimal eq is normalized-string equality');

                // exact typed arithmetic in a predicate
                assertTrue(evalRowFilter({ p: 'cmp', cmp: 'eq',
                    left: { fn: 'add', args: [{ col: 'seq' }, { lit: '1' }] }, right: { lit: '10' } }, nine, typeOf),
                    'bigint add: 9 + 1 == 10');

                // numeric ordering (not lexical) via orderRows with types
                const rows = [
                    row('a', { seq: '100', amount: '1.00' }),
                    row('b', { seq: '9', amount: '10.50' }),
                    row('c', { seq: '10', amount: '2.05' }),
                ];
                const bySeq = orderRows(rows, [{ column: 'seq', dir: 'asc' }], typeOf);
                assertEquals(bySeq.map((r) => r.uuid).join(','), 'b,c,a', 'bigint order: 9 < 10 < 100 (numeric)');
                const byAmount = orderRows(rows, [{ column: 'amount', dir: 'asc' }], typeOf);
                assertEquals(byAmount.map((r) => r.uuid).join(','), 'a,c,b', 'decimal order: 1.00 < 2.05 < 10.50 (numeric)');
            }
        },
        {
            name: '[QRY09] engine: bigint pub-eq lookup + decimal ordering + write-path reject',
            invoke: async () => {
                const { group } = await createLedgerGroup();
                const ledger = await group.getTable('ledger');
                await ledger.insert('l1', { seq: '9', amount: '9.99', qty: 5 });
                await ledger.insert('l2', { seq: '10', amount: '10.00', qty: 10 });
                await ledger.insert('l3', { seq: '100', amount: '2.50', qty: 0 });

                const view = await ledger.getView();

                // pub-eq index lookup on the bigint column returns the exact row
                const bySeq = await view.query({ where: { p: 'cmp', cmp: 'eq', left: { col: 'seq' }, right: { lit: '10' } } });
                assertEquals(uuids(bySeq), 'l2', 'bigint pub-eq lookup matches the canonical-string carrier');

                // numeric decimal ordering through the engine
                const byAmount = await view.query({ orderBy: [{ column: 'amount', dir: 'asc' }] });
                assertEquals(byAmount.map((r) => r.uuid).join(','), 'l3,l1,l2', 'decimal orderBy is numeric (2.50 < 9.99 < 10.00)');

                // numeric bigint ordering (not lexical)
                const bySeqOrder = await view.query({ orderBy: [{ column: 'seq', dir: 'asc' }] });
                assertEquals(bySeqOrder.map((r) => r.uuid).join(','), 'l1,l2,l3', 'bigint orderBy is numeric (9 < 10 < 100)');

                // write-path Layer-1 rejects: reject, never round
                const expectInsertFailure = async (uuid: string, values: Row['values'], why: string, msgIncludes?: string) => {
                    let message: string | undefined;
                    try { await ledger.insert(uuid, values); } catch (e) { message = e instanceof Error ? e.message : String(e); }
                    assertTrue(message !== undefined, why);
                    if (msgIncludes !== undefined) {
                        assertTrue(message!.includes(msgIncludes), `${why}: message should include '${msgIncludes}', got: ${message}`);
                    }
                };
                await expectInsertFailure('bad1', { seq: '007', amount: '1.00' }, 'non-canonical bigint (leading zeros) rejected',
                    "column 'seq' (bigint): '007' is not a canonical bigint");
                await expectInsertFailure('bad2', { seq: '1', amount: '1.5' }, 'wrong-scale decimal rejected (never rounded)',
                    "column 'amount' (decimal): '1.5' is not a canonical decimal(scale=2)");
                await expectInsertFailure('bad3', { seq: '1', amount: '1.005' }, 'over-scale decimal rejected (never rounded)');
                await expectInsertFailure('bad4', { seq: '1', amount: '1.00', qty: 200 }, 'out-of-range integer rejected',
                    "column 'qty' (integer): integer 200 is out of range [0, 100]");
                await expectInsertFailure('bad5', { seq: '1', amount: '1.00', qty: -1 }, 'below-min integer rejected');
            }
        },
    ],
};
