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

async function createItemsGroup() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        seed: 'query-test-schema',
        name: 'inventory',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [itemsTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: 'query-test', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, group, admin };
}

const ITEM_COLUMNS: ColumnTypes = { kind: 'string', qty: 'integer', price: 'float', label: 'string' };

function row(rowId: string, values: Row['values'], owner?: string): Row {
    const r: Row = { rowId, uuid: rowId, values };
    if (owner !== undefined) r.owner = owner;
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

                // owner / anonymous atoms
                const owned = row('o', { kind: 'apple' }, 'key-1');
                assertTrue(evalRowFilter({ p: 'owner', is: 'key-1' }, owned), 'owner match');
                assertFalse(evalRowFilter({ p: 'owner', is: 'key-2' }, owned), 'owner mismatch');
                assertFalse(evalRowFilter({ p: 'anonymous' }, owned), 'owned is not anonymous');
                assertTrue(evalRowFilter({ p: 'anonymous' }, r), 'no owner is anonymous');
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
                expectThrow(() => validateRowQuery({ where: { p: 'owner', is: 42 as unknown as string } }, ITEM_COLUMNS), 'owner.is must be string');
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

                const projected = projectRow(row('x', { kind: 'apple', qty: 3, label: 'hi' }, 'owner-key'), ['kind']);
                assertEquals(projected.uuid, 'x', 'projection keeps uuid');
                assertEquals(projected.owner, 'owner-key', 'projection keeps owner');
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
            name: '[QRY05] engine: owner / anonymous filters',
            invoke: async () => {
                const { group } = await createItemsGroup();
                const items = await group.getTable('items');
                const alice = await makeIdentity();

                await items.insert('anon1', { kind: 'fruit', qty: 1 });
                await items.insert('owned1', { kind: 'fruit', qty: 2 }, alice.keyId, alice);
                await items.insert('anon2', { kind: 'veg', qty: 3 });

                const view = await items.getView();

                const owned = await view.query({ where: { p: 'owner', is: alice.keyId } });
                assertEquals(uuids(owned), 'owned1', 'owner filter returns only alice rows');

                const anon = await view.query({ where: { p: 'anonymous' } });
                assertEquals(uuids(anon), 'anon1,anon2', 'anonymous filter returns owner-less rows');

                const ownedFruit = await view.query({ where: { p: 'and', args: [
                    { p: 'owner', is: alice.keyId },
                    { p: 'cmp', cmp: 'eq', left: { col: 'kind' }, right: { lit: 'fruit' } },
                ] } });
                assertEquals(uuids(ownedFruit), 'owned1', 'owner pushdown + residual kind filter');
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
    ],
};
