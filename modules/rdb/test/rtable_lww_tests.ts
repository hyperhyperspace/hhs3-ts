import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";
import { identitiesTableDef, localIdentityProvider } from "./identity_fixture.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// pub and readonly exercised in every combination:
//   sku  - pub + readonly (witness-style: filterable, fixed at insert)
//   tag  - pub, mutable (updates export fresh pub meta)
//   code - readonly, not pub
//   qty  - plain mutable
//   note - plain mutable with default
function itemsTable(): TableDef {
    return {
        name: 'items',
        columns: {
            sku: { type: 'string', pub: true, readonly: true },
            tag: { type: 'string', pub: true },
            code: { type: 'string', readonly: true, nullable: true },
            qty: { type: 'integer' },
            note: { type: 'string', default: 'none' },
        },
        // permit unauthored updates/deletes: the LWW mechanics, not the
        // default author-is-author restriction (see [ENF]), are under test
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

// For null writes: `memo` is nullable with no default, `tag` is nullable with
// a default and pub (so it can be searched), `title` is NOT NULL.
function notesTable(): TableDef {
    return {
        name: 'notes',
        columns: {
            title: { type: 'string' },
            memo: { type: 'string', nullable: true },
            tag: { type: 'string', nullable: true, pub: true, default: 'none' },
        },
        restrictions: [{ on: 'all', rule: { p: 'true' } }],
    };
}

// `authors` get a local identity provider, so their signed ops verify.
async function createTestEnv(authors?: OwnIdentity[]) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();

    const schemaInit = await RSchemaImpl.create({
        name: 'lww:test_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: authors === undefined ? [itemsTable(), notesTable()] : [itemsTable(), notesTable(), identitiesTableDef()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'lww-test-group',
        seed: 'lww-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...(authors === undefined ? {} : localIdentityProvider(authors)),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const items = await group.getTable('items');
    const notes = await group.getTable('notes');

    return { ctx, schema, group, items, notes, admin };
}

export const rtableLwwTests = {
    title: '[LWW] RTable per-field LWW update tests',
    tests: [
        {
            name: '[LWW01] Update changes carried fields and preserves others',
            invoke: async () => {
                const { items } = await createTestEnv();
                const rowId = deriveRowId('i-1');

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                await items.update(rowId, { qty: 7 });

                const row = (await (await items.getView()).getRow(rowId))!;
                assertEquals(row.values['qty'], 7, 'the updated field should carry the new value');
                assertEquals(row.values['tag'], 'red', 'untouched fields should be preserved');
                assertEquals(row.values['sku'], 'A1', 'readonly fields keep their insert value');
                assertEquals(row.values['note'], 'none', 'schema defaults should still fill absent columns');
            }
        },
        {
            name: '[LWW02] Sequential updates: the latest write wins per field',
            invoke: async () => {
                const { items } = await createTestEnv();
                const rowId = deriveRowId('i-1');

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                await items.update(rowId, { qty: 2, note: 'first' });
                await items.update(rowId, { qty: 3 });

                const row = (await (await items.getView()).getRow(rowId))!;
                assertEquals(row.values['qty'], 3, 'the causally-latest write should win');
                assertEquals(row.values['note'], 'first', 'fields not touched by the later update keep the earlier write');
            }
        },
        {
            name: '[LWW03] Concurrent updates to different fields both land',
            invoke: async () => {
                const { group, items } = await createTestEnv();
                const rowId = deriveRowId('i-1');

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                const base = await (await group.getScopedDag()).getFrontier();

                await items.update(rowId, { qty: 9 }, undefined, base);
                await items.update(rowId, { note: 'branched' }, undefined, base);

                const row = (await (await items.getView()).getRow(rowId))!;
                assertEquals(row.values['qty'], 9, 'branch A write should land');
                assertEquals(row.values['note'], 'branched', 'branch B write should land');
                assertEquals(row.values['tag'], 'red', 'untouched fields stay');
            }
        },
        {
            name: '[LWW04] Concurrent updates to the same field resolve by entry-hash tiebreak',
            invoke: async () => {
                const { group, items } = await createTestEnv();
                const rowId = deriveRowId('i-1');

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                const base = await (await group.getScopedDag()).getFrontier();

                const ha = await items.update(rowId, { qty: 10 }, undefined, base);
                const hb = await items.update(rowId, { qty: 20 }, undefined, base);
                const expected = ha > hb ? 10 : 20;

                const row = (await (await items.getView()).getRow(rowId))!;
                assertEquals(row.values['qty'], expected,
                    'concurrent same-field writes should resolve by the larger entry hash');
            }
        },
        {
            name: '[LWW05] Duplicate concurrent inserts: entry-hash winner provides the base; updates apply on top',
            invoke: async () => {
                const { group, items } = await createTestEnv();
                const rowId = deriveRowId('i-1');
                const base = await (await group.getScopedDag()).getFrontier();

                // same rowId (same uuid, anonymous) inserted on two branches
                // with different non-pub values: one incarnation, hash winner
                // is the value base
                const ha = await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 }, undefined, base);
                const hb = await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 2 }, undefined, base);
                const expectedBase = ha > hb ? 1 : 2;

                const merged = (await (await items.getView()).getRow(rowId))!;
                assertEquals(merged.values['qty'], expectedBase,
                    'the larger-hash insert should provide the base values');

                // a causally-later update beats both insert base writes
                await items.update(rowId, { qty: 5 });
                const after = (await (await items.getView()).getRow(rowId))!;
                assertEquals(after.values['qty'], 5, 'an update on top should win over the insert base');
            }
        },
        {
            name: '[LWW06] Updated values invisible after the permanent delete; old positions still show them',
            invoke: async () => {
                const { group, items } = await createTestEnv();
                const rowId = deriveRowId('i-1');
                const scopedDag = await group.getScopedDag();

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                await items.update(rowId, { qty: 4 });
                const beforeDelete = await scopedDag.getFrontier();

                await items.delete(rowId);

                const now = await items.getView();
                assertFalse(await now.hasRow(rowId), 'the row should be dead at the frontier');
                assertEquals(await now.getRow(rowId), undefined, 'getRow of a deleted row should be undefined');

                const old = await items.getView(beforeDelete, beforeDelete);
                assertEquals((await old.getRow(rowId))!.values['qty'], 4,
                    'the old position should still resolve the updated value (position purity)');
            }
        },
        {
            name: '[LWW07] Update of a readonly column is rejected (pub or not)',
            invoke: async () => {
                const { items } = await createTestEnv();
                const rowId = deriveRowId('i-1');
                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1, code: 'X' });

                const expectUpdateFailure = async (values: { [c: string]: string | number }, why: string) => {
                    let failed = false;
                    try {
                        await items.update(rowId, values);
                    } catch {
                        failed = true;
                    }
                    assertTrue(failed, why);
                };

                await expectUpdateFailure({ sku: 'B2' }, 'updating a pub+readonly column should be rejected');
                await expectUpdateFailure({ code: 'Y' }, 'updating a non-pub readonly column should be rejected');
                await expectUpdateFailure({ qty: 2, sku: 'B2' },
                    'an update mixing a readonly column with valid ones should be rejected whole');

                const row = (await (await items.getView()).getRow(rowId))!;
                assertEquals(row.values['sku'], 'A1', 'sku should be unchanged');
                assertEquals(row.values['code'], 'X', 'code should be unchanged');
                assertEquals(row.values['qty'], 1, 'the rejected mixed update must not partially land');
            }
        },
        {
            name: '[LWW08] Update with unknown column or type mismatch is rejected',
            invoke: async () => {
                const { items } = await createTestEnv();
                const rowId = deriveRowId('i-1');
                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });

                const expectUpdateFailure = async (values: { [c: string]: string | number }, why: string) => {
                    let failed = false;
                    try {
                        await items.update(rowId, values);
                    } catch {
                        failed = true;
                    }
                    assertTrue(failed, why);
                };

                await expectUpdateFailure({ nope: 'x' }, 'an update carrying an unknown column should be rejected');
                await expectUpdateFailure({ qty: 'lots' }, 'an update with a type mismatch should be rejected');
                await expectUpdateFailure({}, 'an empty update should be rejected');
            }
        },
        {
            name: '[LWW09] Update of a dead or missing row is rejected at write time',
            invoke: async () => {
                const { items } = await createTestEnv();

                let missingFailed = false;
                try {
                    await items.update(deriveRowId('ghost'), { qty: 1 });
                } catch {
                    missingFailed = true;
                }
                assertTrue(missingFailed, 'updating a row that was never inserted should be rejected');

                const rowId = deriveRowId('i-1');
                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                await items.delete(rowId);

                let deadFailed = false;
                try {
                    await items.update(rowId, { qty: 2 });
                } catch {
                    deadFailed = true;
                }
                assertTrue(deadFailed, 'updating a (permanently) deleted row should be rejected');
            }
        },
        {
            name: '[LWW10] Signed update by the row author lands',
            invoke: async () => {
                const author = await makeIdentity();
                const { items } = await createTestEnv([author]);
                const rowId = deriveRowId('i-1', author.keyId);

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 }, author);
                await items.update(rowId, { qty: 6 }, author);

                const row = (await (await items.getView()).getRow(rowId))!;
                assertEquals(row.values['qty'], 6, 'the signed update should land');
                assertEquals(row.author, author.keyId, 'authorship comes from the insert');
            }
        },
        {
            name: '[LWW11] Updated pub column is found by its new value',
            invoke: async () => {
                const { items } = await createTestEnv();
                const rowId = deriveRowId('i-1');

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                await items.update(rowId, { tag: 'blue' });

                const view = await items.getView();
                const byNew = await view.findRowIds({ tag: 'blue' });
                assertEquals(byNew.toString(), [rowId].toString(),
                    'the row should be found through the update-exported pub meta');

                const row = (await view.getRow(rowId))!;
                assertEquals(row.values['tag'], 'blue', 'getRow should agree with the search');
            }
        },
        {
            name: '[LWW12] Stale pub meta hits are filtered against resolved values',
            invoke: async () => {
                const { group, items } = await createTestEnv();
                const rowId = deriveRowId('i-1');
                const otherId = deriveRowId('i-2');
                const scopedDag = await group.getScopedDag();

                await items.insert('i-1', { sku: 'A1', tag: 'red', qty: 1 });
                await items.insert('i-2', { sku: 'A2', tag: 'red', qty: 2 });
                const beforeUpdate = await scopedDag.getFrontier();

                await items.update(rowId, { tag: 'blue' });

                // the insert meta for tag='red' still indexes i-1, but its
                // resolved value no longer matches
                const view = await items.getView();
                const byOld = await view.findRowIds({ tag: 'red' });
                assertEquals(byOld.toString(), [otherId].toString(),
                    'a row whose pub value changed away should no longer match its old value');

                // position purity: at the old position both rows still match
                const old = await items.getView(beforeUpdate, beforeUpdate);
                const oldMatches = await old.findRowIds({ tag: 'red' });
                assertEquals(oldMatches.toString(), [rowId, otherId].sort().toString(),
                    'the old position should still see the pre-update value');

                // concurrent same-field pub updates: the loser is filtered too
                const base = await scopedDag.getFrontier();
                const ha = await items.update(rowId, { tag: 'green' }, undefined, base);
                const hb = await items.update(rowId, { tag: 'amber' }, undefined, base);
                const winner = ha > hb ? 'green' : 'amber';
                const loser = ha > hb ? 'amber' : 'green';

                const merged = await items.getView(version(ha, hb), version(ha, hb));
                assertEquals((await merged.findRowIds({ tag: winner })).toString(), [rowId].toString(),
                    'the tiebreak winner value should match');
                assertEquals((await merged.findRowIds({ tag: loser })).length, 0,
                    'the tiebreak loser value should be filtered despite its meta hit');
            }
        },
        {
            name: '[LWW13] A null write clears a column, and an explicit null wins over the default',
            invoke: async () => {
                const { notes } = await createTestEnv();
                const read = async (uuid: string) => (await (await notes.getView()).getRow(deriveRowId(uuid)))!.values;

                // an update to null of a column that held a value
                await notes.insert('n-1', { title: 'a', memo: 'm', tag: 'x' });
                await notes.update(deriveRowId('n-1'), { memo: null, tag: null });
                const cleared = await read('n-1');
                assertTrue(!('memo' in cleared), 'a nulled column with no default is absent from the row');
                assertTrue(!('tag' in cleared), 'a nulled column is absent also where it has a default');
                assertEquals(cleared['title'], 'a', 'the other columns are untouched');

                // an insert with an explicit null, and the same insert without the column
                await notes.insert('n-2', { title: 'a', tag: null });
                await notes.insert('n-3', { title: 'a' });
                assertTrue(!('tag' in await read('n-2')), 'an explicit null in an insert wins over the default');
                assertEquals((await read('n-3'))['tag'], 'none', 'a column left out of an insert reads the default');

                // a value written on top of a null shows again
                await notes.update(deriveRowId('n-2'), { tag: 'y' });
                assertEquals((await read('n-2'))['tag'], 'y', 'a later value replaces the null');
            }
        },
        {
            name: '[LWW14] A value wins over a concurrent null, whichever entry has the larger hash',
            invoke: async () => {
                const { group, notes } = await createTestEnv();
                const scopedDag = await group.getScopedDag();

                // repeat over fresh rows until the null had the larger hash and the smaller one
                let nullLarger = 0;
                let valueLarger = 0;
                for (let i = 0; i < 64 && (nullLarger === 0 || valueLarger === 0); i++) {
                    const uuid = 'n-' + i;
                    const rowId = deriveRowId(uuid);
                    await notes.insert(uuid, { title: 'a', memo: 'old', tag: 'old' });
                    const base = await scopedDag.getFrontier();

                    const hNull = await notes.update(rowId, { memo: null, tag: null }, undefined, base);
                    const hValue = await notes.update(rowId, { memo: 'new', tag: 'new' }, undefined, base);
                    if (hNull > hValue) nullLarger++; else valueLarger++;

                    const row = (await (await notes.getView()).getRow(rowId))!;
                    assertEquals(row.values['memo'], 'new', 'the concurrent value wins over the null');
                    assertEquals(row.values['tag'], 'new', 'the concurrent value wins over the null, with a default too');

                    // each branch on its own still sees what it wrote
                    const onNull = (await (await notes.getView(version(hNull), version(hNull))).getRow(rowId))!;
                    assertTrue(!('memo' in onNull.values) && !('tag' in onNull.values), 'the null branch alone reads NULL');
                }
                assertTrue(nullLarger > 0 && valueLarger > 0, 'both hash orders were exercised');

                // two concurrent nulls: NULL
                await notes.insert('both', { title: 'a', memo: 'old' });
                const base = await scopedDag.getFrontier();
                await notes.update(deriveRowId('both'), { memo: null }, undefined, base);
                await notes.update(deriveRowId('both'), { memo: null, title: 'b' }, undefined, base);
                assertTrue(!('memo' in (await (await notes.getView()).getRow(deriveRowId('both')))!.values), 'two concurrent nulls read NULL');
            }
        },
        {
            name: '[LWW15] A null hides only the writes its writer had seen',
            invoke: async () => {
                const { group, notes } = await createTestEnv();
                const scopedDag = await group.getScopedDag();
                const rowId = deriveRowId('n-1');

                await notes.insert('n-1', { title: 'a', memo: 'v1' });
                const base = await scopedDag.getFrontier();

                // the null's writer had seen v1 and v2; v3 is concurrent with the null
                const hV2 = await notes.update(rowId, { memo: 'v2' }, undefined, base);
                const hNull = await notes.update(rowId, { memo: null }, undefined, version(hV2));
                const atNull = (await (await notes.getView(version(hNull), version(hNull))).getRow(rowId))!;
                assertTrue(!('memo' in atNull.values), 'a null written after a value it had seen hides it');

                await notes.update(rowId, { memo: 'v3' }, undefined, base);
                const merged = (await (await notes.getView()).getRow(rowId))!;
                assertEquals(merged.values['memo'], 'v3', 'a value the null had not seen wins over it');
            }
        },
        {
            name: '[LWW16] Duplicate concurrent inserts: a value wins over a null, and a null over a column left out',
            invoke: async () => {
                const { group, notes } = await createTestEnv();
                const scopedDag = await group.getScopedDag();

                let nullLarger = 0;
                let otherLarger = 0;
                for (let i = 0; i < 64 && (nullLarger === 0 || otherLarger === 0); i++) {
                    const base = await scopedDag.getFrontier();

                    // one insert nulls memo and tag, the other gives memo a value and leaves tag out
                    const uuid = 'n-' + i;
                    const hNull = await notes.insert(uuid, { title: 'a', memo: null, tag: null }, undefined, base);
                    const hOther = await notes.insert(uuid, { title: 'a', memo: 'm' }, undefined, base);
                    if (hNull > hOther) nullLarger++; else otherLarger++;

                    const row = (await (await notes.getView()).getRow(deriveRowId(uuid)))!;
                    assertEquals(row.values['memo'], 'm', "the other insert's value shows, whichever hash is larger");
                    assertTrue(!('tag' in row.values), 'a column one insert nulls and the other leaves out reads NULL, not the default');
                }
                assertTrue(nullLarger > 0 && otherLarger > 0, 'both hash orders were exercised');

                // with no default, a null and a column left out read the same
                const base = await scopedDag.getFrontier();
                await notes.insert('same', { title: 'a', memo: null }, undefined, base);
                await notes.insert('same', { title: 'a' }, undefined, base);
                assertTrue(!('memo' in (await (await notes.getView()).getRow(deriveRowId('same')))!.values), 'both read NULL');
            }
        },
        {
            name: '[LWW17] A nulled pub column is not found by its old value, nor by the default',
            invoke: async () => {
                const { notes } = await createTestEnv();
                const cleared = deriveRowId('n-1');
                const kept = deriveRowId('n-2');
                const defaulted = deriveRowId('n-3');
                const nulledAtInsert = deriveRowId('n-4');

                await notes.insert('n-1', { title: 'a', tag: 'red' });
                await notes.insert('n-2', { title: 'a', tag: 'red' });
                await notes.insert('n-3', { title: 'a' });
                await notes.insert('n-4', { title: 'a', tag: null });
                await notes.update(cleared, { tag: null });

                const view = await notes.getView();
                assertEquals((await view.findRowIds({ tag: 'red' })).toString(), [kept].toString(),
                    'a row whose pub column was nulled is not found by its old value');
                assertEquals((await view.findRowIds({ tag: 'none' })).toString(), [defaulted].toString(),
                    'a lookup by the default finds the row that never wrote the column, not the ones that nulled it');

                // a value written on top of the null is found again
                await notes.update(nulledAtInsert, { tag: 'blue' });
                assertEquals((await (await notes.getView()).findRowIds({ tag: 'blue' })).toString(), [nulledAtInsert].toString(),
                    'a value written after a null is found');
            }
        },
    ],
};
