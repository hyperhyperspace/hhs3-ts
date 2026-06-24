import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";

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

async function createTestEnv() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();

    const schemaInit = await RSchemaImpl.create({
        name: 'lww:test_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [itemsTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'lww-test-group',
        seed: 'lww-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const items = await group.getTable('items');

    return { ctx, schema, group, items, admin };
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
                const { items } = await createTestEnv();
                const author = await makeIdentity();
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
    ],
};
