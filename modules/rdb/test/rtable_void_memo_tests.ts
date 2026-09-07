import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

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

// Regression net for the per-computation verdict memo (VOID_SEMANTICS.md §4,
// "The per-computation memo"). The table deliberately has NO explicit
// restrictions, so updates run under the default `rowAuthor = $author` rule —
// the projection / `doc.pages` path that surfaced the bug. That rule getRow's
// the subject at the update's own position, which void-checks every earlier
// write of the row, each of which getRow's again: T(k) ~ 2^(k-1) without the
// memo. N = 24 is chosen so the unmemoized engine (2^23 diagnoses) blows the
// 60s suite timeout while the memoized one finishes in tens of milliseconds.
// PERM12 / OBSGATE07 are the matching soundness nets (cycle participants are
// not memoized; independent computations do not share a closure).
function pagesTable(): TableDef {
    return {
        name: 'pages',
        columns: {
            title: { type: 'string' },
            deleted: { type: 'boolean' },
        },
    };
}

async function createEnv() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const author = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'voidmemo:schema',
        creators: [{ keyId: author.keyId, publicKey: author.publicKey }],
        tables: [pagesTable()],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'voidmemo-group',
        seed: 'voidmemo-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const pages = await group.getTable('pages');
    return { group, pages, author };
}

export const rtableVoidMemoTests = {
    title: '[VOID_MEMO] Per-closure completed void-verdict memo',
    tests: [
        {
            name: '[VOID_MEMO01] many authored updates of one row: getRow and query stay linear',
            invoke: async () => {
                const { group, pages, author } = await createEnv();
                const uuid = 'p-1';
                const rowId = deriveRowId(uuid, author.keyId);

                await pages.insert(uuid, { title: 'hi', deleted: false }, author);
                const N = 24;
                let lastHash: string | undefined;
                for (let i = 0; i < N; i++) {
                    lastHash = await pages.update(rowId, { deleted: i % 2 === 0 }, author);
                }

                const started = Date.now();
                const view = await (await group.getView()).getTableView('pages');
                const row = await view.getRow(rowId);
                const rows = await view.query({});
                const elapsed = Date.now() - started;

                assertTrue(row !== undefined, 'row is live after the update chain');
                assertEquals(row!.values['deleted'], (N - 1) % 2 === 0, 'last deleted write wins');
                assertEquals(rows.length, 1, 'query returns the single live row');
                assertTrue(elapsed < 5000,
                    `getRow + query after ${N} updates should be linear (got ${elapsed}ms)`);

                const from = await (await group.getScopedDag()).getFrontier();
                assertTrue(lastHash !== undefined, 'at least one update landed');
                assertTrue(!(await group.isEntryVoided(lastHash!, from)),
                    'the last update is not voided');
            },
        },
    ],
};
