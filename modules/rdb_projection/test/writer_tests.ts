// HIP-20's writer through RdbProjection: openWriter, with and without a writer
// key; rowHash, localId and tableNames as a QuerySource; and the author setting
// read by a writer at each write.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { keyIdFromPublicKey, SIGNING_ED25519, type PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64, type RContext } from "@hyper-hyper-space/hhs3_mvt";
import { IDENTITIES_TABLE, registerIdentity, usersSchemaTables } from "@hyper-hyper-space/hhs3_rdb";
import { isWritableTarget, MemoryTarget, WriteRefusedError } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { RdbProjection } from "../src/index.js";
import { addGroups, makeDatabase, makeIdentity, newCtx, open, poll } from "./projection_tests.js";

const identities = () => usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!;

// A database with a signed group, where `writer` is registered, and an
// anonymous one.
async function mixedDb(ctx: RContext) {
    const db = await makeDatabase(ctx, 'writer-db', [
        { name: 'signed', tables: [identities(), open('notes', { body: { type: 'string' } })], idProvider: IDENTITIES_TABLE },
        { name: 'anon', tables: [open('posts', { title: { type: 'string' } })] },
    ]);
    const signed = db.members['signed']!.group;
    const writer = await makeIdentity();
    await registerIdentity(signed, writer);
    return { db, rdb: db.rdb, signed, anon: db.members['anon']!.group, writer };
}

async function refusal(write: Promise<unknown>): Promise<string> {
    try {
        await write;
        return 'no refusal';
    } catch (e) {
        return e instanceof WriteRefusedError ? e.reason : `another error: ${String(e)}`;
    }
}

const authorOf = async (memory: MemoryTarget, table: string, id: number): Promise<unknown> =>
    (await memory.executeQuery({ table })).find((r) => r.id === id)?.author_key_id ?? null;

export const writerTests = {
    title: "[RDBPROJ] HIP-20's writer through the projection",
    tests: [
        {
            name: '[RDBPROJ33] an insert through projection.openWriter() is ingested with the writer key, and project-back leaves its author column as it was',
            invoke: async () => {
                const ctx = newCtx();
                const { rdb, signed, writer } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const projection = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                try {
                    assertTrue(isWritableTarget(projection), 'the projection is a source a writer can be opened from');
                    const w = projection.openWriter();
                    const id = await w.insert('signed_notes', { body: 'hello' });
                    const keyId = await w.writerKeyId();
                    assertEquals(await authorOf(memory, 'signed_notes', id), keyId, "the row reads the writer key's id at once");
                    await projection.sync();
                    const [row] = await (await (await signed.getTable('notes')).getView()).query({});
                    assertEquals(row?.author, writer.keyId, 'rdb signs it with the writer key');
                    assertEquals(await authorOf(memory, 'signed_notes', id), keyId, 'and project-back leaves the author column as it was');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ34] a projection opened without a writer key opens a writer: a write to a group with an identity provider refuses no-writer-key, and one to an anonymous group is captured and ingested',
            invoke: async () => {
                const ctx = newCtx();
                const { rdb, anon } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const projection = await RdbProjection.open(rdb, ctx, memory, { debounceMs: 5 });
                try {
                    const w = projection.openWriter();
                    assertEquals(await refusal(w.insert('signed_notes', { body: 'x' })), 'no-writer-key', 'a signed group refuses');
                    assertEquals(await refusal(w.writerKeyId()), 'no-writer-key', 'and so does writerKeyId()');
                    const id = await w.insert('anon_posts', { title: 't' });
                    await projection.sync();
                    const [row] = await (await (await anon.getTable('posts')).getView()).query({});
                    assertEquals(row?.values.title, 't', "the anonymous group's insert reaches rdb");
                    assertEquals(row?.author, undefined, 'unsigned');
                    assertTrue(await projection.rowHash('anon_posts', id) !== undefined, 'and its row has a row hash');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ35] rowHash, localId and tableNames through the projection, also opened without a writer key and for a group that joins later; an unknown member gives {}',
            invoke: async () => {
                const ctx = newCtx();
                const { db, rdb, writer } = await mixedDb(ctx);
                for (const opts of [{ writer }, {}]) {
                    const memory = new MemoryTarget({ captureChanges: true });
                    const projection = await RdbProjection.open(rdb, ctx, memory, { ...opts, debounceMs: 5 });
                    try {
                        const names = async (member: string) => JSON.stringify(Object.entries(await projection.tableNames(member)).sort());
                        assertEquals(await names('signed'), JSON.stringify([['identities', 'signed_identities'], ['notes', 'signed_notes']]),
                            `the signed group's names${'writer' in opts ? '' : ', without a writer key'}`);
                        assertEquals(await names('anon'), JSON.stringify([['posts', 'anon_posts']]), "and the anonymous group's");
                        assertEquals(await names('nobody'), '[]', 'an unknown member gives an empty map');
                        const id = await projection.openWriter().insert('anon_posts', { title: 'p' });
                        assertEquals(await projection.rowHash('anon_posts', id), undefined, 'a new row has no row hash before ingestion');
                        await projection.sync();
                        const rowHash = await projection.rowHash('anon_posts', id);
                        assertTrue(rowHash !== undefined, 'after it, one');
                        assertEquals(await projection.localId('anon_posts', rowHash!), id, 'which localId maps back');
                    } finally {
                        await projection.stop();
                    }
                }
                const memory = new MemoryTarget({ captureChanges: true });
                const projection = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                try {
                    await addGroups(ctx, db, '1.1.0', [{ name: 'late', tables: [open('notes', { body: { type: 'string' } })] }]);
                    await poll(() => memory.hasTable('late_notes'), 3000);
                    assertEquals(JSON.stringify(await projection.tableNames('late')), JSON.stringify({ notes: 'late_notes' }), 'a group that joins later');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ36] under manual, a group with an identity provider and authorColumn: false that joins later is refused on the reconfigure, through onError, and leaves the target as it was',
            invoke: async () => {
                const ctx = newCtx();
                const { db, rdb, writer } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const errors: unknown[] = [];
                const projection = await RdbProjection.open(rdb, ctx, memory, {
                    writer, author: 'manual', debounceMs: 5, onError: (e) => { errors.push(e); },
                    configOverride: ({ groupName }) => (groupName === 'late' ? { authorColumn: false } : {}),
                });
                try {
                    const tables = memory.listTables().sort().join(',');
                    await addGroups(ctx, db, '1.1.0', [
                        { name: 'late', tables: [identities(), open('notes', { body: { type: 'string' } })], idProvider: IDENTITIES_TABLE },
                    ]);
                    await poll(() => errors.length > 0, 3000);
                    assertTrue(errors[0] instanceof Error && errors[0].message.includes("group 'late'"), `the reconfigure is refused, naming the group: ${String(errors[0])}`);
                    assertEquals(memory.listTables().sort().join(','), tables, 'and the target keeps the tables it had');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ37] a writer opened before the projection reopens with another author setting follows the new one at its next write',
            invoke: async () => {
                const ctx = newCtx();
                const { rdb, writer } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const first = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                const w = memory.openWriter();   // as a client opens one, on the target
                const signedId = await w.insert('signed_notes', { body: 'auto' });
                assertTrue(await authorOf(memory, 'signed_notes', signedId) !== null, 'under auto an insert that names no author is signed');
                await first.stop();
                const second = await RdbProjection.open(rdb, ctx, memory, { writer, author: 'manual', debounceMs: 5 });
                try {
                    const anonymousId = await w.insert('signed_notes', { body: 'manual' });
                    assertEquals(await authorOf(memory, 'signed_notes', anonymousId), null, 'after the reopen under manual, the same writer leaves it anonymous');
                    assertEquals(await refusal(w.insert('signed_notes', { body: 'null', author_key_id: null })), 'no refusal',
                        'and takes a null author, which auto refused');
                } finally {
                    await second.stop();
                }
            },
        },
        {
            name: "[RDBPROJ38] registerKey refuses a malformed key, and a hash that isn't the key's, and stores neither",
            invoke: async () => {
                const ctx = newCtx();
                const { rdb, writer } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const projection = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                const throws = async (call: Promise<unknown>): Promise<boolean> => call.then(() => false, () => true);
                try {
                    const neutral: PublicKey = { suite: SIGNING_ED25519, key: Uint8Array.from({ length: 32 }, (_, i) => (i === 0 ? 1 : 0)) };
                    const neutralHash = keyIdFromPublicKey(neutral, ctx.getHashSuite());
                    assertTrue(await throws(projection.registerKey(neutralHash, serializePublicKeyToBase64(neutral))),
                        "a small-order ed25519 key, which isn't a well-formed key, is refused");
                    assertEquals(await projection.idForKeyHash(neutralHash), undefined, 'and not stored');

                    const other = await makeIdentity();
                    const pk = serializePublicKeyToBase64(other.publicKey);
                    const wrongHash = (await makeIdentity()).keyId;
                    assertTrue(await throws(projection.registerKey(wrongHash, pk)), "a hash that isn't the key's is refused");
                    assertEquals(await projection.idForKeyHash(wrongHash), undefined, 'and not stored');

                    const keyHash = keyIdFromPublicKey(other.publicKey, ctx.getHashSuite());
                    const id = await projection.registerKey(keyHash, pk);
                    assertEquals(await projection.idForKeyHash(keyHash), id, 'a well-formed key under its own hash is stored');
                } finally {
                    await projection.stop();
                }
            },
        },
    ],
};
