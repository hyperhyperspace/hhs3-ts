import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import {
    createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519, sha256, stringToUint8Array,
} from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import {
    formatValidationFailure, resolveRefVersionAtPosition, signPayload, ValidationRejectedError, Version, version,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { Predicate } from "../src/rschema/payload.js";
import { createUsersGroup, registerIdentity, grantCap, revokeCap, USERS_BINDING, USERS_IDENTITIES_PROVIDER, CAPS_TABLE } from "../src/users/users.js";
import { RBlobStoreImpl, rBlobStoreFactory } from "../src/rblob_store/rblob_store.js";
import { CHUNK_BYTES, ChunkPayload, FileHeaderPayload } from "../src/rblob_store/payload.js";
import type { FileSource } from "../src/rblob_store/interfaces.js";
import { RFileMapImpl, rFileMapFactory } from "../src/rfile_map/rfile_map.js";
import { elementIdOf } from "../src/rfile_map/payload.js";
import type { FileElement } from "../src/rfile_map/payload.js";
import type { FilesAccess } from "../src/rfiles/access.js";
import {
    END_LINK, chunkLink, fileHashOf, encodeBase64, decodeCanonicalBase64, isContentHash,
} from "../src/rfiles/hashes.js";
import { filePathReason } from "../src/rfiles/path.js";

// RBlobStore and RFileMap: write access through a bound Users group, upload
// chains and their defensive validation, lanes, resume, deltas, and the
// file map's sections and barrier removes.

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    ctx.getRegistry().register(RBlobStoreImpl.typeId, rBlobStoreFactory);
    ctx.getRegistry().register(RFileMapImpl.typeId, rFileMapFactory);
    return ctx;
}

const WRITER = 'writer';
// Any cap holder writes (the admin's root manager cap counts from genesis).
const CAN_WRITE: Predicate = { p: 'exists', table: USERS_BINDING + '.' + CAPS_TABLE, where: { grantee: '$author' } };

type Fixture = {
    ctx: RContext;
    admin: OwnIdentity;
    group: RTableGroupImpl;
    store: RBlobStoreImpl;
    map: RFileMapImpl;
    access: FilesAccess;
};

async function setup(name: string): Promise<Fixture> {
    const ctx = newCtx();
    const admin = await makeIdentity();
    const { group } = await createUsersGroup(ctx, admin, { seed: name });
    const access: FilesAccess = {
        bindings: { [USERS_BINDING]: group.getId() },
        idProvider: USERS_IDENTITIES_PROVIDER,
        canWrite: CAN_WRITE,
    };
    const store = await ctx.createObject(RBlobStoreImpl.create({ name: 'media', seed: name, access })) as RBlobStoreImpl;
    const map = await ctx.createObject(RFileMapImpl.create({ name: 'media', seed: name, access, blobStore: store.getId() })) as RFileMapImpl;
    return { ctx, admin, group, store, map, access };
}

async function addWriter(f: Fixture): Promise<OwnIdentity> {
    const writer = await makeIdentity();
    await registerIdentity(f.group, writer);
    await grantCap(f.group, f.admin, writer.keyId, WRITER);
    return writer;
}

async function frontierOf(object: { getScopedDag(): Promise<{ getFrontier(): Promise<Version> }> }): Promise<Version> {
    return (await object.getScopedDag()).getFrontier();
}

function patterned(size: number, seed: number): Uint8Array {
    const bytes = new Uint8Array(size);
    let x = seed * 2654435761 >>> 0;
    for (let i = 0; i < size; i++) {
        x = (x * 1103515245 + 12345) >>> 0;
        bytes[i] = x >>> 24;
    }
    return bytes;
}

function source(bytes: Uint8Array, piece: number = 50_000): FileSource {
    return {
        size: bytes.length,
        async *read() {
            for (let i = 0; i < bytes.length; i += piece) yield bytes.subarray(i, Math.min(i + piece, bytes.length));
        },
    };
}

// Fails on the second pass (the upload) after `chunks` chunks.
function interruptedSource(bytes: Uint8Array, chunks: number): FileSource {
    let reads = 0;
    return {
        size: bytes.length,
        async *read() {
            const upload = ++reads === 2;
            for (let i = 0, n = 0; i < bytes.length; i += CHUNK_BYTES, n++) {
                if (upload && n === chunks) throw new Error('interrupted');
                yield bytes.subarray(i, Math.min(i + CHUNK_BYTES, bytes.length));
            }
        },
    };
}

async function readAll(stream: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    let length = 0;
    for await (const part of stream) {
        parts.push(part);
        length += part.length;
    }
    const out = new Uint8Array(length);
    let offset = 0;
    for (const p of parts) {
        out.set(p, offset);
        offset += p.length;
    }
    return out;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
}

function contentHash(label: string): B64Hash {
    return encodeBase64(sha256.hash(stringToUint8Array(label)));
}

function assertList(actual: string[], expected: string[], why: string): void {
    assertEquals(actual.join(','), expected.join(','), why);
}

function sameSet(a: Version, b: Version): boolean {
    return a.size === b.size && [...a].every(h => b.has(h));
}

async function failureOf(fn: () => Promise<unknown>): Promise<string | undefined> {
    try {
        await fn();
        return undefined;
    } catch (e) {
        return e instanceof ValidationRejectedError ? formatValidationFailure(e.why) : (e as Error).message;
    }
}

async function expectRejected(fn: () => Promise<unknown>, messageIncludes: string, why: string): Promise<void> {
    const failure = await failureOf(fn);
    assertTrue(failure !== undefined, why);
    assertTrue(failure!.includes(messageIncludes), `${why}: expected '${messageIncludes}', got: ${failure}`);
}

async function expectInvalid(
    object: { validatePayload(p: json.Literal, at: Version): Promise<{ valid: boolean; why?: { reason: string } }> },
    payload: json.Literal, at: Version, messageIncludes: string, why: string,
): Promise<void> {
    const result = await object.validatePayload(payload, at) as { valid: true } | { valid: false; why: Parameters<typeof formatValidationFailure>[0] };
    assertFalse(result.valid, why);
    const message = result.valid ? '' : formatValidationFailure(result.why);
    assertTrue(message.includes(messageIncludes), `${why}: expected '${messageIncludes}', got: ${message}`);
}

async function chainOf(store: RBlobStoreImpl, tail: B64Hash): Promise<{ hash: B64Hash; payload: json.LiteralMap; at: Version }[]> {
    const dag = await store.getScopedDag();
    const chain: { hash: B64Hash; payload: json.LiteralMap; at: Version }[] = [];
    let hash = tail;
    for (;;) {
        const entry = (await dag.loadEntry(hash))!;
        const payload = entry.payload as json.LiteralMap;
        if (payload['action'] !== 'chunk') break;
        const at = new Set(json.fromSet(entry.header.prevEntryHashes)) as Version;
        chain.unshift({ hash, payload, at });
        hash = [...at][0];
    }
    return chain;
}

// Signs `op` for `author` at `at`, validates and applies it.
async function writeAt(
    object: RBlobStoreImpl | RFileMapImpl, op: json.LiteralMap, author: OwnIdentity, at: Version,
): Promise<B64Hash> {
    const signed = await signPayload(op, author, at);
    const result = await object.validatePayload(signed, at);
    if (!result.valid) throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
    return object.applyPayload(signed, at);
}

function elementOp(action: 'add' | 'remove', element: FileElement): json.LiteralMap {
    return { action, ...element } as json.LiteralMap;
}

export const rfilesTests = {
    title: '[FILES] RBlobStore and RFileMap',
    tests: [
        {
            name: '[FILES01] canonical base64, content hashes and portable paths',
            invoke: async () => {
                assertTrue(decodeCanonicalBase64('AA==') !== undefined, 'a canonical padded group decodes');
                assertEquals(decodeCanonicalBase64('AB=='), undefined, 'nonzero unused bits are not canonical');
                assertEquals(decodeCanonicalBase64('AAA'), undefined, 'a length that is not a multiple of 4 is rejected');
                assertEquals(decodeCanonicalBase64('A A='), undefined, 'whitespace is rejected');
                assertEquals(decodeCanonicalBase64('AAAA', 2), undefined, 'the expected length is enforced');
                assertTrue(isContentHash(END_LINK), 'END is a content hash');
                assertFalse(isContentHash(encodeBase64(new Uint8Array(31))), 'a 31-byte hash is not a content hash');

                const empty = fileHashOf(0, END_LINK);
                assertEquals(empty, fileHashOf(0, END_LINK), 'fileHash is deterministic');
                const one = chunkLink(new Uint8Array([1]), END_LINK);
                assertTrue(fileHashOf(1, one) !== fileHashOf(2, one), 'fileHash commits to the size');

                assertEquals(filePathReason('photos/2026/a.jpg'), undefined, 'a plain path is valid');
                assertEquals(filePathReason('Ωmega/ñandú.txt'), undefined, 'NFC Unicode is valid');
                for (const [path, reason] of [
                    ['', 'is empty'],
                    ['/abs', 'empty segment'],
                    ['a//b', 'empty segment'],
                    ['a/../b', "'..' segment"],
                    ['./a', "'.' segment"],
                    ['a:b', "contains ':'"],
                    ['a\\b', "contains '\\'"],
                    ['a\u0001b', 'control character'],
                    ['dir./x', "ending in '.'"],
                    ['x ', "ending in ' '"],
                    ['con.txt', 'reserved name'],
                    ['LPT1', 'reserved name'],
                    ['n\u0303', 'NFC'],
                    ['\ud800', 'well-formed'],
                    ['x'.repeat(256), 'longer than 255'],
                    [Array(33).fill('a').join('/'), 'more than 32 segments'],
                ] as const) {
                    const got = filePathReason(path);
                    assertTrue(got !== undefined && got.includes(reason), `path '${path}': expected '${reason}', got: ${got}`);
                }
                assertEquals(filePathReason('console.txt'), undefined, 'a reserved stem only matches the whole stem');
            }
        },
        {
            name: '[FILES01b] golden vectors for the content hashes and element ids',
            invoke: async () => {
                assertEquals(END_LINK, '73+bPmipURBBFg3Y9n2kI2xGXtmb7rdq7iGPjo0bL0k=', 'END');
                assertEquals(fileHashOf(0, END_LINK), 'CC2ccIA+D6g+x7X5kayWBcKTF3aRrQLFPK0Zb0kAaOA=', 'the empty file');
                const link = chunkLink(new Uint8Array([1, 2, 3]), END_LINK);
                assertEquals(link, 'KWGlVt8/303u+qYZFIFa/LuvZfGfqBxNL9ynJHjnc2I=', 'the link of a one-chunk file');
                assertEquals(fileHashOf(3, link), 'dPu+iSrmgbQJjwIBikTuFWdlAcdRtt5PXk/xKffksjg=', 'a one-chunk file');
                assertEquals(elementIdOf({ section: 'common', path: 'a/b.txt', fileHash: END_LINK }),
                    'P0gye+bqBHal6+YvQs/lQJ4A0M/TM9ylBoUfhVHKQX0=', 'a common element id');
                assertEquals(elementIdOf({ section: 'key', owner: END_LINK, path: 'a/b.txt', fileHash: END_LINK }),
                    'TxetxsTmbrIAKvpwx07oBi03sRkeZW3ZxBNeVS0LBfI=', 'a key element id');
            }
        },
        {
            name: '[FILES02] putFile and readFile round-trip across chunk boundaries; findFile and dedup',
            invoke: async () => {
                const f = await setup('files02');
                const writer = await addWriter(f);

                const hashes = new Map<number, B64Hash>();
                for (const size of [0, 1, CHUNK_BYTES - 1, CHUNK_BYTES, CHUNK_BYTES + 1, 3 * CHUNK_BYTES - 5]) {
                    const bytes = patterned(size, size + 1);
                    const stored = await f.store.putFile(source(bytes), writer, { lane: 0 });
                    assertEquals(stored.size, size, `size ${size} is recorded`);
                    hashes.set(size, stored.fileHash);
                    assertTrue(sameBytes(await readAll(f.store.readFile(stored.tail)), bytes), `size ${size} reads back`);
                    const found = await f.store.findFile(stored.fileHash);
                    assertEquals(found?.tail, stored.tail, `size ${size} is found by fileHash`);
                    if (size === 0) assertEquals(stored.tail, stored.header, 'an empty file is complete at its header');
                }
                assertEquals(new Set(hashes.values()).size, hashes.size, 'different contents have different hashes');

                const bytes = patterned(CHUNK_BYTES + 7, 99);
                const a = await f.store.putFile(source(bytes, 1000), writer, { lane: 1 });
                const b = await f.store.putFile(source(bytes, 77_777), writer, { lane: 2 });
                assertEquals(a.fileHash, b.fileHash, 'the file hash does not depend on how the source is split');
                assertTrue(a.header !== b.header, 'without dedup, a second upload is a second chain');

                const c = await f.store.putFile(source(bytes), writer, { lane: 3, dedup: true });
                assertTrue(c.header === a.header || c.header === b.header, 'with dedup, an existing chain is reused');

                await expectRejected(() => f.store.putFile({ size: 10, read: source(new Uint8Array(9)).read }, writer, { lane: 0 }),
                    'read 9 bytes, expected 10', 'a short source is refused');
                assertEquals(await f.store.findFile(contentHash('nothing')), undefined, 'an unknown file is not found');
            }
        },
        {
            name: '[FILES03] writes go through the bound provider and canWrite',
            invoke: async () => {
                const f = await setup('files03');
                const writer = await addWriter(f);
                const registered = await makeIdentity();
                await registerIdentity(f.group, registered);
                const stranger = await makeIdentity();
                const file = source(patterned(10, 3));

                await expectRejected(() => f.store.putFile(file, stranger, { lane: 0 }), 'has no key',
                    'a key the provider does not know is rejected');
                await expectRejected(() => f.store.putFile(file, registered, { lane: 0 }), 'is not allowed to write',
                    'a known key without a cap is rejected');
                await f.store.putFile(file, writer, { lane: 0 });
                await f.store.putFile(file, f.admin, { lane: 1 });

                assertEquals(await f.map.canWrite(stranger.keyId), false, 'canWrite is false for an unknown key');
                assertEquals(await f.map.canWrite(registered.keyId), false, 'canWrite is false without a cap');
                assertEquals(await f.map.canWrite(f.admin.keyId), true, 'canWrite holds for the admin');
                assertTrue((await f.map.authorKey(f.admin.keyId)) !== undefined, 'the admin key resolves');
                assertEquals(await f.map.authorKey(stranger.keyId), undefined, 'an unknown key does not resolve');
            }
        },
        {
            name: '[FILES04] malformed store ops are rejected before any positional check',
            invoke: async () => {
                const f = await setup('files04');
                const writer = await addWriter(f);
                const ra = await f.store.refAdvance(writer, 0);
                const at = version(ra);
                const groupId = f.group.getId();
                const groupAt = json.toSet([...await frontierOf(f.group)]);

                const signedRa = await signPayload({ action: 'ref-advance', refId: groupId, refVersion: groupAt, lane: 0 }, writer, at);
                const header = await signPayload({ action: 'file', lane: 0, fileHash: fileHashOf(0, END_LINK), size: 0, first: END_LINK }, writer, at);

                const cases: [json.Literal, string, string][] = [
                    ['x', 'must be an object', 'a non-object'],
                    [{ action: 'delete' }, "unknown RBlobStore action 'delete'", 'an unknown action'],
                    [{ ...signedRa, extra: 1 }, 'ref-advance format is invalid', 'an extra field'],
                    [{ ...signedRa, lane: 6 }, 'ref-advance format is invalid', 'a lane out of range'],
                    [{ ...signedRa, lane: 1.5 }, 'ref-advance format is invalid', 'a fractional lane'],
                    [{ ...signedRa, refId: contentHash('other') }, 'is not the bound group', 'another ref'],
                    [{ ...signedRa, refVersion: {} }, 'refVersion is empty', 'an empty refVersion'],
                    [{ ...signedRa, refVersion: { 'not-a-hash': '' } }, 'malformed entry hash', 'a malformed refVersion hash'],
                    [{ ...signedRa, author: 'abc' }, 'author is not a key id', 'a malformed author'],
                    [{ ...signedRa, signature: 'AB==' }, 'signature is not canonical base64', 'a non-canonical signature'],
                    [{ ...header, size: 0, first: contentHash('x') }, "empty file's first link must be END", 'an empty file with a first link'],
                    [{ ...header, fileHash: contentHash('x') }, 'fileHash does not match', 'a wrong fileHash'],
                    [{ ...header, size: -1 }, 'file header format is invalid', 'a negative size'],
                    [{ ...header, size: 2 ** 37 + 1 }, 'file header format is invalid', 'a size above the limit'],
                    [{ ...header, first: 'short' }, 'hashes are malformed', 'a malformed first link'],
                    [{ action: 'chunk', header: ra, index: 0, bytes: '', next: END_LINK, author: writer.keyId, signature: 'AA==' },
                        'chunk length is out of range', 'an empty chunk'],
                    [{ action: 'chunk', header: ra, index: 0, bytes: 'AB==', next: END_LINK, author: writer.keyId, signature: 'AA==' },
                        'bytes', 'non-canonical chunk bytes'],
                    [{ action: 'chunk', header: ra, index: -1, bytes: 'AA==', next: END_LINK, author: writer.keyId, signature: 'AA==' },
                        'chunk format is invalid', 'a negative index'],
                    [{ action: 'chunk', header: ra, index: 0, bytes: 'A'.repeat(174768), next: END_LINK, author: writer.keyId, signature: 'AA==' },
                        'chunk format is invalid', 'an oversized chunk'],
                ];
                for (const [payload, message, why] of cases) {
                    await expectInvalid(f.store, payload, at, message, why);
                }
                await expectInvalid(f.store, signedRa, version(), 'must follow the create entry', 'an op at the empty position');
            }
        },
        {
            name: '[FILES05] chunk ops are checked against their chain, header and author',
            invoke: async () => {
                const f = await setup('files05');
                const writer = await addWriter(f);
                const other = await addWriter(f);

                const c0 = patterned(CHUNK_BYTES, 1);
                const c1 = patterned(10, 2);
                const link1 = chunkLink(c1, END_LINK);
                const first = chunkLink(c0, link1);
                const size = CHUNK_BYTES + 10;
                const ra = await f.store.refAdvance(writer, 0);
                const header = await writeAt(f.store,
                    { action: 'file', lane: 0, fileHash: fileHashOf(size, first), size, first }, writer, version(ra));

                const chunk = (index: number, bytes: Uint8Array, next: B64Hash): json.LiteralMap =>
                    ({ action: 'chunk', header, index, bytes: encodeBase64(bytes), next });
                const invalid = async (op: json.LiteralMap, author: OwnIdentity, at: Version, message: string, why: string) =>
                    expectInvalid(f.store, await signPayload(op, author, at), at, message, why);

                await invalid(chunk(0, c0, link1), writer, version(ra), 'chunk 0 must follow its header', 'chunk 0 not on its header');
                await invalid(chunk(0, c0, link1), writer, version(header, ra), 'exactly one predecessor', 'a chunk with two predecessors');
                await invalid(chunk(0, patterned(CHUNK_BYTES, 9), link1), writer, version(header), 'does not match its link', 'tampered bytes');
                await invalid(chunk(0, c0.subarray(1), link1), writer, version(header), 'wrong length', 'a short chunk that is not the last');
                await invalid(chunk(0, c0, contentHash('x')), writer, version(header), 'does not match its link', 'a wrong next link');
                await invalid(chunk(0, c0, link1), other, version(header), 'same author as its header', 'another author');
                await invalid(chunk(1, c1, END_LINK), writer, version(header), 'must follow chunk 0', 'chunk 1 on the header');

                const signed0 = await signPayload(chunk(0, c0, link1), writer, version(header));
                await expectInvalid(f.store, { ...signed0, signature: (await signPayload(chunk(0, c0, link1), writer, version(ra))).signature },
                    version(header), 'bad signature', 'a signature for another position');
                const h0 = await writeAt(f.store, chunk(0, c0, link1), writer, version(header));

                await invalid(chunk(2, c1, END_LINK), writer, version(h0), 'must follow chunk 1', 'a skipped index');
                const h1 = await writeAt(f.store, chunk(1, c1, END_LINK), writer, version(h0));
                assertTrue(sameBytes(await readAll(f.store.readFile(h1)), new Uint8Array([...c0, ...c1])), 'the hand-made chain reads back');
                await invalid(chunk(2, c1, END_LINK), writer, version(h1), "past the file's 2 chunks", 'a chunk past the end');

                // a header whose last link does not end in END: its last chunk can't validate
                const bad = patterned(5, 3);
                const badFirst = chunkLink(bad, contentHash('dangling'));
                const ra2 = await f.store.refAdvance(writer, 1);
                const badHeader = await writeAt(f.store,
                    { action: 'file', lane: 1, fileHash: fileHashOf(5, badFirst), size: 5, first: badFirst }, writer, version(ra2));
                await invalid({ action: 'chunk', header: badHeader, index: 0, bytes: encodeBase64(bad), next: contentHash('dangling') },
                    writer, version(badHeader), "last chunk's next link must be END", 'a last chunk that does not end the chain');
            }
        },
        {
            name: '[FILES06] ref-advances: a repeated version is valid, going back is not, a missing target defers',
            invoke: async () => {
                const f = await setup('files06');
                const writer = await addWriter(f);
                const v1 = await frontierOf(f.group);
                await registerIdentity(f.group, await makeIdentity());
                const v2 = await frontierOf(f.group);

                await f.store.refAdvance(writer, 0, v2);
                await f.store.refAdvance(writer, 0, v2);
                await f.map.refAdvance(writer, v2);
                await f.map.refAdvance(writer, v2);
                await expectRejected(() => f.store.refAdvance(writer, 0, v1), 'not monotonic', 'a store ref-advance may not go back');
                await expectRejected(() => f.map.refAdvance(writer, v1), 'not monotonic', 'a map ref-advance may not go back');
                await f.store.refAdvance(writer, 1, v1);   // lane 1 still observes genesis

                const missing = contentHash('missing');
                const failure = await failureOf(() => f.store.refAdvance(writer, 2, version(missing)));
                assertTrue(failure !== undefined && failure.includes('is not present in the replica'),
                    `a target outside the replica throws (defer), got: ${failure}`);
            }
        },
        {
            name: '[FILES07] a revoked writer cannot start an upload, but can finish one it started',
            invoke: async () => {
                const f = await setup('files07');
                const writer = await addWriter(f);
                const bytes = patterned(3 * CHUNK_BYTES, 7);

                await expectRejected(() => f.store.putFile(interruptedSource(bytes, 1), writer, { lane: 4 }), 'interrupted',
                    'the upload stops after one chunk');
                const [tail] = [...await f.store.laneCover(4)];
                const header = ((await (await f.store.getScopedDag()).loadEntry(tail))!.payload as unknown as ChunkPayload).header;
                assertEquals(await f.store.findFile(fileHashOf(bytes.length, (await chainHeader(f.store, header)).first)), undefined,
                    'an unfinished chain is not a stored file');

                await revokeCap(f.group, f.admin, writer.keyId, WRITER);
                await expectRejected(() => f.store.putFile(source(patterned(10, 8)), writer, { lane: 0 }), 'is not allowed to write',
                    'a new upload observes the revocation');

                const resumed = await f.store.putFile(source(bytes), writer, { lane: 4, resume: { header, tail } });
                assertEquals(resumed.header, header, 'the resumed chain keeps its header');
                assertTrue(sameBytes(await readAll(f.store.readFile(resumed.tail)), bytes), 'the resumed file reads back');

                await expectRejected(() => f.store.putFile(source(patterned(3 * CHUNK_BYTES, 1)), writer, { lane: 4, resume: { header, tail } }),
                    'not this file', 'resuming with other contents is refused');
            }
        },
        {
            name: '[FILES08] the chunk shortcut agrees with the generic observation, and a fresh validator accepts every chunk',
            invoke: async () => {
                const f = await setup('files08');
                const writer = await addWriter(f);
                const stored = await f.store.putFile(source(patterned(3 * CHUNK_BYTES + 1, 5)), writer, { lane: 2 });
                await registerIdentity(f.group, await makeIdentity());
                const later = await f.store.putFile(source(patterned(2 * CHUNK_BYTES, 6)), writer, { lane: 2 });

                const dag = await f.store.getScopedDag();
                const groupId = f.group.getId();
                for (const s of [stored, later]) {
                    const observed = await resolveRefVersionAtPosition(dag, groupId, version(s.header), version(s.header));
                    const chain = await chainOf(f.store, s.tail);
                    assertEquals(chain.length, s === stored ? 4 : 2, 'the whole chain is walked');
                    for (const c of chain) {
                        const resolved = await resolveRefVersionAtPosition(dag, groupId, version(c.hash), version(c.hash));
                        assertTrue(sameSet(resolved, observed), `chunk ${c.payload['index']} observes what its header observes`);
                    }
                }
                const first = await resolveRefVersionAtPosition(dag, groupId, version(stored.header), version(stored.header));
                const second = await resolveRefVersionAtPosition(dag, groupId, version(later.header), version(later.header));
                assertFalse(sameSet(first, second), 'the second upload observes the newer group version');

                const fresh = await rBlobStoreFactory.loadObject(f.store.getId(), f.ctx) as RBlobStoreImpl;
                for (const s of [stored, later]) {
                    for (const c of await chainOf(f.store, s.tail)) {
                        const result = await fresh.validatePayload(c.payload, c.at);
                        assertTrue(result.valid, `a fresh validator accepts chunk ${c.payload['index']}`);
                    }
                }
            }
        },
        {
            name: '[FILES09] lanes and store deltas',
            invoke: async () => {
                const f = await setup('files09');
                const writer = await addWriter(f);
                const created = version(f.store.getId());
                const a = await f.store.putFile(source(patterned(2 * CHUNK_BYTES + 3, 1)), writer, { lane: 0 });
                const mid = await frontierOf(f.store);
                const b = await f.store.putFile(source(patterned(1, 2)), writer, { lane: 1 });

                assertEquals(await f.store.laneOf(a.header), 0, 'a header carries its lane');
                assertEquals(await f.store.laneOf(b.tail), 1, 'a chunk carries its header lane');
                assertTrue(sameSet(await f.store.laneCover(0), version(a.tail)), 'lane 0 ends at its chain');
                assertTrue(sameSet(await f.store.laneCover(1), version(b.tail)), 'lane 1 ends at its chain');
                assertTrue(sameSet(await f.store.laneCover(5), created), 'an unused lane starts at the create entry');
                assertTrue(sameSet(await frontierOf(f.store), version(a.tail, b.tail)), 'lanes are concurrent');

                const all = await f.store.computeDelta(created, await frontierOf(f.store));
                assertEquals(all.changes.headers.length, 2, 'both headers are new');
                assertEquals(all.changes.chunks.length, 4, 'every chunk is new');
                assertEquals(all.changes.refAdvances.length, 2, 'both ref-advances are new');
                assertList(all.changes.chunks.filter(c => c.complete).map(c => c.hash).sort(), [a.tail, b.tail].sort(),
                    'the last chunks complete their files');
                assertEquals(all.changes.chunks.find(c => c.hash === b.tail)!.length, 1, 'chunk lengths are reported');

                const tail = await f.store.computeDelta(mid, await frontierOf(f.store));
                assertList(tail.changes.headers.map(h => h.header), [b.header], 'only the second upload is new after mid');
                assertList(tail.changes.chunks.map(c => c.hash), [b.tail], 'with its one chunk');

                const none = await f.store.computeDelta(await frontierOf(f.store), await frontierOf(f.store));
                assertEquals(none.changes.chunks.length + none.changes.headers.length + none.changes.refAdvances.length, 0,
                    'an empty range has no changes');
            }
        },
        {
            name: '[FILES10] file map add, list and remove; sections; several hashes at one path',
            invoke: async () => {
                const f = await setup('files10');
                const writer = await addWriter(f);
                const x = contentHash('x');
                const y = contentHash('y');

                await f.map.add({ section: 'common', path: 'docs/a.txt', fileHash: x }, writer);
                await f.map.add({ section: 'common', path: 'docs/a.txt', fileHash: y }, f.admin);
                await f.map.add({ section: 'key', owner: writer.keyId, path: 'notes.md', fileHash: x }, writer);
                const listed = await f.map.list();
                assertEquals(listed.length, 3, 'three elements are listed');
                assertList(listed.filter(e => e.path === 'docs/a.txt').map(e => e.fileHash).sort(), [x, y].sort(),
                    'two hashes coexist at one path');
                assertEquals(listed.find(e => e.section === 'key')!.owner, writer.keyId, 'the key element keeps its owner');

                await f.map.remove({ section: 'common', path: 'docs/a.txt', fileHash: x }, f.admin);
                await f.map.remove({ section: 'common', path: 'missing.txt', fileHash: x }, f.admin);
                const after = await f.map.list();
                assertList(after.map(e => `${e.section}:${e.path}:${e.fileHash === x ? 'x' : 'y'}`).sort(),
                    ['common:docs/a.txt:y', 'key:notes.md:x'], 'the removed element is gone; removing an absent one is a no-op');

                const view = await f.map.getView();
                assertTrue(await view.has({ section: 'common', path: 'docs/a.txt', fileHash: y }), 'the view has the remaining element');
                assertFalse(await view.has({ section: 'common', path: 'docs/a.txt', fileHash: x }), 'and not the removed one');
            }
        },
        {
            name: '[FILES11] file map ops: owner rule, paths, hashes, formats and access',
            invoke: async () => {
                const f = await setup('files11');
                const writer = await addWriter(f);
                const registered = await makeIdentity();
                await registerIdentity(f.group, registered);
                const x = contentHash('x');

                await expectRejected(() => f.map.add({ section: 'key', owner: f.admin.keyId, path: 'a', fileHash: x }, writer),
                    'must be by its owner', "a write in another key's folder");
                await expectRejected(() => f.map.remove({ section: 'key', owner: f.admin.keyId, path: 'a', fileHash: x }, writer),
                    'must be by its owner', "a remove in another key's folder");
                await expectRejected(() => f.map.add({ section: 'key', path: 'a', fileHash: x }, writer),
                    'needs an owner', 'a key write without an owner');
                await expectRejected(() => f.map.add({ section: 'common', owner: writer.keyId, path: 'a', fileHash: x }, writer),
                    'has no owner', 'a common write with an owner');
                await expectRejected(() => f.map.add({ section: 'common', path: 'a/../b', fileHash: x }, writer),
                    "path has a '..' segment", 'a path that escapes');
                await expectRejected(() => f.map.add({ section: 'common', path: 'a', fileHash: 'nope' }, writer),
                    'fileHash is malformed', 'a malformed file hash');
                await expectRejected(() => f.map.add({ section: 'common', path: 'a', fileHash: x }, registered),
                    'is not allowed to write', 'a key without a cap');

                const at = await frontierOf(f.map);
                const add = await signPayload({ action: 'add', section: 'common', path: 'a', fileHash: x }, writer, at);
                await expectInvalid(f.map, { ...add, extra: true }, at, 'add format is invalid', 'an extra field');
                await expectInvalid(f.map, { ...add, section: 'shared' }, at, 'add format is invalid', 'an unknown section');
                await expectInvalid(f.map, { ...add, action: 'put' }, at, "unknown RFileMap action 'put'", 'an unknown action');
                await expectInvalid(f.map, { ...add, path: 'b' }, at, 'bad signature', 'a payload changed after signing');
                const ra = await signPayload({ action: 'ref-advance', refId: contentHash('g'), refVersion: json.toSet([x]) }, writer, at);
                await expectInvalid(f.map, ra, at, 'is not the bound group', 'a ref-advance of another object');
                await expectInvalid(f.map, { ...ra, refId: f.group.getId(), lane: 0 }, at, 'ref-advance format is invalid',
                    'a lane on a map ref-advance');
            }
        },
        {
            name: '[FILES12] file map removes are barriers',
            invoke: async () => {
                const f = await setup('files12');
                const g = version(f.map.getId());
                const a = f.admin;
                const has = async (e: FileElement, at: Version, from: Version = at) => (await f.map.getView(at, from)).has(e);
                const e: FileElement = { section: 'common', path: 'e', fileHash: contentHash('e') };

                const a1 = await writeAt(f.map, elementOp('add', e), a, g);
                const r = await writeAt(f.map, elementOp('remove', e), a, version(a1));
                const a2 = await writeAt(f.map, elementOp('add', e), a, version(a1));
                assertTrue(await has(e, version(a2)), 'present on the add branch alone');
                assertFalse(await has(e, version(a2), version(r, a2)), 'a concurrent remove known to `from` removes it');
                assertFalse(await has(e, version(r, a2)), 'a concurrent remove wins at the merge');
                const a3 = await writeAt(f.map, elementOp('add', e), a, version(r, a2));
                assertTrue(await has(e, version(a3)), 'an add after the merge is present');

                // two adds in the cover, one after the remove, one concurrent to it
                const e2: FileElement = { section: 'common', path: 'e2', fileHash: contentHash('e2') };
                const b1 = await writeAt(f.map, elementOp('add', e2), a, g);
                const rb = await writeAt(f.map, elementOp('remove', e2), a, version(b1));
                const b2 = await writeAt(f.map, elementOp('add', e2), a, version(rb));
                const b3 = await writeAt(f.map, elementOp('add', e2), a, version(b1));
                assertTrue(await has(e2, version(b2, b3)), 'the add after the remove survives');
                assertTrue(await has(e2, version(b2, b3), version(b2, b3, a3)), 'the same through the general check');

                // two adds in the cover, each concurrent to a remove below the other
                const e3: FileElement = { section: 'common', path: 'e3', fileHash: contentHash('e3') };
                const unrelated = await writeAt(f.map, elementOp('add', { ...e3, path: 'u' }), a, g);
                const r1 = await writeAt(f.map, elementOp('remove', e3), a, g);
                const r2 = await writeAt(f.map, elementOp('remove', e3), a, version(unrelated));
                const x = await writeAt(f.map, elementOp('add', e3), a, version(r2));
                const y = await writeAt(f.map, elementOp('add', e3), a, version(r1));
                assertTrue(await has(e3, version(x)), 'x alone is present');
                assertFalse(await has(e3, version(x, y)), 'x and y are each removed by the remove below the other');
                assertFalse(await has(e3, version(x, y), version(x, y, a3)), 'the same through the general check');

                const listed = await f.map.list(version(a3, b2, b3, x, y));
                assertList(listed.map(l => l.path).sort(), ['e', 'e2', 'u'], 'list applies the same rule');
            }
        },
        {
            name: '[FILES13] file map deltas',
            invoke: async () => {
                const f = await setup('files13');
                const g = version(f.map.getId());
                const a = f.admin;
                const e: FileElement = { section: 'common', path: 'e', fileHash: contentHash('e') };
                const k: FileElement = { section: 'key', owner: a.keyId, path: 'k', fileHash: contentHash('k') };

                const a1 = await writeAt(f.map, elementOp('add', e), a, g);
                const r = await writeAt(f.map, elementOp('remove', e), a, version(a1));
                const a2 = await writeAt(f.map, elementOp('add', e), a, version(a1));
                const k1 = await writeAt(f.map, elementOp('add', k), a, version(a1));

                const d1 = await f.map.computeDelta(g, version(a1));
                assertList(d1.changes.added.map(l => l.path), ['e'], 'the first add is added');
                assertEquals(d1.changes.removed.length, 0, 'nothing is removed');

                const d2 = await f.map.computeDelta(version(a1), version(r, a2, k1));
                assertList(d2.changes.added.map(l => l.path), ['k'], 'the key element is added');
                assertList(d2.changes.removed.map(l => l.path), ['e'], 'the barrier removes e');

                const d3 = await f.map.computeDelta(version(r), version(a2));
                assertList(d3.changes.added.map(l => l.path), ['e'], 'across branches, e is present on the add branch only');
                const d4 = await f.map.computeDelta(version(r), version(a1));
                assertList(d4.changes.added.map(l => l.path), ['e'], 'going back before the remove re-adds e');
                assertEquals(d4.changes.removed.length, 0, 'and removes nothing');
                assertEquals(d2.revisionBound.size > 0, true, 'a delta carries a revision bound');
            }
        },
        {
            name: '[FILES14] a genesis-admitted key writes without a ref-advance; a moved group is observed first',
            invoke: async () => {
                const f = await setup('files14');
                const dag = await f.map.getScopedDag();
                const e: FileElement = { section: 'common', path: 'first', fileHash: contentHash('first') };

                const h1 = await f.map.add(e, f.admin);
                const prevs1 = [...json.fromSet((await dag.loadEntry(h1))!.header.prevEntryHashes)];
                assertList(prevs1, [f.map.getId()], 'the first add sits right on the create entry');

                await registerIdentity(f.group, await makeIdentity());
                const h2 = await f.map.add({ ...e, path: 'second' }, f.admin);
                const [prev2] = [...json.fromSet((await dag.loadEntry(h2))!.header.prevEntryHashes)];
                const ra = (await dag.loadEntry(prev2))!.payload as json.LiteralMap;
                assertEquals(ra['action'], 'ref-advance', 'after the group moves, a ref-advance comes first');
                assertTrue(sameSet(new Set(json.fromSet(ra['refVersion'] as json.Set)), await frontierOf(f.group)),
                    'to the group frontier');

                const h3 = await f.map.add({ ...e, path: 'third' }, f.admin);
                assertList([...json.fromSet((await dag.loadEntry(h3))!.header.prevEntryHashes)], [h2],
                    'no second ref-advance while the group stays put');

                const headerAt = version(f.store.getId());
                const header = await signPayload({ action: 'file', lane: 0, fileHash: fileHashOf(0, END_LINK), size: 0, first: END_LINK }, f.admin, headerAt);
                assertTrue((await f.store.validatePayload(header, headerAt)).valid, 'a store header by a genesis-admitted key needs no ref-advance');
            }
        },
    ],
};

async function chainHeader(store: RBlobStoreImpl, header: B64Hash): Promise<FileHeaderPayload> {
    return (await (await store.getScopedDag()).loadEntry(header))!.payload as unknown as FileHeaderPayload;
}
