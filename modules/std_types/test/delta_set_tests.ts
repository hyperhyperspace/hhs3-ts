import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { set } from "@hyper-hyper-space/hhs3_util";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { DeltaChanges, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap/rcap.js";
import { RSet, rSetFactory } from "../src/types/rset/rset.js";
import type { RSetChanges, RSetDelta } from "../src/types/rset/rset.js";
import { serializePublicKeyToBase64 } from "../src/authorship.js";
import { normalizeRSetDelta } from "./delta_parity/normalize.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function hashElement(element: string): string {
    return hashSuite.hashToB64(stringToUint8Array(json.toStringNormalized(element)));
}

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function computeRSetWithStrategy(
    rset: RSet,
    strategy: 'full' | 'bounded',
    start: ReturnType<typeof version>,
    end: ReturnType<typeof version>,
): Promise<RSetDelta> {
    rset.setDeltaStrategy(strategy);
    return await rset.computeDelta(start, end) as RSetDelta;
}

// The bounded delta between `start` and `end`, checked against the full one.
async function boundedSetDelta(rset: RSet, start: Version, end: Version): Promise<RSetDelta> {
    const full = await computeRSetWithStrategy(rset, 'full', start, end);
    const bounded = await computeRSetWithStrategy(rset, 'bounded', start, end);
    assertEquals(
        JSON.stringify(normalizeRSetDelta(bounded)),
        JSON.stringify(normalizeRSetDelta(full)),
        'the bounded delta matches the full one',
    );
    return bounded;
}

function isExactly(v: Version, ...hashes: B64Hash[]): boolean {
    return v.size === hashes.length && hashes.every((h) => v.has(h));
}

function permissionedContext() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);
    ctx.getRegistry().register(RSet.typeId, rSetFactory);
    return ctx;
}

async function addIdentities(cap: RCap, author: OwnIdentity, ...identities: OwnIdentity[]): Promise<void> {
    for (const identity of identities) {
        await cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), author);
    }
}

// An RCap where admin is managed by the creator and write by admin, and a set
// over it whose adds and deletes require write. Bob is an identity.
async function writeCapSet(seed: string, barrierDelete: boolean) {
    const ctx = permissionedContext();
    const [admin, bob] = [await makeIdentity(), await makeIdentity()];
    const cap = (await ctx.createObject(await RCap.create({
        seed: seed + '-cap',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: { admin: { managedBy: ['creator'] }, write: { managedBy: ['admin'] } },
    }))) as RCap;
    const rset = (await ctx.createObject(await RSet.create({
        seed: seed + '-set',
        initialElements: [],
        hashAlgorithm: 'sha256',
        supportBarrierDelete: barrierDelete,
        capabilityRef: cap.getId(),
        capRequirements: { add: 'write', delete: 'write' },
    }))) as RSet;
    await addIdentities(cap, admin, bob);
    return { ctx, cap, rset, admin, bob };
}

// Bob's grant g, then two entries on branches off it: t1 (an identity) and
// r, a revoke of Bob's write. r is concurrent with t1, so a view at t1 whose
// `from` holds r fails Bob's write.
async function concurrentRevokeOfBob(cap: RCap, admin: OwnIdentity, bob: OwnIdentity) {
    const g = await cap.grant(bob.keyId, 'write', admin);
    const dave = await makeIdentity();
    const t1 = await cap.addIdentity(dave.keyId, serializePublicKeyToBase64(dave.publicKey), admin, version(g));
    const r = await cap.revoke(bob.keyId, 'write', admin, version(g));
    return { g, t1, r };
}

export const deltaTests = {
    title: '[DELTA] RSet computeDelta tests',
    tests: [
        {
            name: '[DELTA01] Plain RSet: added elements appear in delta',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const init = await RSet.create({
                    seed: 'delta-01',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const rset = (await ctx.createObject(init)) as RSet;
                const creationVersion = version(rset.getId());

                await rset.add('alpha');
                await rset.add('beta');

                const dag = (await ctx.getDag(rset.getId()))!;
                const endVersion = await dag.getFrontier();

                const delta = await rset.computeDelta(creationVersion, endVersion) as RSetDelta;

                assertEquals(delta.added.length, 2, 'should have 2 added elements');
                assertTrue(delta.added.includes(hashElement('alpha')), 'alpha should be in added');
                assertTrue(delta.added.includes(hashElement('beta')), 'beta should be in added');
                assertEquals(delta.removed.length, 0, 'should have 0 removed elements');
                assertEquals(delta.validityChanges.length, 0, 'no validity changes for plain set');
                assertTrue(
                    set.eq(delta.getRevisionBound(), creationVersion),
                    'revision bound is the meet (create op for a linear extension from creation)',
                );
            }
        },
        {
            name: '[DELTA02] Plain RSet: add then delete, net-zero and partial deltas',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const init = await RSet.create({
                    seed: 'delta-02',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const rset = (await ctx.createObject(init)) as RSet;
                const creationVersion = version(rset.getId());

                await rset.add('X');
                const dag = (await ctx.getDag(rset.getId()))!;
                const afterAdd = await dag.getFrontier();

                await rset.delete('X');
                const afterDelete = await dag.getFrontier();

                const netZeroDelta = await rset.computeDelta(creationVersion, afterDelete) as RSetDelta;
                assertEquals(netZeroDelta.added.length, 0, 'net-zero: nothing added');
                assertEquals(netZeroDelta.removed.length, 0, 'net-zero: nothing removed');

                const partialDelta = await rset.computeDelta(afterAdd, afterDelete) as RSetDelta;
                assertEquals(partialDelta.added.length, 0, 'partial: nothing added');
                assertEquals(partialDelta.removed.length, 1, 'partial: X removed');
                assertTrue(partialDelta.removed.includes(hashElement('X')), 'X should be in removed');
            }
        },
        {
            name: '[DELTA03] Permissioned RSet: revocation causes validity change and removal',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const admin = await makeIdentity();
                const bob = await makeIdentity();

                const capInit = await RCap.create({
                    seed: 'delta-cap-03',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: {
                        'admin':  { managedBy: ['creator'] },
                        'write':  { managedBy: ['admin'] },
                    },
                });
                const cap = (await ctx.createObject(capInit)) as RCap;

                const setInit = await RSet.create({
                    seed: 'delta-set-03',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                });
                const rset = (await ctx.createObject(setInit)) as RSet;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);

                const capFork = await (await cap.getScopedDag()).getFrontier();
                await cap.grant(bob.keyId, 'write', admin, capFork);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                await rset.addSigned('Y', bob);

                const setDag = (await ctx.getDag(rset.getId()))!;
                const beforeRevoke = await setDag.getFrontier();

                await cap.revoke(bob.keyId, 'write', admin, capFork);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const afterRevoke = await setDag.getFrontier();

                const delta = await rset.computeDelta(beforeRevoke, afterRevoke) as RSetDelta;

                assertEquals(delta.removed.length, 1, 'Y should be removed');
                assertTrue(delta.removed.includes(hashElement('Y')), 'Y hash should be in removed');

                assertTrue(delta.validityChanges.length >= 1, 'should have at least 1 validity change');
                const bobChange = delta.validityChanges.find(vc => vc.author === bob.keyId);
                assertTrue(bobChange !== undefined, 'should have a validity change for Bob');
                assertTrue(bobChange!.wasValid, 'Bob add was valid before revoke');
                assertFalse(bobChange!.nowValid, 'Bob add is void after revoke');
                assertEquals(bobChange!.action, 'add', 'action should be add');
                assertEquals(bobChange!.elementHash, hashElement('Y'), 'element should be Y');
            }
        },
        {
            name: '[DELTA04] computeDelta throws when END does not extend START',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const admin = await makeIdentity();
                const bob = await makeIdentity();

                const capInit = await RCap.create({
                    seed: 'delta-cap-04',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: {
                        'admin':  { managedBy: ['creator'] },
                        'write':  { managedBy: ['admin'] },
                    },
                });
                const cap = (await ctx.createObject(capInit)) as RCap;

                const setInit = await RSet.create({
                    seed: 'delta-set-04',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                });
                const rset = (await ctx.createObject(setInit)) as RSet;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                await cap.grant(bob.keyId, 'write', admin);
                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = (await ctx.getDag(rset.getId()))!;
                const beforeRevoke = await setDag.getFrontier();

                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const afterRevoke = await setDag.getFrontier();

                let threw = false;
                try {
                    await rset.computeDelta(afterRevoke, beforeRevoke);
                } catch (e) {
                    threw = ((e as Error).message).indexOf('requires END to extend START') >= 0;
                }
                assertTrue(threw, 'should throw when end does not extend start');
            }
        },
        {
            name: '[DELTA05] Plain RSet: default delta reports revision bound at the meet',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const init = await RSet.create({
                    seed: 'delta-05',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const rset = (await ctx.createObject(init)) as RSet;
                const dag = (await ctx.getDag(rset.getId()))!;

                await rset.add('alpha');
                const start = await dag.getFrontier();

                await rset.add('beta');
                await rset.delete('alpha');
                const end = await dag.getFrontier();

                const delta = await computeRSetWithStrategy(rset, 'bounded', start, end);
                const reference = await computeRSetWithStrategy(rset, 'full', start, end);

                assertEquals(
                    JSON.stringify(normalizeRSetDelta(delta)),
                    JSON.stringify(normalizeRSetDelta(reference)),
                    'default bounded delta should match reference scan on extending interval',
                );
                assertTrue(delta.added.includes(hashElement('beta')), 'beta should be added');
                assertTrue(delta.removed.includes(hashElement('alpha')), 'alpha should be removed');
                assertTrue(
                    set.eq(delta.getRevisionBound(), start),
                    'revision bound equals the meet (== start for a linear extension)',
                );
            }
        },
        {
            name: '[DELTA06] Permissioned RSet: the bound drops below an unstable ref-advance on a concurrent revoke',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const admin = await makeIdentity();
                const bob = await makeIdentity();

                const capInit = await RCap.create({
                    seed: 'delta-cap-06',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: {
                        'admin':  { managedBy: ['creator'] },
                        'write':  { managedBy: ['admin'] },
                    },
                });
                const cap = (await ctx.createObject(capInit)) as RCap;

                const setInit = await RSet.create({
                    seed: 'delta-set-06',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                });
                const rset = (await ctx.createObject(setInit)) as RSet;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);

                const capFork = await (await cap.getScopedDag()).getFrontier();
                await cap.grant(bob.keyId, 'write', admin, capFork);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = (await ctx.getDag(rset.getId()))!;

                await rset.addSigned('Y', bob);

                const beforeRevoke = await setDag.getFrontier();

                // Revoke concurrent with the grant -> at capV2 the grant is barred.
                await cap.revoke(bob.keyId, 'write', admin, capFork);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const afterRevoke = await setDag.getFrontier();

                const delta = await computeRSetWithStrategy(rset, 'bounded', beforeRevoke, afterRevoke);
                const reference = await computeRSetWithStrategy(rset, 'full', beforeRevoke, afterRevoke);

                assertEquals(
                    JSON.stringify(normalizeRSetDelta(delta)),
                    JSON.stringify(normalizeRSetDelta(reference)),
                    'default bounded delta should match reference scan across the revoke',
                );
                // ra1, the advance to capV1, is unstable, so the bound drops below it,
                // to the create op, and ra1 is walked.
                assertTrue(
                    set.eq(delta.getRevisionBound(), version(rset.getId())),
                    'the bound drops below the ref-advance just below the meet',
                );
                assertTrue(delta.removed.includes(hashElement('Y')), 'Y should be removed');
                const bobChange = delta.validityChanges.find(vc => vc.author === bob.keyId);
                assertTrue(bobChange !== undefined, 'should report Bob validity change');
                assertTrue(bobChange!.wasValid, 'Bob add was valid before revoke');
                assertFalse(bobChange!.nowValid, 'Bob add is void after revoke');
            }
        },
        {
            name: '[DELTA07] computeDelta throws when START is not in the history of END',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const init = await RSet.create({
                    seed: 'delta-07',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const rset = (await ctx.createObject(init)) as RSet;
                const dag = (await ctx.getDag(rset.getId()))!;

                await rset.add('alpha');
                const early = await dag.getFrontier();
                await rset.add('beta');
                const late = await dag.getFrontier();

                let threw = false;
                try {
                    // late does not extend early in the START position -> forkA is non-empty.
                    await rset.computeDelta(late, early);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'should throw when end does not extend start');
            }
        },
        {
            name: '[DELTA08] Permissioned RSet: the bound is the RSet meet when all ref-advances are stable',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const admin = await makeIdentity();
                const bob = await makeIdentity();

                const capInit = await RCap.create({
                    seed: 'delta-cap-08',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: {
                        'admin':  { managedBy: ['creator'] },
                        'write':  { managedBy: ['admin'] },
                    },
                });
                const cap = (await ctx.createObject(capInit)) as RCap;

                const setInit = await RSet.create({
                    seed: 'delta-set-08',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                });
                const rset = (await ctx.createObject(setInit)) as RSet;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);

                await cap.grant(bob.keyId, 'write', admin);
                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                await rset.addSigned('Y', bob);

                const setDag = (await ctx.getDag(rset.getId()))!;
                const start = await setDag.getFrontier();

                // Sequential revoke (after the grant) -> at capV2 the RCap evolves cleanly, so the
                // RCap revision bound stays at capV1. Every RSet ref-advance is then stable (its
                // referenced RCap version is at or below the bound), so the bound stays at the
                // RSet meet (use-before-revoke keeps Y, so the delta is empty).
                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const end = await setDag.getFrontier();

                const delta = await computeRSetWithStrategy(rset, 'bounded', start, end);
                const reference = await computeRSetWithStrategy(rset, 'full', start, end);

                assertEquals(
                    JSON.stringify(normalizeRSetDelta(delta)),
                    JSON.stringify(normalizeRSetDelta(reference)),
                    'default bounded delta should match reference scan (Y survives by use-before-revoke)',
                );
                assertEquals(delta.removed.length, 0, 'Y is not removed (use-before-revoke)');

                assertTrue(delta.getRevisionBound().size > 0, 'revision bound should be non-empty');

                // All ref-advances are stable, so the bound is the RSet meet, which sits at
                // or below start (forkA empty against start).
                const boundVsStart = await setDag.findForkPosition(delta.getRevisionBound(), start);
                assertEquals(boundVsStart.forkA.size, 0, 'revision bound must be at or below start');

                // ...and strictly above the create op: the meet is a real bound, not the trivial
                // whole-DAG fallback.
                assertFalse(
                    set.eq(delta.getRevisionBound(), version(rset.getId())),
                    'revision bound should be the meet, above the create op (not the trivial fallback)',
                );
            }
        },
        {
            name: '[DELTA09] Permissioned RSet: a stable element below the meet is not spuriously reported',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const admin = await makeIdentity();
                const bob = await makeIdentity();

                const capInit = await RCap.create({
                    seed: 'delta-cap-09',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: {
                        'admin':  { managedBy: ['creator'] },
                        'write':  { managedBy: ['admin'] },
                    },
                });
                const cap = (await ctx.createObject(capInit)) as RCap;

                const setInit = await RSet.create({
                    seed: 'delta-set-09',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                });
                const rset = (await ctx.createObject(setInit)) as RSet;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                await cap.grant(bob.keyId, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                // X and Z are added (and stay valid) well before the start position.
                await rset.addSigned('X', bob);
                await rset.addSigned('Z', bob);

                const setDag = (await ctx.getDag(rset.getId()))!;
                const start = await setDag.getFrontier();

                await rset.addSigned('W', bob);
                const end = await setDag.getFrontier();

                const delta = await computeRSetWithStrategy(rset, 'bounded', start, end);
                const reference = await computeRSetWithStrategy(rset, 'full', start, end);

                assertEquals(
                    JSON.stringify(normalizeRSetDelta(delta)),
                    JSON.stringify(normalizeRSetDelta(reference)),
                    'default bounded delta should match reference scan when only W is new',
                );
                assertTrue(delta.added.includes(hashElement('W')), 'W should be added');
                assertFalse(delta.added.includes(hashElement('X')), 'stable X should not be reported as added');
                assertFalse(delta.removed.includes(hashElement('X')), 'stable X should not be reported as removed');
                assertFalse(delta.added.includes(hashElement('Z')), 'stable Z should not be reported as added');
                assertEquals(delta.removed.length, 0, 'nothing should be removed');
            }
        },
        {
            name: '[DELTA10] Permissioned RSet: the bound passes a higher unstable ref-advance and drops below the lowest one',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const admin = await makeIdentity();
                const bob = await makeIdentity();

                const capInit = await RCap.create({
                    seed: 'delta-cap-10',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: {
                        'admin':  { managedBy: ['creator'] },
                        'write':  { managedBy: ['admin'] },
                    },
                });
                const cap = (await ctx.createObject(capInit)) as RCap;

                const setInit = await RSet.create({
                    seed: 'delta-set-10',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                });
                const rset = (await ctx.createObject(setInit)) as RSet;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);

                const capFork = await (await cap.getScopedDag()).getFrontier();
                await cap.grant(bob.keyId, 'write', admin, capFork);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = (await ctx.getDag(rset.getId()))!;

                await rset.addSigned('X', bob);

                // Advance the cap a second time (a clean, sequential grant) and ref-advance to it.
                // This stacks a second ref-advance (ra2 -> capV2) above ra1, with addX between them.
                await cap.grant(bob.keyId, 'admin', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                await rset.addSigned('Y', bob);
                const start = await setDag.getFrontier();

                // Revoke 'write' concurrent with the original grant (attached at capFork) -> the RCap
                // revision bound drops below capV1, so both ra1 (-> capV1) and ra2 (-> capV2) are
                // unstable. The descent must pass ra2 and settle at ra1, the lowest unstable one.
                await cap.revoke(bob.keyId, 'write', admin, capFork);
                const capV3 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV3, admin);

                const end = await setDag.getFrontier();

                const delta = await computeRSetWithStrategy(rset, 'bounded', start, end);
                const reference = await computeRSetWithStrategy(rset, 'full', start, end);

                assertEquals(
                    JSON.stringify(normalizeRSetDelta(delta)),
                    JSON.stringify(normalizeRSetDelta(reference)),
                    'default bounded delta should match reference scan across a multi-level descent',
                );

                // The bound drops below the lowest unstable ref-advance (ra1), reached by
                // descending past ra2, to the create op. A bound stuck below ra2 would skip
                // addX and drop its validity flip, breaking the match.
                assertTrue(
                    set.eq(delta.getRevisionBound(), version(rset.getId())),
                    'the bound drops below the lowest unstable ref-advance (ra1), below addX',
                );
                assertTrue(delta.removed.includes(hashElement('X')), 'X is voided by the concurrent revoke');
                assertTrue(delta.removed.includes(hashElement('Y')), 'Y is voided by the concurrent revoke');
            }
        },
        {
            name: '[DELTA11] Nested RSet-of-RSet: nested map carries inner changes under a single root span',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const outerInit = await RSet.create({
                    seed: 'delta-11-outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const outer = (await ctx.createObject(outerInit)) as RSet;

                const dag = (await ctx.getDag(outer.getId()))!;
                const start = await dag.getFrontier();

                const innerInit = await RSet.create({
                    seed: 'delta-11-inner',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const innerHash = await outer.add(innerInit);

                const inner = (await (await outer.getView()).loadRObjectByHash(innerHash)) as RSet;

                await inner.add('alpha');
                await inner.add('beta');

                const end = await dag.getFrontier();

                const delta = await computeRSetWithStrategy(outer, 'bounded', start, end);

                // Single root span.
                assertTrue(set.eq(delta.start, start), 'delta start matches');
                assertTrue(set.eq(delta.end, end), 'delta end matches');

                // Outer membership: the inner object was added in the interval.
                assertTrue(delta.added.includes(innerHash), 'inner object appears as added in the outer set');
                assertEquals(delta.removed.length, 0, 'nothing removed from the outer set');

                // Nested map carries the inner set's own changes.
                const innerChanges = delta.nested.get(innerHash);
                assertTrue(innerChanges !== undefined, 'nested map has an entry for the inner object');
                assertEquals(innerChanges!.type, RSet.typeId, 'nested entry type is RSet');
                const innerAdded = (innerChanges!.changes as { added: string[] }).added;
                assertTrue(innerAdded.includes(hashElement('alpha')), 'inner alpha is added');
                assertTrue(innerAdded.includes(hashElement('beta')), 'inner beta is added');
            }
        },
        {
            name: '[DELTA12] Nested RSet-of-RSet: pure inner change reports nested-only delta',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSet.typeId, rSetFactory);

                const outerInit = await RSet.create({
                    seed: 'delta-12-outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const outer = (await ctx.createObject(outerInit)) as RSet;

                const innerInit = await RSet.create({
                    seed: 'delta-12-inner',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const innerHash = await outer.add(innerInit);

                const inner = (await (await outer.getView()).loadRObjectByHash(innerHash)) as RSet;
                await inner.add('alpha');

                // Start AFTER the inner object exists and already holds 'alpha'.
                const dag = (await ctx.getDag(outer.getId()))!;
                const start = await dag.getFrontier();

                await inner.add('gamma');
                const end = await dag.getFrontier();

                const delta = await computeRSetWithStrategy(outer, 'bounded', start, end);

                // The outer set's own membership is unchanged across the interval.
                assertEquals(delta.added.length, 0, 'no outer membership added');
                assertEquals(delta.removed.length, 0, 'no outer membership removed');

                // Only the inner set changed: gamma added, alpha untouched (stable below start).
                const innerChanges = delta.nested.get(innerHash);
                assertTrue(innerChanges !== undefined, 'nested map has an entry for the inner object');
                const innerAdded = (innerChanges!.changes as { added: string[] }).added;
                assertTrue(innerAdded.includes(hashElement('gamma')), 'inner gamma is added');
                assertFalse(innerAdded.includes(hashElement('alpha')), 'stable inner alpha is not reported');
            }
        },
        {
            name: '[DELTA13] Nested permissioned RSet: the full delta reports an inner add voided through a side-branch ref-advance, at any depth',
            invoke: async () => {
                const ctx = permissionedContext();
                const [admin, bob] = [await makeIdentity(), await makeIdentity()];
                const outer = (await ctx.createObject(await RSet.create({
                    seed: 'delta-13-outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                }))) as RSet;
                const dag = (await ctx.getDag(outer.getId()))!;

                // The [SET_BOUND01] shape inside a permissioned set held by
                // `parent`, over its own RCap: from `end`, Bob's add X of 'Y'
                // turns void and 'Y' leaves the set.
                const flipInside = async (parent: RSet, seed: string) => {
                    const cap = (await ctx.createObject(await RCap.create({
                        seed: seed + '-cap',
                        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                        initialCaps: { admin: { managedBy: ['creator'] }, write: { managedBy: ['admin'] } },
                    }))) as RCap;
                    await addIdentities(cap, admin, bob);
                    const { g, t1, r } = await concurrentRevokeOfBob(cap, admin, bob);

                    const hash = await parent.add(await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        capabilityRef: cap.getId(),
                        capRequirements: { add: 'write', delete: 'write' },
                    }));
                    const inner = (await (await parent.getView()).loadRObjectByHash(hash)) as RSet;
                    const R0 = await inner.refAdvance(version(g), admin);
                    const X = await inner.addSigned('Y', bob, version(R0));
                    const T = await inner.refAdvance(version(t1), admin, version(R0));
                    const N = await inner.refAdvance(version(t1, r), admin, version(X));
                    const [start, end] = [version(X, T), version(N, T)];
                    assertTrue(set.eq(await dag.getFrontier(), end), 'end is the outer frontier');
                    assertTrue(await (await inner.getView(start, start)).has('Y'), 'Y is in the inner set at start');
                    assertFalse(await (await inner.getView(end, end)).has('Y'), 'Y is not in the inner set at end');
                    return { hash, X, start, end };
                };
                const checkFlip = (changes: DeltaChanges | undefined, X: B64Hash, where: string) => {
                    assertTrue(changes !== undefined, 'the nested map has an entry for the ' + where);
                    const { removed, validityChanges } = changes!.changes as RSetChanges;
                    assertTrue(removed.includes(hashElement('Y')), 'Y is removed from the ' + where);
                    const change = validityChanges.find((vc) => vc.entryHash === X);
                    assertTrue(change !== undefined && change.wasValid && !change.nowValid, 'X turns void in the ' + where);
                };

                const child = await flipInside(outer, 'delta-13-child');
                const childDelta = await computeRSetWithStrategy(outer, 'full', child.start, child.end);
                checkFlip(childDelta.nested.get(child.hash), child.X, 'child');

                const middleHash = await outer.add(await RSet.create({
                    seed: 'delta-13-middle',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                }));
                const middle = (await (await outer.getView()).loadRObjectByHash(middleHash)) as RSet;
                const grandchild = await flipInside(middle, 'delta-13-grandchild');
                const grandchildDelta = await computeRSetWithStrategy(outer, 'full', grandchild.start, grandchild.end);
                checkFlip(grandchildDelta.nested.get(middleHash)?.nested.get(grandchild.hash), grandchild.X, 'grandchild');
            }
        },
        {
            name: '[SET_BOUND01] an add below the meet, widened by a side-branch ref-advance, flips when a new ref-advance imports a revoke concurrent with that version',
            invoke: async () => {
                const { cap, rset, admin, bob } = await writeCapSet('set-bound-01', false);
                const { g, t1, r } = await concurrentRevokeOfBob(cap, admin, bob);

                // R0 ── X ── N        start = {X, T}, end = {N, T}
                //   \
                //    T                T publishes t1, N publishes {t1, r}
                //
                // T is concurrent with X, so X's refAt is t1 at both horizons. At
                // `end`, refFrom also holds r, which is concurrent with t1, so
                // Bob's write fails. RCap's bound between t1 and {t1, r} is g, so
                // T is unstable; it is not in the meet's history.
                const R0 = await rset.refAdvance(version(g), admin);
                const X = await rset.addSigned('Y', bob, version(R0));
                const T = await rset.refAdvance(version(t1), admin, version(R0));
                const N = await rset.refAdvance(version(t1, r), admin, version(X));

                const delta = await boundedSetDelta(rset, version(X, T), version(N, T));
                assertTrue(delta.removed.includes(hashElement('Y')), 'Y is removed');
                const change = delta.validityChanges.find((vc) => vc.entryHash === X);
                assertTrue(change !== undefined && change.wasValid && !change.nowValid, 'X turns void');
                assertTrue(isExactly(delta.getRevisionBound(), R0), 'the bound drops below T, so X is walked');
            }
        },
        {
            name: '[SET_BOUND02] the RCap bound is taken between the versions observed at start and at end, not at the meet',
            invoke: async () => {
                const ctx = permissionedContext();
                const [C, Rt, S, A, B, Q] = [
                    await makeIdentity(), await makeIdentity(), await makeIdentity(),
                    await makeIdentity(), await makeIdentity(), await makeIdentity(),
                ];
                // Q is added at s1, after G.
                const cap = (await ctx.createObject(await RCap.create({
                    seed: 'set-bound-02-cap',
                    creators: [{ keyId: C.keyId, publicKey: C.publicKey }],
                    initialCaps: {
                        root: { managedBy: ['creator'] },
                        super: { managedBy: ['root'] },
                        admin: { managedBy: ['super'] },
                        write: { managedBy: ['admin'] },
                    },
                }))) as RCap;
                const rset = (await ctx.createObject(await RSet.create({
                    seed: 'set-bound-02-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write' },
                }))) as RSet;

                // In RCap:
                //
                //     gA ── G ── s1 ── V        T revokes A's admin, concurrent with G
                //       \          \            V revokes S's super, concurrent with T
                //        T          W           W revokes Rt's root, concurrent with V
                //
                // Bob's grant G is killed by T when T is live. T is live at
                // {s1, T}, void at {V, T} (V is live), and live again at
                // {V, W, T} (W voids V).
                await addIdentities(cap, C, Rt, S, A, B);
                await cap.grant(Rt.keyId, 'root', C);
                await cap.grant(S.keyId, 'super', Rt);
                const gA = await cap.grant(A.keyId, 'admin', S);
                const G = await cap.grant(B.keyId, 'write', A, version(gA));
                const T = await cap.revoke(A.keyId, 'admin', S, version(gA));
                const s1 = await cap.addIdentity(Q.keyId, serializePublicKeyToBase64(Q.publicKey), C, version(G));
                const V = await cap.revoke(S.keyId, 'super', Rt, version(s1));
                const W = await cap.revoke(Rt.keyId, 'root', C, version(s1));

                // In the set:
                //
                //     RG ── X ── RM ── P ── N       start = {P, RS}, end = {N, RS}
                //                  \
                //                   RS              RG: G, RM: {s1, T}, RS: {V, T}, N: {V, W, T}
                //
                // X is Bob's add, read at G throughout. It is valid at `start`
                // (T void) and void at `end` (T live). The meet is P, which
                // observes {s1, T}; there and at `end` T is live, so a bound
                // taken from the meet sees no change in RCap below s1.
                const RG = await rset.refAdvance(version(G), C);
                const X = await rset.addSigned('Y', B, version(RG));
                const RM = await rset.refAdvance(version(s1, T), C, version(X));
                const P = await rset.addSigned('Z', C, version(RM));
                const RS = await rset.refAdvance(version(V, T), C, version(RM));
                const N = await rset.refAdvance(version(V, W, T), C, version(P));

                const delta = await boundedSetDelta(rset, version(P, RS), version(N, RS));
                assertTrue(delta.removed.includes(hashElement('Y')), 'Y is removed');
                const change = delta.validityChanges.find((vc) => vc.entryHash === X);
                assertTrue(change !== undefined && change.wasValid && !change.nowValid, 'X turns void');
                assertTrue(isExactly(delta.getRevisionBound(), rset.getId()), 'the bound drops below RG, so X is walked');
            }
        },
        {
            name: '[SET_BOUND03] a side-branch barrier delete whose authorization flips lowers the bound below it',
            invoke: async () => {
                const { cap, rset, admin, bob } = await writeCapSet('set-bound-03', true);
                const { g, t1, r } = await concurrentRevokeOfBob(cap, admin, bob);

                //            ┌── D                start = {D, R}, end = {D, N}
                // R0 ── a ───┤
                //            └── v ── R ── N      R publishes t1, N publishes {t1, r}
                //
                // D is Bob's barrier delete of e. R is concurrent with D, so D's
                // authorization flips at `end`, when its concurrent fold holds r.
                // The view at v reads D, a barrier concurrent with it, so it
                // sees e at `end` and not at `start`: the bound must drop below v.
                // R alone lowers it only to v.
                const R0 = await rset.refAdvance(version(g), admin);
                const a = await rset.addSigned('e', admin, version(R0));
                const D = await rset.deleteWithBarrierSigned('e', bob, version(a));
                const v = await rset.addSigned('f', admin, version(a));
                const R = await rset.refAdvance(version(t1), admin, version(v));
                const N = await rset.refAdvance(version(t1, r), admin, version(R));

                const delta = await boundedSetDelta(rset, version(D, R), version(D, N));
                assertTrue(delta.added.includes(hashElement('e')), 'e is back once D turns void');
                const change = delta.validityChanges.find((vc) => vc.entryHash === D);
                assertTrue(change !== undefined && change.wasValid && !change.nowValid, 'D turns void');
                assertTrue(isExactly(delta.getRevisionBound(), a), 'the bound drops below D');
            }
        },
        {
            name: '[SET_BOUND05] a stable ref-advance that turns void through its widened cut lowers the bound below it',
            invoke: async () => {
                const ctx = permissionedContext();
                const [C, A, B, D] = [await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity()];
                const cap = (await ctx.createObject(await RCap.create({
                    seed: 'set-bound-05-cap',
                    creators: [{ keyId: C.keyId, publicKey: C.publicKey }],
                    initialCaps: { admin: { managedBy: ['creator'] }, steward: { managedBy: ['admin'] }, write: { managedBy: ['admin'] } },
                }))) as RCap;
                const rset = (await ctx.createObject(await RSet.create({
                    seed: 'set-bound-05-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    capabilityRef: cap.getId(),
                    capRequirements: { add: 'write', delete: 'write', refAdvance: ['steward'] },
                }))) as RSet;
                await addIdentities(cap, C, A, B);
                await cap.grant(A.keyId, 'steward', C);

                // RCap:  c0 ── vY ── vZ      c0 grants Bob write, vY revokes it. vZ is an identity.
                //               \            rY revokes A's steward, concurrent with vZ only.
                //                rY
                const c0 = await cap.grant(B.keyId, 'write', C);
                const vY = await cap.revoke(B.keyId, 'write', C, version(c0));
                const vZ = await cap.addIdentity(D.keyId, serializePublicKeyToBase64(D.publicKey), C, version(vY));
                const rY = await cap.revoke(A.keyId, 'steward', C, version(vY));

                // Set:  R0 ── X ── Z ── N    X is Bob's add. Y is A's advance to {vY}; Z and N
                //         \                  are the creator's, to {vZ} and to {vZ, rY}.
                //          Y                 start = {Y, Z}, end = {Y, N}
                //
                // At `start`, Y is live and its vY voids X. Y's cut is widened by Z to vZ,
                // which rY is concurrent with, so at `end` Y is void and X is valid. vY is
                // stable, and Phase A puts the bound on X: only the flip reaches X.
                const R0 = await rset.refAdvance(version(c0), C);
                const X = await rset.addSigned('el-1', B, version(R0));
                const Y = await rset.refAdvance(version(vY), A, version(R0));
                const Z = await rset.refAdvance(version(vZ), C, version(X));
                const N = await rset.refAdvance(version(vZ, rY), C, version(Z));

                const delta = await boundedSetDelta(rset, version(Y, Z), version(Y, N));
                assertTrue(delta.added.includes(hashElement('el-1')), 'el-1 is added');
                const change = delta.validityChanges.find((vc) => vc.entryHash === X);
                assertTrue(change !== undefined && !change.wasValid && change.nowValid, 'X turns valid');
                assertTrue(isExactly(delta.getRevisionBound(), R0), 'the bound drops below Y, so X is walked');
            }
        },
        {
            name: '[SET_BOUND04] the bounded delta rejects a permissioned nested set, at any depth; the full one does not',
            invoke: async () => {
                const ctx = permissionedContext();
                const admin = await makeIdentity();
                const cap = (await ctx.createObject(await RCap.create({
                    seed: 'set-bound-04-cap',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    initialCaps: { write: { managedBy: ['creator'] } },
                }))) as RCap;
                const nestedSet = (seed: string, permissioned: boolean) => RSet.create({
                    seed,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                    ...(permissioned
                        ? { capabilityRef: cap.getId(), capRequirements: { add: 'write' } }
                        : { contentType: RSet.typeId }),
                });

                const outer = (await ctx.createObject(await RSet.create({
                    seed: 'set-bound-04-outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                }))) as RSet;
                const dag = (await ctx.getDag(outer.getId()))!;

                const rejects = async (start: Version, end: Version, what: string) => {
                    await computeRSetWithStrategy(outer, 'full', start, end);
                    let threw = false;
                    try {
                        await computeRSetWithStrategy(outer, 'bounded', start, end);
                    } catch (e) {
                        threw = (e as Error).message.indexOf('permissioned nested set') >= 0;
                    }
                    assertTrue(threw, 'the bounded delta rejects ' + what);
                };

                const beforeChild = await dag.getFrontier();
                await outer.add(await nestedSet('set-bound-04-child', true));
                await rejects(beforeChild, await dag.getFrontier(), 'a permissioned child');

                const middleHash = await outer.add(await nestedSet('set-bound-04-middle', false));
                const middle = (await (await outer.getView()).loadRObjectByHash(middleHash)) as RSet;
                const beforeGrandchild = await dag.getFrontier();
                await middle.add(await nestedSet('set-bound-04-grandchild', true));
                await rejects(beforeGrandchild, await dag.getFrontier(), 'a permissioned grandchild');
            }
        },
    ]
};
