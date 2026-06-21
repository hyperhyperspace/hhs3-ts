import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { set } from "@hyper-hyper-space/hhs3_util";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap/rcap.js";
import { RSet, rSetFactory } from "../src/types/rset/rset.js";
import type { RSetDelta } from "../src/types/rset/rset.js";
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
            name: '[DELTA06] Permissioned RSet: woven floor on concurrent revoke',
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
                const afterRa1 = await setDag.getFrontier();

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
                assertTrue(
                    set.eq(delta.getRevisionBound(), afterRa1),
                    'floor descent lands at the ref-advance just below the meet (tight floor)',
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
            name: '[DELTA08] Permissioned RSet: woven floor lands at the RSet meet when all ref-advances are stable',
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
                // referenced RCap version is at or below the bound), so the descent finds nothing
                // unstable and the woven floor lands at the RSet meet (use-before-revoke keeps Y,
                // so the delta is empty).
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

                // All ref-advances are stable, so the woven floor is the RSet meet, which sits at
                // or below start (forkA empty against start).
                const boundVsStart = await setDag.findForkPosition(delta.getRevisionBound(), start);
                assertEquals(boundVsStart.forkA.size, 0, 'revision bound must be at or below start');

                // ...and strictly above the create op: the meet is a real floor, not the trivial
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
            name: '[DELTA10] Permissioned RSet: descent passes a higher ref-advance to settle at the lowest unstable one',
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
                const afterRa1 = await setDag.getFrontier();

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

                // The floor is the lowest unstable ref-advance (ra1), reached by descending past ra2.
                // A floor stuck at ra2 would skip addX and drop its validity flip, breaking the match.
                assertTrue(
                    set.eq(delta.getRevisionBound(), afterRa1),
                    'descent settles at the lowest unstable ref-advance (ra1), below addX',
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
    ]
};
