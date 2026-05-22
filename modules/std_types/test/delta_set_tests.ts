import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap.js";
import { RSet, rSetFactory } from "../src/types/rset.js";
import type { RSetDelta } from "../src/types/rset.js";
import { serializePublicKeyToBase64 } from "../src/authorship.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

function hashElement(element: string): string {
    return hashSuite.hashToB64(stringToUint8Array(json.toStringNormalized(element)));
}

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
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
                assertEquals(delta.getRevisionBound().size, 0, 'brute-force revision bound is empty');
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

                const fullDelta = await rset.computeDelta(creationVersion, afterDelete) as RSetDelta;
                assertEquals(fullDelta.added.length, 0, 'net-zero: nothing added');
                assertEquals(fullDelta.removed.length, 0, 'net-zero: nothing removed');

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
                await cap.grant(bob.keyId, 'write', cap.getId(), admin, capFork);

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
            name: '[DELTA04] Permissioned RSet: reverse delta shows revalidation',
            invoke: async () => {
                // Reuses the same setup as DELTA03 but computes the delta in
                // reverse (from after-revoke back to before-revoke). Y should
                // appear in added with a validity change wasValid=false -> nowValid=true.
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

                const capFork = await (await cap.getScopedDag()).getFrontier();
                await cap.grant(bob.keyId, 'write', cap.getId(), admin, capFork);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                await rset.addSigned('Y', bob);

                const setDag = (await ctx.getDag(rset.getId()))!;
                const beforeRevoke = await setDag.getFrontier();

                await cap.revoke(bob.keyId, 'write', admin, capFork);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const afterRevoke = await setDag.getFrontier();

                // Reverse delta: from after-revoke back to before-revoke
                const delta = await rset.computeDelta(afterRevoke, beforeRevoke) as RSetDelta;

                assertEquals(delta.added.length, 1, 'Y should be added (became valid)');
                assertTrue(delta.added.includes(hashElement('Y')), 'Y hash should be in added');
                assertEquals(delta.removed.length, 0, 'nothing removed');

                assertTrue(delta.validityChanges.length >= 1, 'should have at least 1 validity change');
                const bobChange = delta.validityChanges.find(vc => vc.author === bob.keyId);
                assertTrue(bobChange !== undefined, 'should have a validity change for Bob');
                assertFalse(bobChange!.wasValid, 'Bob add was invalid at after-revoke');
                assertTrue(bobChange!.nowValid, 'Bob add is valid at before-revoke');
                assertEquals(bobChange!.action, 'add', 'action should be add');
                assertEquals(bobChange!.elementHash, hashElement('Y'), 'element should be Y');
            }
        },
    ]
};
