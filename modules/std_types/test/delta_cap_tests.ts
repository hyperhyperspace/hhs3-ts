import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap.js";
import type { RCapDelta } from "../src/types/rcap.js";
import { serializePublicKeyToBase64 } from "../src/authorship.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function createTestCap(admin: OwnIdentity) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);

    const init = await RCap.create({
        seed: 'delta-cap-test',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: {
            'admin':  { managedBy: ['creator'] },
            'write':  { managedBy: ['admin'] },
        },
    });
    const cap = (await ctx.createObject(init)) as RCap;
    return { ctx, cap };
}

export const deltaCapTests = {
    title: '[CAP_DELTA] RCap computeDelta tests',
    tests: [
        {
            name: '[CAP_DELTA01] Add identity and grant: delta shows identity and grant changes',
            invoke: async () => {
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                const creationVersion = version(cap.getId());

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                await cap.grant(bob.keyId, 'write', cap.getId(), admin);

                const dag = (await ctx.getDag(cap.getId()))!;
                const endVersion = await dag.getFrontier();

                const delta = await cap.computeDelta(creationVersion, endVersion) as RCapDelta;

                assertEquals(delta.identityChanges.length, 1, 'one identity added');
                assertEquals(delta.identityChanges[0].keyId, bob.keyId, 'Bob identity');
                assertTrue(delta.identityChanges[0].added, 'Bob was added');

                assertEquals(delta.capabilityChanges.length, 0, 'no capability changes (initial caps unchanged)');

                assertEquals(delta.grantChanges.length, 1, 'one grant change');
                assertEquals(delta.grantChanges[0].keyId, bob.keyId, 'grant is for Bob');
                assertEquals(delta.grantChanges[0].capName, 'write', 'grant is for write');
                assertFalse(delta.grantChanges[0].wasGranted, 'was not granted');
                assertTrue(delta.grantChanges[0].nowGranted, 'now granted');

                assertEquals(delta.getRevisionBound().size, 0, 'brute-force revision bound is empty');
            }
        },
        {
            name: '[CAP_DELTA02] Grant then revoke: partial delta shows flip, full delta is net zero',
            invoke: async () => {
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                const creationVersion = version(cap.getId());

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                await cap.grant(bob.keyId, 'write', cap.getId(), admin);

                const dag = (await ctx.getDag(cap.getId()))!;
                const afterGrant = await dag.getFrontier();

                await cap.revoke(bob.keyId, 'write', admin);

                const afterRevoke = await dag.getFrontier();

                const partialDelta = await cap.computeDelta(afterGrant, afterRevoke) as RCapDelta;
                assertEquals(partialDelta.grantChanges.length, 1, 'partial: one grant change');
                assertTrue(partialDelta.grantChanges[0].wasGranted, 'partial: was granted');
                assertFalse(partialDelta.grantChanges[0].nowGranted, 'partial: now not granted');

                const fullDelta = await cap.computeDelta(creationVersion, afterRevoke) as RCapDelta;
                assertEquals(fullDelta.grantChanges.length, 0, 'full: net zero grant changes');
                assertEquals(fullDelta.identityChanges.length, 1, 'full: Bob identity still added');
            }
        },
        {
            name: '[CAP_DELTA03] Create and delete capability: delta captures existence changes',
            invoke: async () => {
                const admin = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                const dag = (await ctx.getDag(cap.getId()))!;
                const beforeCreate = await dag.getFrontier();

                await cap.createCap('deploy', ['admin'], admin);
                const afterCreate = await dag.getFrontier();

                await cap.deleteCap('deploy', admin);
                const afterDelete = await dag.getFrontier();

                const createDelta = await cap.computeDelta(beforeCreate, afterCreate) as RCapDelta;
                assertEquals(createDelta.capabilityChanges.length, 1, 'one capability change after create');
                assertEquals(createDelta.capabilityChanges[0].capName, 'deploy', 'capName is deploy');
                assertFalse(createDelta.capabilityChanges[0].existed, 'did not exist before');
                assertTrue(createDelta.capabilityChanges[0].exists, 'exists after create');

                const deleteDelta = await cap.computeDelta(afterCreate, afterDelete) as RCapDelta;
                assertEquals(deleteDelta.capabilityChanges.length, 1, 'one capability change after delete');
                assertEquals(deleteDelta.capabilityChanges[0].capName, 'deploy', 'capName is deploy');
                assertTrue(deleteDelta.capabilityChanges[0].existed, 'existed before delete');
                assertFalse(deleteDelta.capabilityChanges[0].exists, 'does not exist after delete');
            }
        },
        {
            name: '[CAP_DELTA04] Concurrent barrier revoke voids grant: delta captures flip',
            invoke: async () => {
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                const dag = (await ctx.getDag(cap.getId()))!;
                const forkPoint = await (await cap.getScopedDag()).getFrontier();

                // Concurrent: grant and revoke from the same fork point
                await cap.grant(alice.keyId, 'write', cap.getId(), admin, forkPoint);
                await cap.revoke(alice.keyId, 'write', admin, forkPoint);

                const afterMerge = await dag.getFrontier();

                // Verify the view: revoke barrier wins
                const view = await cap.getView();
                assertFalse(await view.hasCapability(alice.keyId, 'write'), 'revoke barrier should win');

                // Delta from fork point (no grant) to merged (still no grant due to barrier)
                const delta = await cap.computeDelta(forkPoint, afterMerge) as RCapDelta;
                assertEquals(delta.grantChanges.length, 0, 'net zero: grant voided by concurrent barrier revoke');

                // Delta from just the grant branch to merged: grant flips to not-granted
                const grantView = await cap.getView(forkPoint, forkPoint);
                assertFalse(await grantView.hasCapability(alice.keyId, 'write'), 'Alice had no write at fork point');

                // Grant Alice from forkPoint, take that version as start
                // We already have the grant in the DAG; let's use a version that sees only the grant
                const scopedDag = await cap.getScopedDag();
                const grantCover = await scopedDag.findCoverWithFilter(
                    afterMerge,
                    { containsValues: { grants: ['write:' + alice.keyId] } },
                );
                // Find the grant entry hash
                let grantHash: string | undefined;
                for (const h of grantCover) {
                    const entry = await scopedDag.loadEntry(h);
                    if (entry && (entry.payload as any).action === 'grant') {
                        grantHash = h;
                        break;
                    }
                }
                assertTrue(grantHash !== undefined, 'should find the grant entry');

                const afterGrantOnly = version(grantHash!);
                const grantOnlyView = await cap.getView(afterGrantOnly, afterGrantOnly);
                assertTrue(await grantOnlyView.hasCapability(alice.keyId, 'write'), 'Alice has write in grant-only view');

                const grantToMerge = await cap.computeDelta(afterGrantOnly, afterMerge) as RCapDelta;
                assertEquals(grantToMerge.grantChanges.length, 1, 'grant flips to revoked');
                assertTrue(grantToMerge.grantChanges[0].wasGranted, 'was granted');
                assertFalse(grantToMerge.grantChanges[0].nowGranted, 'now revoked');
                assertEquals(grantToMerge.grantChanges[0].capName, 'write', 'capName is write');
                assertEquals(grantToMerge.grantChanges[0].keyId, alice.keyId, 'keyId is Alice');
            }
        },
    ]
};
