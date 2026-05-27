import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap/rcap.js";
import { serializePublicKeyToBase64 } from "../src/authorship.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function createTestEnv(opts?: {
    extraCaps?: { [k: string]: { managedBy: string[] } };
    enrollCapability?: string;
}) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);

    const admin = await makeIdentity();

    const caps: { [k: string]: { managedBy: string[] } } = {
        'admin':  { managedBy: ['creator'] },
        'enroll': { managedBy: ['admin'] },
        'write':  { managedBy: ['admin'] },
        'read':   { managedBy: ['admin'] },
        ...opts?.extraCaps,
    };

    const init = await RCap.create({
        seed: 'cap-test',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: caps,
        enrollCapability: opts?.enrollCapability,
    });

    const cap = (await ctx.createObject(init)) as RCap;

    return { ctx, cap, admin };
}

export const rcapTests = {
    title: '[CAP] RCap capability tests',
    tests: [
        {
            name: '[CAP01] Create with initial capabilities',
            invoke: async () => {
                const { cap } = await createTestEnv();

                const view = await cap.getView();
                assertTrue(await view.capabilityExists('admin'), 'admin capability should exist');
                assertTrue(await view.capabilityExists('write'), 'write capability should exist');
                assertTrue(await view.capabilityExists('read'), 'read capability should exist');
                assertTrue(await view.capabilityExists('enroll'), 'enroll capability should exist');
                assertFalse(await view.capabilityExists('nonexistent'), 'nonexistent capability should not exist');
            }
        },
        {
            name: '[CAP02] Add identity and verify isIdentity',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();

                const alice = await makeIdentity();

                const view0 = await cap.getView();
                assertTrue(await view0.isIdentity(admin.keyId), 'creator should be an identity');
                assertFalse(await view0.isIdentity(alice.keyId), 'alice should not yet be an identity');

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                const view1 = await cap.getView();
                assertTrue(await view1.isIdentity(alice.keyId), 'alice should now be an identity');
            }
        },
        {
            name: '[CAP03] Grant requires registered identity',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                let grantFailed = false;
                try {
                    await cap.grant(
                        alice.keyId, 'write',
                        admin,
                    );
                } catch {
                    grantFailed = true;
                }

                const view = await cap.getView();
                assertFalse(await view.hasCapability(alice.keyId, 'write'), 'unregistered identity should not have capability');
            }
        },
        {
            name: '[CAP04] Grant and check hasCapability',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                );

                const view = await cap.getView();
                assertTrue(await view.hasCapability(alice.keyId, 'write'), 'alice should have write');
                assertFalse(await view.hasCapability(alice.keyId, 'read'), 'alice should not have read');
            }
        },
        {
            name: '[CAP05] Revoke removes capability',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                );

                const view1 = await cap.getView();
                assertTrue(await view1.hasCapability(alice.keyId, 'write'), 'alice should have write before revoke');

                await cap.revoke(
                    alice.keyId, 'write',
                    admin,
                );

                const view2 = await cap.getView();
                assertFalse(await view2.hasCapability(alice.keyId, 'write'), 'alice should not have write after revoke');
            }
        },
        {
            name: '[CAP06] Concurrent grant and revoke: revoke wins (barrier)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                const addIdFrontier = await (await cap.getScopedDag()).getFrontier();

                const grantHash = await cap.grant(
                    alice.keyId, 'write',
                    admin,
                    addIdFrontier,
                );

                const revokeHash = await cap.revoke(
                    alice.keyId, 'write',
                    admin,
                    addIdFrontier,
                );

                const grantVersion = version(grantHash);
                const viewFromGrant = await cap.getView(grantVersion);
                assertFalse(await viewFromGrant.hasCapability(alice.keyId, 'write'),
                    'concurrent revoke barrier should override grant');

                const latestView = await cap.getView();
                assertFalse(await latestView.hasCapability(alice.keyId, 'write'),
                    'latest view should not have write after concurrent revoke barrier');
            }
        },
        {
            name: '[CAP07] createCap and deleteCap',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();

                await cap.createCap(
                    'deploy', ['admin'],
                    admin,
                );

                const view1 = await cap.getView();
                assertTrue(await view1.capabilityExists('deploy'), 'deploy should exist after createCap');

                await cap.deleteCap(
                    'deploy',
                    admin,
                );

                const view2 = await cap.getView();
                assertFalse(await view2.capabilityExists('deploy'), 'deploy should not exist after deleteCap');
            }
        },
        {
            name: '[CAP08] Concurrent createCap and unrelated delete does not void initial origin',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();

                const frontier = await (await cap.getScopedDag()).getFrontier();

                const createHash = await cap.createCap(
                    'deploy', ['admin'],
                    admin,
                    frontier,
                );

                const createVersion = version(createHash);

                const deleteHash = await cap.deleteCap(
                    'write',
                    admin,
                    frontier,
                );

                const createView = await cap.getView(createVersion);
                assertTrue(await createView.capabilityExists('deploy'), 'deploy should exist in its own branch');
                assertTrue(await createView.capabilityExists('write'),
                    'write should survive: concurrent delete is not in create origin history');
            }
        },
        {
            name: '[CAP09] capOrigin voiding: delete and re-create voids old grants',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                );

                const view1 = await cap.getView();
                assertTrue(await view1.hasCapability(alice.keyId, 'write'), 'alice should have write');

                await cap.deleteCap(
                    'write',
                    admin,
                );

                const view2 = await cap.getView();
                assertFalse(await view2.capabilityExists('write'), 'write should not exist after delete');
                assertFalse(await view2.hasCapability(alice.keyId, 'write'), 'alice should not have write after delete');

                await cap.createCap(
                    'write', ['admin'],
                    admin,
                );

                const view3 = await cap.getView();
                assertTrue(await view3.capabilityExists('write'), 'write should exist after re-create');
                assertFalse(await view3.hasCapability(alice.keyId, 'write'),
                    'old grant should be voided: capOrigin points to stale create entry');

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                );

                const view4 = await cap.getView();
                assertTrue(await view4.hasCapability(alice.keyId, 'write'),
                    'new grant with correct capOrigin should work');
            }
        },
        {
            name: '[CAP10] Creator always has all capabilities (irrevocable)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();

                const view = await cap.getView();
                assertTrue(await view.hasCapability(admin.keyId, 'admin'), 'creator should have admin');
                assertTrue(await view.hasCapability(admin.keyId, 'write'), 'creator should have write');
                assertTrue(await view.hasCapability(admin.keyId, 'read'), 'creator should have read');
                assertTrue(await view.hasCapability(admin.keyId, 'enroll'), 'creator should have enroll');
                assertTrue(await view.hasCapability(admin.keyId, 'nonexistent'),
                    'creator should pass hasCapability for any name');
            }
        },
        {
            name: '[CAP11] managedBy delegation: authorized delegation works',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const manager = await makeIdentity();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    manager.keyId, serializePublicKeyToBase64(manager.publicKey),
                    admin,
                );
                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                await cap.grant(
                    manager.keyId, 'admin',
                    admin,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    manager,
                );

                const view = await cap.getView();
                assertTrue(await view.hasCapability(alice.keyId, 'write'),
                    'manager with admin cap should be able to grant write');
            }
        },
        {
            name: '[CAP12] managedBy delegation: unauthorized delegation fails',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const bob = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );
                await cap.addIdentity(
                    bob.keyId, serializePublicKeyToBase64(bob.publicKey),
                    admin,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                );

                let failed = false;
                try {
                    await cap.grant(
                        bob.keyId, 'read',
                        alice,
                    );
                } catch {
                    failed = true;
                }

                const view = await cap.getView();
                assertFalse(await view.hasCapability(bob.keyId, 'read'),
                    'alice (write only) should not be able to grant read (managedBy: admin)');
            }
        },
        {
            name: '[CAP13] managedBy validation: dangling references rejected',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RCap.typeId, rCapFactory);

                const admin = await makeIdentity();

                let rejected = false;
                try {
                    const init = await RCap.create({
                        seed: 'bad-caps',
                        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                        initialCaps: {
                            'write': { managedBy: ['nonexistent-cap'] },
                        },
                    });
                    await ctx.createObject(init);
                } catch {
                    rejected = true;
                }

                assertTrue(rejected, 'creation with dangling managedBy reference should fail');
            }
        },
        {
            name: '[CAP14] Enrollment capability controls add-identity',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const enrollManager = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();

                await cap.addIdentity(
                    enrollManager.keyId, serializePublicKeyToBase64(enrollManager.publicKey),
                    admin,
                );
                await cap.grant(
                    enrollManager.keyId, 'enroll',
                    admin,
                );

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    enrollManager,
                );

                const view = await cap.getView();
                assertTrue(await view.isIdentity(alice.keyId), 'enrollManager should be able to add alice');

                await cap.addIdentity(
                    bob.keyId, serializePublicKeyToBase64(bob.publicKey),
                    admin,
                );
                await cap.grant(
                    bob.keyId, 'write',
                    admin,
                );

                const charlie = await makeIdentity();
                let addFailed = false;
                try {
                    await cap.addIdentity(
                        charlie.keyId, serializePublicKeyToBase64(charlie.publicKey),
                        bob,
                    );
                } catch {
                    addFailed = true;
                }

                const view2 = await cap.getView();
                assertFalse(await view2.isIdentity(charlie.keyId),
                    'bob (write only, no enroll) should not be able to add identities');
            }
        },
        {
            name: '[CAP15] Transitive revocation: revoking grantor invalidates downstream grants',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const manager = await makeIdentity();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    manager.keyId, serializePublicKeyToBase64(manager.publicKey),
                    admin,
                );
                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                await cap.grant(
                    manager.keyId, 'admin',
                    admin,
                );
                await cap.grant(
                    alice.keyId, 'write',
                    manager,
                );

                const view1 = await cap.getView();
                assertTrue(await view1.hasCapability(alice.keyId, 'write'),
                    'alice should have write via manager');

                await cap.revoke(
                    manager.keyId, 'admin',
                    admin,
                );

                const view2 = await cap.getView();
                assertFalse(await view2.hasCapability(manager.keyId, 'admin'),
                    'manager should no longer have admin');
                assertFalse(await view2.hasCapability(alice.keyId, 'write'),
                    'alice write should be invalidated: her grantor lost admin');
            }
        },
        {
            name: '[CAP16] Transitive revocation: concurrent grant by different authors, one revoked',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const manager1 = await makeIdentity();
                const manager2 = await makeIdentity();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    manager1.keyId, serializePublicKeyToBase64(manager1.publicKey),
                    admin,
                );
                await cap.addIdentity(
                    manager2.keyId, serializePublicKeyToBase64(manager2.publicKey),
                    admin,
                );
                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                await cap.grant(
                    manager1.keyId, 'admin',
                    admin,
                );
                await cap.grant(
                    manager2.keyId, 'admin',
                    admin,
                );

                const preGrantFrontier = await (await cap.getScopedDag()).getFrontier();

                const grant1Hash = await cap.grant(
                    alice.keyId, 'write',
                    manager1,
                    preGrantFrontier,
                );
                const grant2Hash = await cap.grant(
                    alice.keyId, 'write',
                    manager2,
                    preGrantFrontier,
                );

                const view1 = await cap.getView();
                assertTrue(await view1.hasCapability(alice.keyId, 'write'),
                    'alice should have write via two concurrent grants');

                await cap.revoke(
                    manager1.keyId, 'admin',
                    admin,
                );

                const view2 = await cap.getView();
                assertTrue(await view2.hasCapability(alice.keyId, 'write'),
                    'alice should still have write: manager2 grant is still valid');

                await cap.revoke(
                    manager2.keyId, 'admin',
                    admin,
                );

                const view3 = await cap.getView();
                assertFalse(await view3.hasCapability(alice.keyId, 'write'),
                    'alice should lose write: both grantors lost admin');
            }
        },
        {
            name: '[CAP17] Initial capability origin is discoverable via create-op caps metadata',
            invoke: async () => {
                const { cap } = await createTestEnv();

                const view = await cap.getView();
                assertTrue(await view.capabilityExists('write'), 'write should exist');

                const origins = await view.currentCapCreationVersion('write');
                assertTrue(origins.has(cap.getId()),
                    'initial capability surviving origins should include create-op hash');
            }
        },
        {
            name: '[CAP18] Candidate-based delete barriers only void concurrent capability origins',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();

                const base = await (await cap.getScopedDag()).getFrontier();

                const deleteHash = await cap.deleteCap(
                    'write',
                    admin,
                    base,
                );

                const createHashB = await cap.createCap(
                    'write', ['admin'],
                    admin,
                    version(deleteHash),
                );

                const view = await cap.getView();
                assertTrue(await view.capabilityExists('write'),
                    'write should survive because re-created origin is not concurrent with delete');

                const origins = await view.currentCapCreationVersion('write');
                assertTrue(origins.has(createHashB),
                    'surviving origins should include the non-concurrent re-create origin');
            }
        },
        {
            name: '[CAP19] Active-origin consistency with capability existence',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();

                const before = await cap.getView();
                assertTrue(await before.capabilityExists('write'), 'write should exist initially');
                assertTrue((await before.currentCapCreationVersion('write')).size > 0,
                    'surviving origins should be non-empty when capability exists');

                await cap.deleteCap(
                    'write',
                    admin,
                );

                const after = await cap.getView();
                assertFalse(await after.capabilityExists('write'), 'write should not exist after delete');
                assertTrue((await after.currentCapCreationVersion('write')).size === 0,
                    'surviving origins should be empty when capability does not exist');
            }
        },
        {
            name: '[CAP20] Per-candidate revoke: surviving grant keeps capability',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                const base = await (await cap.getScopedDag()).getFrontier();

                const earlyGrantHash = await cap.grant(
                    alice.keyId, 'write',
                    admin,
                    base,
                );

                const revokeHash = await cap.revoke(
                    alice.keyId, 'write',
                    admin,
                    base,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                    version(revokeHash),
                );

                const view = await cap.getView();
                assertTrue(await view.hasCapability(alice.keyId, 'write'),
                    'alice should still have write: re-grant after revoke is not concurrent with it');
            }
        },
        {
            name: '[CAP21] Per-candidate revoke: all grants barred removes capability',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();

                await cap.addIdentity(
                    alice.keyId, serializePublicKeyToBase64(alice.publicKey),
                    admin,
                );

                const base = await (await cap.getScopedDag()).getFrontier();

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                    base,
                );

                await cap.grant(
                    alice.keyId, 'write',
                    admin,
                    base,
                );

                await cap.revoke(
                    alice.keyId, 'write',
                    admin,
                    base,
                );

                const view = await cap.getView();
                assertFalse(await view.hasCapability(alice.keyId, 'write'),
                    'alice should not have write: revoke is concurrent with both grants');
            }
        },
    ]
};
