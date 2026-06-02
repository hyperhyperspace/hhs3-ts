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
            name: '[CAP15] Transitive revocation: revoking grantor does not invalidate downstream grants made earlier (use-before-revoke)',
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
                assertTrue(await view2.hasCapability(alice.keyId, 'write'),
                    'alice should still have write: her grant predates the revoke (use-before-revoke)');
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
                assertTrue(await view3.hasCapability(alice.keyId, 'write'),
                    'alice should still have write: both grants predate their revokes (use-before-revoke)');
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
        {
            name: '[CAP22] Masking: an earlier valid grant survives under a later authority-voided grant (see-through cover)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const manager1 = await makeIdentity();
                const manager2 = await makeIdentity();
                const alice = await makeIdentity();
                const carol = await makeIdentity();

                await cap.addIdentity(manager1.keyId, serializePublicKeyToBase64(manager1.publicKey), admin);
                await cap.addIdentity(manager2.keyId, serializePublicKeyToBase64(manager2.publicKey), admin);
                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                await cap.grant(manager1.keyId, 'admin', admin);
                await cap.grant(manager2.keyId, 'admin', admin);

                // Earlier, durable grant of write to alice by manager1 -- stays valid throughout.
                await cap.grant(alice.keyId, 'write', manager1);

                // Fork: manager2 re-grants write to alice (causally dominates manager1's grant)
                // concurrently with admin revoking manager2's admin. manager2's grant is
                // therefore authority-voided when observed from the merge. A naive cover would
                // stop at this dominating-but-invalid grant and wrongly report no write.
                const fork = await (await cap.getScopedDag()).getFrontier();
                const g2 = await cap.grant(alice.keyId, 'write', manager2, fork);
                const rv = await cap.revoke(manager2.keyId, 'admin', admin, fork);
                await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey),
                    admin, version(g2, rv),
                );

                const view = await cap.getView();
                assertTrue(await view.hasCapability(alice.keyId, 'write'),
                    'alice keeps write: the see-through cover skips manager2\'s voided grant and reaches manager1\'s valid grant');
            }
        },
        {
            name: '[CAP23] Authority-voided revoke is seen through: capability survives',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const manager = await makeIdentity();
                const alice = await makeIdentity();
                const carol = await makeIdentity();

                await cap.addIdentity(manager.keyId, serializePublicKeyToBase64(manager.publicKey), admin);
                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                await cap.grant(manager.keyId, 'admin', admin);

                // alice's write is granted by the creator -> unconditionally valid.
                await cap.grant(alice.keyId, 'write', admin);

                // Fork: manager revokes alice's write concurrently with admin revoking
                // manager's admin. manager's revoke is authority-voided when observed from
                // the merge and must be seen through, leaving alice's earlier write intact.
                const fork = await (await cap.getScopedDag()).getFrontier();
                const rAlice = await cap.revoke(alice.keyId, 'write', manager, fork);
                const rManager = await cap.revoke(manager.keyId, 'admin', admin, fork);
                await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey),
                    admin, version(rAlice, rManager),
                );

                const view = await cap.getView();
                assertTrue(await view.hasCapability(alice.keyId, 'write'),
                    'alice keeps write: manager\'s revoke is authority-voided by the concurrent admin revoke and seen through');
            }
        },
        {
            name: '[CAP24] Merged concurrent grant and revoke: grant-anchored B1 voids at the merge',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const carol = await makeIdentity();

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                const base = await (await cap.getScopedDag()).getFrontier();
                const g = await cap.grant(alice.keyId, 'write', admin, base);
                const r = await cap.revoke(alice.keyId, 'write', admin, base);

                // Merge both concurrent ops below a single observation point (at == from).
                await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey),
                    admin, version(g, r),
                );

                const view = await cap.getView();
                assertFalse(await view.hasCapability(alice.keyId, 'write'),
                    'write is voided at the merge: the revoke is concurrent with the grant (grant-anchored B1)');
            }
        },
        {
            name: '[CAP25] Three-level delegation: concurrent grandparent revoke does not void the leaf grant',
            invoke: async () => {
                const { cap, admin } = await createTestEnv({ extraCaps: { post: { managedBy: ['write'] } } });
                const manager = await makeIdentity();
                const sub = await makeIdentity();
                const alice = await makeIdentity();
                const carol = await makeIdentity();

                await cap.addIdentity(manager.keyId, serializePublicKeyToBase64(manager.publicKey), admin);
                await cap.addIdentity(sub.keyId, serializePublicKeyToBase64(sub.publicKey), admin);
                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                // Chain: admin -> manager (admin) -> sub (write) -> alice (post).
                await cap.grant(manager.keyId, 'admin', admin);
                await cap.grant(sub.keyId, 'write', manager);

                // Fork: sub grants post to alice (the leaf) concurrently with admin revoking
                // manager's admin (the grandparent's authority). Each grant in the chain
                // predates that revoke, so use-before-revoke keeps the whole chain valid.
                const fork = await (await cap.getScopedDag()).getFrontier();
                const gPost = await cap.grant(alice.keyId, 'post', sub, fork);
                const rManager = await cap.revoke(manager.keyId, 'admin', admin, fork);
                await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey),
                    admin, version(gPost, rManager),
                );

                const view = await cap.getView();
                assertTrue(await view.hasCapability(alice.keyId, 'post'),
                    'alice keeps post: every grant in the chain predates the concurrent grandparent revoke (use-before-revoke at depth)');
            }
        },
        {
            name: '[CAP26] Use-anchored barrier: a concurrent revoke voids a capability when observed from a wider `from` (from != at)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const carol = await makeIdentity();

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);

                const g1 = await cap.grant(alice.keyId, 'admin', admin);
                // alice USES admin here to grant bob write:
                const g2 = await cap.grant(bob.keyId, 'write', alice, version(g1));
                // revoke alice's admin, concurrent with g2 (both children of g1, so the
                // revoke is a DESCENDANT of g1 -- B1 anchored at g1 cannot see it):
                const old = await cap.revoke(alice.keyId, 'admin', admin, version(g1));
                const m = await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey),
                    admin, version(g2, old),
                );
                const end = version(m);

                // hasCapability is a pure function of (at, from): at the use point itself
                // (from == at == g2) the concurrent revoke is not yet observed, so alice
                // holds admin.
                const atUse = await cap.getView(version(g2), version(g2));
                assertTrue(await atUse.hasCapability(alice.keyId, 'admin'),
                    'alice holds admin at the use point (concurrent revoke not yet observed)');

                // Same `at`, wider `from`: observed from the merge, the revoke is visible and
                // concurrent with the use g2, so it voids. This is a top-level from != at
                // query that ONLY the use-anchored barrier (B2) catches -- the revoke is a
                // descendant of g1, so the grant-anchored B1 does not.
                const fromEnd = await cap.getView(version(g2), end);
                assertFalse(await fromEnd.hasCapability(alice.keyId, 'admin'),
                    'alice admin is voided observed from the merge: revoke is concurrent with the use (B2, from != at)');

                // Transitive consequence: bob's write, conferred via alice's now-voided admin,
                // also falls away when observed from the merge.
                const bobFromEnd = await cap.getView(end, end);
                assertFalse(await bobFromEnd.hasCapability(bob.keyId, 'write'),
                    'bob loses write observed from the merge: alice admin concurrently voided at the use');
            }
        },
        {
            name: '[CAP27] Multi-hash use point: a revoke concurrent with every element voids (collapse-X, B2)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const d1 = await makeIdentity();
                const d2 = await makeIdentity();

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                // alice is granted write by the creator (unconditionally valid).
                const g = await cap.grant(alice.keyId, 'write', admin);

                // Three concurrent children of g: two filler add-identity ops form the
                // multi-hash use point, and a revoke of alice:write sits on a third branch
                // (so the revoke is concurrent with BOTH a1 and a2).
                const a1 = await cap.addIdentity(d1.keyId, serializePublicKeyToBase64(d1.publicKey), admin, version(g));
                const a2 = await cap.addIdentity(d2.keyId, serializePublicKeyToBase64(d2.publicKey), admin, version(g));
                const u = await cap.revoke(alice.keyId, 'write', admin, version(g));

                // Pinned at the multi-hash use point itself: the revoke is on a concurrent
                // branch, not yet observed, so the collapsed use point still holds write.
                const atPin = await cap.getView(version(a1, a2), version(a1, a2));
                assertTrue(await atPin.hasCapability(alice.keyId, 'write'),
                    'write holds at the multi-hash use point: the concurrent revoke is not yet observed');

                // Same use point, observed from a wider `from` that includes the revoke. The
                // revoke is concurrent with the collapsed node X (concurrent with every
                // element of `at`), so the use-anchored barrier (B2) voids it.
                const fromWide = await cap.getView(version(a1, a2), version(a1, a2, u));
                assertFalse(await fromWide.hasCapability(alice.keyId, 'write'),
                    'write is voided: the revoke is concurrent with every element of the use point (collapse-X, B2)');
            }
        },
        {
            name: '[CAP28] Multi-hash use point: revoke after one branch, grant concurrent with it -> grant-anchored B1 voids',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const d1 = await makeIdentity();

                const idA = await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                const b0 = version(idA);

                // Branch 2 carries the grant; branch 1 is a filler add-identity. The two are
                // concurrent and together form the multi-hash use point.
                const a2 = await cap.grant(alice.keyId, 'write', admin, b0);
                const a1 = await cap.addIdentity(d1.keyId, serializePublicKeyToBase64(d1.publicKey), admin, b0);

                // The revoke sits after a1, so it is NOT concurrent with the whole use point
                // (B2 stays silent), but it IS concurrent with the grant a2.
                const u = await cap.revoke(alice.keyId, 'write', admin, version(a1));

                const view = await cap.getView(version(a1, a2), version(a2, u));
                assertFalse(await view.hasCapability(alice.keyId, 'write'),
                    'write is voided: B2 defers (revoke is after a1) but the grant a2 is concurrent with the revoke (grant-anchored B1)');
            }
        },
        {
            name: '[CAP29] Multi-hash use point: revoke after one branch but later than the grant -> survives (use-before-revoke)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const d1 = await makeIdentity();
                const d2 = await makeIdentity();

                const idA = await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);

                // The grant is a common ancestor of the whole fork.
                const g = await cap.grant(alice.keyId, 'write', admin, version(idA));

                // Fork into two concurrent filler branches -> the multi-hash use point.
                const a1 = await cap.addIdentity(d1.keyId, serializePublicKeyToBase64(d1.publicKey), admin, version(g));
                const a2 = await cap.addIdentity(d2.keyId, serializePublicKeyToBase64(d2.publicKey), admin, version(g));

                // Revoke after a1 -- later than the grant on that branch, absent on the other.
                const u = await cap.revoke(alice.keyId, 'write', admin, version(a1));

                const view = await cap.getView(version(a1, a2), version(a2, u));
                assertTrue(await view.hasCapability(alice.keyId, 'write'),
                    'write survives: same fork as CAP28 but the grant precedes the revoke, so the grant-anchored B1 sees no concurrent revoke (use-before-revoke)');
            }
        },
        {
            name: '[CAP30] Multi-hash use point: revoke after both branches -> survives at the use point (use-before-revoke)',
            invoke: async () => {
                const { cap, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const d1 = await makeIdentity();
                const d2 = await makeIdentity();

                const idA = await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                const g = await cap.grant(alice.keyId, 'write', admin, version(idA));

                const a1 = await cap.addIdentity(d1.keyId, serializePublicKeyToBase64(d1.publicKey), admin, version(g));
                const a2 = await cap.addIdentity(d2.keyId, serializePublicKeyToBase64(d2.publicKey), admin, version(g));

                // The revoke merges both branches: strictly after the whole use point.
                const u = await cap.revoke(alice.keyId, 'write', admin, version(a1, a2));

                // Pinned at the use point, observed from the revoke: the revoke is strictly
                // later than the collapsed node, so it is concurrent with neither the use
                // point (B2) nor the grant (B1).
                const atUse = await cap.getView(version(a1, a2), version(u));
                assertTrue(await atUse.hasCapability(alice.keyId, 'write'),
                    'write holds at the use point: the revoke is strictly after the whole frontier (use-before-revoke)');

                // Contrast: once the revoke is in the use's own past, it does revoke.
                const afterRevoke = await cap.getView(version(u), version(u));
                assertFalse(await afterRevoke.hasCapability(alice.keyId, 'write'),
                    'write is gone once the revoke is in the use\'s own past (sequential revoke)');
            }
        },
    ]
};
