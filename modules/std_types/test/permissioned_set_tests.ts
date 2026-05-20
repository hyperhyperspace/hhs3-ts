import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap.js";
import { RSet, rSetFactory } from "../src/types/rset.js";
import { serializePublicKeyToBase64 } from "../src/authorship.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function createTestEnv(opts?: {
    extraCaps?: { [k: string]: { managedBy: string[] } };
    enrollCapability?: string;
    capRequirements?: { add?: string; delete?: string; refAdvance?: string[]; refAdvanceCreators?: boolean };
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    initialElements?: string[];
}) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);
    ctx.getRegistry().register(RSet.typeId, rSetFactory);

    const admin = await makeIdentity();

    const caps: { [k: string]: { managedBy: string[] } } = {
        'admin':  { managedBy: ['creator'] },
        'enroll': { managedBy: ['admin'] },
        'write':  { managedBy: ['admin'] },
        'read':   { managedBy: ['admin'] },
        ...opts?.extraCaps,
    };

    const capInit = await RCap.create({
        seed: 'perm-set-cap-test',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: caps,
        enrollCapability: opts?.enrollCapability,
    });

    const cap = (await ctx.createObject(capInit)) as RCap;

    const setInit = await RSet.create({
        seed: 'perm-set-test',
        initialElements: (opts?.initialElements ?? []) as any[],
        hashAlgorithm: 'sha256',
        supportBarrierAdd: opts?.supportBarrierAdd ?? false,
        supportBarrierDelete: opts ? (opts.supportBarrierDelete ?? false) : true,
        capabilityRef: cap.getId(),
        capRequirements: opts?.capRequirements ?? { add: 'write', delete: 'write' },
    });

    const rset = (await ctx.createObject(setInit)) as RSet;

    return { ctx, cap, rset, admin };
}

async function registerAndGrant(
    cap: RCap,
    identity: OwnIdentity,
    capName: string,
    admin: OwnIdentity,
): Promise<void> {
    await cap.addIdentity(
        identity.keyId, serializePublicKeyToBase64(identity.publicKey),
        admin,
    );
    await cap.grant(
        identity.keyId, capName, cap.getId(),
        admin,
    );
}

export const permissionedSetTests = {
    title: '[PSET] Permissioned RSet tests',
    tests: [
        {
            name: '[PSET01] Authorized add succeeds',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const alice = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);

                const capFrontier = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capFrontier, admin);

                await rset.addSigned('hello', alice);
                const view = await rset.getView();
                assertTrue(await view.has('hello'), 'authorized add should succeed');
            }
        },
        {
            name: '[PSET02] Unauthorized add fails validation',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const bob = await makeIdentity();
                await cap.addIdentity(
                    bob.keyId, serializePublicKeyToBase64(bob.publicKey),
                    admin,
                );

                const capFrontier = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capFrontier, admin);

                let threw = false;
                try {
                    await rset.addSigned('data', bob);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'unauthorized add should throw');
            }
        },
        {
            name: '[PSET03] Authorized delete succeeds',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const alice = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);

                const capFrontier = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capFrontier, admin);

                await rset.addSigned('item', alice);
                await rset.deleteSigned('item', alice);

                const view = await rset.getView();
                assertFalse(await view.has('item'), 'item should be deleted');
            }
        },
        {
            name: '[PSET04] Unsigned add throws on permissioned set',
            invoke: async () => {
                const { rset } = await createTestEnv();

                let threw = false;
                try {
                    await rset.add('nope');
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'unsigned add should throw on permissioned set');
            }
        },
        {
            name: '[PSET05] Unsigned delete throws on permissioned set',
            invoke: async () => {
                const { rset } = await createTestEnv();

                let threw = false;
                try {
                    await rset.delete('nope');
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'unsigned delete should throw on permissioned set');
            }
        },
        {
            name: '[PSET06] getReferences returns capabilityRef',
            invoke: async () => {
                const { cap, rset } = await createTestEnv();

                const view = await rset.getView();
                const refs = await view.getReferences();
                assertTrue(refs.length === 1, 'should have exactly one reference');
                assertTrue(refs[0] === cap.getId(), 'reference should be the capability object');
            }
        },
        {
            name: '[PSET07] Creator can add without explicit grant',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const capFrontier = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capFrontier, admin);

                await rset.addSigned('by-creator', admin);
                const view = await rset.getView();
                assertTrue(await view.has('by-creator'), 'creator should be able to add');
            }
        },
        {
            name: '[PSET08] Ref-advance updates RCap version for authorization',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const alice = await makeIdentity();

                // Before granting Alice, ref-advance so the set sees current RCap state
                const capFrontier0 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capFrontier0, admin);

                // Alice can't add yet (not registered, not granted)
                let threw = false;
                try {
                    await rset.addSigned('early', alice);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'alice should not be able to add before grant');

                // Grant Alice write cap
                await registerAndGrant(cap, alice, 'write', admin);

                // Without ref-advance, Alice still can't add (set doesn't see new grants)
                threw = false;
                try {
                    await rset.addSigned('still-early', alice);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'alice should not be able to add before ref-advance');

                // ref-advance to incorporate the grant
                const capFrontier1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capFrontier1, admin);

                // Now Alice can add
                await rset.addSigned('now-ok', alice);
                const view = await rset.getView();
                assertTrue(await view.has('now-ok'), 'alice should be able to add after ref-advance');
            }
        },
        {
            name: '[PSET09] Peeling: valid add remains visible after void concurrent add',
            invoke: async () => {
                // Alice (authorized) adds X. Bob (authorized) adds X concurrently.
                // Bob's write cap is revoked, ref-advanced.
                // X should still be present via Alice's add (Bob's void entry peeled through).
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const alice = await makeIdentity();
                const bob = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                // Alice adds X at current frontier
                const setDag = await rset.getScopedDag();
                const frontier1 = await setDag.getFrontier();
                await rset.addSigned('X', alice, frontier1);

                // Bob adds X concurrently (from the same frontier)
                await rset.addSigned('X', bob, frontier1);

                // Before revocation, X is present
                const viewBefore = await rset.getView();
                assertTrue(await viewBefore.has('X'), 'X should be present before revocation');

                // Revoke Bob's write
                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                // X should still be present via Alice's authorized add
                const viewAfter = await rset.getView();
                assertTrue(await viewAfter.has('X'), 'X should remain via Alice\'s authorized add (Bob\'s peeled)');
            }
        },
        {
            name: '[PSET10] Sequential revocation does not void past authorized add',
            invoke: async () => {
                // Bob adds Y (authorized). Then Bob's write is revoked and
                // ref-advanced sequentially. Since the ref-advance is not
                // concurrent with Bob's add, Y remains present.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                await rset.addSigned('Y', bob);

                const viewBefore = await rset.getView();
                assertTrue(await viewBefore.has('Y'), 'Y should be present before revocation');

                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const viewAfter = await rset.getView();
                assertTrue(await viewAfter.has('Y'), 'Y should survive sequential revocation');
            }
        },
        {
            name: '[PSET11] Authorized delete after void add',
            invoke: async () => {
                // Alice (authorized) adds X. Bob (authorized) also adds X.
                // Revoke Bob. Carol deletes X. X should be gone because Carol's
                // authorized delete trumps Alice's add.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const carol = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);
                await registerAndGrant(cap, bob, 'write', admin);
                await registerAndGrant(cap, carol, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = await rset.getScopedDag();
                const frontier1 = await setDag.getFrontier();
                await rset.addSigned('X', alice, frontier1);
                await rset.addSigned('X', bob, frontier1);

                // Carol deletes X (authorized)
                await rset.deleteSigned('X', carol);

                const view = await rset.getView();
                assertFalse(await view.has('X'), 'X should be deleted by authorized Carol');
            }
        },
        {
            name: '[PSET12] Sequential transitive revocation does not void past authorized add',
            invoke: async () => {
                // Admin grants Manager the 'admin' cap.
                // Manager grants Alice 'write'.
                // Alice adds Z (authorized via delegation chain).
                // Admin revokes Manager's 'admin' cap, ref-advances sequentially.
                // Z should survive because the ref-advance is not concurrent
                // with Alice's add.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const manager = await makeIdentity();
                const alice = await makeIdentity();

                await registerAndGrant(cap, manager, 'admin', admin);
                await registerAndGrant(cap, alice, 'enroll', admin);

                // Manager grants Alice 'write' (Manager has 'admin' which managedBy 'write')
                await cap.grant(
                    alice.keyId, 'write', cap.getId(),
                    manager,
                );

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                // Alice adds Z
                await rset.addSigned('Z', alice);

                const viewBefore = await rset.getView();
                assertTrue(await viewBefore.has('Z'), 'Z should be present with valid delegation chain');

                // Revoke Manager's 'admin' cap -- breaks the chain
                await cap.revoke(manager.keyId, 'admin', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const viewAfter = await rset.getView();
                assertTrue(await viewAfter.has('Z'), 'Z should survive sequential transitive revocation');
            }
        },
        {
            name: '[PSET13] Initial elements are always authorized',
            invoke: async () => {
                // Create a permissioned set with initial elements -- these are
                // from the creation payload and are never authored, so they
                // must be treated as authorized regardless.
                const { rset } = await createTestEnv({ initialElements: ['genesis'] });

                const view = await rset.getView();
                assertTrue(await view.has('genesis'), 'initial elements should be authorized');
            }
        },
        {
            name: '[PSET14] Ref-advance by creator succeeds by default',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const capFrontier = await (await cap.getScopedDag()).getFrontier();
                // Creator should be able to ref-advance (refAdvanceCreators defaults to true)
                await rset.refAdvance(capFrontier, admin);
                // No throw = success
                assertTrue(true, 'creator ref-advance should succeed');
            }
        },
        {
            name: '[PSET15] Ref-advance by non-creator with refAdvance cap',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv({
                    capRequirements: {
                        add: 'write',
                        delete: 'write',
                        refAdvance: ['admin'],
                    },
                });

                const manager = await makeIdentity();
                await registerAndGrant(cap, manager, 'admin', admin);

                // First ref-advance to see the grant (by creator)
                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                // Manager should be able to ref-advance (holds 'admin' cap)
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, manager);
                assertTrue(true, 'manager ref-advance should succeed');
            }
        },
        {
            name: '[PSET16] extractForeignDeps returns cap ref for permissioned set',
            invoke: async () => {
                const { cap, rset } = await createTestEnv();

                const deps = rset.extractForeignDeps({action: 'add', element: 'x'}, version());
                assertTrue(deps !== undefined, 'should return foreign deps');
                assertTrue(deps!.length === 1, 'should have one dep');
                assertTrue(deps![0].dagId === cap.getId(), 'dep should point to the RCap');
            }
        },
        {
            name: '[PSET17] Concurrent revocation voids unauthorized add',
            invoke: async () => {
                // Bob has write. Both the add and the revocation + ref-advance
                // fork from the same RSet frontier, making them concurrent.
                // The ref-advance barrier should void Bob's add.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = await rset.getScopedDag();
                const forkPoint = await setDag.getFrontier();

                // Branch A: Bob adds Y
                await rset.addSigned('Y', bob, forkPoint);

                // Branch B (concurrent): revoke Bob, ref-advance
                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin, forkPoint);

                const view = await rset.getView();
                assertFalse(await view.has('Y'), 'Y should be void: concurrent ref-advance revoked Bob');
            }
        },
        {
            name: '[PSET18] Concurrent transitive revocation voids add',
            invoke: async () => {
                // Manager delegates write to Alice. Alice adds Z concurrently
                // with a ref-advance that carries Manager's admin revocation.
                // Z should be void.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const manager = await makeIdentity();
                const alice = await makeIdentity();

                await registerAndGrant(cap, manager, 'admin', admin);
                await registerAndGrant(cap, alice, 'enroll', admin);

                await cap.grant(
                    alice.keyId, 'write', cap.getId(),
                    manager,
                );

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = await rset.getScopedDag();
                const forkPoint = await setDag.getFrontier();

                // Branch A: Alice adds Z
                await rset.addSigned('Z', alice, forkPoint);

                // Branch B (concurrent): revoke Manager's admin, ref-advance
                await cap.revoke(manager.keyId, 'admin', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin, forkPoint);

                const view = await rset.getView();
                assertFalse(await view.has('Z'), 'Z should be void: concurrent transitive revocation');
            }
        },
        {
            name: '[PSET19] Concurrent non-barrier add survives non-barrier delete',
            invoke: async () => {
                // Alice adds X and Bob deletes X concurrently (both from the
                // same fork point). Barriers are disabled. A non-barrier delete
                // should only deactivate adds in its causal past, so the
                // concurrent add should survive.
                const { cap, rset, admin } = await createTestEnv({
                    capRequirements: { add: 'write', delete: 'write' },
                    supportBarrierAdd: false,
                    supportBarrierDelete: false,
                });

                const alice = await makeIdentity();
                const bob = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                // Alice adds X sequentially (so it exists for Bob to delete)
                await rset.addSigned('X', alice);

                const setDag = await rset.getScopedDag();
                const forkPoint = await setDag.getFrontier();

                // Branch A: Alice re-adds X
                await rset.addSigned('X', alice, forkPoint);

                // Branch B (concurrent): Bob deletes X (non-barrier)
                await rset.deleteSigned('X', bob, forkPoint);

                const view = await rset.getView();
                assertTrue(await view.has('X'), 'X should survive: concurrent non-barrier delete cannot void concurrent add');
            }
        },
        {
            name: '[PSET20] Sequential ref-advance with concurrent RCap branch voids add',
            invoke: async () => {
                // Bob is granted write on the main RCap branch. RSet ref-advances to V1,
                // Bob adds Y sequentially. A concurrent RCap branch (forking before the
                // grant) revokes Bob with a barrier; merged into V2. Sequential ref-advance
                // to V2 should void Y via RCap.getView(V1, V2) compositional revision.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await cap.addIdentity(
                    bob.keyId, serializePublicKeyToBase64(bob.publicKey),
                    admin,
                );

                const capFork = await (await cap.getScopedDag()).getFrontier();

                await cap.grant(bob.keyId, 'write', cap.getId(), admin, capFork);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                await rset.addSigned('Y', bob);

                const viewBefore = await rset.getView();
                assertTrue(await viewBefore.has('Y'), 'Y should be present before concurrent RCap revoke merges');

                await cap.revoke(bob.keyId, 'write', admin, capFork);

                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                const viewAfter = await rset.getView();
                assertFalse(await viewAfter.has('Y'), 'Y should be void: concurrent RCap barrier revoke revises authorization at V1');
            }
        },
    ]
};
