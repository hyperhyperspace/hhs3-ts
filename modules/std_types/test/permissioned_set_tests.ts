import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { formatValidationFailure, version, createRefAdvancePayload, extractRefVersion, refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap/rcap.js";
import { RSet, rSetFactory } from "../src/types/rset/rset.js";
import { serializePublicKeyToBase64, signPayload } from "../src/authorship.js";

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
        identity.keyId, capName,
        admin,
    );
}

// A set whose ref-advances need 'steward' (managed by admin), with the
// attacker, a non-creator who holds it, and Bob, an identity without write.
// c0 is the RCap frontier after that setup.
async function stewardEnv() {
    const env = await createTestEnv({
        extraCaps: { steward: { managedBy: ['admin'] } },
        capRequirements: { add: 'write', delete: 'write', refAdvance: ['steward'] },
    });
    const [attacker, bob] = [await makeIdentity(), await makeIdentity()];
    await registerAndGrant(env.cap, attacker, 'steward', env.admin);
    await env.cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), env.admin);
    const c0 = await (await env.cap.getScopedDag()).getFrontier();
    return { ...env, attacker, bob, c0 };
}

// A ref-advance of `rset`'s RCap to `refVersion`, signed by `author` at `at`.
function signedRefAdvance(cap: RCap, refVersion: Version, author: OwnIdentity, at: Version) {
    return signPayload(createRefAdvancePayload(cap.getId(), refVersion) as unknown as json.LiteralMap, author, at);
}

async function addIdentityAt(cap: RCap, identity: OwnIdentity, admin: OwnIdentity, at: Version) {
    return cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), admin, at);
}

export const permissionedSetTests = {
    title: '[PSET] Permissioned RSet tests',
    tests: [
        {
            name: '[PSET00] loadRCap returns registry object via getObject',
            invoke: async () => {
                const { ctx, cap, rset } = await createTestEnv();
                const loaded = await rset.loadRCap();
                const fromCtx = await ctx.getObject(cap.getId());
                assertTrue(loaded !== undefined, 'loadRCap should return the RCap');
                assertTrue(loaded === fromCtx, 'loadRCap should return the same instance as getObject');
            },
        },
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
                    alice.keyId, 'write',
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
                assertTrue(deps![0].objectId === cap.getId(), 'dep should point to the RCap');
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
            name: '[PSET18] Sequential transitive revocation does not void an earlier add (use-before-revoke)',
            invoke: async () => {
                // Manager delegates write to Alice. Alice adds Z concurrently
                // with a ref-advance that carries Manager's admin revocation.
                // The revoke is causally AFTER Alice's write grant in the RCap DAG,
                // so Alice's write is valid at use-time (use-before-revoke) and Z
                // survives: revoking the grantor's admin does not retroactively
                // void grants the grantor already made.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const manager = await makeIdentity();
                const alice = await makeIdentity();

                await registerAndGrant(cap, manager, 'admin', admin);
                await registerAndGrant(cap, alice, 'enroll', admin);

                await cap.grant(
                    alice.keyId, 'write',
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
                assertTrue(await view.has('Z'), 'Z should survive: Alice\'s write predates the admin revoke (use-before-revoke)');
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

                await cap.grant(bob.keyId, 'write', admin, capFork);

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
        {
            name: '[PSET21] Admission-vs-view: validated add voided by concurrent RSet barrier ref-advance',
            invoke: async () => {
                // Bob is authorized and adds Y at forkPoint (passes selfValidate).
                // A concurrent branch revokes Bob and ref-advances from the same
                // forkPoint. The add passed validation (causal check) but the view
                // voids it via the concurrent barrier.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = await rset.getScopedDag();
                const forkPoint = await setDag.getFrontier();

                // Branch A: Bob adds Y (succeeds — validation passes at forkPoint)
                const addHash = await rset.addSigned('Y', bob, forkPoint);

                // Branch B (concurrent): revoke Bob, ref-advance
                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin, forkPoint);

                // Re-check: validatePayload at the add's original position still passes
                // (causal-only view sees capV1, where Bob is authorized)
                const addEntry = await setDag.loadEntry(addHash);
                assertTrue(addEntry !== undefined, 'add entry should exist in the DAG');
                const valid = (await rset.validatePayload(addEntry!.payload, forkPoint)).valid;
                assertTrue(valid, 'validatePayload should pass at admission position (causal-only)');

                // But the frontier view voids it via the concurrent barrier ref-advance
                const view = await rset.getView();
                assertFalse(await view.has('Y'), 'Y should be void at frontier despite passing validation');
            }
        },
        {
            name: '[PSET22] Admission-vs-view: validated add voided by RCap-internal barriers (compositional revision)',
            invoke: async () => {
                // Bob is granted write on the main RCap branch. RSet ref-advances
                // to V1. Bob adds Y sequentially (passes validation). A concurrent
                // RCap branch (forking before the grant) revokes Bob with a barrier.
                // Sequential ref-advance to V2 (merged) voids Y via compositional
                // revision: rcap.getView(V1, V2) exposes the concurrent barrier
                // revoke inside the RCap.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await cap.addIdentity(
                    bob.keyId, serializePublicKeyToBase64(bob.publicKey),
                    admin,
                );

                const capFork = await (await cap.getScopedDag()).getFrontier();

                await cap.grant(bob.keyId, 'write', admin, capFork);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                // Bob adds Y sequentially (passes validation — causal RCap version
                // is V1, where Bob is authorized)
                const setDag = await rset.getScopedDag();
                const addAt = await setDag.getFrontier();
                const addHash = await rset.addSigned('Y', bob);

                const viewBefore = await rset.getView();
                assertTrue(await viewBefore.has('Y'), 'Y should be present before concurrent RCap revoke merges');

                // Concurrent RCap branch: revoke Bob (barrier) from capFork
                await cap.revoke(bob.keyId, 'write', admin, capFork);

                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                // Re-check: validatePayload at the add's original position still passes
                const addEntry = await setDag.loadEntry(addHash);
                assertTrue(addEntry !== undefined, 'add entry should exist in the DAG');
                const valid = (await rset.validatePayload(addEntry!.payload, addAt)).valid;
                assertTrue(valid, 'validatePayload should pass at admission position (causal RCap V1 authorizes Bob)');

                // But the frontier view voids it via compositional revision
                const viewAfter = await rset.getView();
                assertFalse(await viewAfter.has('Y'), 'Y should be void: compositional rcap.getView(V1, V2) exposes concurrent barrier revoke');
            }
        },
        {
            name: '[PSET23] Explicit getView(at, from) revision changes element visibility',
            invoke: async () => {
                // Demonstrates that the `from` parameter directly controls
                // barrier incorporation: the same `at` yields different element
                // visibility depending on `from`.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = await rset.getScopedDag();
                const forkPoint = await setDag.getFrontier();

                // Branch A: Bob adds Y
                await rset.addSigned('Y', bob, forkPoint);

                // Capture the version that includes only branch A (add)
                const branchAVersion = await setDag.getFrontier();

                // Branch B (concurrent): revoke Bob, ref-advance
                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin, forkPoint);

                const fullFrontier = await setDag.getFrontier();

                // Unrevised view at branchA: Y is present (Bob authorized in causal history)
                const unrevisedView = await rset.getView(branchAVersion, branchAVersion);
                assertTrue(await unrevisedView.has('Y'), 'unrevised view at branchA should show Y');

                // Revised view: same `at` but `from` includes the barrier branch
                const revisedView = await rset.getView(branchAVersion, fullFrontier);
                assertFalse(await revisedView.has('Y'), 'revised view (from=fullFrontier) should void Y');

                // Default frontier view should also void Y
                const defaultView = await rset.getView();
                assertFalse(await defaultView.has('Y'), 'default frontier view should void Y');
            }
        },
        {
            name: '[PSET24] resolveRefVersion returns widened version after concurrent barrier ref-advance',
            invoke: async () => {
                // After a concurrent barrier ref-advance, resolveRefVersion
                // should return a widened version that includes the barrier's
                // RCap hashes — and the unrevised version should not.
                const { cap, rset, admin } = await createTestEnv({ capRequirements: { add: 'write', delete: 'write' } });

                const bob = await makeIdentity();
                await registerAndGrant(cap, bob, 'write', admin);

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                const setDag = await rset.getScopedDag();
                const forkPoint = await setDag.getFrontier();
                const capRef = cap.getId();

                // Branch A: Bob adds Y
                await rset.addSigned('Y', bob, forkPoint);
                const branchAVersion = await setDag.getFrontier();

                // Branch B (concurrent): revoke Bob, ref-advance to capV2
                await cap.revoke(bob.keyId, 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin, forkPoint);

                const fullFrontier = await setDag.getFrontier();

                // Unrevised: resolveRefVersion should return capV1 hashes only
                const unrevisedView = await rset.getView(branchAVersion, branchAVersion);
                const unrevisedRef = await unrevisedView.resolveRefVersion(capRef);
                for (const h of capV1) {
                    assertTrue(unrevisedRef.has(h), 'unrevised ref version should contain capV1 hash ' + h);
                }
                for (const h of capV2) {
                    if (!capV1.has(h)) {
                        assertFalse(unrevisedRef.has(h), 'unrevised ref version should NOT contain capV2-only hash ' + h);
                    }
                }

                // Revised: resolveRefVersion should be widened to include capV2 hashes
                const revisedView = await rset.getView(branchAVersion, fullFrontier);
                const revisedRef = await revisedView.resolveRefVersion(capRef);
                for (const h of capV2) {
                    assertTrue(revisedRef.has(h), 'revised ref version should contain capV2 hash ' + h);
                }

                // Canonical: the revised cut drops heads the new version dominates, so it
                // need not have more elements. It is strictly above the unrevised cut.
                const capDag = await cap.getCausalDag();
                assertTrue(await refVersionAtOrAbove(capDag, revisedRef, unrevisedRef),
                    'revised ref version is at or above the unrevised one');
                assertFalse(await refVersionAtOrAbove(capDag, unrevisedRef, revisedRef),
                    'revised ref version is strictly above the unrevised one');
            }
        },
        {
            name: '[PSET25] A concurrent RCap revoke pulled in by a later sequential ref-advance voids an earlier add (use-anchored barrier)',
            invoke: async () => {
                // The entry's rcapAt is `{u}` (the ref-advance it sees), strictly above
                // alice's write grant. A revoke concurrent with `{u}` is folded into rcapFrom
                // by a later, sequential-in-the-RSet ref-advance, but not into the entry's
                // rcapAt. Kill 2 at rcapAt, read from that rcapFrom, voids it. This guards the
                // geometric rcapFrom: the later ref-advance moves it, whatever the verdicts of
                // the ref-advances in the entry's past.
                const { cap, rset, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const dave = await makeIdentity();

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                const gAlice = await cap.grant(alice.keyId, 'write', admin);

                // `u`: an unrelated RCap op layered on top of alice's grant. The entry's
                // ref-advance points here, so the entry is checked at `{u}` (above the grant).
                const u = await cap.addIdentity(
                    dave.keyId, serializePublicKeyToBase64(dave.publicKey), admin, version(gAlice),
                );

                await rset.refAdvance(version(u), admin);
                await rset.addSigned('E', alice);

                // Concurrent with `u` (both children of the grant): revoke alice's write.
                await cap.revoke(alice.keyId, 'write', admin, version(gAlice));
                const capFull = await (await cap.getScopedDag()).getFrontier();   // {u, r}

                // A later, sequential-in-the-RSet-DAG ref-advance pulls the revoke into the
                // observation frontier (rcapFrom) without folding it into the entry's rcapAt.
                await rset.refAdvance(capFull, admin);

                const view = await rset.getView();
                assertFalse(await view.has('E'),
                    'E is voided: the concurrent revoke, observed via the later ref-advance, is concurrent with the entry\'s rcapAt (Kill 2)');
            }
        },
        {
            name: '[PSET26] Backward ref-advance rejected',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const capV1 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV1, admin);

                await registerAndGrant(cap, await makeIdentity(), 'write', admin);
                const capV2 = await (await cap.getScopedDag()).getFrontier();
                await rset.refAdvance(capV2, admin);

                let threw = false;
                try {
                    await rset.refAdvance(capV1, admin);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'backward ref-advance should be rejected');
            }
        },
        {
            name: '[PSET27] A signed add replayed at a later position is rejected',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();

                const alice = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);
                await rset.refAdvance(await (await cap.getScopedDag()).getFrontier(), admin);

                const hAdd = await rset.addSigned('hello', alice);
                await rset.deleteSigned('hello', alice);

                const dag = await rset.getScopedDag();
                const entry = (await dag.loadEntry(hAdd))!;
                const original = version(...json.fromSet(entry.header.prevEntryHashes));
                assertTrue((await rset.validatePayload(entry.payload, original)).valid,
                    'the stored add validates at its own position');

                const replayed = await rset.validatePayload(entry.payload, await dag.getFrontier());
                assertFalse(replayed.valid, 'the stored add replayed after the delete should not validate');
                assertTrue(!replayed.valid && formatValidationFailure(replayed.why).includes('signature'),
                    'the replayed add should fail signature verification');
            }
        },
        {
            name: '[PSET28] A signed inner op re-wrapped for a sibling nested element is rejected',
            invoke: async () => {
                const { ctx, cap, admin } = await createTestEnv();

                const alice = await makeIdentity();
                await registerAndGrant(cap, alice, 'write', admin);
                const capFrontier = await (await cap.getScopedDag()).getFrontier();

                const outer = (await ctx.createObject(await RSet.create({
                    seed: 'pset28-outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                }))) as RSet;

                const addInner = async (seed: string) => {
                    const hash = await outer.add(await RSet.create({
                        seed,
                        initialElements: [],
                        hashAlgorithm: 'sha256',
                        capabilityRef: cap.getId(),
                        capRequirements: { add: 'write', delete: 'write' },
                    }));
                    const inner = await (await outer.getView()).loadRObjectByHash(hash) as RSet;
                    await inner.refAdvance(capFrontier, admin);
                    return { hash, inner };
                };
                const a = await addInner('pset28-inner-a');
                const b = await addInner('pset28-inner-b');

                // at the outer frontier both elements have advanced to alice's grant
                const outerDag = await outer.getScopedDag();
                const hAdd = await a.inner.addSigned('hello', alice, await outerDag.getFrontier());

                const entry = (await outerDag.loadEntry(hAdd))!;
                const at = version(...json.fromSet(entry.header.prevEntryHashes));
                const stored = entry.payload as json.LiteralMap;
                assertTrue(stored['elementHash'] === a.hash, 'the stored entry is an update of element A');
                assertTrue((await outer.validatePayload(stored, at)).valid,
                    'the stored update validates at its own position');

                const inner = stored['updatePayload'] as json.LiteralMap;
                const resignedForB = await signPayload(inner, alice, at, [{ elmt: b.hash }]);
                assertTrue((await outer.validatePayload({ ...stored, elementHash: b.hash, updatePayload: resignedForB }, at)).valid,
                    'the same add signed for element B would be accepted at this position');

                const rewrapped = await outer.validatePayload({ ...stored, elementHash: b.hash }, at);
                assertFalse(rewrapped.valid, 'the stored add re-wrapped for element B should not validate');
                assertTrue(!rewrapped.valid && formatValidationFailure(rewrapped.why).includes('signature'),
                    'the re-wrapped add should fail signature verification');
            }
        },
        {
            name: '[PSET29] a ref-advance version with a dominated ancestor is canonicalized on write and rejected on admission',
            invoke: async () => {
                const { cap, rset, admin } = await createTestEnv();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const a = await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                const b = await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin, version(a));

                const at = await (await rset.getScopedDag()).getFrontier();
                const raw = await signPayload(
                    createRefAdvancePayload(cap.getId(), version(a, b)) as unknown as json.LiteralMap, admin, at,
                );
                const rejected = await rset.validatePayload(raw, at);
                assertTrue(!rejected.valid && formatValidationFailure(rejected.why).includes('not an antichain'),
                    'a ref-advance carrying a dominated ancestor is rejected');

                const written = await rset.refAdvance(version(a, b), admin, at);
                const stored = extractRefVersion((await (await rset.getScopedDag()).loadEntry(written))!.payload as never);
                assertTrue(stored.size === 1 && stored.has(b), 'the writer publishes the minimal cover');
            }
        },
        {
            name: '[PSET30] ref-advance admission reads the imported version: a revoke in its past rejects an author who holds the cap at the parent cut',
            invoke: async () => {
                const { cap, rset, admin, attacker, c0 } = await stewardEnv();
                const R0 = await rset.refAdvance(c0, admin);
                const r = await cap.revoke(attacker.keyId, 'steward', admin, c0);

                const at = version(R0);
                const imported = await rset.validatePayload(await signedRefAdvance(cap, version(r), attacker, at), at);
                assertFalse(imported.valid, 'the attacker lacks steward at the imported version');
                const parentCut = await rset.validatePayload(await signedRefAdvance(cap, c0, attacker, at), at);
                assertTrue(parentCut.valid, 'importing the parent cut, the attacker still holds steward');
            }
        },
        {
            name: '[PSET31] ref-advance admission reads the imported version: a grant only in its past admits an author who lacks the cap at the parent cut',
            invoke: async () => {
                const { cap, rset, admin, bob, c0 } = await stewardEnv();
                const R0 = await rset.refAdvance(c0, admin);
                const g = await cap.grant(bob.keyId, 'steward', admin, c0);

                const at = version(R0);
                const result = await rset.validatePayload(await signedRefAdvance(cap, version(g), bob, at), at);
                assertTrue(result.valid, 'Bob holds steward at the imported version');
            }
        },
        {
            name: '[PSET32] a creator ref-advances by default, and a non-creator cannot when no cap is listed',
            invoke: async () => {
                {
                    const { cap, rset, admin } = await createTestEnv();
                    const bob = await makeIdentity();
                    await registerAndGrant(cap, bob, 'write', admin);
                    const v = await (await cap.getScopedDag()).getFrontier();
                    const at = await (await rset.getScopedDag()).getFrontier();
                    assertTrue((await rset.validatePayload(await signedRefAdvance(cap, v, admin, at), at)).valid,
                        'a creator ref-advances when refAdvance is omitted');
                    assertFalse((await rset.validatePayload(await signedRefAdvance(cap, v, bob, at), at)).valid,
                        'a non-creator cannot ref-advance when no cap is listed');
                }
                {
                    const { cap, rset, admin, c0 } = await stewardEnv();
                    const at = await (await rset.getScopedDag()).getFrontier();
                    assertTrue((await rset.validatePayload(await signedRefAdvance(cap, c0, admin, at), at)).valid,
                        'a creator ref-advances when caps are listed and refAdvanceCreators is left at its default');
                }
            }
        },
        {
            name: '[PSET33] a mixed-cut ref-advance whose author is revoked concurrently is void, and its version drops out of a concurrent fold',
            invoke: async () => {
                const { cap, rset, admin, attacker, bob, c0 } = await stewardEnv();
                const [dave, carol] = [await makeIdentity(), await makeIdentity()];

                // RCap:  c0 ── v1 ── r ── idC ── gC    r revokes the attacker's steward;
                //          \                           gC grants Carol write.
                //           vAdv                       vAdv grants Bob write, concurrent with v1 and r.
                const v1 = await addIdentityAt(cap, dave, admin, c0);
                const vAdv = await cap.grant(bob.keyId, 'write', admin, c0);
                const r = await cap.revoke(attacker.keyId, 'steward', admin, version(v1));
                const idC = await addIdentityAt(cap, carol, admin, version(r));
                const gC = await cap.grant(carol.keyId, 'write', admin, version(idC));

                // Set:  R0 ── A ── addY      A: the attacker advances to {v1, vAdv}, admitted
                //         \                       at that version, where r is not in the past.
                //          Adm ── addZ       Adm: the admin advances to {gC}.
                const R0 = await rset.refAdvance(c0, admin);
                const A = await rset.refAdvance(version(v1, vAdv), attacker, version(R0));
                const Adm = await rset.refAdvance(version(gC), admin, version(R0));
                await rset.addSigned('Y', bob, version(A));
                await rset.addSigned('Z', carol, version(Adm));

                // From the frontier, r is concurrent with {v1, vAdv} and visible, so A is
                // void. Bob's add reads Adm's {gC} and R0's c0, and holds no write there.
                const view = await rset.getView();
                assertFalse(await view.has('Y'), "Y is absent: A is void, so vAdv drops out of Bob's add's fold");
                assertTrue(await view.has('Z'), "Z stays: the admin's advance is live");
                assertFalse(await view.isRefAdvanceLive(A), "the attacker's advance is void");
                assertTrue(await view.isRefAdvanceLive(Adm), "the admin's advance is live");
            }
        },
        {
            name: '[PSET34] an add after a ref-advance that turns void reads the live ref-advance below it',
            invoke: async () => {
                const { cap, rset, admin, attacker, bob, c0 } = await stewardEnv();

                // RCap:  c0 ── vAdv    vAdv grants Bob write.
                //          \
                //           r          r revokes the attacker's steward, concurrent with vAdv.
                const vAdv = await cap.grant(bob.keyId, 'write', admin, c0);
                const r = await cap.revoke(attacker.keyId, 'steward', admin, c0);

                // Set:  L ── Y ── E ── N    L: the admin's advance to c0. Y: the attacker's to
                //                           {vAdv}. E: Bob's add. N: the admin's to {vAdv, r}.
                const L = await rset.refAdvance(c0, admin);
                const Y = await rset.refAdvance(version(vAdv), attacker, version(L));
                const E = await rset.addSigned('E', bob, version(Y));

                const before = await rset.getView(version(E), version(E));
                assertTrue(await before.has('E'), 'before N, E stays');
                assertTrue(await before.isRefAdvanceLive(Y), 'before N, Y is live');

                await rset.refAdvance(version(vAdv, r), admin, version(E));

                // N brings r into refFrom, not into E's refAt. r is concurrent with Y's
                // cut, so Y is void and E's fold sees through it to L.
                const view = await rset.getView();
                assertFalse(await view.has('E'), 'E is absent: Y is void, so E reads L, where Bob lacks write');
                assertFalse(await view.isRefAdvanceLive(Y), 'Y is void');
            }
        },
        {
            name: '[PSET35] ref-advances to incomparable versions stay live and do not consult each other',
            invoke: async () => {
                const { cap, rset, admin, attacker, bob } = await stewardEnv();
                const [sam, carol] = [await makeIdentity(), await makeIdentity()];
                await registerAndGrant(cap, sam, 'steward', admin);
                await cap.addIdentity(carol.keyId, serializePublicKeyToBase64(carol.publicKey), admin);
                const c1 = await (await cap.getScopedDag()).getFrontier();

                // va grants Bob write and vb grants Carol write, on incomparable branches.
                const va = await cap.grant(bob.keyId, 'write', admin, c1);
                const vb = await cap.grant(carol.keyId, 'write', admin, c1);
                const R0 = await rset.refAdvance(c1, admin);
                const A1 = await rset.refAdvance(version(va), attacker, version(R0));
                const A2 = await rset.refAdvance(version(vb), sam, version(R0));
                await rset.addSigned('B', bob, version(A1));
                await rset.addSigned('C', carol, version(A2));

                const view = await rset.getView();
                assertTrue(await view.has('B'), 'B, authorized only on va, stays');
                assertTrue(await view.has('C'), 'C, authorized only on vb, stays');
                assertTrue(await view.isRefAdvanceLive(A1) && await view.isRefAdvanceLive(A2), 'both advances are live');
            }
        },
        {
            name: '[PSET36] concurrent reads on one view that share a ref-advance verdict agree with reads one at a time',
            invoke: async () => {
                const { cap, rset, admin, attacker, bob, c0 } = await stewardEnv();
                const vAdv = await cap.grant(bob.keyId, 'write', admin, c0);
                const r = await cap.revoke(attacker.keyId, 'steward', admin, c0);

                // [PSET34]'s shape with several adds after the attacker's advance Y: Bob's
                // read Y's verdict, the admin's does not need it.
                const L = await rset.refAdvance(c0, admin);
                const Y = await rset.refAdvance(version(vAdv), attacker, version(L));
                const elements = ['E1', 'E2', 'E3', 'K'];
                for (const element of elements) {
                    await rset.addSigned(element, element === 'K' ? admin : bob, version(Y));
                }
                await rset.refAdvance(version(vAdv, r), admin);

                const shared = await rset.getView();
                const together = await Promise.all(elements.map((element) => shared.has(element)));
                const oneAtATime: boolean[] = [];
                const fresh = await rset.getView();
                for (const element of elements) oneAtATime.push(await fresh.has(element));
                assertEquals(JSON.stringify(together), JSON.stringify(oneAtATime), 'concurrent reads agree with reads one at a time');
                assertEquals(JSON.stringify(together), JSON.stringify([false, false, false, true]), "only the admin's add stays");
            }
        },
    ]
};
