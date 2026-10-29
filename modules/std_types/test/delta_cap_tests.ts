import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { set } from "@hyper-hyper-space/hhs3_util";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap/rcap.js";
import type { RCapDelta } from "../src/types/rcap/rcap.js";
import { serializePublicKeyToBase64 } from "../src/authorship.js";
import { normalizeRCapDelta } from "./delta_parity/normalize.js";

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

async function computeWithStrategy(
    cap: RCap,
    strategy: 'full' | 'bounded',
    start: ReturnType<typeof version>,
    end: ReturnType<typeof version>,
): Promise<RCapDelta> {
    cap.setDeltaStrategy(strategy);
    return await cap.computeDelta(start, end) as RCapDelta;
}

// The bounded delta, after checking it against the full one and the bound-only
// computation.
async function boundedDelta(cap: RCap, start: Version, end: Version): Promise<RCapDelta> {
    const full = await computeWithStrategy(cap, 'full', start, end);
    const bounded = await computeWithStrategy(cap, 'bounded', start, end);
    assertEquals(
        JSON.stringify(normalizeRCapDelta(bounded)),
        JSON.stringify(normalizeRCapDelta(full)),
        'the bounded delta matches the full one',
    );
    assertTrue(set.eq(await cap.computeRevisionBound(start, end), bounded.getRevisionBound()),
        'computeRevisionBound returns the bounded delta\'s bound');
    return bounded;
}

function isExactly(v: Version, ...hashes: B64Hash[]): boolean {
    return v.size === hashes.length && hashes.every((h) => v.has(h));
}

async function addIdentities(cap: RCap, author: OwnIdentity, ...identities: OwnIdentity[]): Promise<void> {
    for (const identity of identities) {
        await cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), author);
    }
}

// The bounded-delta counterexample for RCap's meet-based bound:
//
//   ... gS - gA - G - s1 - N        start = {s1, T}, end = {N, T}
//             \
//              T
//
// super is managed by the creator, admin by super, write by admin. gS grants S
// super, gA grants A admin, G is A granting B write, and T is S revoking A's
// admin, concurrent with G and s1. N follows s1: with `nVoidsT` it revokes S's
// super, which voids T and so revives G; otherwise it adds an identity.
async function revokeChainHistory(nVoidsT: boolean) {
    const R = await makeIdentity();
    const S = await makeIdentity();
    const A = await makeIdentity();
    const B = await makeIdentity();
    const Z = await makeIdentity();
    const W = await makeIdentity();
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);
    const cap = (await ctx.createObject(await RCap.create({
        seed: 'cap-bound-revoke-chain',
        creators: [{ keyId: R.keyId, publicKey: R.publicKey }],
        initialCaps: {
            'super': { managedBy: ['creator'] },
            'admin': { managedBy: ['super'] },
            'write': { managedBy: ['admin'] },
        },
    }))) as RCap;
    await addIdentities(cap, R, S, A, B);

    const gS = await cap.grant(S.keyId, 'super', R);
    const gA = await cap.grant(A.keyId, 'admin', R, version(gS));
    const G = await cap.grant(B.keyId, 'write', A, version(gA));
    const s1 = await cap.addIdentity(Z.keyId, serializePublicKeyToBase64(Z.publicKey), R, version(G));
    const T = await cap.revoke(A.keyId, 'admin', S, version(gA));
    const N = nVoidsT
        ? await cap.revoke(S.keyId, 'super', R, version(s1))
        : await cap.addIdentity(W.keyId, serializePublicKeyToBase64(W.publicKey), R, version(s1));
    return { cap, B, gA, s1, T, N, start: version(s1, T), end: version(N, T) };
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
                await cap.grant(bob.keyId, 'write', admin);

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

                assertTrue(
                    set.eq(delta.getRevisionBound(), creationVersion),
                    'revision bound is the meet (create op for a linear extension from creation)',
                );
            }
        },
        {
            name: '[CAP_DELTA02] Grant then revoke: partial delta shows flip, from-creation delta is net zero',
            invoke: async () => {
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                const creationVersion = version(cap.getId());

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                await cap.grant(bob.keyId, 'write', admin);

                const dag = (await ctx.getDag(cap.getId()))!;
                const afterGrant = await dag.getFrontier();

                await cap.revoke(bob.keyId, 'write', admin);

                const afterRevoke = await dag.getFrontier();

                const partialDelta = await cap.computeDelta(afterGrant, afterRevoke) as RCapDelta;
                assertEquals(partialDelta.grantChanges.length, 1, 'partial: one grant change');
                assertTrue(partialDelta.grantChanges[0].wasGranted, 'partial: was granted');
                assertFalse(partialDelta.grantChanges[0].nowGranted, 'partial: now not granted');

                const fromCreationDelta = await cap.computeDelta(creationVersion, afterRevoke) as RCapDelta;
                assertEquals(fromCreationDelta.grantChanges.length, 0, 'from creation: net zero grant changes');
                assertEquals(fromCreationDelta.identityChanges.length, 1, 'from creation: Bob identity still added');
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
                await cap.grant(alice.keyId, 'write', admin, forkPoint);
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
        {
            name: '[CAP_DELTA05] Default bounded delta matches reference scan across baseline scenarios',
            invoke: async () => {
                // Scenario 1: add identity + grant
                {
                    const admin = await makeIdentity();
                    const bob = await makeIdentity();
                    const { ctx, cap } = await createTestCap(admin);
                    const start = version(cap.getId());

                    await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                    await cap.grant(bob.keyId, 'write', admin);
                    const end = await (await ctx.getDag(cap.getId()))!.getFrontier();

                    const delta = await computeWithStrategy(cap, 'bounded', start, end);
                    const reference = await computeWithStrategy(cap, 'full', start, end);
                    assertEquals(
                        JSON.stringify(normalizeRCapDelta(delta)),
                        JSON.stringify(normalizeRCapDelta(reference)),
                        'scenario 1 should match reference scan',
                    );
                }

                // Scenario 2: grant then revoke
                {
                    const admin = await makeIdentity();
                    const bob = await makeIdentity();
                    const { ctx, cap } = await createTestCap(admin);

                    await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                    const dag = (await ctx.getDag(cap.getId()))!;
                    const start = await dag.getFrontier();

                    await cap.grant(bob.keyId, 'write', admin);
                    await cap.revoke(bob.keyId, 'write', admin);
                    const end = await dag.getFrontier();

                    const delta = await computeWithStrategy(cap, 'bounded', start, end);
                    const reference = await computeWithStrategy(cap, 'full', start, end);
                    assertEquals(
                        JSON.stringify(normalizeRCapDelta(delta)),
                        JSON.stringify(normalizeRCapDelta(reference)),
                        'scenario 2 should match reference scan',
                    );
                }

                // Scenario 3: create then delete capability
                {
                    const admin = await makeIdentity();
                    const { ctx, cap } = await createTestCap(admin);
                    const dag = (await ctx.getDag(cap.getId()))!;
                    const start = await dag.getFrontier();

                    await cap.createCap('deploy', ['admin'], admin);
                    await cap.deleteCap('deploy', admin);
                    const end = await dag.getFrontier();

                    const delta = await computeWithStrategy(cap, 'bounded', start, end);
                    const reference = await computeWithStrategy(cap, 'full', start, end);
                    assertEquals(
                        JSON.stringify(normalizeRCapDelta(delta)),
                        JSON.stringify(normalizeRCapDelta(reference)),
                        'scenario 3 should match reference scan',
                    );
                }

                // Scenario 4: concurrent grant/revoke from same fork point
                {
                    const admin = await makeIdentity();
                    const alice = await makeIdentity();
                    const { ctx, cap } = await createTestCap(admin);

                    await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                    const dag = (await ctx.getDag(cap.getId()))!;
                    const start = await dag.getFrontier();

                    await cap.grant(alice.keyId, 'write', admin, start);
                    await cap.revoke(alice.keyId, 'write', admin, start);
                    const end = await dag.getFrontier();

                    const delta = await computeWithStrategy(cap, 'bounded', start, end);
                    const reference = await computeWithStrategy(cap, 'full', start, end);
                    assertEquals(
                        JSON.stringify(normalizeRCapDelta(delta)),
                        JSON.stringify(normalizeRCapDelta(reference)),
                        'scenario 4 should match reference scan',
                    );
                }
            }
        },
        {
            name: '[CAP_DELTA06] revisionBound equals the fork-point meet',
            invoke: async () => {
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                const start = version(cap.getId());
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                const end = await (await ctx.getDag(cap.getId()))!.getFrontier();

                const delta = await computeWithStrategy(cap, 'bounded', start, end);

                assertTrue(
                    set.eq(delta.getRevisionBound(), start),
                    'revisionBound equals the meet (== start for a linear add)',
                );
            }
        },
        {
            name: '[CAP_DELTA07] computeDelta throws when START is not in history(END)',
            invoke: async () => {
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), admin);
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);

                const dag = (await ctx.getDag(cap.getId()))!;
                const forkPoint = await dag.getFrontier();

                const grantA = await cap.grant(alice.keyId, 'write', admin, forkPoint);
                const grantB = await cap.grant(bob.keyId, 'write', admin, forkPoint);

                let threw = false;
                try {
                    await cap.computeDelta(version(grantA), version(grantB));
                } catch (e) {
                    threw = ((e as Error).message).indexOf('requires END to extend START') >= 0;
                }

                assertTrue(threw, 'should throw when end does not extend start');
            }
        },
        {
            name: '[CAP_DELTA08] Deleted capability suppresses grant diffs',
            invoke: async () => {
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);
                const dag = (await ctx.getDag(cap.getId()))!;

                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                await cap.createCap('deploy', ['admin'], admin);
                const start = await dag.getFrontier();

                const deployOrigins = await (await cap.getView()).currentCapCreationVersion('deploy');
                assertTrue(deployOrigins.size > 0, 'deploy origins should be non-empty after create-cap');
                await cap.grant(bob.keyId, 'deploy', admin);
                await cap.deleteCap('deploy', admin);
                const end = await dag.getFrontier();

                const delta = await computeWithStrategy(cap, 'bounded', start, end);
                const reference = await computeWithStrategy(cap, 'full', start, end);

                assertEquals(delta.grantChanges.length, 0, 'should suppress grant diffs for deleted cap');
                assertEquals(reference.grantChanges.length, 0, 'reference scan should suppress grant diffs for deleted cap');

                const deployChange = delta.capabilityChanges.find(c => c.capName === 'deploy');
                const referenceDeployChange = reference.capabilityChanges.find(c => c.capName === 'deploy');
                assertTrue(deployChange !== undefined, 'should report deploy capability change');
                assertTrue(referenceDeployChange !== undefined, 'reference scan should report deploy capability change');
                assertFalse(deployChange!.exists, 'deploy should not exist at END');
                assertFalse(referenceDeployChange!.exists, 'reference scan: deploy should not exist at END');
            }
        },
        {
            // Layer 1: END merges an old branch whose barrier voids a grant on the
            // shared trunk below `common`. Stopping at common/commonFrontier would
            // miss the transitive (bob, write) flip; the meet ({g1}) reaches g2.
            //
            // createOp -> addAlice -> addBob -> g1 -> g2 ----\
            //                                    \            m -> END
            //                                     old -------/
            //   g1  = grant alice admin
            //   g2  = grant bob write (via alice's admin)   <-- transitively flips
            //   old = revoke alice admin (after g1, concurrent with g2; barrier)
            //   m   = merge(g2, old) = add identity carol
            name: '[CAP_DELTA09] merged branch transitively voids a trunk grant',
            invoke: async () => {
                const R = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const carol = await makeIdentity();
                const { ctx, cap } = await createTestCap(R);
                const dag = (await ctx.getDag(cap.getId()))!;

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), R);
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), R);

                const g1 = await cap.grant(alice.keyId, 'admin', R);
                const g2 = await cap.grant(bob.keyId, 'write', alice);
                const start = version(g2);

                const old = await cap.revoke(alice.keyId, 'admin', R, version(g1));
                const m = await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey), R, version(g2, old),
                );
                const end = version(m);

                const delta = await computeWithStrategy(cap, 'bounded', start, end);
                const reference = await computeWithStrategy(cap, 'full', start, end);

                const writeFlip = delta.grantChanges.find(c => c.keyId === bob.keyId && c.capName === 'write');
                assertTrue(
                    writeFlip !== undefined && writeFlip.wasGranted && !writeFlip.nowGranted,
                    'bob.write flips true->false transitively',
                );

                assertEquals(
                    JSON.stringify(normalizeRCapDelta(delta)),
                    JSON.stringify(normalizeRCapDelta(reference)),
                    'CAP_DELTA09 should match reference scan',
                );
            }
        },
        {
            // Layer 2: two concurrent fork siblings. The antichain of `common`
            // ({b1, b2}) would stop the walk at b1 and miss (bob, write); the meet
            // ({P}, their shared parent) sits strictly below both and reaches b1.
            //
            // ... -> P --> b1 ------------------\
            //         \                          m -> END
            //          b2 --> y1 ---------------/
            //   P  = grant alice admin                       (meet of b1, b2)
            //   b1 = grant bob write (via alice's admin)     <-- transitively flips
            //   b2 = add identity carol  (concurrent branch anchor)
            //   y1 = revoke alice admin (after b2, concurrent with b1; barrier)
            //   m  = merge(b1, y1) = add identity dave
            name: '[CAP_DELTA10] meet sits below concurrent fork roots',
            invoke: async () => {
                const R = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const carol = await makeIdentity();
                const dave = await makeIdentity();
                const { ctx, cap } = await createTestCap(R);

                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), R);
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), R);

                const P = await cap.grant(alice.keyId, 'admin', R);

                const b1 = await cap.grant(bob.keyId, 'write', alice, version(P));
                const b2 = await cap.addIdentity(
                    carol.keyId, serializePublicKeyToBase64(carol.publicKey), R, version(P),
                );
                const start = version(b1, b2);

                const y1 = await cap.revoke(alice.keyId, 'admin', R, version(b2));
                const m = await cap.addIdentity(
                    dave.keyId, serializePublicKeyToBase64(dave.publicKey), R, version(b1, y1),
                );
                const end = version(m);

                const delta = await computeWithStrategy(cap, 'bounded', start, end);
                const reference = await computeWithStrategy(cap, 'full', start, end);

                const writeFlip = delta.grantChanges.find(c => c.keyId === bob.keyId && c.capName === 'write');
                assertTrue(
                    writeFlip !== undefined && writeFlip.wasGranted && !writeFlip.nowGranted,
                    'bob.write flips true->false transitively (below the concurrent fork roots)',
                );

                assertEquals(
                    JSON.stringify(normalizeRCapDelta(delta)),
                    JSON.stringify(normalizeRCapDelta(reference)),
                    'CAP_DELTA10 should match reference scan',
                );
            }
        },
        {
            // Linear chain, no forks. (bob, write) is granted below the meet and never
            // changes; only (alice, write) flips in (start, end]. The admissibility
            // semantics must not spuriously re-evaluate the stable below-meet pair, so
            // default delta and reference scan neither report a (bob, write) change.
            name: '[CAP_DELTA11] below-meet sequential grant stays stable',
            invoke: async () => {
                const R = await makeIdentity();
                const manager = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const { cap } = await createTestCap(R);

                await cap.addIdentity(manager.keyId, serializePublicKeyToBase64(manager.publicKey), R);
                await cap.addIdentity(alice.keyId, serializePublicKeyToBase64(alice.publicKey), R);
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), R);

                await cap.grant(manager.keyId, 'admin', R);
                await cap.grant(bob.keyId, 'write', manager);

                const start = await (await cap.getScopedDag()).getFrontier();

                // The only change after START: manager grants alice write.
                await cap.grant(alice.keyId, 'write', manager);
                const end = await (await cap.getScopedDag()).getFrontier();

                const delta = await computeWithStrategy(cap, 'bounded', start, end);
                const reference = await computeWithStrategy(cap, 'full', start, end);

                const aliceFlip = delta.grantChanges.find(c => c.keyId === alice.keyId && c.capName === 'write');
                assertTrue(
                    aliceFlip !== undefined && !aliceFlip.wasGranted && aliceFlip.nowGranted,
                    'alice.write flips false->true (the only grant change)',
                );

                const bobChange = delta.grantChanges.find(c => c.keyId === bob.keyId && c.capName === 'write');
                assertTrue(bobChange === undefined, 'bob.write is stable below the meet -> no delta entry');

                assertEquals(
                    JSON.stringify(normalizeRCapDelta(delta)),
                    JSON.stringify(normalizeRCapDelta(reference)),
                    'CAP_DELTA11 should match reference scan',
                );
            }
        },
        {
            name: '[CAP_BOUND01] a side-branch revoke voided by a new entry lowers the bound, and the grant it revives is reported',
            invoke: async () => {
                const h = await revokeChainHistory(true);
                const delta = await boundedDelta(h.cap, h.start, h.end);
                const writeFlip = delta.grantChanges.find(c => c.keyId === h.B.keyId && c.capName === 'write');
                assertTrue(writeFlip !== undefined && !writeFlip.wasGranted && writeFlip.nowGranted, 'B gains write');
                assertTrue(isExactly(delta.getRevisionBound(), h.gA), "the bound drops to the revoke's predecessor");
            },
        },
        {
            name: '[CAP_BOUND02] a side-branch revoke that does not change keeps the bound at the meet',
            invoke: async () => {
                const h = await revokeChainHistory(false);
                const delta = await boundedDelta(h.cap, h.start, h.end);
                assertTrue(!delta.grantChanges.some(c => c.keyId === h.B.keyId), "B's write does not change");
                assertTrue(isExactly(delta.getRevisionBound(), h.s1), 'the bound stays at the meet');
            },
        },
        {
            name: '[CAP_BOUND03] re-creating a deleted capability reports the grants it revives',
            invoke: async () => {
                const R = await makeIdentity();
                const B = await makeIdentity();
                const { cap } = await createTestCap(R);
                await addIdentities(cap, R, B);
                await cap.createCap('dyn', ['admin'], R);
                await cap.grant(B.keyId, 'dyn', R);
                await cap.deleteCap('dyn', R);
                const start = await (await cap.getScopedDag()).getFrontier();
                await cap.createCap('dyn', ['admin'], R);
                const end = await (await cap.getScopedDag()).getFrontier();

                const delta = await boundedDelta(cap, start, end);
                assertTrue(delta.capabilityChanges.some(c => c.capName === 'dyn' && !c.existed && c.exists), 'dyn reappears');
                const dynFlip = delta.grantChanges.find(c => c.keyId === B.keyId && c.capName === 'dyn');
                assertTrue(dynFlip !== undefined && !dynFlip.wasGranted && dynFlip.nowGranted, 'B holds dyn again');
            },
        },
        {
            name: '[CAP_BOUND04] computeRevisionBound returns the delta\'s bound, also for versions with a dominated hash',
            invoke: async () => {
                for (const nVoidsT of [true, false]) {
                    const h = await revokeChainHistory(nVoidsT);
                    const delta = await boundedDelta(h.cap, h.start, h.end);
                    const padded = await h.cap.computeRevisionBound(version(...h.start, h.gA), version(...h.end, h.gA));
                    assertTrue(set.eq(padded, delta.getRevisionBound()), 'a dominated hash does not change the bound');
                }
            },
        },
        {
            name: '[CAP_BOUND05] getCapabilities lists a create-cap that has a later capability entry above it',
            invoke: async () => {
                const R = await makeIdentity();
                const { cap } = await createTestCap(R);
                await cap.createCap('y', ['admin'], R);
                const afterY = await (await cap.getScopedDag()).getFrontier();
                await cap.createCap('z', ['admin'], R);
                await cap.deleteCap('z', R);

                const caps = (await (await cap.getView()).getCapabilities()).sort();
                assertEquals(JSON.stringify(caps), JSON.stringify(['admin', 'write', 'y']), 'y is listed, z is deleted');
                const earlier = (await (await cap.getView(afterY, afterY)).getCapabilities()).sort();
                assertEquals(JSON.stringify(earlier), JSON.stringify(['admin', 'write', 'y']), 'z is not listed before it exists');
            },
        },
    ]
};
