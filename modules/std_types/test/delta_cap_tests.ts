import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { set } from "@hyper-hyper-space/hhs3_util";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RCap, rCapFactory } from "../src/types/rcap/rcap.js";
import type { RCapDelta } from "../src/types/rcap/rcap.js";
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

function normalizeDelta(delta: RCapDelta) {
    const identityChanges = [...delta.identityChanges].sort((a, b) => a.keyId.localeCompare(b.keyId));
    const capabilityChanges = [...delta.capabilityChanges].sort((a, b) => a.capName.localeCompare(b.capName));
    const grantChanges = [...delta.grantChanges].sort((a, b) => {
        const capCmp = a.capName.localeCompare(b.capName);
        if (capCmp !== 0) return capCmp;
        return a.keyId.localeCompare(b.keyId);
    });
    return { identityChanges, capabilityChanges, grantChanges };
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
                await cap.grant(bob.keyId, 'write', admin);

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
            name: '[CAP_DELTA05] Differential: bounded equals full across baseline scenarios',
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

                    const full = await computeWithStrategy(cap, 'full', start, end);
                    const bounded = await computeWithStrategy(cap, 'bounded', start, end);
                    assertEquals(
                        JSON.stringify(normalizeDelta(bounded)),
                        JSON.stringify(normalizeDelta(full)),
                        'scenario 1 should match',
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

                    const full = await computeWithStrategy(cap, 'full', start, end);
                    const bounded = await computeWithStrategy(cap, 'bounded', start, end);
                    assertEquals(
                        JSON.stringify(normalizeDelta(bounded)),
                        JSON.stringify(normalizeDelta(full)),
                        'scenario 2 should match',
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

                    const full = await computeWithStrategy(cap, 'full', start, end);
                    const bounded = await computeWithStrategy(cap, 'bounded', start, end);
                    assertEquals(
                        JSON.stringify(normalizeDelta(bounded)),
                        JSON.stringify(normalizeDelta(full)),
                        'scenario 3 should match',
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

                    const full = await computeWithStrategy(cap, 'full', start, end);
                    const bounded = await computeWithStrategy(cap, 'bounded', start, end);
                    assertEquals(
                        JSON.stringify(normalizeDelta(bounded)),
                        JSON.stringify(normalizeDelta(full)),
                        'scenario 4 should match',
                    );
                }
            }
        },
        {
            name: '[CAP_DELTA06] bounded revisionBound equals the fork-point meet',
            invoke: async () => {
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const { ctx, cap } = await createTestCap(admin);

                const start = version(cap.getId());
                await cap.addIdentity(bob.keyId, serializePublicKeyToBase64(bob.publicKey), admin);
                const end = await (await ctx.getDag(cap.getId()))!.getFrontier();

                const full = await computeWithStrategy(cap, 'full', start, end);
                const bounded = await computeWithStrategy(cap, 'bounded', start, end);

                assertEquals(full.getRevisionBound().size, 0, 'full keeps empty revisionBound');
                assertTrue(
                    set.eq(bounded.getRevisionBound(), start),
                    'bounded revisionBound equals the meet (== start for a linear add)',
                );
            }
        },
        {
            name: '[CAP_DELTA07] bounded throws when START is not in history(END)',
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

                cap.setDeltaStrategy('bounded');

                let threw = false;
                try {
                    await cap.computeDelta(version(grantA), version(grantB));
                } catch (e) {
                    threw = ((e as Error).message).indexOf('requires END to extend START') >= 0;
                }

                assertTrue(threw, 'bounded should throw when forkA is non-empty');
            }
        },
        {
            name: '[CAP_DELTA08] Deleted capability suppresses grant diffs in both strategies',
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

                const full = await computeWithStrategy(cap, 'full', start, end);
                const bounded = await computeWithStrategy(cap, 'bounded', start, end);

                assertEquals(full.grantChanges.length, 0, 'full should suppress grant diffs for deleted cap');
                assertEquals(bounded.grantChanges.length, 0, 'bounded should suppress grant diffs for deleted cap');

                const fullDeployChange = full.capabilityChanges.find(c => c.capName === 'deploy');
                const boundedDeployChange = bounded.capabilityChanges.find(c => c.capName === 'deploy');
                assertTrue(fullDeployChange !== undefined, 'full should report deploy capability change');
                assertTrue(boundedDeployChange !== undefined, 'bounded should report deploy capability change');
                assertFalse(fullDeployChange!.exists, 'full: deploy should not exist at END');
                assertFalse(boundedDeployChange!.exists, 'bounded: deploy should not exist at END');
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
            name: '[CAP_DELTA09] bounded matches full when a merged branch transitively voids a trunk grant',
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

                const full = await computeWithStrategy(cap, 'full', start, end);
                const bounded = await computeWithStrategy(cap, 'bounded', start, end);

                const writeFlip = full.grantChanges.find(c => c.keyId === bob.keyId && c.capName === 'write');
                assertTrue(
                    writeFlip !== undefined && writeFlip.wasGranted && !writeFlip.nowGranted,
                    'bob.write flips true->false transitively',
                );

                assertEquals(
                    JSON.stringify(normalizeDelta(bounded)),
                    JSON.stringify(normalizeDelta(full)),
                    'CAP_DELTA09 bounded should match full',
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
            name: '[CAP_DELTA10] bounded matches full when the meet sits below concurrent fork roots',
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

                const full = await computeWithStrategy(cap, 'full', start, end);
                const bounded = await computeWithStrategy(cap, 'bounded', start, end);

                const writeFlip = full.grantChanges.find(c => c.keyId === bob.keyId && c.capName === 'write');
                assertTrue(
                    writeFlip !== undefined && writeFlip.wasGranted && !writeFlip.nowGranted,
                    'bob.write flips true->false transitively (below the concurrent fork roots)',
                );

                assertEquals(
                    JSON.stringify(normalizeDelta(bounded)),
                    JSON.stringify(normalizeDelta(full)),
                    'CAP_DELTA10 bounded should match full',
                );
            }
        },
        {
            // Linear chain, no forks. (bob, write) is granted below the meet and never
            // changes; only (alice, write) flips in (start, end]. The admissibility
            // semantics must not spuriously re-evaluate the stable below-meet pair, so
            // bounded equals full and neither reports a (bob, write) change.
            name: '[CAP_DELTA11] below-meet sequential grant stays stable; bounded matches full',
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

                const full = await computeWithStrategy(cap, 'full', start, end);
                const bounded = await computeWithStrategy(cap, 'bounded', start, end);

                const aliceFlip = full.grantChanges.find(c => c.keyId === alice.keyId && c.capName === 'write');
                assertTrue(
                    aliceFlip !== undefined && !aliceFlip.wasGranted && aliceFlip.nowGranted,
                    'alice.write flips false->true (the only grant change)',
                );

                const bobChange = full.grantChanges.find(c => c.keyId === bob.keyId && c.capName === 'write');
                assertTrue(bobChange === undefined, 'bob.write is stable below the meet -> no delta entry');

                assertEquals(
                    JSON.stringify(normalizeDelta(bounded)),
                    JSON.stringify(normalizeDelta(full)),
                    'CAP_DELTA11 bounded should match full',
                );
            }
        },
    ]
};
