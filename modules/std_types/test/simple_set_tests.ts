import { formatValidationFailure, RContext, RObject, RObjectConfig, TypeRegistryMap, version } from "@hyper-hyper-space/hhs3_mvt";
import { RSet, rSetFactory } from "../src/types/rset/rset.js";
import { hashElement } from "../src/types/rset/hash.js";
import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createMockRContext } from "./mock_rcontext.js";
import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, createBasicCrypto, HASH_SHA256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";

const crypto = createBasicCrypto();

function createTestCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSet.typeId, rSetFactory);
    return ctx;
}

const createTestEnvironment = async (initialElements: Array<json.Literal> = [], barrierOpts?: { supportBarrierAdd?: boolean, supportBarrierDelete?: boolean }) => {
    const ctx = createTestCtx();

    const init = await RSet.create(
        {
            seed: 'set00',
            initialElements: initialElements,
            hashAlgorithm: 'sha256',
            supportBarrierAdd: barrierOpts?.supportBarrierAdd ?? false,
            supportBarrierDelete: barrierOpts ? (barrierOpts.supportBarrierDelete ?? false) : true,
        }
    );

    const set = (await ctx.createObject(init)) as RSet;

    return { ctx, set, setId: set.getId() };
};

export const simpleSetTests = {
    title: '[SET] Small set tests with flat DAG indexing',
    tests: [
        {
            name: '[SET00] Test sequential additions and deletions',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                await set.add('alpha');
                await set.add('beta');
                await set.add('gamma');
                await set.add('delta');

                await set.delete('beta');
                await set.deleteWithBarrier('gamma');

                await set.add('epsilon');
                await set.deleteWithBarrier('delta');

                const view = await set.getView();

                assertTrue(await view.has('alpha'), 'alpha should remain present');
                assertTrue(await view.has('epsilon'), 'epsilon should remain present');
                assertFalse(await view.has('beta'), 'beta should have been removed');
                assertFalse(await view.has('gamma'), 'gamma should have been removed');
                assertFalse(await view.has('delta'), 'delta should have been barrier-deleted');
            }
        },
        {
            name: '[SET01] Test concurrent additions and historical views',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                const alphaHash = await set.add('alpha');
                const alphaVersion = version(alphaHash);

                const betaHash = await set.add('beta', alphaVersion);
                const betaVersion = version(betaHash);

                const gammaHash = await set.add('gamma', alphaVersion);
                const gammaVersion = version(gammaHash);

                const ancestorView = await set.getView(alphaVersion);
                assertTrue(await ancestorView.has('alpha'), 'alpha should be visible at its own version');
                assertFalse(await ancestorView.has('beta'), 'beta should not exist before it was added');
                assertFalse(await ancestorView.has('gamma'), 'gamma should not exist before it was added');

                const betaView = await set.getView(betaVersion);
                assertTrue(await betaView.has('alpha'), 'alpha should be visible when reading beta branch');
                assertTrue(await betaView.has('beta'), 'beta should be visible when reading beta branch');
                assertFalse(await betaView.has('gamma'), 'gamma should not leak into beta branch view');

                const gammaView = await set.getView(gammaVersion);
                assertTrue(await gammaView.has('alpha'), 'alpha should be visible when reading gamma branch');
                assertTrue(await gammaView.has('gamma'), 'gamma should be visible when reading gamma branch');
                assertFalse(await gammaView.has('beta'), 'beta should not leak into gamma branch view');

                const frontierView = await set.getView();
                assertTrue(await frontierView.has('beta'), 'latest view should include beta');
                assertTrue(await frontierView.has('gamma'), 'latest view should include gamma');
            }
        },
        {
            name: '[SET02a] Test barrier add across concurrent versions',
            invoke: async () => {
                const { set } = await createTestEnvironment([], { supportBarrierAdd: true });

                const rootHash = await set.add('root');
                const rootVersion = version(rootHash);

                const leftHash = await set.add('left', rootVersion);
                const rightHash = await set.add('right', rootVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                await set.addWithBarrier('barrier-delta', leftVersion);

                const rightViewAfterBarrierAdd = await set.getView(rightVersion);
                assertTrue(await rightViewAfterBarrierAdd.has('barrier-delta'), 'barrier add should apply to concurrent versions');

                const latestView = await set.getView();
                assertTrue(await latestView.has('barrier-delta'), 'latest view should still include barrier add');
                assertTrue(await latestView.has('root'), 'root should still be present');
            }
        },
        {
            name: '[SET02c] A barrier add after one head joins the set at the two-head frontier',
            invoke: async () => {
                const { set } = await createTestEnvironment([], { supportBarrierAdd: true });

                const rootHash = await set.add('root');
                const a = await set.add('left', version(rootHash));
                const b = await set.add('right', version(rootHash));
                const added = await set.addWithBarrier('barrier-delta', version(a));

                const view = await set.getView(version(a, b), version(added, b));
                assertTrue(await view.has('barrier-delta'),
                    'the barrier add is not after the other head, so it is concurrent with the frontier');
            }
        },
        {
            name: '[SET02b] Test barrier delete across concurrent versions',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                const rootHash = await set.add('root');
                const rootVersion = version(rootHash);

                const leftHash = await set.add('left', rootVersion);
                const rightHash = await set.add('right', rootVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                const rootLiteralHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('root')));
                await set.deleteWithBarrierByHash(rootLiteralHash, leftVersion);

                const rightOnlyView = await set.getView(rightVersion, rightVersion);
                assertTrue(await rightOnlyView.has('root'), 'root should still be present in non-merged right branch');

                const mergedView = await set.getView();
                assertFalse(await mergedView.has('root'), 'barrier delete should void root once branches merge');
            }
        },
        {
            name: '[SET02d] A barrier delete after one head removes an add on the other head',
            invoke: async () => {
                const { set } = await createTestEnvironment();
                const added = await set.add('x');
                const a = await set.add('left', version(added));
                const b = await set.add('right', version(added));
                const elementHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('x')));
                const deleted = await set.deleteWithBarrierByHash(elementHash, version(a));

                const view = await set.getView(version(a, b), version(deleted, b));
                assertFalse(await view.has('x'),
                    'the delete is after one head and not after the other, so it removes x');
            }
        },
        {
            name: '[SET02e] A barrier delete concurrent with the frontier and after the add removes the element',
            invoke: async () => {
                const { set } = await createTestEnvironment();
                const added = await set.add('x');
                const branch = await set.add('y', version(added));
                const elementHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('x')));
                const deleted = await set.deleteWithBarrierByHash(elementHash, version(added));

                const view = await set.getView(version(branch), version(deleted, branch));
                assertFalse(await view.has('x'),
                    'the delete is after the add and concurrent with the frontier, so the element is gone');
            }
        },
        {
            name: '[SET03] Test reading initial elements across history',
            invoke: async () => {
                const { set } = await createTestEnvironment(['alpha', 'beta']);

                const creationVersion = version(set.getId());
                const initialView = await set.getView(creationVersion);
                assertTrue(await initialView.has('alpha'), 'alpha should be present at creation');
                assertTrue(await initialView.has('beta'), 'beta should be present at creation');

                await set.add('gamma');
                await set.delete('alpha');

                const latestView = await set.getView();
                assertFalse(await latestView.has('alpha'), 'alpha should have been deleted from the latest view');
                assertTrue(await latestView.has('beta'), 'beta should still be present in the latest view');
                assertTrue(await latestView.has('gamma'), 'gamma should be present after being added');

                const historicalView = await set.getView(creationVersion);
                assertTrue(await historicalView.has('alpha'), 'historical view should retain alpha');
                assertTrue(await historicalView.has('beta'), 'historical view should retain beta');
                assertFalse(await historicalView.has('gamma'), 'historical view should not include future elements');
            }
        },
        {
            name: '[SET04] Test barrier deletes affecting initial elements',
            invoke: async () => {
                const { set } = await createTestEnvironment(['root', 'shared']);

                const creationVersion = version(set.getId());

                const leftHash = await set.add('left', creationVersion);
                const rightHash = await set.add('right', creationVersion);

                const leftVersion = version(leftHash);
                const rightVersion = version(rightHash);

                const sharedHash = crypto.hash(HASH_SHA256).hashToB64(stringToUint8Array(json.toStringNormalized('shared')));
                await set.deleteWithBarrierByHash(sharedHash, leftVersion);

                const rightOnlyView = await set.getView(rightVersion, rightVersion);
                assertTrue(await rightOnlyView.has('shared'), 'shared should still be present in non-merged right branch');
                assertTrue(await rightOnlyView.has('root'), 'other initial elements should remain');

                const ancestorView = await set.getView(creationVersion, creationVersion);
                assertTrue(await ancestorView.has('shared'), 'ancestor view should still see shared');

                const mergedView = await set.getView();
                assertFalse(await mergedView.has('shared'), 'barrier delete should void shared once branches merge');
                assertTrue(await mergedView.has('root'), 'root should survive in merged view');
            }
        },
        {
            name: '[SET05] Test delete then re-add of the same element',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                await set.add('alpha');
                await set.add('beta');

                const viewBefore = await set.getView();
                assertTrue(await viewBefore.has('alpha'), 'alpha should be present after initial add');
                assertTrue(await viewBefore.has('beta'), 'beta should be present after initial add');

                await set.delete('alpha');

                const viewAfterDelete = await set.getView();
                assertFalse(await viewAfterDelete.has('alpha'), 'alpha should be gone after delete');
                assertTrue(await viewAfterDelete.has('beta'), 'beta should survive unrelated delete');

                await set.add('alpha');

                const viewAfterReAdd = await set.getView();
                assertTrue(await viewAfterReAdd.has('alpha'), 'alpha should reappear after re-add');
                assertTrue(await viewAfterReAdd.has('beta'), 'beta should still be present');

                await set.delete('alpha');
                await set.add('alpha');

                const viewAfterSecondCycle = await set.getView();
                assertTrue(await viewAfterSecondCycle.has('alpha'), 'alpha should survive a second delete/re-add cycle');
            }
        },
        {
            name: '[SET06] Test delete then re-add of an initial element',
            invoke: async () => {
                const { set } = await createTestEnvironment(['alpha', 'beta']);

                const viewInitial = await set.getView();
                assertTrue(await viewInitial.has('alpha'), 'alpha should be present from initial elements');

                await set.delete('alpha');

                const viewAfterDelete = await set.getView();
                assertFalse(await viewAfterDelete.has('alpha'), 'alpha should be gone after delete');
                assertTrue(await viewAfterDelete.has('beta'), 'beta should survive unrelated delete');

                await set.add('alpha');

                const viewAfterReAdd = await set.getView();
                assertTrue(await viewAfterReAdd.has('alpha'), 'alpha should reappear after re-add');
                assertTrue(await viewAfterReAdd.has('beta'), 'beta should still be present');
            }
        },
        {
            name: '[SET07] Test barrier-delete then re-add of the same element',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                const addHash = await set.add('alpha');
                const addVersion = version(addHash);

                await set.deleteWithBarrier('alpha');

                const viewAfterBarrierDelete = await set.getView();
                assertFalse(await viewAfterBarrierDelete.has('alpha'), 'alpha should be gone after barrier delete');

                await set.add('alpha');

                const viewAfterReAdd = await set.getView();
                assertTrue(await viewAfterReAdd.has('alpha'), 'alpha should reappear after re-add following barrier delete');

                const historicalView = await set.getView(addVersion);
                assertTrue(await historicalView.has('alpha'), 'historical view should still see alpha at its original add');
            }
        },
        {
            name: '[SET08] Set without barrier support: basic add and delete work',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'no-barrier-set',
                    initialElements: ['alpha'],
                    hashAlgorithm: 'sha256',
                });

                const set = (await ctx.createObject(init)) as RSet;

                const view0 = await set.getView();
                assertTrue(await view0.has('alpha'), 'initial element alpha should be present');

                await set.add('beta');
                await set.add('gamma');

                const view1 = await set.getView();
                assertTrue(await view1.has('alpha'), 'alpha should still be present');
                assertTrue(await view1.has('beta'), 'beta should be present after add');
                assertTrue(await view1.has('gamma'), 'gamma should be present after add');

                await set.delete('beta');

                const view2 = await set.getView();
                assertFalse(await view2.has('beta'), 'beta should be gone after delete');
                assertTrue(await view2.has('alpha'), 'alpha should survive unrelated delete');
                assertTrue(await view2.has('gamma'), 'gamma should survive unrelated delete');
            }
        },
        {
            name: '[SET09] Set without barrier support: addWithBarrier throws',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'no-barrier-add-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const set = (await ctx.createObject(init)) as RSet;

                let threw = false;
                try {
                    await set.addWithBarrier('alpha');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'addWithBarrier should throw when barrier add is not supported');
            }
        },
        {
            name: '[SET10] Set without barrier support: deleteWithBarrier throws',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'no-barrier-delete-set',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });

                const set = (await ctx.createObject(init)) as RSet;

                await set.add('alpha');

                let threw = false;
                try {
                    await set.deleteWithBarrier('alpha');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'deleteWithBarrier should throw when barrier delete is not supported');
            }
        },
        {
            name: '[SET11] Deleting a non-existent element throws when acceptRedundantDelete is false',
            invoke: async () => {
                const { set } = await createTestEnvironment();

                let threw = false;
                try {
                    await set.delete('nonexistent');
                } catch (e) {
                    threw = true;
                }
                assertTrue(threw, 'delete of non-existent element should throw');

                await set.add('alpha');
                const view = await set.getView();
                assertTrue(await view.has('alpha'), 'set should remain usable after a failed delete');
            }
        },
        {
            name: '[SET14] a delete after a re-add removes the element at a cut that also holds an earlier add',
            invoke: async () => {
                const { set } = await createTestEnvironment();
                const added = await set.add('a');
                const fork = version(added);
                const a = await set.add('a', fork);
                const d = await set.delete('a', fork);
                const a2 = await set.add('a', version(d));
                const nd = await set.delete('a', version(a2));

                const view = await set.getView(version(a, nd), version(a, nd));
                assertFalse(await view.has('a'),
                    'the later delete is a barrier and it is in the cover, so the element is absent');
            }
        },
        {
            name: '[SET15] deleteByHash on a barrier set stores barrier: true',
            invoke: async () => {
                const { set } = await createTestEnvironment();
                await set.add('alpha');
                const deleted = await set.deleteByHash(hashElement('alpha'));
                const entry = await (await set.getScopedDag()).loadEntry(deleted);
                const payload = entry!.payload as { barrier?: boolean };
                assertTrue(payload.barrier === true, 'a plain delete on a barrier set is stored as a barrier');
            }
        },
        {
            name: '[SET16] a delete whose barrier bit disagrees with the set is rejected',
            invoke: async () => {
                const { set } = await createTestEnvironment();
                await set.add('alpha');
                const elementHash = hashElement('alpha');
                const at = await (await set.getScopedDag()).getFrontier();

                const missing = await set.validatePayload({ action: 'delete', elementHash }, at);
                assertTrue(!missing.valid && formatValidationFailure(missing.why).includes('barrier set requires barrier deletes'),
                    'a barrier set rejects a delete with no barrier bit');

                const bitFalse = await set.validatePayload({ action: 'delete', elementHash, barrier: false }, at);
                assertTrue(!bitFalse.valid && formatValidationFailure(bitFalse.why).includes('barrier set requires barrier deletes'),
                    'a barrier set rejects a delete with barrier: false');

                const plain = await createTestEnvironment([], { supportBarrierDelete: false });
                await plain.set.add('alpha');
                const plainAt = await (await plain.set.getScopedDag()).getFrontier();
                const bitTrue = await plain.set.validatePayload(
                    { action: 'delete', elementHash, barrier: true }, plainAt,
                );
                assertTrue(!bitTrue.valid && formatValidationFailure(bitTrue.why).includes('barrier delete is not supported'),
                    'a non-barrier set rejects a delete with barrier: true');
            }
        },
    ],
};
