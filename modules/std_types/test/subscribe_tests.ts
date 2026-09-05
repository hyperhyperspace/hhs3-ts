import { RContext, RootScopedDag, Version } from "@hyper-hyper-space/hhs3_mvt";
import { RSet, rSetFactory } from "../src/types/rset/rset.js";
import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createMockRContext } from "./mock_rcontext.js";

function createTestCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSet.typeId, rSetFactory);
    return ctx;
}

function versionsEqual(a: Version, b: Version): boolean {
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}

export const subscribeTests = {
    title: '[SUB] RObject.subscribe reactivity tests',
    tests: [
        {
            name: '[SUB00] Nested mutation notifies the element and its ancestor, but not siblings',
            invoke: async () => {
                const ctx = createTestCtx();

                const outerInit = await RSet.create({
                    seed: 'sub-outer',
                    contentType: RSet.typeId,
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const outerSet = (await ctx.createObject(outerInit)) as RSet;

                const addNested = async (seed: string) => {
                    const init = await RSet.create({ seed, initialElements: [], hashAlgorithm: 'sha256' });
                    const hash = await outerSet.add(init);
                    const view = await outerSet.getView();
                    return (await view.loadRObjectByHash(hash)) as RSet;
                };

                const setA = await addNested('sub-nested-A');
                const setB = await addNested('sub-nested-B');

                let outerCount = 0;
                let aCount = 0;
                let bCount = 0;

                await outerSet.subscribe(() => { outerCount++; });
                await setA.subscribe(() => { aCount++; });
                await setB.subscribe(() => { bCount++; });

                await outerSet.getView();
                await setA.getView();
                await setB.getView();

                const outerBefore = outerCount;

                await setA.add('only-in-a');

                assertTrue(aCount >= 1, 'the mutated nested set A should be notified');
                assertTrue(outerCount > outerBefore, 'the ancestor (root) set should be notified of nested growth');
                assertTrue(bCount === 0, 'sibling set B must NOT be notified (no false positive)');
            }
        },
        {
            name: '[SUB01] Lazy activation: physical DAG is observed only while a subscriber is registered',
            invoke: async () => {
                const ctx = createTestCtx();

                const d = (await ctx.getDag('sub-lazy-dag'))!;

                let added = 0;
                let removed = 0;
                const origAdd = d.addListener.bind(d);
                const origRemove = d.removeListener.bind(d);
                (d as unknown as { addListener: (l: unknown) => void }).addListener = (l: unknown) => {
                    added++;
                    return origAdd(l as never);
                };
                (d as unknown as { removeListener: (l: unknown) => void }).removeListener = (l: unknown) => {
                    removed++;
                    return origRemove(l as never);
                };

                const root = new RootScopedDag(d);
                assertTrue(added === 0, 'no physical listener before any subscriber');

                const l1 = () => {};
                const l2 = () => {};

                root.addListener(l1);
                assertTrue(added === 1, 'first subscriber arms the physical DAG');

                root.addListener(l2);
                assertTrue(added === 1, 'a second subscriber does not re-arm the physical DAG');

                root.removeListener(l1);
                assertTrue(removed === 0, 'still armed while one subscriber remains');

                root.removeListener(l2);
                assertTrue(removed === 1, 'the last unsubscribe disarms the physical DAG');
            }
        },
        {
            name: '[SUB02] Consumer contract: subscribe, read baseline, then a change fires with a forward-moved version',
            invoke: async () => {
                const ctx = createTestCtx();

                const init = await RSet.create({
                    seed: 'sub-contract',
                    initialElements: [],
                    hashAlgorithm: 'sha256',
                });
                const set = (await ctx.createObject(init)) as RSet;

                let notifications = 0;
                let lastVersion: Version | undefined = undefined;
                await set.subscribe((v: Version) => { notifications++; lastVersion = v; });

                const baseline = await set.getScopedDag().then((sd) => sd.getFrontier());

                const addHash = await set.add('hello');

                assertTrue(notifications >= 1, 'at least one notification should follow the change');
                assertTrue(lastVersion !== undefined, 'the notification should carry a version');
                assertFalse(versionsEqual(lastVersion!, baseline), 'the delivered version should move forward from the baseline');
                assertTrue(lastVersion!.has(addHash), 'the delivered frontier should include the appended entry');
            }
        },
    ],
};
