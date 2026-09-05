import { testing } from '@hyper-hyper-space/hhs3_util';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';
import { dag } from '@hyper-hyper-space/hhs3_dag';
import { ScopedDagSubscription } from '../src/mvt.js';
import { RootScopedDag } from '../src/dag/dag_nesting.js';

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore(),
    );
    return dag.create(store, index, sha256);
}

function countListeners(d: dag.Dag): { added: number; removed: number } {
    const counts = { added: 0, removed: 0 };
    const origAdd = d.addListener.bind(d);
    const origRemove = d.removeListener.bind(d);
    d.addListener = (listener) => {
        counts.added++;
        origAdd(listener);
    };
    d.removeListener = (listener) => {
        counts.removed++;
        origRemove(listener);
    };
    return counts;
}

async function testSubscribeResolvesAfterAddListener() {
    const raw = createTestDag();
    const counts = countListeners(raw);
    const scoped = new RootScopedDag(raw);

    let resolveHeld: (v: RootScopedDag) => void = () => { throw new Error('unresolved'); };
    const held = new Promise<RootScopedDag>((r) => { resolveHeld = r; });
    const sub = new ScopedDagSubscription(() => held);

    let fired = 0;
    const pending = sub.subscribe(() => { fired++; });
    testing.assertTrue(counts.added === 0, 'listener is not armed before the provider resolves');

    resolveHeld(scoped);
    await pending;
    testing.assertTrue(counts.added === 1, 'subscribe resolves only after addListener');

    await scoped.append({ n: 1 }, {});
    testing.assertTrue(fired >= 1, 'a mutation after await subscribe notifies without a timer flush');
}

async function testSecondSubscribeSharesInFlightGate() {
    const raw = createTestDag();
    const counts = countListeners(raw);
    const scoped = new RootScopedDag(raw);

    let resolveHeld: (v: RootScopedDag) => void = () => { throw new Error('unresolved'); };
    const held = new Promise<RootScopedDag>((r) => { resolveHeld = r; });
    let providerCalls = 0;
    const sub = new ScopedDagSubscription(() => {
        providerCalls++;
        return held;
    });

    let aDone = false;
    let bDone = false;
    const pA = sub.subscribe(() => {}).then(() => { aDone = true; });
    const pB = sub.subscribe(() => {}).then(() => { bDone = true; });

    testing.assertTrue(!aDone && !bDone, 'neither subscribe resolves before the provider');
    testing.assertTrue(providerCalls === 1, 'a second subscribe while attach is pending does not re-open');

    resolveHeld(scoped);
    await Promise.all([pA, pB]);
    testing.assertTrue(aDone && bDone, 'both waiters resolve on the shared gate');
    testing.assertTrue(counts.added === 1, 'the shared gate arms the physical DAG once');
}

async function testUnsubscribeDuringAttachDoesNotLeak() {
    const raw = createTestDag();
    const counts = countListeners(raw);
    const scoped = new RootScopedDag(raw);

    let resolveHeld: (v: RootScopedDag) => void = () => { throw new Error('unresolved'); };
    const held = new Promise<RootScopedDag>((r) => { resolveHeld = r; });
    const sub = new ScopedDagSubscription(() => held);

    const cb = () => {};
    const pending = sub.subscribe(cb);
    sub.unsubscribe(cb);
    resolveHeld(scoped);
    await pending;

    testing.assertTrue(counts.added === 0, 'bailing attach never calls addListener');
    testing.assertTrue(counts.removed === 0, 'no listener to remove');
}

async function testResubscribeAfterFullUnsubscribeRearms() {
    const raw = createTestDag();
    const counts = countListeners(raw);
    const scoped = new RootScopedDag(raw);
    const sub = new ScopedDagSubscription(async () => scoped);

    const cb1 = () => {};
    await sub.subscribe(cb1);
    testing.assertTrue(counts.added === 1, 'first subscribe arms');
    sub.unsubscribe(cb1);
    testing.assertTrue(counts.removed === 1, 'last unsubscribe disarms');

    const cb2 = () => {};
    await sub.subscribe(cb2);
    testing.assertTrue(counts.added === 2, 'a later subscribe re-arms (attachGate was cleared)');
}

async function testStaleGateDoesNotClearNewAttach() {
    const raw = createTestDag();
    const counts = countListeners(raw);
    const scoped = new RootScopedDag(raw);

    const resolvers: Array<(v: RootScopedDag) => void> = [];
    const sub = new ScopedDagSubscription(() => {
        return new Promise<RootScopedDag>((r) => { resolvers.push(r); });
    });

    const cb1 = () => {};
    const pA = sub.subscribe(cb1);
    sub.unsubscribe(cb1);
    const cb2 = () => {};
    const pB = sub.subscribe(cb2);

    testing.assertTrue(resolvers.length === 2, 'unsubscribe-all then resubscribe starts a fresh attach');

    resolvers[0](scoped);
    await pA;
    testing.assertTrue(counts.added === 1, 'stale gate A may arm when callbacks are present again');

    resolvers[1](scoped);
    await pB;
    testing.assertTrue(counts.added === 1, 'gate B sees the listener already on and does not re-arm');
    testing.assertTrue(counts.removed === 0, 'stale gate A must not detach the new listener');
}

export const subscribeSuite = {
    title: 'ScopedDagSubscription attach gate',
    tests: [
        { name: '[SUB-GATE00] subscribe resolves only after addListener', invoke: testSubscribeResolvesAfterAddListener },
        { name: '[SUB-GATE01] second subscribe while attach pending shares the gate', invoke: testSecondSubscribeSharesInFlightGate },
        { name: '[SUB-GATE02] unsubscribe during in-flight subscribe does not leak a listener', invoke: testUnsubscribeDuringAttachDoesNotLeak },
        { name: '[SUB-GATE03] after full unsubscribe a new subscribe re-arms', invoke: testResubscribeAfterFullUnsubscribeRearms },
        { name: '[SUB-GATE04] stale gate after unsubscribe-all + resubscribe does not disarm the new listener', invoke: testStaleGateDoesNotClearNewAttach },
    ],
};
