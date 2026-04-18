import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { dag, Dag, position } from "../src/index.js";
import { MemLevelIndexStore } from "../src/idx/level/level_idx_mem_store.js";

function createMemDag(): Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.level.createDagLevelIndex(store, new MemLevelIndexStore());
    return dag.create(store, index, sha256);
}

async function testListenerFiresOnAppend() {
    const d = createMemDag();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);

    await d.append({ x: 1 }, {});
    assertTrue(called >= 1, 'listener should have been called at least once');

    d.removeListener(listener);
}

async function testListenerNotCalledBeforeAppend() {
    const d = createMemDag();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);

    assertTrue(called === 0, 'listener should not have been called before any append');

    await d.append({ x: 1 }, {});
    assertTrue(called >= 1, 'listener should fire after append');

    d.removeListener(listener);
}

async function testMultipleListenersAllFire() {
    const d = createMemDag();
    let calledA = 0;
    let calledB = 0;
    const listenerA = () => { calledA++; };
    const listenerB = () => { calledB++; };
    d.addListener(listenerA);
    d.addListener(listenerB);

    await d.append({ x: 1 }, {});
    assertTrue(calledA >= 1, 'listener A should have been called');
    assertTrue(calledB >= 1, 'listener B should have been called');

    d.removeListener(listenerA);
    d.removeListener(listenerB);
}

async function testRemoveListenerStopsDelivery() {
    const d = createMemDag();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);
    d.removeListener(listener);

    await d.append({ x: 1 }, {});
    assertTrue(called === 0, 'removed listener should not have been called');
}

async function testListenerFiresOncePerTransaction() {
    const d = createMemDag();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);

    await d.append({ a: 1 }, {});
    const countAfterFirst = called;

    await d.append({ b: 2 }, {});
    assertTrue(called >= 2, 'listener should fire at least once per append (got ' + called + ')');
    assertTrue(countAfterFirst >= 1, 'listener should have fired after first append');

    d.removeListener(listener);
}

async function testFrontierUpToDateWhenListenerFires() {
    const d = createMemDag();
    let frontierInListener: Set<string> | undefined;

    const h1 = await d.append({ first: true }, {});

    const listener = () => {
        d.getFrontier().then(f => { frontierInListener = f; });
    };
    d.addListener(listener);

    const h2 = await d.append({ second: true }, {}, position(h1));

    await new Promise(resolve => setTimeout(resolve, 10));

    assertTrue(frontierInListener !== undefined, 'listener should have captured frontier');
    assertTrue(frontierInListener!.has(h2), 'frontier seen by listener should include the new entry');
    assertTrue(!frontierInListener!.has(h1), 'frontier seen by listener should not include the superseded entry');

    d.removeListener(listener);
}

export const growthEventSuite = {
    title: "\n[MEM_GROW] Memory DagStore Growth Event Tests\n",
    tests: [
        { name: "[MEM_GROW_00] Listener fires on append",                invoke: testListenerFiresOnAppend },
        { name: "[MEM_GROW_01] Listener not called before first append",  invoke: testListenerNotCalledBeforeAppend },
        { name: "[MEM_GROW_02] Multiple listeners all fire",              invoke: testMultipleListenersAllFire },
        { name: "[MEM_GROW_03] removeListener stops delivery",            invoke: testRemoveListenerStopsDelivery },
        { name: "[MEM_GROW_04] Listener fires once per transaction",      invoke: testListenerFiresOncePerTransaction },
        { name: "[MEM_GROW_05] Frontier up to date when listener fires",  invoke: testFrontierUpToDateWhenListenerFires },
    ],
};
