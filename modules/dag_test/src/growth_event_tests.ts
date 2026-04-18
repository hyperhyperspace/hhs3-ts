import { Dag, position } from "@hyper-hyper-space/hhs3_dag";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { DagFactory } from "./backend_tests.js";

async function testListenerFiresOnAppend(factory: DagFactory) {
    const d = await factory();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);

    await d.append({ x: 1 }, {});
    assertTrue(called >= 1, 'listener should have been called at least once');

    d.removeListener(listener);
}

async function testListenerNotCalledBeforeAppend(factory: DagFactory) {
    const d = await factory();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);

    assertTrue(called === 0, 'listener should not have been called before any append');

    await d.append({ x: 1 }, {});
    assertTrue(called >= 1, 'listener should fire after append');

    d.removeListener(listener);
}

async function testMultipleListenersAllFire(factory: DagFactory) {
    const d = await factory();
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

async function testRemoveListenerStopsDelivery(factory: DagFactory) {
    const d = await factory();
    let called = 0;
    const listener = () => { called++; };
    d.addListener(listener);
    d.removeListener(listener);

    await d.append({ x: 1 }, {});
    assertTrue(called === 0, 'removed listener should not have been called');
}

async function testListenerFiresOncePerTransaction(factory: DagFactory) {
    const d = await factory();
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

async function testFrontierUpToDateWhenListenerFires(factory: DagFactory) {
    const d = await factory();
    let frontierInListener: Set<string> | undefined;

    const h1 = await d.append({ first: true }, {});

    const listener = () => {
        d.getFrontier().then(f => { frontierInListener = f; });
    };
    d.addListener(listener);

    const h2 = await d.append({ second: true }, {}, position(h1));

    // getFrontier() inside the listener is async; give it a microtask to resolve
    await new Promise(resolve => setTimeout(resolve, 10));

    assertTrue(frontierInListener !== undefined, 'listener should have captured frontier');
    assertTrue(frontierInListener!.has(h2), 'frontier seen by listener should include the new entry');
    assertTrue(!frontierInListener!.has(h1), 'frontier seen by listener should not include the superseded entry');

    d.removeListener(listener);
}

export function createGrowthEventSuite(
    tag: string,
    factory: DagFactory
): { title: string; tests: Array<{ name: string; invoke: () => Promise<void> }> } {
    const t = (n: number) => `[${tag}_${String(n).padStart(2, '0')}]`;
    return {
        title: `\n[${tag}] Growth Event Tests\n`,
        tests: [
            { name: `${t(0)} Listener fires on append`,                invoke: () => testListenerFiresOnAppend(factory) },
            { name: `${t(1)} Listener not called before first append`,  invoke: () => testListenerNotCalledBeforeAppend(factory) },
            { name: `${t(2)} Multiple listeners all fire`,              invoke: () => testMultipleListenersAllFire(factory) },
            { name: `${t(3)} removeListener stops delivery`,            invoke: () => testRemoveListenerStopsDelivery(factory) },
            { name: `${t(4)} Listener fires once per transaction`,      invoke: () => testListenerFiresOncePerTransaction(factory) },
            { name: `${t(5)} Frontier up to date when listener fires`,  invoke: () => testFrontierUpToDateWhenListenerFires(factory) },
        ],
    };
}
