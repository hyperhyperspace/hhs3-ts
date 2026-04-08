import { dag, Dag, position, Position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import { set } from "@hyper-hyper-space/hhs3_util";
import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createD1, createD3 } from "./dag_create.js";

export type DagFactory = () => Dag | Promise<Dag>;

async function testBasicAppendAndLoad(factory: DagFactory) {
    const d = await factory();

    const h1 = await d.append({ 'a': 1 }, {});
    const h2 = await d.append({ 'b': 2 }, { 'key': json.toSet(['val']) }, position(h1));
    const h3 = await d.append({ 'c': 3 }, {}, position(h1));

    const e1 = await d.loadEntry(h1);
    assertTrue(e1 !== undefined, 'entry 1 should exist');
    assertTrue(json.equals(e1!.payload, { 'a': 1 }), 'entry 1 payload mismatch');

    const e2 = await d.loadEntry(h2);
    assertTrue(e2 !== undefined, 'entry 2 should exist');
    assertTrue(json.equals(e2!.payload, { 'b': 2 }), 'entry 2 payload mismatch');

    const hdr = await d.loadHeader(h1);
    assertTrue(hdr !== undefined, 'header 1 should exist');

    const missing = await d.loadEntry('nonexistent');
    assertTrue(missing === undefined, 'missing entry should be undefined');
}

async function testFrontierTracking(factory: DagFactory) {
    const d = await factory();

    const h1 = await d.append({ 'a': 1 }, {});

    let frontier = await d.getFrontier();
    assertTrue(frontier.size === 1, 'frontier should have 1 entry');
    assertTrue(frontier.has(h1), 'frontier should contain h1');

    const h2 = await d.append({ 'b': 2 }, {}, position(h1));

    frontier = await d.getFrontier();
    assertTrue(frontier.size === 1, 'frontier should still have 1 entry after linear append');
    assertTrue(frontier.has(h2), 'frontier should contain h2');

    const h3 = await d.append({ 'c': 3 }, {}, position(h1));

    frontier = await d.getFrontier();
    assertTrue(frontier.size === 2, 'frontier should have 2 entries after fork');
    assertTrue(frontier.has(h2), 'frontier should contain h2 after fork');
    assertTrue(frontier.has(h3), 'frontier should contain h3 after fork');

    const h4 = await d.append({ 'd': 4 }, {}, position(h2, h3));

    frontier = await d.getFrontier();
    assertTrue(frontier.size === 1, 'frontier should have 1 entry after merge');
    assertTrue(frontier.has(h4), 'frontier should contain h4 after merge');
}

async function testLoadAllEntries(factory: DagFactory) {
    const d = await factory();

    const h1 = await d.append({ 'a': 1 }, {});
    const h2 = await d.append({ 'b': 2 }, {}, position(h1));
    const h3 = await d.append({ 'c': 3 }, {}, position(h2));

    const entries = [];
    for await (const e of d.loadAllEntries()) {
        entries.push(e);
    }

    assertEquals(entries.length, 3, 'should load 3 entries');
    assertEquals(entries[0].hash, h1, 'first entry should be h1 (topo order)');
    assertEquals(entries[1].hash, h2, 'second entry should be h2');
    assertEquals(entries[2].hash, h3, 'third entry should be h3');
}

async function testForkPosition(factory: DagFactory) {
    const d = await factory();

    const a = await d.append({ 'a': 1 }, {});
    const b1 = await d.append({ 'b1': 1 }, {}, position(a));
    const b2 = await d.append({ 'b2': 1 }, {}, position(a));

    const fp = await d.findForkPosition(new Set([b1]), new Set([b2]));

    assertTrue(fp.commonFrontier.has(a), 'commonFrontier should contain a');
    assertTrue(fp.forkA.has(b1), 'forkA should contain b1');
    assertTrue(fp.forkB.has(b2), 'forkB should contain b2');
}

async function testMinimalCover(factory: DagFactory) {
    const d = await factory();

    const a = await d.append({ 'a': 1 }, {});
    const b = await d.append({ 'b': 1 }, {}, position(a));
    const c = await d.append({ 'c': 1 }, {}, position(b));

    const cover = await d.findMinimalCover(new Set([a, b, c]));
    assertTrue(cover.size === 1, 'minimal cover should have 1 entry');
    assertTrue(cover.has(c), 'minimal cover should contain only c');
}

async function testCoverWithFilter(factory: DagFactory) {
    const d = await factory();
    const h = await createD3(d);

    const cp1 = await d.findCoverWithFilter(
        position(h['b1'], h['b2']),
        { containsKeys: ['p1'] }
    );
    assertTrue(set.eq(cp1, position(h['b1'], h['b2'])), 'filter on p1 key failed');

    const cp2 = await d.findCoverWithFilter(
        position(h['b1'], h['b2']),
        { containsValues: { p2: ['2'] } }
    );
    assertTrue(set.eq(cp2, position(h['b2'])), 'filter on p2 value failed');

    const cp23 = await d.findCoverWithFilter(
        position(h['b1'], h['b2']),
        { containsValues: { p2: ['3'] } }
    );
    assertTrue(set.eq(cp23, position()), 'filter on p2=3 should return empty');

    const cp2too = await d.findCoverWithFilter(
        position(h['c1'], h['b2']),
        { containsKeys: ['p1', 'p2'] }
    );
    assertTrue(set.eq(cp2too, position(h['c1'], h['b2'])), 'filter on p1+p2 keys failed');
}

async function testConcurrentCoverWithFilter(factory: DagFactory) {
    const d = await factory();
    const h = await createD3(d);

    const cc1 = await d.findConcurrentCoverWithFilter(
        position(h['c1'], h['b2']),
        position(h['b1']),
        { containsKeys: ['p1'] }
    );
    assertTrue(set.eq(cc1, position(h['b2'])), 'concurrent (b1) filter on p1 key failed');

    const cc2 = await d.findConcurrentCoverWithFilter(
        position(h['c1']),
        position(h['b1']),
        { containsValues: { p2: ['4'] } }
    );
    assertTrue(set.eq(cc2, position()), 'concurrent (b1) filter on p2=4 should return empty');

    const cc3 = await d.findConcurrentCoverWithFilter(
        position(h['d1'], h['d2']),
        position(h['d1']),
        { containsKeys: ['p1'] }
    );
    assertTrue(set.eq(cc3, position(h['d2'])), 'concurrent (d1) filter on p1 key failed');

    const cc4 = await d.findConcurrentCoverWithFilter(
        position(h['d1'], h['d2'], h['b2']),
        position(h['d1']),
        { containsKeys: ['p1'] }
    );
    assertTrue(set.eq(cc4, position(h['d2'], h['b2'])), 'concurrent (d1) filter on p1 key failed (v2)');
}

async function testDagCopy(factory: DagFactory) {
    const src = await factory();
    const dst = await factory();

    const a = await src.append({ 'a': 1 }, {});
    const b = await src.append({ 'b': 1 }, { 'key': json.toSet(['v']) }, position(a));
    const c = await src.append({ 'c': 1 }, {}, position(a));
    const d_hash = await src.append({ 'd': 1 }, {}, position(b, c));

    await dag.copy(src, dst);

    const srcFrontier = await src.getFrontier();
    const dstFrontier = await dst.getFrontier();
    assertTrue(set.eq(srcFrontier, dstFrontier), 'frontiers should match after copy');

    const dstEntry = await dst.loadEntry(d_hash);
    assertTrue(dstEntry !== undefined, 'copied entry should exist');
}

export function createBackendTestSuite(
    tag: string,
    factory: DagFactory
): { title: string; tests: Array<{ name: string; invoke: () => Promise<void> }> } {
    const t = (n: number) => `[${tag}_${String(n).padStart(2, '0')}]`;
    return {
        title: `\n[${tag}] Backend Conformance Tests\n`,
        tests: [
            { name: `${t(0)} Basic append and load`,           invoke: () => testBasicAppendAndLoad(factory) },
            { name: `${t(1)} Frontier tracking`,               invoke: () => testFrontierTracking(factory) },
            { name: `${t(2)} Load all entries in topo order`,  invoke: () => testLoadAllEntries(factory) },
            { name: `${t(3)} Fork position`,                   invoke: () => testForkPosition(factory) },
            { name: `${t(4)} Minimal cover`,                   invoke: () => testMinimalCover(factory) },
            { name: `${t(5)} Cover with filter`,               invoke: () => testCoverWithFilter(factory) },
            { name: `${t(6)} Concurrent cover with filter`,    invoke: () => testConcurrentCoverWithFilter(factory) },
            { name: `${t(7)} DAG copy`,                        invoke: () => testDagCopy(factory) },
        ],
    };
}
