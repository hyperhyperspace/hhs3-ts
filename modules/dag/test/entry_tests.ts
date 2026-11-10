import { sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { dag, Dag } from "../src/index.js";
import { MemLevelIndexStore } from "../src/idx/level/level_idx_mem_store.js";

function createMemDag(): Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.level.createDagLevelIndex(store, new MemLevelIndexStore());
    return dag.create(store, index, sha256);
}

async function rejects(run: () => Promise<unknown>): Promise<string | undefined> {
    try {
        await run();
    } catch (e: any) {
        return String(e.message);
    }
    return undefined;
}

// A lone surrogate has no UTF-8 encoding, so a payload holding one would hash
// like the same payload with U+FFFD in its place. Such a payload cannot become
// an entry.
async function testRejectsIllFormedPayload() {
    const d = createMemDag();
    const root = await d.append({ x: 1 }, {});

    const payloads: json.Literal[] = [
        { s: 'x\ud800y' },
        { s: 'x\ud801y' },
        { nested: [{ s: '\udc00' }] },
        { 'k\ud800': 1 },
    ];

    for (const payload of payloads) {
        const appendError = await rejects(() => d.append(payload, {}, dag.position(root)));
        assertTrue(appendError !== undefined && appendError.includes('not well-formed Unicode'),
            `append rejects ${JSON.stringify(payload)}: ${appendError}`);
        const hashError = await rejects(() => d.computeEntryHash(payload, dag.position(root)));
        assertTrue(hashError !== undefined && hashError.includes('not well-formed Unicode'),
            `computeEntryHash rejects ${JSON.stringify(payload)}: ${hashError}`);
    }

    const nullError = await rejects(() => d.append({ a: null } as unknown as json.Literal, {}, dag.position(root)));
    assertTrue(nullError !== undefined && nullError.includes('contains null'), `append rejects null: ${nullError}`);

    const frontier = await d.getFrontier();
    assertEquals(frontier.size, 1, 'nothing was appended');
    assertTrue(frontier.has(root), 'the frontier is still the root');
}

async function testAcceptsReplacementCharacter() {
    const d = createMemDag();
    const payload = { s: 'x�y', pair: 'a😀b' };

    const hash = await d.append(payload, {});
    const entry = await d.loadEntry(hash);
    assertTrue(entry !== undefined && json.eq(entry.payload, payload), 'the payload is stored as given');
    assertEquals(await d.computeEntryHash(payload), hash, 'the hash is the one computeEntryHash gives');
}

export const entrySuite = {
    title: "\n[ENTRY] Entry creation\n",
    tests: [
        { name: "[ENTRY_00] A payload with an ill-formed string or key is rejected (small DAG)", invoke: testRejectsIllFormedPayload },
        { name: "[ENTRY_01] U+FFFD and surrogate pairs are accepted (small DAG)",                invoke: testAcceptsReplacementCharacter },
    ],
};
