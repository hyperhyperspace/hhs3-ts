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

function nestedArrays(depth: number): json.Literal {
    let value: json.Literal = 1;
    for (let i = 0; i < depth; i++) value = [value];
    return value;
}

// A payload nests at most json.MAX_LITERAL_DEPTH levels. JSON text can nest
// far deeper, and the code that walks a payload runs out of stack at a depth
// that differs between runtimes, so the limit is fixed and counted.
async function testRejectsDeepPayload() {
    const d = createMemDag();
    const root = await d.append({ x: 1 }, {});

    const tooDeep = nestedArrays(json.MAX_LITERAL_DEPTH + 1);
    const appendError = await rejects(() => d.append(tooDeep, {}, dag.position(root)));
    assertTrue(appendError !== undefined && appendError.includes('more than 128 levels of nesting'),
        `append rejects a payload nested one level too deep: ${appendError}`);
    const hashError = await rejects(() => d.computeEntryHash(tooDeep, dag.position(root)));
    assertTrue(hashError !== undefined && hashError.includes('more than 128 levels of nesting'),
        `computeEntryHash rejects it too: ${hashError}`);

    const frontier = await d.getFrontier();
    assertEquals(frontier.size, 1, 'nothing was appended');
    assertTrue(frontier.has(root), 'the frontier is still the root');

    const atLimit = nestedArrays(json.MAX_LITERAL_DEPTH);
    const hash = await d.append(atLimit, {}, dag.position(root));
    const entry = await d.loadEntry(hash);
    assertTrue(entry !== undefined && json.eq(entry.payload, atLimit), 'a payload nested exactly to the limit is stored');
    assertEquals(await d.computeEntryHash(atLimit, dag.position(root)), hash, 'the hash is the one computeEntryHash gives');
}

// What gets hashed is the RFC 8785 text of the payload: arrays in index order,
// keys sorted, control characters escaped. Each expected text is written out
// here and hashed without the serializer.
async function testHashInputIsRfc8785() {
    const d = createMemDag();
    const utf8 = new TextEncoder();
    const hashOf = (text: string) => sha256.hashToB64(utf8.encode(text));
    const NL = String.fromCharCode(10), TAB = String.fromCharCode(9), BS = String.fromCharCode(92);

    const cases: [string, json.Literal, string][] = [
        ['a 12-element array', { list: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] }, '{"list":[0,1,2,3,4,5,6,7,8,9,10,11]}'],
        ['a newline and a tab', { text: 'a' + NL + 'b' + TAB + 'c' }, '{"text":"a' + BS + 'nb' + BS + 'tc"}'],
        ['keys given out of order', { b: 1, a: [true, 'x'] }, '{"a":[true,"x"],"b":1}'],
    ];

    let prev: string | undefined = undefined;
    for (const [label, payload, text] of cases) {
        const hash: string = await d.append(payload, {}, prev === undefined ? undefined : dag.position(prev));
        const entry = await d.loadEntry(hash);
        const payloadHash = hashOf(text);
        assertEquals(entry!.header.payloadHash, payloadHash, `payload hash of ${label}`);

        const prevs = prev === undefined ? '{}' : '{"' + prev + '":""}';
        assertEquals(hash, hashOf('{"payloadHash":"' + payloadHash + '","prevEntryHashes":' + prevs + '}'), `entry hash of ${label}`);
        prev = hash;
    }
}

export const entrySuite = {
    title: "\n[ENTRY] Entry creation\n",
    tests: [
        { name: "[ENTRY_00] A payload with an ill-formed string or key is rejected (small DAG)", invoke: testRejectsIllFormedPayload },
        { name: "[ENTRY_01] U+FFFD and surrogate pairs are accepted (small DAG)",                invoke: testAcceptsReplacementCharacter },
        { name: "[ENTRY_02] A payload nested deeper than 128 levels is rejected (small DAG)",    invoke: testRejectsDeepPayload },
        { name: "[ENTRY_03] The hash input is the RFC 8785 text of the payload (small DAG)",     invoke: testHashInputIsRfc8785 },
    ],
};
