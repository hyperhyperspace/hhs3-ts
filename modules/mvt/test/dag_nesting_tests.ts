import { testing } from '@hyper-hyper-space/hhs3_util';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';
import { dag, EntryMetaFilter, MetaProps, position, Position } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';
import { Literal } from '@hyper-hyper-space/hhs3_json/dist/literal.js';

import {
    DagScope,
    NestedScopedDag,
    RootScopedDag,
} from '../src/dag/dag_nesting.js';

function createTestDag(): dag.Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.flat.createFlatIndex(
        store,
        new dag.idx.flat.mem.MemFlatIndexStore(),
    );
    return dag.create(store, index, sha256);
}

async function testRootScopedLoadAllEntries() {
    const rawDag = createTestDag();
    const scopedDag = new RootScopedDag(rawDag);

    const h1 = await scopedDag.append({ n: 1 }, {});
    const h2 = await scopedDag.append({ n: 2 }, {}, position(h1));
    const h3 = await scopedDag.append({ n: 3 }, {}, position(h2));

    const hashes: string[] = [];
    const payloads: json.Literal[] = [];
    for await (const entry of scopedDag.loadAllEntries()) {
        hashes.push(entry.hash);
        payloads.push(entry.payload);
    }

    testing.assertTrue(hashes.length === 3, 'root scoped loadAllEntries should return every entry');
    testing.assertTrue(hashes[0] === h1 && hashes[1] === h2 && hashes[2] === h3, 'entries should be in topo order');
    testing.assertTrue(
        json.toStringNormalized(payloads[0]) === json.toStringNormalized({ n: 1 }),
        'payloads should match appended values',
    );
    testing.assertTrue(
        json.toStringNormalized(payloads[2]) === json.toStringNormalized({ n: 3 }),
        'last payload should match third append',
    );
}

const CHILD_SCOPE_ID = 'child-scope-test';

class MockChildScope implements DagScope {
    private start: Position;
    private context: string;

    constructor(start: Position = position(), context: string = CHILD_SCOPE_ID) {
        this.start = start;
        this.context = context;
    }

    signingContext(): Literal {
        return { child: this.context };
    }

    startAt(): Position {
        return this.start;
    }

    startEmpty(): boolean {
        return true;
    }

    baseFilter(): EntryMetaFilter {
        return { containsValues: { scope: [CHILD_SCOPE_ID] } };
    }

    wrapPayload(payload: Literal, _at: Position): Literal {
        return { wrapped: payload };
    }

    unwrapPayload(payload: Literal, _at: Position): Literal {
        const p = payload as { wrapped?: Literal };
        return p['wrapped'] ?? payload;
    }

    wrapMeta(meta: MetaProps, _wrappedPayload: Literal, _at: Position): MetaProps {
        return {
            ...meta,
            scope: json.toSet([CHILD_SCOPE_ID]),
        };
    }

    unwrapMeta(meta: MetaProps, _wrappedPayload: Literal, _at: Position): MetaProps {
        const inner: MetaProps = {};
        for (const key in meta) {
            if (key !== 'scope') {
                inner[key] = meta[key];
            }
        }
        return inner;
    }

    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter {
        return filter;
    }
}

async function testNestedScopedLoadAllEntries() {
    const rawDag = createTestDag();
    const rootScoped = new RootScopedDag(rawDag);
    const childScoped = new NestedScopedDag(rootScoped, new MockChildScope());

    const otherMeta: MetaProps = { scope: json.toSet(['other-scope']) };
    await rootScoped.append({ label: 'other' }, otherMeta);

    const h1 = await childScoped.append({ label: 'alpha' }, {});
    await childScoped.append({ label: 'beta' }, {}, position(h1));

    const childEntries: { label: string }[] = [];
    for await (const entry of childScoped.loadAllEntries()) {
        childEntries.push(entry.payload as { label: string });
    }

    testing.assertTrue(childEntries.length === 2, 'nested scoped loadAllEntries should only yield scoped entries');
    testing.assertTrue(
        childEntries[0]['label'] === 'alpha' && childEntries[1]['label'] === 'beta',
        'nested entries should have unwrapped payloads',
    );

    let rootCount = 0;
    for await (const _ of rootScoped.loadAllEntries()) {
        rootCount++;
    }
    testing.assertTrue(rootCount === 3, 'root scoped stream should still include all physical entries');
}

function samePosition(a: Position, b: Position): boolean {
    return a.size === b.size && [...a].every((h) => b.has(h));
}

async function testResolvePosition() {
    const rawDag = createTestDag();
    const rootScoped = new RootScopedDag(rawDag);

    const h0 = await rootScoped.append({ label: 'parent' }, { scope: json.toSet(['other-scope']) });
    testing.assertTrue(samePosition(await rootScoped.resolvePosition(), position(h0)),
        'root resolvePosition() should default to the frontier');
    testing.assertTrue(samePosition(await rootScoped.resolvePosition(position('x')), position('x')),
        'root resolvePosition(at) should pass at through');

    const childScoped = new NestedScopedDag(rootScoped, new MockChildScope(position(h0)));

    testing.assertTrue(samePosition(await childScoped.resolvePosition(), position(h0)),
        'an empty nested scope should resolve to its start position');
    testing.assertTrue(samePosition(await childScoped.resolvePosition(position()), position(h0)),
        'an explicit empty position should resolve to the start position');

    const resolved = await childScoped.resolvePosition();
    const h1 = await childScoped.append({ label: 'alpha' }, {}, resolved);
    const entry = await rawDag.loadEntry(h1);
    const prevs = position(...json.fromSet(entry!.header.prevEntryHashes));
    testing.assertTrue(samePosition(prevs, resolved),
        'the resolved position should be exactly the appended entry predecessors');

    testing.assertTrue(samePosition(await childScoped.resolvePosition(), position(h1)),
        'a non-empty nested scope should resolve to its own frontier');
    testing.assertTrue(samePosition(await childScoped.resolvePosition(position(h0, h1)), position(h0, h1)),
        'a non-empty explicit position should pass through');
}

async function testSigningScope() {
    const rootScoped = new RootScopedDag(createTestDag());
    const childA = new NestedScopedDag(rootScoped, new MockChildScope(position(), 'a'));
    const childAB = new NestedScopedDag(childA, new MockChildScope(position(), 'b'));

    testing.assertTrue(json.toStringNormalized(rootScoped.signingScope()) === '[]',
        'the root signing scope should be empty');
    testing.assertTrue(json.toStringNormalized(childA.signingScope()) === json.toStringNormalized([{ child: 'a' }]),
        'one nesting level should contribute its context');
    testing.assertTrue(json.toStringNormalized(childAB.signingScope()) === json.toStringNormalized([{ child: 'a' }, { child: 'b' }]),
        'two nesting levels should accumulate contexts outermost first');
}

export const dagNestingSuite = {
    title: 'DAG nesting (ScopedDag)',
    tests: [
        { name: '[DNS00] RootScopedDag loadAllEntries returns all entries in topo order', invoke: testRootScopedLoadAllEntries },
        { name: '[DNS01] NestedScopedDag loadAllEntries filters and unwraps one layer', invoke: testNestedScopedLoadAllEntries },
        { name: '[DNS02] resolvePosition matches the position append records', invoke: testResolvePosition },
        { name: '[DNS03] signingScope accumulates contexts through nesting', invoke: testSigningScope },
    ],
};
