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
    startAt(): Position {
        return position();
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

export const dagNestingSuite = {
    title: 'DAG nesting (ScopedDag)',
    tests: [
        { name: '[DNS00] RootScopedDag loadAllEntries returns all entries in topo order', invoke: testRootScopedLoadAllEntries },
        { name: '[DNS01] NestedScopedDag loadAllEntries filters and unwraps one layer', invoke: testNestedScopedLoadAllEntries },
    ],
};
