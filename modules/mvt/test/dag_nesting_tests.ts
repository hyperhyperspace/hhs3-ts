import { testing } from '@hyper-hyper-space/hhs3_util';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';
import { dag, EntryMetaFilter, EntryPredicate, joinFilters, MetaProps, position, Position } from '@hyper-hyper-space/hhs3_dag';
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

// A child scope whose id is both the meta tag and the base filter, so two
// siblings can share one root DAG.
class IdScope implements DagScope {
    constructor(private readonly id: string, private readonly start: Position = position()) {}

    signingContext(): Literal {
        return { child: this.id };
    }

    startAt(): Position {
        return this.start;
    }

    startEmpty(): boolean {
        return true;
    }

    baseFilter(): EntryMetaFilter {
        return { containsValues: { scope: [this.id] } };
    }

    wrapPayload(payload: Literal, _at: Position): Literal {
        return { wrapped: payload };
    }

    unwrapPayload(payload: Literal, _at: Position): Literal {
        const p = payload as { wrapped?: Literal };
        return p['wrapped'] ?? payload;
    }

    wrapMeta(meta: MetaProps, _wrappedPayload: Literal, _at: Position): MetaProps {
        return { ...meta, scope: json.toSet([this.id]) };
    }

    unwrapMeta(meta: MetaProps, _wrappedPayload: Literal, _at: Position): MetaProps {
        const inner: MetaProps = {};
        for (const key in meta) {
            if (key !== 'scope') inner[key] = meta[key];
        }
        return inner;
    }

    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter {
        return filter;
    }
}

const BARRIER: MetaProps = { barrier: json.toSet(['t']) };
const BARRIER_FILTER: EntryMetaFilter = { containsValues: { barrier: ['t'] } };

function scopedPair(id: string): { raw: dag.Dag; root: RootScopedDag; child: NestedScopedDag; scope: IdScope } {
    const raw = createTestDag();
    const root = new RootScopedDag(raw);
    const scope = new IdScope(id);
    return { raw, root, child: new NestedScopedDag(root, scope), scope };
}

// c in scope, then an in-scope barrier r and an out-of-scope i, both after c.
// The use point {i} is outside the scope, and r is still concurrent with it.
async function testUsePointOutsideScope() {
    const { root, child } = scopedPair('in');
    const c = await child.append({ n: 'c' }, {});
    const r = await child.append({ n: 'r' }, BARRIER, position(c));
    const i = await root.append({ n: 'i' }, {}, position(c));

    const cover = await child.findConcurrentCoverWithFilter(position(r, i), position(i), BARRIER_FILTER);
    testing.assertTrue(samePosition(cover, position(r)),
        'an in-scope barrier concurrent with an out-of-scope use point is in the cover');
}

// n1 in scope and s outside it are concurrent. Barrier b is after n1 only,
// so it is concurrent with the mixed cut {n1, s}.
async function testMixedHeads() {
    const { root, child } = scopedPair('in');
    const n1 = await child.append({ n: 'n1' }, {}, position());
    const s = await root.append({ n: 's' }, {}, position());
    const b = await child.append({ n: 'b' }, BARRIER, position(n1));

    const cover = await child.findConcurrentCoverWithFilter(position(b, s), position(n1, s), BARRIER_FILTER);
    testing.assertTrue(cover.has(b),
        'a barrier after only the in-scope head is concurrent with the mixed cut');
}

// s1 is after n1, so {s1} and {n1, s1} have the same causal past. The barrier
// appended at {n1} is concurrent with both.
async function testCausalPast() {
    const { root, child } = scopedPair('in');
    const n1 = await child.append({ n: 'n1' }, {});
    const s1 = await root.append({ n: 's1' }, {}, position(n1));
    const r = await child.append({ n: 'r' }, BARRIER, position(n1));
    const from = position(r, s1);

    const atS1 = await child.findConcurrentCoverWithFilter(from, position(s1), BARRIER_FILTER);
    const atBoth = await child.findConcurrentCoverWithFilter(from, position(n1, s1), BARRIER_FILTER);
    testing.assertTrue(samePosition(atS1, atBoth),
        'a dominated in-scope head must not change the scoped concurrent cover');
    testing.assertTrue(atS1.has(r) && atBoth.has(r),
        'the barrier at {n1} is concurrent with {s1} and with {n1, s1}');
}

function lcg(seed: number): () => number {
    let state = seed >>> 0;
    return () => {
        state = (Math.imul(1664525, state) + 1013904223) >>> 0;
        return state / 0x100000000;
    };
}

// On a DAG mixing two scopes and parent entries, the scoped call equals the
// raw call with the scope filter joined, and it ignores dominated heads.
async function testRandomizedScopedConcurrentCover() {
    const raw = createTestDag();
    const root = new RootScopedDag(raw);
    const scopeA = new IdScope('a');
    const childA = new NestedScopedDag(root, scopeA);
    const childB = new NestedScopedDag(root, new IdScope('b'));
    const rand = lcg(20261023);
    const pick = <T>(xs: T[]): T => xs[Math.floor(rand() * xs.length)]!;

    const hashes: string[] = [];
    const writers = [childA, childB, root];
    for (let i = 0; i < 40; i++) {
        const after = hashes.length === 0
            ? position()
            : position(pick(hashes), ...(rand() < 0.5 ? [pick(hashes)] : []));
        const meta: MetaProps = rand() < 0.4 ? { ...BARRIER } : {};
        const hash = await pick(writers).append({ i }, meta, after);
        hashes.push(hash);
    }

    const frontier = await raw.getFrontier();
    const joined = joinFilters(scopeA.baseFilter(), scopeA.wrapFilter({}));
    for (let n = 0; n < 50; n++) {
        const heads = [pick(hashes)];
        if (rand() < 0.7) heads.push(pick(hashes));
        if (rand() < 0.4) heads.push(pick(hashes));
        const S = position(...heads);
        const from = rand() < 0.5 ? frontier : position(pick(hashes), ...(rand() < 0.5 ? [pick(hashes)] : []));

        const scoped = await childA.findConcurrentCoverWithFilter(from, S, {});
        const rawCover = await raw.findConcurrentCoverWithFilter(from, S, joined);
        testing.assertTrue(samePosition(scoped, rawCover),
            'the scoped concurrent cover is the raw call with the scope filter joined');

        const minimal = await raw.findMinimalCover(S);
        const onMinimal = await childA.findConcurrentCoverWithFilter(from, minimal, {});
        testing.assertTrue(samePosition(scoped, onMinimal),
            'a dominated head must not change the scoped concurrent cover');
    }
}

// A scope that wraps the payload under its id and tags the meta with a key of
// its own, so that scopes can be stacked.
class LayerScope implements DagScope {
    constructor(private readonly id: string) {}

    private tag(): string {
        return 'scope-' + this.id;
    }

    signingContext(): Literal {
        return { child: this.id };
    }

    startAt(): Position {
        return position();
    }

    startEmpty(): boolean {
        return true;
    }

    baseFilter(): EntryMetaFilter {
        return { containsValues: { [this.tag()]: ['t'] } };
    }

    wrapPayload(payload: Literal, _at: Position): Literal {
        return { [this.id]: payload };
    }

    unwrapPayload(payload: Literal, _at: Position): Literal {
        return (payload as { [id: string]: Literal })[this.id];
    }

    wrapMeta(meta: MetaProps, _wrappedPayload: Literal, _at: Position): MetaProps {
        return { ...meta, [this.tag()]: json.toSet(['t']) };
    }

    unwrapMeta(meta: MetaProps, _wrappedPayload: Literal, _at: Position): MetaProps {
        const inner: MetaProps = {};
        for (const key in meta) {
            if (key !== this.tag()) inner[key] = meta[key];
        }
        return inner;
    }

    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter {
        return filter;
    }
}

// c and d are concurrent barriers two scopes down. A predicate that rejects c
// by its payload makes both covers see through it, so it must get the entry
// as the querying scope's loadEntry returns it, not the wrapped one.
async function testPredicateSeesScopedEntry() {
    const root = new RootScopedDag(createTestDag());
    const inner = new NestedScopedDag(new NestedScopedDag(root, new LayerScope('a')), new LayerScope('b'));
    const c = await inner.append({ n: 'c' }, BARRIER);
    const d = await inner.append({ n: 'd' }, BARRIER, position());

    const notC: EntryPredicate = async (hash, entry) => {
        const scoped = (await inner.loadEntry(hash))!;
        testing.assertTrue(
            json.toStringNormalized(entry.payload) === json.toStringNormalized(scoped.payload)
                && json.toStringNormalized(entry.meta) === json.toStringNormalized(scoped.meta),
            'the predicate gets the entry loadEntry returns',
        );
        return (entry.payload as { n: string })['n'] !== 'c';
    };

    const cover = await inner.findCoverWithFilter(position(c, d), BARRIER_FILTER, notC);
    testing.assertTrue(samePosition(cover, position(d)), 'the cover sees through c');
    const concurrent = await inner.findConcurrentCoverWithFilter(position(c, d), position(d), BARRIER_FILTER, notC);
    testing.assertTrue(concurrent.size === 0, 'the concurrent cover sees through c');
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
        { name: '[DNS04] an in-scope barrier applies at an out-of-scope use point', invoke: testUsePointOutsideScope },
        { name: '[DNS05] a barrier after only the in-scope head is concurrent with mixed heads', invoke: testMixedHeads },
        { name: '[DNS06] a dominated head does not change the scoped concurrent cover', invoke: testCausalPast },
        { name: '[DNS07] scoped concurrent cover matches the raw call and ignores dominated heads', invoke: testRandomizedScopedConcurrentCover },
        { name: '[DNS08] a cover-query predicate sees the entry as the scope loads it, through nesting', invoke: testPredicateSeesScopedEntry },
    ],
};
