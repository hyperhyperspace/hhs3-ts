// HIP-29's cycle: no projection over a pending change, the warning for a group
// without a writer key, the retry after an error, and one lock per target.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
import { IDENTITIES_TABLE, registerIdentity, usersSchemaTables, type RTableGroupImpl } from "@hyper-hyper-space/hhs3_rdb";
import { ChainSchemaChangeError, CheckpointMovedError, MemoryTarget, type BidirectionalTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { RdbProjection } from "../src/index.js";
import { addGroups, makeDatabase, makeIdentity, newCtx, open, poll } from "./projection_tests.js";

type Hook = (...args: unknown[]) => void | Promise<void>;

// `inner` with a hook before each named method, and one after each method
// named `after:<method>`. Every other method passes through. `quiet` hides the
// target's listeners, so a local write signals nothing and only a cycle the
// projection schedules itself finds it.
function wrap(inner: MemoryTarget, hooks: { [method: string]: Hook }, opts: { quiet?: boolean } = {}): BidirectionalTarget {
    const hidden = new Set(opts.quiet === true ? ['addCommitListener', 'removeCommitListener', 'addChangeListener', 'removeChangeListener'] : []);
    return new Proxy(inner, {
        get(obj, prop, receiver) {
            if (typeof prop === 'string' && hidden.has(prop)) return undefined;
            const value = Reflect.get(obj, prop, receiver);
            if (typeof value !== 'function') return value;
            const before = typeof prop === 'string' ? hooks[prop] : undefined;
            const after = typeof prop === 'string' ? hooks[`after:${prop}`] : undefined;
            if (before === undefined && after === undefined) return value.bind(obj);
            return async (...args: unknown[]) => {
                if (before !== undefined) await before(...args);
                const result = await value.apply(obj, args);
                if (after !== undefined) await after(...args);
                return result;
            };
        },
    }) as unknown as BidirectionalTarget;
}

// A gate a hook can wait on until the test opens it.
function gate(): { wait: () => Promise<void>; open: () => void; reached: boolean } {
    let release: () => void = () => undefined;
    const promise = new Promise<void>((resolve) => { release = resolve; });
    const g = {
        reached: false,
        wait: async () => { g.reached = true; await promise; },
        open: () => release(),
    };
    return g;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function rows(group: RTableGroupImpl, table: string) {
    return (await (await group.getTable(table)).getView()).query({});
}

async function forumDb(ctx: RContext) {
    const db = await makeDatabase(ctx, 'pending-db', [{ name: 'forum', tables: [open('posts', { title: { type: 'string' } })] }]);
    return { db, forum: db.members['forum']!.group, rdb: db.rdb };
}

// A database with one signed group: its identity provider is its own
// `identities` table, where `writer` is registered.
async function signedDb(ctx: RContext) {
    const db = await makeDatabase(ctx, 'signed-db', [{
        name: 'signed',
        tables: [usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!, open('notes', { body: { type: 'string' } })],
        idProvider: IDENTITIES_TABLE,
    }]);
    const group = db.members['signed']!.group;
    const writer = await makeIdentity();
    await registerIdentity(group, writer);
    return { db, group, rdb: db.rdb, writer };
}

// A database with a signed group and an anonymous one.
async function mixedDb(ctx: RContext) {
    const db = await makeDatabase(ctx, 'mixed-db', [
        {
            name: 'signed',
            tables: [usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!, open('notes', { body: { type: 'string' } })],
            idProvider: IDENTITIES_TABLE,
        },
        { name: 'anon', tables: [open('posts', { title: { type: 'string' } })] },
    ]);
    return { db, signed: db.members['signed']!.group, anon: db.members['anon']!.group, rdb: db.rdb };
}

// The author column of a row a native insert wrote, or null.
const authorOf = async (memory: MemoryTarget, table: string, localId: number): Promise<unknown> =>
    (await memory.executeQuery({ table })).find((r) => r.id === localId)?.author_key_id ?? null;

export const pendingTests = {
    title: '[RDBPROJ] HIP-29: no projection over a pending change',
    tests: [
        {
            name: '[RDBPROJ16] a projection refused by a change captured during the pass schedules the next cycle itself, which ingests it and projects',
            invoke: async () => {
                const ctx = newCtx();
                const { forum, rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                let armed = false;
                let wrote = false;
                const target = wrap(memory, {
                    apply: async () => {
                        if (!armed || wrote) return;
                        wrote = true;
                        memory.localInsert('forum_posts', { title: 'during' });   // no signal: the target is quiet
                    },
                }, { quiet: true });
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5 });
                try {
                    armed = true;
                    await (await forum.getTable('posts')).insert('p1', { title: 'remote' });   // a cycle starts
                    await poll(() => wrote);
                    let titles: string[] = [];
                    await poll(() => {
                        void rows(forum, 'posts').then((r) => { titles = r.map((x) => String(x.values.title)).sort(); });
                        return titles.includes('during');
                    }, 3000);
                    await poll(() => memory.getRowIds('forum_posts').length === 2, 3000);
                    assertTrue(!await memory.hasPendingChanges(), 'the change was ingested, and the projection applied');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: "[RDBPROJ17] a change written between open()'s first cycle and arm() is ingested, with no signal",
            invoke: async () => {
                const ctx = newCtx();
                const { forum, rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                let wrote = false;
                const target = wrap(memory, {
                    // after open()'s first apply, before it arms
                    'after:apply': async () => {
                        if (wrote) return;
                        wrote = true;
                        memory.localInsert('forum_posts', { title: 'early' });
                    },
                }, { quiet: true });
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5 });
                try {
                    assertTrue(wrote, 'the write was made while open() ran');
                    let titles: string[] = [];
                    await poll(() => {
                        void rows(forum, 'posts').then((r) => { titles = r.map((x) => String(x.values.title)); });
                        return titles.includes('early');
                    }, 3000);
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ18] under a steady stream of local writes, sync() returns and stop() completes',
            invoke: async () => {
                const ctx = newCtx();
                const { rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                let streaming = false;
                let n = 0;
                // A write lands at every drain: the outbox never stays empty.
                const target = wrap(memory, {
                    drainChanges: async () => { if (streaming) memory.localInsert('forum_posts', { title: `w${n++}` }); },
                });
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5 });
                try {
                    streaming = true;
                    memory.localInsert('forum_posts', { title: 'start' });
                    const within = <T>(p: Promise<T>, ms: number, what: string): Promise<T> => Promise.race([
                        p, new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`${what} did not return`)), ms)),
                    ]);
                    await within(projection.sync(), 5000, 'sync()');
                    await sleep(50);
                    await within(projection.stop(), 5000, 'stop()');
                } finally {
                    streaming = false;
                    await projection.stop();
                }
                assertTrue(n > 1, `writes kept arriving (${n})`);
            },
        },
        {
            name: '[RDBPROJ19] without a writer key for a signed group, a pending change of it is warned about once, holds remote changes back without a loop, and is ingested once a projection opens with the key',
            invoke: async () => {
                const ctx = newCtx();
                const { group, rdb, writer } = await signedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });

                // Written with the key, then left pending.
                const first = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                await first.stop();
                memory.localInsert('signed_notes', { body: 'pending' });
                const checkpoint = await memory.getCheckpoint(group.getId());

                const warnings: string[] = [];
                let cycles = 0;
                const keyless = await RdbProjection.open(rdb, ctx, memory, {
                    debounceMs: 5, onWarning: (m) => warnings.push(m), onResult: () => { cycles += 1; },
                });
                try {
                    assertEquals(warnings.length, 1, 'it opens, and warns once');
                    assertTrue(warnings[0].includes(group.getId()), 'the warning names the group');
                    await (await group.getTable('notes')).insert('remote-1', { body: 'remote' }, writer);
                    await sleep(150);
                    assertTrue([...(await memory.getCheckpoint(group.getId()))!].join() === [...checkpoint!].join(),
                        'no projection applies while the change is pending');
                    const settled = cycles;
                    await sleep(150);
                    assertTrue(cycles - settled <= 1, `no cycle loops (${cycles - settled} more)`);
                    assertEquals(warnings.length, 1, 'still one warning');
                    assertEquals((await memory.executeQuery({ table: 'signed_notes' })).length, 1, 'reads work');
                } finally {
                    await keyless.stop();
                }

                // Opened with the key: the change is ingested, then a projection applies.
                const keyed = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                try {
                    assertTrue(!await memory.hasPendingChanges(), 'the change is ingested');
                    const bodies = (await rows(group, 'notes')).map((r) => String(r.values.body)).sort();
                    assertEquals(bodies.join(','), 'pending,remote', 'it reached rdb');
                    assertEquals((await memory.executeQuery({ table: 'signed_notes' })).length, 2, 'and the remote change is projected');
                } finally {
                    await keyed.stop();
                }

                // A change captured after an open over an empty outbox: the same,
                // with the warning when a cycle first meets it.
                const later: string[] = [];
                const again = await RdbProjection.open(rdb, ctx, memory, { debounceMs: 5, onWarning: (m) => later.push(m) });
                try {
                    assertEquals(later.length, 0, 'no warning over an empty outbox');
                    memory.localInsert('signed_notes', { body: 'later' });
                    await poll(() => later.length === 1, 2000);
                    memory.localInsert('signed_notes', { body: 'later-2' });
                    await sleep(100);
                    assertEquals(later.length, 1, 'once per group');
                } finally {
                    await again.stop();
                }
            },
        },
        {
            name: "[RDBPROJ20] the projection records the writer record when it opens: the writer key, the hash suite and the author setting, auto by default, and nothing per group",
            invoke: async () => {
                const ctx = newCtx();
                const { rdb } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                try {
                    const record = await memory.readWriterRecord();
                    assertEquals(record?.writerKeyId, await memory.idForKeyHash('default', writer.keyId), "the writer key's id");
                    assertEquals(record?.hashSuite, ctx.getHashSuite().name, 'the hash suite');
                    assertEquals(record?.author, 'auto', 'the setting, auto by default');
                    assertEquals(Object.keys(record ?? {}).sort().join(','), 'author,hashSuite,writerKeyId', 'and nothing per group');
                } finally {
                    await projection.stop();
                }

                const bare = new MemoryTarget({ captureChanges: true });
                const readOnly = await RdbProjection.open(rdb, ctx, bare, { debounceMs: 5 });
                try {
                    const record = await bare.readWriterRecord();
                    assertEquals(record?.writerKeyId, undefined, 'without a writer, no writer key');
                    assertEquals(record?.author, 'auto', 'and the rest');
                } finally {
                    await readOnly.stop();
                }
            },
        },
        {
            name: "[RDBPROJ21] the author option: manual is recorded, and a signed group's native insert with no author stays anonymous; under manual a signed group with no author column is refused, and an anonymous one opens",
            invoke: async () => {
                const ctx = newCtx();
                const { rdb } = await mixedDb(ctx);
                const writer = await makeIdentity();
                const openWith = async (opts: Parameters<typeof RdbProjection.open>[3]) => {
                    const memory = new MemoryTarget({ captureChanges: true });
                    const p = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5, ...opts });
                    const author = (await memory.readWriterRecord())?.author;
                    const localId = memory.localInsert('signed_notes', { body: 'x' });
                    const filled = await authorOf(memory, 'signed_notes', localId);
                    await p.stop();
                    return { author, filled };
                };
                const manual = await openWith({ author: 'manual' });
                assertEquals(manual.author, 'manual', 'the option is recorded');
                assertEquals(manual.filled, null, "and a signed group's insert that names no author stays anonymous");
                const auto = await openWith({});
                assertEquals(auto.author, 'auto', 'auto by default');
                assertTrue(auto.filled !== null, 'where the same insert gets the writer key');

                const memory = new MemoryTarget({ captureChanges: true });
                let refusal: unknown;
                try {
                    await RdbProjection.open(rdb, ctx, memory, {
                        writer, author: 'manual', configOverride: ({ groupName }) => (groupName === 'signed' ? { authorColumn: false } : {}),
                    });
                } catch (e) {
                    refusal = e;
                }
                assertTrue(refusal instanceof Error && refusal.message.includes("group 'signed'"),
                    `under manual a signed group with no author column is refused, naming it: ${String(refusal)}`);
                assertEquals(await memory.readWriterRecord(), undefined, 'before anything is written');
                assertEquals(memory.listTables().length, 0, 'not even a table');

                const anonOnly = new MemoryTarget({ captureChanges: true });
                const p = await RdbProjection.open(rdb, ctx, anonOnly, {
                    writer, author: 'manual', debounceMs: 5,
                    configOverride: ({ groupName }) => (groupName === 'anon' ? { authorColumn: false } : {}),
                });
                await p.stop();
                assertEquals((await anonOnly.readWriterRecord())?.author, 'manual', 'an anonymous group with no author column opens under manual');
            },
        },
        {
            name: "[RDBPROJ31] the writer record is written once, when the projection opens: a reconfigure that adds a group leaves it as it was",
            invoke: async () => {
                const ctx = newCtx();
                const { db, rdb } = await mixedDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                let writes = 0;
                const recordWriter = memory.recordWriter.bind(memory);
                memory.recordWriter = async (record) => { writes++; await recordWriter(record); };
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                try {
                    assertEquals(writes, 1, 'written at open');
                    const before = JSON.stringify(await memory.readWriterRecord());
                    await addGroups(ctx, db, '1.1.0', [{ name: 'late', tables: [open('notes', { body: { type: 'string' } })] }]);
                    await poll(() => memory.hasTable('late_notes'), 3000);
                    assertEquals(writes, 1, 'and not again when a group joins');
                    assertEquals(JSON.stringify(await memory.readWriterRecord()), before, 'so it reads as it was');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: "[RDBPROJ32] a configOverride that returns author changes nothing: the record holds the projection's setting, and every signed group follows it",
            invoke: async () => {
                const ctx = newCtx();
                const { rdb } = await mixedDb(ctx);
                const writer = await makeIdentity();
                const openWith = async (author: 'auto' | 'manual', override: 'auto' | 'manual') => {
                    const memory = new MemoryTarget({ captureChanges: true });
                    const p = await RdbProjection.open(rdb, ctx, memory, {
                        writer, author, debounceMs: 5,
                        configOverride: ({ groupName }) => (groupName === 'signed' ? { author: override } : {}),
                    });
                    const recorded = (await memory.readWriterRecord())?.author;
                    const localId = memory.localInsert('signed_notes', { body: 'x' });
                    const filled = await authorOf(memory, 'signed_notes', localId);
                    await p.stop();
                    return { recorded, filled };
                };
                const manual = await openWith('manual', 'auto');
                assertEquals(manual.recorded, 'manual', "under manual, an override of auto isn't recorded");
                assertEquals(manual.filled, null, 'and the insert that names no author stays anonymous');
                const auto = await openWith('auto', 'manual');
                assertEquals(auto.recorded, 'auto', "under auto, an override of manual isn't recorded");
                assertTrue(auto.filled !== null, 'and the insert gets the writer key');
            },
        },
        {
            name: "[RDBPROJ23] without a writer key, an anonymous group's pending change is ingested, and a projection then applies",
            invoke: async () => {
                const ctx = newCtx();
                const { forum, rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const projection = await RdbProjection.open(rdb, ctx, memory, { debounceMs: 5 });
                try {
                    memory.localInsert('forum_posts', { title: 'anon' });
                    let titles: string[] = [];
                    await poll(() => {
                        void rows(forum, 'posts').then((r) => { titles = r.map((x) => String(x.values.title)); });
                        return titles.includes('anon');
                    }, 3000);
                    await poll(() => memory.getRowIds('forum_posts').length === 1, 3000);
                    assertTrue(!await memory.hasPendingChanges(), 'ingested, and projected');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ25] a cycle that ends in an error is retried after 1 s, then 2 s, with no other write to trigger it, and a success resets the delay',
            invoke: async () => {
                const ctx = newCtx();
                const { forum, rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const failing = new Set<number>();
                const applies: number[] = [];
                const target = wrap(memory, {
                    apply: async () => {
                        applies.push(Date.now());
                        if (failing.has(applies.length)) throw new Error('a passing error');
                    },
                }, { quiet: true });
                const errors: unknown[] = [];
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5, onError: (e) => errors.push(e) });
                try {
                    const base = applies.length;
                    for (const n of [1, 2, 4]) failing.add(base + n);
                    const posts = await forum.getTable('posts');
                    await posts.insert('p1', { title: 'one' });   // the cycle fails, then twice more
                    await poll(() => applies.length >= base + 3, 5000);
                    const gap1 = applies[base + 1] - applies[base];
                    const gap2 = applies[base + 2] - applies[base + 1];
                    assertTrue(gap1 >= 900 && gap1 < 1900, `the first retry after about 1 s (${gap1} ms)`);
                    assertTrue(gap2 >= 1900 && gap2 < 3500, `the second after about 2 s (${gap2} ms)`);
                    await poll(() => memory.getRowIds('forum_posts').length === 1, 3000);

                    await posts.insert('p2', { title: 'two' });   // fails once more
                    await poll(() => applies.length >= base + 5, 5000);
                    const gap3 = applies[base + 4] - applies[base + 3];
                    assertTrue(gap3 >= 900 && gap3 < 1900, `after a success the delay starts at 1 s again (${gap3} ms)`);
                    assertEquals(errors.length, 3, 'each failed cycle reached onError');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ26] a group joins while a cycle is held before its settle, and sync() is then called: it waits, and the pending change is ingested once',
            invoke: async () => {
                const ctx = newCtx();
                const { db, forum, rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const hold = gate();
                let holding = false;
                let drainsWhileHeld = 0;
                const target = wrap(memory, {
                    commitIngest: async () => { if (holding) { holding = false; await hold.wait(); } },
                    drainChanges: async () => { if (hold.reached && !released) drainsWhileHeld += 1; },
                });
                let released = false;
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5 });
                try {
                    holding = true;
                    memory.localInsert('forum_posts', { title: 'once' });   // a reactive cycle starts
                    await poll(() => hold.reached, 3000);
                    await addGroups(ctx, db, '1.1.0', [{ name: 'late', tables: [open('notes', { body: { type: 'string' } })] }]);
                    await sleep(50);
                    const explicit = projection.sync();
                    await sleep(100);
                    assertEquals(drainsWhileHeld, 0, 'the second cycle waits for the first');
                    released = true;
                    hold.open();
                    await explicit;
                    const titles = (await rows(forum, 'posts')).map((r) => String(r.values.title));
                    assertEquals(titles.filter((t) => t === 'once').length, 1, 'the pending change is ingested once');
                } finally {
                    hold.open();
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ27] a group joins while a cycle is held before its apply, and an index reconcile is then called: it waits, the cycle applies, the spec is installed, and nothing throws',
            invoke: async () => {
                const ctx = newCtx();
                const { db, forum, rdb } = await forumDb(ctx);
                const memory = new MemoryTarget({ captureChanges: true });
                const hold = gate();
                let holding = false;
                const target = wrap(memory, {
                    apply: async () => { if (holding) { holding = false; await hold.wait(); } },
                });
                const errors: unknown[] = [];
                const writer = await makeIdentity();
                const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5, onError: (e) => errors.push(e) });
                try {
                    holding = true;
                    await (await forum.getTable('posts')).insert('p1', { title: 'remote' });   // a cycle, held at its apply
                    await poll(() => hold.reached, 3000);
                    await addGroups(ctx, db, '1.1.0', [{ name: 'late', tables: [open('notes', { body: { type: 'string' } })] }]);
                    let done = false;
                    const reconcile = projection.reconcileIndexes({
                        indexes: [{ name: 'by_title', group: 'forum', table: 'posts', columns: ['title'] }],
                    }).then((r) => { done = true; return r; });
                    await sleep(100);
                    assertTrue(!done, 'the reconcile waits for the cycle');
                    hold.open();
                    const report = await reconcile;
                    assertEquals(report.status, 'installed', 'the spec is installed');
                    await poll(() => memory.getRowIds('forum_posts').length === 1, 3000);
                    assertEquals(errors.length, 0, 'nothing throws');
                } finally {
                    hold.open();
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ22] a moved checkpoint at the apply or at the settle, or a delta with a schema action at the end of a pass, halts the projection: sync() throws it, onError fires once, no cycle runs again, and reads keep working',
            invoke: async () => {
                for (const where of ['apply', 'commitIngest'] as const) {
                    const ctx = newCtx();
                    const { forum, rdb } = await forumDb(ctx);
                    const memory = new MemoryTarget({ captureChanges: true });
                    let armed = false;
                    let calls = 0;
                    const target = wrap(memory, {
                        [where]: () => {
                            if (!armed) return;
                            calls += 1;
                            throw new CheckpointMovedError(forum.getId());
                        },
                    });
                    const errors: unknown[] = [];
                    const writer = await makeIdentity();
                    const projection = await RdbProjection.open(rdb, ctx, target, { writer, debounceMs: 5, onError: (e) => errors.push(e) });
                    try {
                        armed = true;
                        if (where === 'apply') await (await forum.getTable('posts')).insert('p1', { title: 'remote' });
                        else memory.localInsert('forum_posts', { title: 'local' });
                        let thrown: unknown;
                        try { await projection.sync(); } catch (e) { thrown = e; }
                        assertTrue(thrown instanceof CheckpointMovedError, `${where}: sync() throws the moved checkpoint, got ${String(thrown)}`);
                        await sleep(50);
                        assertEquals(errors.length, 1, `${where}: onError fires once`);
                        assertTrue(projection.haltError() instanceof CheckpointMovedError, `${where}: the projection is halted`);

                        const seen = calls;
                        await (await forum.getTable('posts')).insert('p2', { title: 'later' });
                        memory.localInsert('forum_posts', { title: 'later' });
                        await sleep(150);
                        assertEquals(calls, seen, `${where}: no cycle runs again`);
                        let again: unknown;
                        try { await projection.sync(); } catch (e) { again = e; }
                        assertTrue(again === thrown, `${where}: sync() keeps throwing it`);
                        assertEquals(errors.length, 1, `${where}: and onError fired only once`);
                        assertTrue((await memory.executeQuery({ table: 'forum_posts' })).length >= 0, `${where}: reads work`);
                    } finally {
                        await projection.stop();
                    }
                }

                // At the end of a pass: a delta with a schema action. No chain changes
                // a schema, so the bound group's delta is made to report a table drop
                // once the case arms it, through the context the projection loads it from.
                const ctx = newCtx();
                const db = await makeDatabase(ctx, 'halt-end', [
                    { name: 'perm', tables: [open('caps', { label: { type: 'string', pub: true } })] },
                    { name: 'items', bindings: { perm: 'perm' }, tables: [{
                        name: 'items', columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'perm.caps', where: { label: 'grant' } } }],
                    }] },
                ]);
                const itemsId = db.members['items']!.group.getId();
                let armed = false;
                const faked = <T extends object>(group: T): T => new Proxy(group, {
                    get(obj, prop, receiver) {
                        const value = Reflect.get(obj, prop, receiver);
                        if (prop !== 'computeDelta') return typeof value === 'function' ? value.bind(obj) : value;
                        return async (...args: unknown[]) => {
                            const delta = await (value as (...a: unknown[]) => Promise<{ schemaChanges: { tableChanges: unknown[] }; opVerdictChanges: unknown[] }>).apply(obj, args);
                            if (!armed) return delta;
                            return { schemaChanges: { ...delta.schemaChanges, tableChanges: [{ table: 'items', existedBefore: true, existsAfter: false }] },
                                opVerdictChanges: delta.opVerdictChanges };
                        };
                    },
                });
                const ctxView = new Proxy(ctx, {
                    get(obj, prop, receiver) {
                        const value = Reflect.get(obj, prop, receiver);
                        if (prop !== 'getObject') return typeof value === 'function' ? value.bind(obj) : value;
                        return async (id: string) => {
                            const o = await obj.getObject(id);
                            return id === itemsId ? faked(o as object) : o;
                        };
                    },
                });
                const memory = new MemoryTarget({ captureChanges: true });
                const errors: unknown[] = [];
                const projection = await RdbProjection.open(db.rdb, ctxView, memory, { writer: await makeIdentity(), debounceMs: 5, onError: (e) => errors.push(e) });
                try {
                    armed = true;
                    memory.localInsert('perm_caps', { label: 'grant' });
                    memory.localInsert('items_items', { name: 'thing' });   // items observes perm first: its chain holds an observe
                    let thrown: unknown;
                    try { await projection.sync(); } catch (e) { thrown = e; }
                    assertTrue(thrown instanceof ChainSchemaChangeError, `end: a delta with a schema action stops the pass, got ${String(thrown)}`);
                    await sleep(50);
                    assertEquals(errors.length, 1, 'end: onError fires once');
                    assertTrue(projection.haltError() instanceof ChainSchemaChangeError, 'end: the projection is halted');
                } finally {
                    await projection.stop();
                }
            },
        },
        {
            name: '[RDBPROJ24] a pass stops at the first change of a signed group with no writer key: the anonymous group\'s change before it is ingested, the one after it waits, it warns once and no cycle loops; opened with the key, the rest is ingested',
            invoke: async () => {
                const ctx = newCtx();
                const { signed, anon, rdb } = await mixedDb(ctx);
                const writer = await makeIdentity();
                await registerIdentity(signed, writer);
                const memory = new MemoryTarget({ captureChanges: true });
                const first = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                await first.stop();
                memory.localInsert('anon_posts', { title: 'before' });
                memory.localInsert('signed_notes', { body: 'held' });
                memory.localInsert('anon_posts', { title: 'after' });

                const warnings: string[] = [];
                let cycles = 0;
                const keyless = await RdbProjection.open(rdb, ctx, memory, {
                    debounceMs: 5, onWarning: (m) => warnings.push(m), onResult: () => { cycles += 1; },
                });
                try {
                    assertEquals((await rows(anon, 'posts')).map((r) => String(r.values.title)).join(','), 'before', 'the change before the stop is ingested');
                    assertEquals((await memory.drainChanges()).changes.map((c) => c.table).join(','), 'signed_notes,anon_posts', 'the rest waits');
                    assertEquals(warnings.length, 1, 'it warns once');
                    const settled = cycles;
                    await sleep(150);
                    assertTrue(cycles - settled <= 1, `no cycle loops (${cycles - settled} more)`);
                    assertEquals(warnings.length, 1, 'still one warning');
                } finally {
                    await keyless.stop();
                }

                const keyed = await RdbProjection.open(rdb, ctx, memory, { writer, debounceMs: 5 });
                try {
                    assertTrue(!await memory.hasPendingChanges(), 'the rest is ingested');
                    assertEquals((await rows(signed, 'notes')).map((r) => String(r.values.body)).join(','), 'held', 'the signed change');
                    assertEquals((await rows(anon, 'posts')).map((r) => String(r.values.title)).sort().join(','), 'after,before', 'and the one after it');
                } finally {
                    await keyed.stop();
                }
            },
        },
    ],
};
