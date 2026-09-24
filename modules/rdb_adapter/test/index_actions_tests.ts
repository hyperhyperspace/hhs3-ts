import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory, RSchemaView, TableDef, MigrationRule,
    ColumnDef, FKs, IdProvider,
} from "@hyper-hyper-space/hhs3_rdb";

import {
    AdapterConfig, CheckpointMovedError, IndexDecl, IndexSpec, ResolvedIndex, SchemaAction,
} from "../src/types.js";
import {
    groupIndexDecls, planIndexActions, resolveIndexes, validateIndexSpec, withIndexActions,
} from "../src/index_actions.js";
import { reconcileIndexes } from "../src/index_reconcile.js";
import { MemoryTarget } from "../src/memory_target.js";
import { projectGroup } from "../src/project.js";
import type { GroupProjection } from "../src/ingest_orchestrator.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

// ---------------------------------------------------------------------------
// Pure fixtures: a mock schema view (only what resolution reads).
// ---------------------------------------------------------------------------

type MockTable = { columns: Record<string, ColumnDef>; fks?: FKs; provider?: IdProvider };

function mockView(tables: Record<string, MockTable>): RSchemaView {
    return {
        getTableNames: () => Object.keys(tables),
        getTable: (n: string) => tables[n] === undefined ? undefined : { name: n, columns: tables[n]!.columns },
        getFKs: (n: string) => tables[n]?.fks ?? {},
        getIdProvider: (n: string) => tables[n]?.provider,
    } as unknown as RSchemaView;
}

const forumView = mockView({
    posts: { columns: { title: { type: 'string' }, code: { type: 'string', pub: true } } },
    comments: {
        columns: {
            body: { type: 'string' },
            post: { type: 'string', nullable: true },
            grantee: { type: 'identity', nullable: true },
        },
        fks: { post: 'posts' },
    },
    identities: {
        columns: { handle: { type: 'string' }, keyId: { type: 'string' }, publicKey: { type: 'string' } },
        provider: { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' },
    },
});

function decl(name: string, table: string, columns: IndexDecl['columns'], extra: Partial<IndexDecl> = {}): IndexDecl {
    return { name, group: 'forum', table, columns, ...extra };
}

function resolveOne(d: IndexDecl, config: AdapterConfig = {}): ResolvedIndex {
    const { resolved, pending } = resolveIndexes([d], 'gid', forumView, config);
    assertEquals(pending.length, 0, `'${d.name}' resolves (pending: ${JSON.stringify(pending)})`);
    return resolved[0]!;
}

function kinds(actions: SchemaAction[]): string[] {
    return actions.map((a) => a.kind === 'ensure-index' ? `ensure:${a.index.table}.${a.index.name}`
        : a.kind === 'drop-index' ? `drop:${a.table}.${a.name}` : a.kind);
}

// ---------------------------------------------------------------------------
// End-to-end fixtures: a real rdb group driven into the (strict) MemoryTarget.
// ---------------------------------------------------------------------------

function tables(): TableDef[] {
    return [
        {
            name: 'ledger',
            columns: {
                ref: { type: 'string', pub: true, readonly: true },
                memo: { type: 'string', nullable: true },
                amount: { type: 'decimal', constraints: { scale: 2 } },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'tags',
            columns: { code: { type: 'string', pub: true } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'posts',
            columns: { title: { type: 'string' } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'comments',
            columns: { body: { type: 'string' }, post: { type: 'string', nullable: true } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

async function createGroup(name = 'books') {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    const admin: OwnIdentity = await createIdentity(SIGNING_ED25519, hashSuite);
    const schemaInit = await RSchemaImpl.create({
        name, creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }], tables: tables(),
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const groupInit = await RTableGroupImpl.create({
        name, seed: name, schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const members: GroupProjection[] = [{ group, config: {} }];
    return { schema, group, admin, members };
}

async function deploy(schema: RSchemaImpl, group: RTableGroupImpl, admin: OwnIdentity, rules: MigrationRule[]) {
    await schema.updateSchema(rules, admin, 'migrate');
    await group.deploy(await (await schema.getScopedDag()).getFrontier());
}

async function indexNames(target: MemoryTarget): Promise<string[]> {
    return (await target.getIndexState()).materialized.map((m) => `${m.table}.${m.name}`).sort();
}

function spec(version: number, indexes: IndexDecl[], extra: Partial<IndexSpec> = {}): IndexSpec {
    return { version, indexes, ...extra };
}

function bookDecl(name: string, table: string, columns: IndexDecl['columns'], extra: Partial<IndexDecl> = {}): IndexDecl {
    return { name, group: 'books', table, columns, ...extra };
}

async function expectReject(fn: () => Promise<unknown>, match: string | (new (...a: never[]) => Error), why: string) {
    let err: unknown;
    try { await fn(); } catch (e) { err = e; }
    assertTrue(err !== undefined, `${why}: expected a throw`);
    if (typeof match === 'string') {
        assertTrue(err instanceof Error && err.message.includes(match), `${why}: got ${String(err)}`);
    } else {
        assertTrue(err instanceof match, `${why}: got ${String(err)}`);
    }
}

export const indexActionsTests = {
    title: '[IDX] rdb_adapter projection-local indexes',
    tests: [
        {
            name: '[IDX01] validateIndexSpec: structure, reserved prefix, per-group uniqueness',
            invoke: async () => {
                const ok = (s: IndexSpec, why: string) => assertEquals(validateIndexSpec(s), undefined, why);
                const bad = (s: IndexSpec, match: string, why: string) => {
                    const reason = validateIndexSpec(s);
                    assertTrue(reason !== undefined && reason.includes(match), `${why}: got ${String(reason)}`);
                };
                ok(spec(0, []), 'empty spec at version 0');
                ok(spec(1, [decl('by_author', 'posts', ['@author', 'title'])]), '@author pseudo-column');
                ok(spec(1, [decl('by_title', 'posts', ['title'], { options: { anything: [1, 'x'] } })]),
                    'options are opaque to the core');
                ok(spec(1, [decl('by_title', 'posts', ['title']), { ...decl('by_title', 'posts', ['title']), group: 'other' }]),
                    'the same name in two groups is fine');
                bad(spec(1.5, []), 'non-negative integer', 'fractional version');
                bad(spec(-1, []), 'non-negative integer', 'negative version');
                bad(spec(1, [{ ...decl('x', 'posts', ['title']), group: '' }]), 'must name its group', 'empty group');
                bad(spec(1, [decl('bad name', 'posts', ['title'])]), 'not a valid identifier', 'bad index name');
                bad(spec(1, [decl('pub__title', 'posts', ['title'])]), 'reserved', 'pub__ prefix is reserved');
                bad(spec(1, [decl('x', 'posts', ['title']), decl('x', 'comments', ['body'])]), 'declared twice',
                    'duplicate name within one group');
                bad(spec(1, [decl('x', 'posts', [])]), 'at least one column', 'no columns');
                bad(spec(1, [decl('x', 'posts', ['title', 'title'])]), 'twice', 'duplicate column');
                bad(spec(1, [decl('x', 'posts', [{ column: 'title', desc: true } as unknown as string])]),
                    "belong in the target's options", 'object column entries are refused');
                bad(spec(1, [decl('x', 'posts', ['@who'])]), 'not a valid identifier', 'unknown pseudo-column');
                bad(spec(1, [decl('x', 'no such', ['title'])]), 'table', 'bad table identifier');
            },
        },
        {
            name: '[IDX02] resolveIndexes maps rdb names to target names (renames, FK/identity/provider companions, @author)',
            invoke: async () => {
                const config: AdapterConfig = {
                    tableNames: { posts: 'forum_posts', comments: 'forum_comments' },
                    columnNames: { posts: { title: 'headline' } },
                };
                const byTitle = resolveOne(decl('by_title', 'posts', ['title', '@author']), config);
                assertEquals(byTitle.table, 'forum_posts', 'table rename (group prefix) applies');
                assertEquals(byTitle.groupId, 'gid', 'group id recorded');
                assertEquals(JSON.stringify(byTitle.columns),
                    JSON.stringify([{ rdb: 'title', target: 'headline' }, { rdb: '@author', target: 'author_key_id' }]),
                    'column rename + author column');

                const byPost = resolveOne(decl('by_post', 'comments', ['post', 'grantee']), config);
                assertEquals(byPost.columns.map((c) => c.target).join(','), 'post_id,grantee_key_id',
                    'FK companion + identity key-ref companion');

                const byKey = resolveOne(decl('by_key', 'identities', ['keyId']));
                assertEquals(byKey.columns[0]!.target, 'key_id', 'provider keyIdColumn projects as key_id');

                const plain = resolveOne(decl('by_title', 'posts', ['title']));
                const opts = { columns: { title: { collate: 'NOCASE' } } };
                const withOpts = resolveOne(decl('by_title', 'posts', ['title'], { options: opts }));
                assertTrue(plain.fingerprint !== withOpts.fingerprint, 'options are part of the index fingerprint');
                assertEquals(JSON.stringify(withOpts.options), JSON.stringify(opts),
                    'options are carried verbatim');
                assertEquals(resolveOne(decl('by_title', 'posts', ['title'])).fingerprint, plain.fingerprint,
                    'resolution is deterministic');
            },
        },
        {
            name: '[IDX03] resolveIndexes: missing table/column, unprojected columns, and disabled author are pending',
            invoke: async () => {
                const { resolved, pending } = resolveIndexes([
                    decl('a', 'nope', ['x']),
                    decl('b', 'posts', ['title', 'missing']),
                    decl('c', 'identities', ['publicKey']),
                    decl('d', 'posts', ['@author']),
                    decl('e', 'posts', ['title']),
                ], 'gid', forumView, { authorColumn: false });
                assertEquals(resolved.map((r) => r.name).join(','), 'e', 'only the complete declaration resolves');
                const byName = new Map(pending.map((p) => [p.name, p.missing.join('; ')]));
                assertEquals(byName.get('a'), "table 'nope'", 'missing table');
                assertEquals(byName.get('b'), "column 'missing'", 'missing column');
                assertEquals(byName.get('c'), "column 'publicKey' (not projected)", 'provider public key is never projected');
                assertEquals(byName.get('d'), '@author (author column disabled)', 'author column disabled');
            },
        },
        {
            name: '[IDX04] groupIndexDecls filters by group and expands indexPub into pub__<column> decls',
            invoke: async () => {
                const s = spec(1, [decl('by_title', 'posts', ['title']), { ...decl('elsewhere', 'posts', ['title']), group: 'other' }]);
                assertEquals(groupIndexDecls(s, 'forum', forumView).map((d) => d.name).join(','), 'by_title',
                    'only this group\'s declarations');
                assertEquals(groupIndexDecls(undefined, 'forum', forumView).length, 0, 'no spec, no declarations');
                const pub = groupIndexDecls({ ...s, indexPub: true }, 'forum', forumView);
                assertEquals(pub.map((d) => `${d.table}.${d.name}`).join(','), 'posts.by_title,posts.pub__code',
                    'one pub__ index per pub column');
                assertEquals(s.indexes.length, 2, 'the spec itself is not mutated');
            },
        },
        {
            name: '[IDX05] planIndexActions: steady state, removal, changed definition, and ordering',
            invoke: async () => {
                const byTitle = resolveOne(decl('by_title', 'posts', ['title']));
                const byBody = resolveOne(decl('by_body', 'comments', ['body']));
                const steady = planIndexActions([byTitle], [byTitle], []);
                assertEquals(steady.drops.length + steady.ensures.length, 0, 'identical materialized index survives');

                const removed = planIndexActions([], [byTitle], []);
                assertEquals(kinds(removed.drops).join(','), 'drop:posts.by_title', 'no longer desired -> drop');

                const changed = resolveOne(decl('by_title', 'posts', ['title'], { options: { columns: { title: { desc: true } } } }));
                const redefined = planIndexActions([changed], [byTitle], []);
                assertEquals(kinds([...redefined.drops, ...redefined.ensures]).join(','),
                    'drop:posts.by_title,ensure:posts.by_title', 'changed definition -> drop + ensure');

                const added = planIndexActions([byTitle, byBody], [byTitle], []);
                assertEquals(kinds(added.ensures).join(','), 'ensure:comments.by_body', 'only the new index is ensured');

                const schema: SchemaAction[] = [{ kind: 'drop-column', table: 'posts', column: 'title' }];
                const merged = withIndexActions(schema, { drops: removed.drops, ensures: added.ensures });
                assertEquals(kinds(merged).join(','), 'drop:posts.by_title,drop-column,ensure:comments.by_body',
                    'index drops run before schema actions, ensures after');
                assertTrue(withIndexActions(schema, { drops: [], ensures: [] }) === schema, 'no index work -> same array');
            },
        },
        {
            name: '[IDX06] planIndexActions: dying tables/columns drop the index even when it is still desired',
            invoke: async () => {
                const byPost = resolveOne(decl('by_post', 'comments', ['post']));
                const byTitle = resolveOne(decl('by_title', 'posts', ['title']));

                // Column reincarnation (drop + add in one delta): same resolved form, still rebuilt.
                const reincarnated = planIndexActions([byTitle], [byTitle], [
                    { kind: 'drop-column', table: 'posts', column: 'title' },
                    { kind: 'add-column', table: 'posts', column: 'title', def: { type: 'string' } },
                ]);
                assertEquals(kinds([...reincarnated.drops, ...reincarnated.ensures]).join(','),
                    'drop:posts.by_title,ensure:posts.by_title', 'reincarnated column -> drop + ensure');

                // Table dropped or recreated (reprojection): the index goes with it and is rebuilt.
                for (const kind of ['drop-table', 'create-table'] as const) {
                    const action = (kind === 'drop-table'
                        ? { kind, table: 'posts', syncTable: 'posts_sync' }
                        : { kind, table: 'posts', syncTable: 'posts_sync', primaryKey: 'id', columns: [] }) as SchemaAction;
                    const p = planIndexActions([byTitle], [byTitle], [action]);
                    assertEquals(kinds([...p.drops, ...p.ensures]).join(','), 'drop:posts.by_title,ensure:posts.by_title',
                        `${kind} kills the index`);
                }

                // Dying table with nothing desired (declaration went pending): drop only.
                const gone = planIndexActions([], [byTitle], [{ kind: 'drop-table', table: 'posts', syncTable: 'posts_sync' }]);
                assertEquals(kinds([...gone.drops, ...gone.ensures]).join(','), 'drop:posts.by_title', 'drop only');

                // FK flip: `post` was plain, now resolves to `post_id`.
                const wasPlain: ResolvedIndex = {
                    ...byPost, columns: [{ rdb: 'post', target: 'post' }], fingerprint: 'plain-form',
                };
                const flip = planIndexActions([byPost], [wasPlain], [
                    { kind: 'drop-column', table: 'comments', column: 'post' },
                    { kind: 'add-column', table: 'comments', column: 'post_id', def: { type: 'integer', nullable: true } },
                ]);
                assertEquals(kinds([...flip.drops, ...flip.ensures]).join(','), 'drop:comments.by_post,ensure:comments.by_post',
                    'FK flip -> drop the old-column index, ensure on the companion');

                // An unrelated dying column leaves the index alone.
                const other = planIndexActions([byTitle], [byTitle], [{ kind: 'drop-column', table: 'posts', column: 'code' }]);
                assertEquals(other.drops.length + other.ensures.length, 0, 'unrelated drop-column is ignored');
            },
        },
        {
            name: '[IDX07] reconcileIndexes version gate: installed, unchanged, conflict, skipped-older, dry-run',
            invoke: async () => {
                const { group, admin, members } = await createGroup();
                await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                const target = new MemoryTarget();
                await projectGroup(group, target);

                const v1 = spec(1, [bookDecl('by_memo', 'ledger', ['memo'])]);
                const r1 = await reconcileIndexes(members, target, v1);
                assertEquals(r1.status, 'installed', 'first spec installs');
                assertEquals(kinds(r1.actions).join(','), 'ensure:ledger.by_memo', 'first spec ensures its index');
                assertEquals((await indexNames(target)).join(','), 'ledger.by_memo', 'index record materialized');
                assertEquals((await target.getIndexState()).spec?.version, 1, 'spec stored as installed');

                const again = await reconcileIndexes(members, target, v1);
                assertEquals(again.status, 'unchanged', 'same version + content is a no-op');
                assertEquals(again.actions.length, 0, 'no-op carries no actions');

                await expectReject(() => reconcileIndexes(members, target, spec(1, [])), 'bump the version',
                    'same version, different content');
                const older = await reconcileIndexes(members, target, spec(0, []));
                assertEquals(older.status, 'skipped-older', 'older version is skipped');
                assertEquals((await indexNames(target)).join(','), 'ledger.by_memo', 'skipped spec changes nothing');

                const v2 = spec(2, [bookDecl('by_ref', 'ledger', ['ref'])]);
                const dry = await reconcileIndexes(members, target, v2, { dryRun: true });
                assertEquals(dry.status, 'dry-run', 'dry run reports');
                assertEquals(kinds(dry.actions).join(','), 'drop:ledger.by_memo,ensure:ledger.by_ref', 'dry run plans drops then ensures');
                assertEquals((await target.getIndexState()).spec?.version, 1, 'dry run installs nothing');
                assertEquals((await indexNames(target)).join(','), 'ledger.by_memo', 'dry run changes no index');

                const r2 = await reconcileIndexes(members, target, v2);
                assertEquals(r2.status, 'installed', 'newer version installs');
                assertEquals((await indexNames(target)).join(','), 'ledger.by_ref', 'old index dropped, new one built');
            },
        },
        {
            name: '[IDX08] reconcileIndexes: invalid specs throw; pending covers missing tables/columns and absent groups',
            invoke: async () => {
                const { group, members } = await createGroup();
                const target = new MemoryTarget();
                await projectGroup(group, target);
                await expectReject(() => reconcileIndexes(members, target, spec(1, [bookDecl('pub__x', 'ledger', ['memo'])])),
                    'reserved', 'structurally invalid spec is refused before touching the target');
                await expectReject(
                    () => reconcileIndexes(members, target, spec(1, [{ ...bookDecl('by_memo', 'ledger', ['memo']), options: {} }])),
                    'takes no index options', 'the memory target refuses any options, even {}');
                assertEquals((await target.getIndexState()).spec, undefined, 'refused options install nothing');

                const r = await reconcileIndexes(members, target, spec(1, [
                    bookDecl('by_memo', 'ledger', ['memo']),
                    bookDecl('later', 'invoices', ['total']),
                    bookDecl('typo', 'ledger', ['mmeo']),
                    { name: 'elsewhere', group: 'other', table: 't', columns: ['x'] },
                ]));
                assertEquals(r.status, 'installed', 'pending declarations do not block the install');
                const missing = new Map(r.pending.map((p) => [p.name, p.missing.join('; ')]));
                assertEquals(missing.get('later'), "table 'invoices'", 'missing table is pending');
                assertEquals(missing.get('typo'), "column 'mmeo'", 'missing column (typo) is pending');
                assertEquals(missing.get('elsewhere'), "group 'other'", 'a group not in the projection is pending');
                assertEquals((await indexNames(target)).join(','), 'ledger.by_memo', 'the complete declaration is built');
            },
        },
        {
            name: '[IDX09] apply-time maintenance: drop-column drops the index first; re-adding the column rebuilds it',
            invoke: async () => {
                const { schema, group, admin, members } = await createGroup();
                await (await group.getTable('ledger')).insert('l1', { ref: 'R-1', amount: '1.00', memo: 'm' }, admin);
                const target = new MemoryTarget();
                await projectGroup(group, target);
                await reconcileIndexes(members, target, spec(1, [
                    bookDecl('by_memo', 'ledger', ['memo', 'ref']),
                    bookDecl('by_ref', 'ledger', ['ref']),
                ]));

                // The strict MemoryTarget refuses to drop an indexed column, so this
                // succeeding proves the index drop is ordered before the drop-column.
                await deploy(schema, group, admin, [{ rule: 'drop-column', table: 'ledger', column: 'memo' }]);
                await projectGroup(group, target);
                assertEquals((await indexNames(target)).join(','), 'ledger.by_ref',
                    'index on the dropped column is gone; unrelated index survives');

                await deploy(schema, group, admin, [
                    { rule: 'add-column', table: 'ledger', column: 'memo', def: { type: 'string', nullable: true } },
                ]);
                await projectGroup(group, target);
                assertEquals((await indexNames(target)).join(','), 'ledger.by_memo,ledger.by_ref',
                    'pending declaration completes when its column returns');
            },
        },
        {
            name: '[IDX10] apply-time maintenance: drop-table and re-added table; FK flip moves the index to the companion',
            invoke: async () => {
                const { schema, group, admin, members } = await createGroup();
                const target = new MemoryTarget();
                await projectGroup(group, target);
                await reconcileIndexes(members, target, spec(1, [
                    bookDecl('by_code', 'tags', ['code']),
                    bookDecl('by_post', 'comments', ['post']),
                ]));
                assertEquals((await indexNames(target)).join(','), 'comments.by_post,tags.by_code', 'both built');

                await deploy(schema, group, admin, [{ rule: 'drop-table', table: 'tags' }]);
                await projectGroup(group, target);
                assertEquals((await indexNames(target)).join(','), 'comments.by_post', 'dropped table takes its index');

                await deploy(schema, group, admin, [{ rule: 'add-table', def: {
                    name: 'tags', columns: { code: { type: 'string' } }, restrictions: [{ on: 'all', rule: { p: 'true' } }],
                } }]);
                await projectGroup(group, target);
                assertEquals((await indexNames(target)).join(','), 'comments.by_post,tags.by_code',
                    'the re-added table gets its index back');

                await deploy(schema, group, admin, [{ rule: 'set-fks', table: 'comments', fks: { post: 'posts' } }]);
                await projectGroup(group, target);
                const flipped = (await target.getIndexState()).materialized.find((m) => m.name === 'by_post');
                assertEquals(flipped?.columns[0]?.target, 'post_id', 'the index follows the FK companion column');
            },
        },
        {
            name: '[IDX11] the installed spec is built by the initial projection; indexPub; stale expectIndexSpec throws',
            invoke: async () => {
                const { group, members } = await createGroup();
                const target = new MemoryTarget();

                // Nothing projected yet: the spec is recorded, with no actions.
                const r = await reconcileIndexes(members, target, spec(1, [bookDecl('by_memo', 'ledger', ['memo'])], { indexPub: true }));
                assertEquals(r.status, 'installed', 'installs on an empty target');
                assertEquals(r.actions.length, 0, 'no group materialized -> nothing to build yet');

                await projectGroup(group, target);
                assertEquals((await indexNames(target)).join(','), 'ledger.by_memo,ledger.pub__ref,tags.pub__code',
                    'the initial projection builds the installed spec, including indexPub');

                const cp = await target.getCheckpoint(group.getId()) as Version;
                await expectReject(
                    () => target.apply(group.getId(), [], [], cp, undefined, cp, 'not-the-installed-spec'),
                    CheckpointMovedError, 'a batch planned against another spec is refused');
                await expectReject(
                    () => target.apply(group.getId(), [], [], cp, undefined, cp, null),
                    CheckpointMovedError, 'a batch planned against no spec is refused once one is installed');
                await target.apply(group.getId(), [], [], cp, undefined, cp, (await target.getIndexState()).specFingerprint);
            },
        },
    ],
};
