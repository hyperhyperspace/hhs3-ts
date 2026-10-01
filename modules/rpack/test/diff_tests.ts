import { applyMigrationRules, type MigrationRule, type TableDef } from "@hyper-hyper-space/hhs3_rdb";
import { parseScript, type SourceKeyLabels } from "@hyper-hyper-space/hhs3_rdb_lang";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import type { ReleaseDraft } from "../src/draft.js";
import { formatDraft, formatDraftSql, statusSummary } from "../src/format_draft.js";
import type { KeyDirectory } from "../src/keys.js";
import { tableDifferences } from "../src/model.js";
import { Released } from "../src/released.js";
import { diffTables } from "../src/schema_diff.js";
import { readSource } from "../src/source.js";
import { DOC_SCHEMA, EDITOR_SOURCE, USER_SCHEMA, devKeys, draftFor, releaseFor } from "./fixtures.js";

function assertParses(draft: ReleaseDraft, labels?: SourceKeyLabels): void {
    const text = formatDraftSql(draft, labels).join('\n');
    const parsed = parseScript(text);
    assertTrue(parsed.ok, `the draft is valid C-SQL: ${parsed.ok ? '' : parsed.diagnostics.map((d) => d.message).join('; ')}\n${text}`);
}

// The tables of schema `s` as C-SQL describes them.
async function tablesOf(keys: KeyDirectory, body: string, schema = 's'): Promise<Map<string, TableDef>> {
    const source = `CREATE SCHEMA ${schema} CREATORS ($dev) AS (\n${body}\n);\nCREATE CATALOG c CREATORS ($dev) AS (TABLEGROUP g USING SCHEMA ${schema});`;
    return (await readSource(source, keys, 'dev')).model.schemas.get(schema)!.tables;
}

async function schemaTables(keys: KeyDirectory, sql: string, name: string, extra = ''): Promise<Map<string, TableDef>> {
    const users = name === 'hhs:doc' ? `${USER_SCHEMA}\n` : '';
    const groups = name === 'hhs:doc'
        ? 'TABLEGROUP user USING SCHEMA hhs:user USING IDENTITIES identities, TABLEGROUP doc USING SCHEMA hhs:doc BIND user => user USING IDENTITIES user.identities'
        : 'TABLEGROUP user USING SCHEMA hhs:user USING IDENTITIES identities';
    const source = `${users}${sql}\n${extra}\nCREATE CATALOG c CREATORS ($dev) AS (${groups});`;
    return (await readSource(source, keys, 'dev')).model.schemas.get(name)!.tables;
}

// Diffs, then applies the rules with rdb's applier: they must apply, and
// reach the desired tables.
function reach(working: Map<string, TableDef>, desired: Map<string, TableDef>, what: string): MigrationRule[] {
    const diff = diffTables('s', working, desired);
    assertEquals(diff.refusals.map((r) => r.message).join('; '), '', `${what}: no refusals`);
    const tables = new Map(working);
    const applied = applyMigrationRules(tables, diff.rules);
    assertTrue(applied.ok, `${what}: the rules apply (${applied.ok ? '' : `rule ${applied.index}: ${applied.reason}`})`);
    assertEquals(tableDifferences('s', tables, desired).join('; '), '', `${what}: the rules reach the desired tables`);
    return diff.rules;
}

const kinds = (rules: MigrationRule[]) => rules.map((r) => `${r.rule} ${'table' in r ? r.table : r.def.name}${'column' in r ? `.${r.column}` : ''}`).join(', ');

export const diffTests = [
    {
        name: '[RPACK06] the schema diff: rule kinds and phases, cycles, drop chains, effective equality, refusals',
        invoke: async () => {
            const { keys } = await devKeys();

            const before = await tablesOf(keys, `
  TABLE a (x string, y string NULL),
  TABLE gone (z string)`);
            const after = await tablesOf(keys, `
  TABLE a (x string, w string NULL) NO CONCURRENT DELETES ALLOW insert IF EXISTS b WHERE b.k = 'x',
  TABLE b (k string PUB)`);
            assertEquals(kinds(reach(before, after, 'every kind')),
                'add-column a.w, add-table b, set-restrictions a, set-concurrent-deletes a, drop-table gone, drop-column a.y',
                'the five phases, in order');

            const cycle = await tablesOf(keys, `
  TABLE k (v string),
  TABLE p (qId string NULL REFERENCES q),
  TABLE q (pId string NULL REFERENCES p),
  TABLE r (pId string REFERENCES p)`);
            const plain = await tablesOf(keys, 'TABLE k (v string)');
            assertEquals(kinds(reach(plain, cycle, 'an FK cycle among new tables')),
                'add-table p, add-table q, add-table r, set-fks p, set-fks q',
                'tables on a cycle are added bare and get their FKs after; the others after what they reference');
            assertTrue(reach(plain, cycle, 'again')[0]!.rule === 'add-table' && (reach(plain, cycle, 'again')[0] as { def: TableDef }).def.fks === undefined,
                'the bare table has no FKs');

            const chain = await tablesOf(keys, `
  TABLE k (v string),
  TABLE a (v string),
  TABLE b (aId string REFERENCES a),
  TABLE c (bId string REFERENCES b),
  TABLE x (yId string NULL REFERENCES y),
  TABLE y (xId string NULL REFERENCES x)`);
            assertEquals(kinds(reach(chain, plain, 'drop chains and a dropped cycle')),
                'set-fks x, set-fks y, drop-table c, drop-table b, drop-table a, drop-table x, drop-table y',
                'referencing tables go first; a dropped cycle is cleared first');

            const exists = await tablesOf(keys, `
  TABLE caps (label string PUB, grantee string PUB),
  TABLE docs (body string) ALLOW insert IF EXISTS caps WHERE caps.label = 'writer'`);
            const narrowed = await tablesOf(keys, `
  TABLE caps (grantee string PUB),
  TABLE docs (body string) ALLOW insert IF EXISTS caps WHERE caps.grantee = 'alice'`);
            assertEquals(kinds(reach(exists, narrowed, "a column another table's EXISTS uses")),
                'set-restrictions docs, drop-column caps.label', 'the restriction moves off the column before it goes');

            const explicit = await tablesOf(keys, `
  TABLE t (v string READONLY) CONCURRENT DELETES ALLOW all IF t.v = 'a'`);
            const spelled = await tablesOf(keys, `
  TABLE t (v string READONLY) ALLOW insert IF t.v = 'a' ALLOW update IF t.v = 'a' ALLOW delete IF t.v = 'a'`);
            assertEquals(reach(explicit, spelled, 'effective equality').length, 0, 'the same effective settings give no rules');

            const retyped = diffTables('s', await tablesOf(keys, 'TABLE t (v string, n integer NULL)'), await tablesOf(keys, 'TABLE t (v string, n string NULL, m integer)'));
            assertEquals(retyped.rules.length, 0, 'a refused diff has no rules');
            assertTrue(retyped.refusals.some((r) => r.message.includes('t.n: its definition changes') && r.hint!.includes('DROP COLUMN t.n')), 'a redefined column is refused, with the reset');
            assertTrue(retyped.refusals.some((r) => r.message.includes('t.m: a new NOT NULL column needs a DEFAULT')), 'a NOT NULL column without a default is refused');
            const provider = diffTables('s',
                await tablesOf(keys, 'TABLE ids (keyId string PUB READONLY, publicKey string PUB READONLY)'),
                await tablesOf(keys, 'TABLE ids (keyId string PUB READONLY, publicKey string PUB READONLY) IDENTITY PROVIDER'));
            assertTrue(provider.refusals.some((r) => r.hint!.includes('DROP TABLE ids')), 'an identity-provider change is refused, with the reset');

            const doc = await schemaTables(keys, DOC_SCHEMA, 'hhs:doc');
            const user = await schemaTables(keys, USER_SCHEMA, 'hhs:user');
            const edits: [string, string, Map<string, TableDef>][] = [];
            const docEdit = async (what: string, sql: string) => edits.push([what, 'hhs:doc', await schemaTables(keys, sql, 'hhs:doc')]);
            const userEdit = async (what: string, sql: string) => edits.push([what, 'hhs:user', await schemaTables(keys, sql, 'hhs:user')]);
            await docEdit('a tag column', DOC_SCHEMA.replace('deleted boolean', 'deleted boolean,\n    tag string NULL PUB'));
            await docEdit('no blocks', DOC_SCHEMA.replace(/,\n\n  TABLE blocks[\s\S]*\$author\n/, '\n'));
            await docEdit('comments on blocks', DOC_SCHEMA.replace(/\n\);$/, ",\n\n  TABLE comments (\n    blockId string REFERENCES blocks,\n    text string\n  ) ALLOW all IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author\n);"));
            await docEdit('pages deletable', DOC_SCHEMA.replace('ALLOW delete IF false', 'ALLOW delete IF true'));
            await docEdit('blocks without the FK', DOC_SCHEMA.replace('pageId string READONLY REFERENCES pages', 'pageId string READONLY'));
            await userEdit('no concurrent deletes', USER_SCHEMA.replace(') CONCURRENT DELETES', ') NO CONCURRENT DELETES'));
            await userEdit('a profiles table', USER_SCHEMA.replace(/\n\);$/, ',\n\n  TABLE profiles (\n    ownerId string PUB READONLY REFERENCES identities,\n    bio string NULL\n  )\n);'));
            for (const [what, schema, edited] of edits) {
                const original = schema === 'hhs:doc' ? doc : user;
                reach(original, edited, `${what}, forwards`);
                reach(edited, original, `${what}, backwards`);
            }
        },
    },
    {
        name: '[RPACK07] the catalog diff: new groups after what they bind, pins and merges, params, refusals',
        invoke: async () => {
            const dev = await devKeys();
            const released = await Released.open([]);
            try {
                const first = await releaseFor(dev, released, EDITOR_SOURCE, '1.0.0', []);
                assertTrue(first.draft.first, 'no parents: the first release');
                assertEquals(first.draft.add.map((g) => g.name).join(','), 'user,doc', 'the first release adds every group');
                const firstText = formatDraft(first.draft, dev.keys.labels()).join('\n');
                assertTrue(firstText.includes("CREATE CATALOG editor CREATORS ($dev) VERSION '1.0.0' PARAMS (:admin identity) AS ("), `the genesis: ${firstText}`);
                assertTrue(firstText.includes("TABLEGROUP user USING SCHEMA hhs:user AT LATEST"), 'groups pin the new schemas');
                assertParses(first.draft, dev.keys.labels());
                const r100 = released.resolve('1.0.0');

                const tags = 'CREATE SCHEMA hhs:tags CREATORS ($dev) AS (\n  TABLE tags (label string PUB)\n);';
                const v110 = EDITOR_SOURCE
                    .replace('deleted boolean', 'deleted boolean,\n    summary string NULL')
                    .replace('-- :admin', `${tags}\n\n-- :admin`)
                    .replace('PARAMS (:admin identity)', 'PARAMS (:admin identity, :moderator identity)')
                    .replace(/\n\);\n$/, ',\n  TABLEGROUP tags USING SCHEMA hhs:tags BIND doc => doc,\n  TABLEGROUP notes USING SCHEMA hhs:tags BIND tags => tags\n);\n');
                const second = await releaseFor(dev, released, v110, '1.1.0', [r100]);
                assertEquals(second.draft.add.map((g) => g.name).join(','), 'tags,notes', 'a new group comes after the new group it binds');
                assertEquals(second.draft.changes.map((c) => c.group).join(','), 'doc', "the group whose schema moved gets a change");
                assertEquals(second.draft.params.map((p) => p.name).join(','), 'moderator', 'the new param is declared');
                assertTrue(second.draft.warnings.some((w) => w.startsWith(':moderator is new')), 'with a warning');
                const secondText = formatDraft(second.draft, dev.keys.labels()).join('\n');
                for (const line of [
                    "ALTER SCHEMA hhs:doc VERSION '1.1.0' AS (",
                    'ADD COLUMN pages.summary string NULL',
                    "CREATE SCHEMA hhs:tags CREATORS ($dev) VERSION '1.1.0' AS (",
                    "ALTER CATALOG editor VERSION '1.1.0' PARAMS (:moderator identity) AS (",
                    "UPDATE SCHEMA hhs:doc TO LATEST ON doc",
                    "ADD TABLEGROUP tags USING SCHEMA hhs:tags AT LATEST",
                ]) assertTrue(secondText.includes(line), `the draft shows '${line}':\n${secondText}`);
                assertParses(second.draft, dev.keys.labels());
                const r110 = released.resolve('1.1.0');

                await releaseFor(dev, released, EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    tag string NULL'), '1.0.1', [r100]);
                const r101 = released.resolve('1.0.1');
                const v120 = v110.replace('summary string NULL', 'summary string NULL,\n    tag string NULL');
                const merge = await releaseFor(dev, released, v120, '1.2.0', [r110, r101]);
                assertEquals(merge.draft.schemas.length, 0, "the parents' merge already has both columns");
                assertEquals(merge.draft.changes.map((c) => c.group).join(','), 'doc', 'the group the parents disagree on is set');
                const mergeLine = formatDraft(merge.draft).find((l) => l.includes('UPDATE SCHEMA hhs:doc'))!;
                assertEquals(mergeLine.trim(), 'UPDATE SCHEMA hhs:doc TO LATEST ON doc', 'the merge sets the group at LATEST');
                const mergePin = merge.draft.pins.get('hhs:doc');
                assertTrue(mergePin?.kind === 'entries' && mergePin.entries.length === 2, 'pinned at both parents\' versions');
                assertParses(merge.draft, dev.keys.labels());
                const r120 = released.resolve('1.2.0');

                const refusals = async (source: string, version = '1.3.0', parents = [r120]) => (await draftFor(dev, released, source, version, parents)).refusals.map((r) => r.message).join('\n');
                assertTrue((await refusals(v120.replace(',\n  TABLEGROUP notes USING SCHEMA hhs:tags BIND tags => tags', ''))).includes('group notes is missing from target-catalog.sql'), 'a removed group is refused');
                assertTrue((await refusals(v120.replace("name = 'Admin'", "name = 'Root'"))).includes('group user: its rows change'), 'a changed group is refused, naming the field');
                assertTrue((await refusals(v120.replace(', :moderator identity', ''))).includes('param :moderator is missing'), 'a removed param is refused');
                assertTrue((await refusals(v120.replace(':moderator identity', ':moderator string'))).includes('param :moderator changes type'), 'a retyped param is refused');
                assertTrue((await refusals(v120.replace('CREATE CATALOG editor CREATORS ($dev)', 'CREATE CATALOG editor CREATORS ($dev, $alice)'))).includes("catalog's CREATORS differ"), 'changed creators are refused');
                assertTrue((await refusals(v120, '1.1.5')).includes('1.1.5 is not above its parent 1.2.0'), 'a version below a parent is refused');
                assertTrue((await refusals(v120, '1.0.1', [r100])).includes('release 1.0.1 already exists'), 'an existing version is refused');
                assertTrue((await refusals(v120, '1.2.1')).includes('nothing to release'), 'an unchanged release is refused');

                const partial = await draftFor(dev, released, v110.replace('summary string NULL', 'summary string NULL,\n    more string NULL'), '1.3.0', [r110]);
                assertTrue(partial.warnings.some((w) => w.startsWith('1.0.1') && w.includes('not in its past')), `lower releases left out are warned about: ${partial.warnings.join('; ')}`);
            } finally {
                await released.close();
            }
        },
    },
    {
        name: '[RPACK11] a stated schema VERSION is the version the release gives it, on its own numbering',
        invoke: async () => {
            const dev = await devKeys();
            const released = await Released.open([]);
            const docAt = (source: string, v: string) => source.replace('CREATE SCHEMA hhs:doc CREATORS ($dev)', `CREATE SCHEMA hhs:doc CREATORS ($dev) VERSION '${v}'`);
            const versionsAt = async (hash: string, schema: string) => (await released.base([hash])).schemas.get(schema)!.view.getVersions().join(',');
            const refusalsOf = async (source: string, version: string, parents: Parameters<typeof draftFor>[4]) =>
                (await draftFor(dev, released, source, version, parents)).refusals.map((r) => `${r.where?.line ?? ''}:${r.message}`).join('\n');
            try {
                const first = await releaseFor(dev, released, docAt(EDITOR_SOURCE, '3.0.0'), '1.0.0', []);
                const firstText = formatDraft(first.draft, dev.keys.labels()).join('\n');
                assertTrue(firstText.includes("CREATE SCHEMA hhs:doc CREATORS ($dev) VERSION '3.0.0' AS ("), `a stated version is created at: ${firstText}`);
                assertTrue(firstText.includes("CREATE SCHEMA hhs:user CREATORS ($dev) VERSION '1.0.0' AS ("), 'an omitted one takes the release version');
                assertTrue(firstText.includes("TABLEGROUP doc USING SCHEMA hhs:doc AT LATEST"), `the group pins it: ${firstText}`);
                assertParses(first.draft, dev.keys.labels());
                assertEquals(await versionsAt(first.produced.hash, 'hhs:doc'), '3.0.0', 'the signed schema is at its stated version');
                assertEquals(await versionsAt(first.produced.hash, 'hhs:user'), '1.0.0', 'and the other at the release version');
                const r100 = released.resolve('1.0.0');

                const summary = (source: string) => source.replace('deleted boolean', 'deleted boolean,\n    summary string NULL');
                const stale = await refusalsOf(summary(docAt(EDITOR_SOURCE, '3.0.0')), '1.1.0', [r100]);
                assertTrue(stale.includes("schema hhs:doc: VERSION '3.0.0' is not above 3.0.0; raise it"), `a changed schema must raise its stated version: ${stale}`);
                assertTrue(/^\d+:schema hhs:doc/m.test(stale), `the refusal has a location: ${stale}`);
                const lower = await refusalsOf(summary(docAt(EDITOR_SOURCE, '1.1.0')), '1.1.0', [r100]);
                assertTrue(lower.includes("VERSION '1.1.0' is not above 3.0.0"), `a stated version below the schema's is refused, whatever the release version: ${lower}`);

                const v110 = summary(docAt(EDITOR_SOURCE, '7.0.0'));
                const second = await releaseFor(dev, released, v110, '1.1.0', [r100]);
                const secondText = formatDraft(second.draft, dev.keys.labels()).join('\n');
                assertTrue(secondText.includes("ALTER SCHEMA hhs:doc VERSION '7.0.0' AS ("), `the update is at the stated version: ${secondText}`);
                assertTrue(secondText.includes("UPDATE SCHEMA hhs:doc TO LATEST ON doc"), `and the group moves to it: ${secondText}`);
                assertParses(second.draft, dev.keys.labels());
                assertEquals(await versionsAt(second.produced.hash, 'hhs:doc'), '7.0.0', 'the signed update is at 7.0.0');
                const r110 = released.resolve('1.1.0');

                const userChange = (source: string) => source.replace('grantee identity PUB READONLY', 'grantee identity PUB READONLY,\n    memo string NULL');
                const unchanged = await draftFor(dev, released, userChange(v110), '1.2.0', [r110]);
                assertEquals(unchanged.refusals.length, 0, `an unchanged schema may state the version it has: ${unchanged.refusals.map((r) => r.message).join('; ')}`);
                assertTrue(formatDraft(unchanged, dev.keys.labels()).join('\n').includes("ALTER SCHEMA hhs:user VERSION '1.2.0' AS ("), 'a changed schema with no VERSION takes the release version');
                const bumped = await refusalsOf(userChange(v110.replace("VERSION '7.0.0'", "VERSION '8.0.0'")), '1.2.0', [r110]);
                assertTrue(bumped.includes("schema hhs:doc: VERSION '8.0.0' but nothing in it changes (it is at 7.0.0); a version needs a change"), `a bump without a change is refused: ${bumped}`);
            } finally {
                await released.close();
            }
        },
    },
    {
        name: '[RPACK14] FILES: shipped with the catalog, ADD FILES for a new one, refused when changed or removed',
        invoke: async () => {
            const dev = await devKeys();
            const labels = dev.keys.labels();
            const released = await Released.open([]);
            const withFiles = (source: string, item: string) => source.replace(/\n\);\n$/, `,\n  ${item}\n);\n`);
            const media = withFiles(EDITOR_SOURCE,
                "FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author");
            try {
                const first = await releaseFor(dev, released, media, '1.0.0', []);
                assertEquals(first.draft.addFiles.map((f) => f.name).join(','), 'media', 'the first release adds every FILES');
                const firstText = formatDraft(first.draft, labels).join('\n');
                assertTrue(firstText.includes('  FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF EXISTS user.caps'), `the genesis lists it: ${firstText}`);
                assertParses(first.draft, labels);
                assertTrue((await released.base([first.produced.hash])).files.has('media'), 'the signed release holds the FILES');
                const r100 = released.resolve('1.0.0');

                const v110 = withFiles(media, 'FILES attachments USING IDENTITIES user.identities ALLOW WRITE IF true');
                const second = await releaseFor(dev, released, v110, '1.1.0', [r100]);
                assertEquals(second.draft.addFiles.map((f) => f.name).join(','), 'attachments', 'a new FILES is added');
                assertEquals(second.draft.add.length + second.draft.changes.length + second.draft.schemas.length, 0, 'and nothing else');
                const secondText = formatDraft(second.draft, labels).join('\n');
                assertTrue(secondText.includes("ALTER CATALOG editor VERSION '1.1.0' AS (\n  ADD FILES attachments\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF true\n);"),
                    `the draft adds it: ${secondText}`);
                assertParses(second.draft, labels);
                assertEquals(statusSummary(second.draft), 'added 1 FILES', 'the summary counts it');
                const r110 = released.resolve('1.1.0');

                const refusals = async (source: string) => (await draftFor(dev, released, source, '1.2.0', [r110])).refusals.map((r) => r.message).join('\n');
                const changed = await refusals(v110.replace("ALLOW WRITE IF EXISTS user.caps WHERE user.caps.label = 'writer'", "ALLOW WRITE IF EXISTS user.caps WHERE user.caps.label = 'editor'"));
                assertTrue(changed.includes('FILES media: its ALLOW WRITE change, and FILES definitions are immutable, use a new name'), `a changed FILES is refused: ${changed}`);
                const rebound = await refusals(v110.replace('FILES attachments USING IDENTITIES user.identities', 'FILES attachments BIND u => user USING IDENTITIES u.identities'));
                assertTrue(rebound.includes('FILES attachments: its BIND alias, identity table change'), `a new alias is a change: ${rebound}`);
                const removed = await refusals(media);
                assertTrue(removed.includes('FILES attachments is missing from target-catalog.sql'), `a removed FILES is refused: ${removed}`);
            } finally {
                await released.close();
            }
        },
    },
    {
        name: '[RPACK16] a FILES left out once its group drops what it reads becomes read-only: a warning, once; listed again unchanged, it is writable',
        invoke: async () => {
            const dev = await devKeys();
            const labels = dev.keys.labels();
            const released = await Released.open([]);
            const withFiles = (source: string, item: string) => source.replace(/\n\);\n$/, `,\n  ${item}\n);\n`);
            const withUploaders = (source: string) => source.replace('  TABLE caps (', '  TABLE uploaders (\n    grantee identity PUB READONLY\n  ),\n\n  TABLE caps (');
            const withMemo = (source: string) => source.replace('grantee identity PUB READONLY\n  ) CONCURRENT DELETES', 'grantee identity PUB READONLY,\n    memo string NULL\n  ) CONCURRENT DELETES');
            const media = withUploaders(withFiles(EDITOR_SOURCE,
                'FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF EXISTS user.uploaders WHERE user.uploaders.grantee = $author'));
            const readOnly = 'FILES media becomes read-only in 1.1.0: ALLOW WRITE IF reads user.uploaders: '
                + 'schema hhs:user has no table uploaders; it stays in the catalog, and a new FILES can take its place';
            const filesWarnings = (draft: ReleaseDraft) => draft.warnings.filter((w) => w.startsWith('FILES '));
            const refusalsOf = (draft: ReleaseDraft) => draft.refusals.map((r) => r.message).join('\n');
            try {
                await releaseFor(dev, released, media, '1.0.0', []);
                const r100 = released.resolve('1.0.0');

                const kept = await draftFor(dev, released, withMemo(media), '1.1.0', [r100]);
                assertEquals(kept.changes.map((c) => c.group).join(','), 'user', 'the user group changes');
                assertEquals(filesWarnings(kept).join('; '), '', 'a change that keeps what the FILES reads warns nothing');
                const removed = refusalsOf(await draftFor(dev, released, withMemo(withUploaders(EDITOR_SOURCE)), '1.1.0', [r100]));
                assertTrue(removed.includes("FILES media is missing from target-catalog.sql; FILES can't be removed"), `a FILES that still fits can't be left out: ${removed}`);

                const second = await releaseFor(dev, released, EDITOR_SOURCE, '1.1.0', [r100]);
                assertEquals(filesWarnings(second.draft).join('; '), readOnly, 'leaving out the FILES whose table is dropped warns');
                assertTrue(formatDraft(second.draft, labels).join('\n').includes(readOnly), 'the draft shows the warning');
                const after = await released.base([second.produced.hash]);
                assertTrue(after.files.get('media')?.readOnly !== undefined, 'the catalog accepts the release, and keeps the FILES, read-only');
                assertTrue(!after.model.files.has('media'), 'the model target-catalog.sql is compared to leaves it out');
                const r110 = released.resolve('1.1.0');

                const later = await draftFor(dev, released, withMemo(EDITOR_SOURCE), '1.2.0', [r110]);
                assertEquals(refusalsOf(later), '', 'a later change is not refused');
                assertEquals(filesWarnings(later).join('; '), '', 'a FILES already read-only in the parents is not warned about again');

                const reused = refusalsOf(await draftFor(dev, released, withFiles(EDITOR_SOURCE, 'FILES media USING IDENTITIES user.identities ALLOW WRITE IF true'), '1.2.0', [r110]));
                assertTrue(reused.includes("FILES media is read-only in the parents, and its definition can't change"), `a read-only FILES keeps its name: ${reused}`);

                const restored = await releaseFor(dev, released, media, '1.2.0', [r110]);
                assertEquals(restored.draft.addFiles.length, 0, 'a read-only FILES listed again unchanged is not added again');
                assertTrue((await released.base([restored.produced.hash])).model.files.has('media'), 'with its table back, it is writable again');
            } finally {
                await released.close();
            }
        },
    },
];
