import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    buildVersion, importSchema, initProject, logReleases, newVersion, publishRelease, releaseVersion, RpackError, statusOf,
    type RpackContext,
} from "../src/commands.js";
import { isSchemaReleaseFile, parseAnyReleaseFile, parseReleaseFile, parseSchemaReleaseFile } from "../src/format.js";
import { formatReleasePreview, type ReleasePreview } from "../src/format_draft.js";
import { MemoryProject, parseRpackConfig, SCHEMA_SOURCE_FILE, SOURCE_FILE, UPGRADE_MANUAL_FILE, workPath } from "../src/project.js";
import { readLockedImports, resolveCatalogInRegistry } from "../src/registry.js";
import { readSource } from "../src/source.js";
import { verifyRelease } from "../src/verify.js";
import { writeSource } from "../src/write_source.js";
import { devKeys, expectThrows, PASSPHRASE } from "./fixtures.js";

const COMMENTS_100 = `CREATE SCHEMA hhs:comments PARAMS (:maxLen integer DEFAULT 200) AS (
  TABLE comments (
    targetId string READONLY REFERENCES target.items,
    body string PUB,
    len integer READONLY
  ) ALLOW insert IF comments.len <= :maxLen
);
`;

const COMMENTS_110 = `CREATE SCHEMA hhs:comments PARAMS (:maxLen integer DEFAULT 200, :allowAnon boolean DEFAULT false) AS (
  TABLE comments (
    targetId string READONLY REFERENCES target.items,
    body string PUB,
    len integer READONLY,
    pinned boolean NULL
  ) ALLOW insert IF comments.len <= :maxLen
);
`;

async function expectRpackError(fn: () => Promise<unknown>, check: (err: RpackError) => void, what: string): Promise<void> {
    try {
        await fn();
    } catch (err) {
        if (err instanceof RpackError) {
            check(err);
            return;
        }
        throw err;
    }
    throw new Error(`${what}: expected RpackError`);
}

export const composabilityTests = [
    {
        name: '[RPACK20] schema pack repository: keyless init --schema, build, release, params, verify, and tamper checks',
        invoke: async () => {
            const schemaProj = new MemoryProject();
            const ctx: RpackContext = { project: schemaProj };

            await initProject(schemaProj, 'hhs:comments', '', { kind: 'schema' });
            const cfg = parseRpackConfig(schemaProj.files.get('rpack.json')!);
            assertEquals(cfg.kind, 'schema', 'rpack.json records kind = schema');
            assertEquals(cfg.name, 'hhs:comments', 'rpack.json records schema name');

            // 1.0.0
            await newVersion(ctx, '1.0.0');
            assertTrue(schemaProj.files.get(workPath('1.0.0', SCHEMA_SOURCE_FILE))!.includes('CREATE SCHEMA hhs:comments'),
                'schema skeleton written to target-schema.sql');

            // CREATORS in target-schema.sql is refused
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE),
                COMMENTS_100.replace('CREATE SCHEMA hhs:comments', 'CREATE SCHEMA hhs:comments CREATORS ($dev)'));
            await expectRpackError(
                () => buildVersion(ctx, '1.0.0'),
                (err) => assertTrue(err.lines.join('\n').includes('remove CREATORS'), `expected CREATORS refusal, got: ${err.lines.join('\n')}`),
                'CREATORS in target-schema.sql',
            );

            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), COMMENTS_100);
            const b100 = await buildVersion(ctx, '1.0.0');
            assertEquals(b100.draft.refusals.length, 0, '1.0.0 builds cleanly');
            assertTrue(schemaProj.files.get(workPath('1.0.0', 'build/update.sql'))!.includes(':maxLen integer DEFAULT 200'),
                'build/update.sql renders PARAMS');

            const r100 = await releaseVersion(ctx, '1.0.0');
            const f100Text = schemaProj.files.get(`releases/${r100.produced.name}.rpack`)!;
            const f100 = parseSchemaReleaseFile(f100Text);
            assertTrue(isSchemaReleaseFile(f100), 'produced file is a SchemaReleaseFile');
            assertEquals(f100.manifest.kind, 'schema', 'manifest.kind is schema');
            assertEquals(f100.steps.length, 1, '1.0.0 has 1 step');
            const v100 = await verifyRelease(f100Text);
            assertTrue(v100.ok, '1.0.0 verifies');

            // 1.1.0: adding a param without DEFAULT on ALTER SCHEMA is refused
            await newVersion(ctx, '1.1.0');
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE),
                COMMENTS_110.replace(':allowAnon boolean DEFAULT false', ':allowAnon boolean'));
            const bBadParam = await buildVersion(ctx, '1.1.0');
            assertTrue(bBadParam.draft.refusals.some((r) => r.message.includes('needs a DEFAULT')),
                'new parameter without DEFAULT on schema update is refused');

            // 1.1.0 with DEFAULT builds and releases
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            const r110 = await releaseVersion(ctx, '1.1.0');
            const f110Text = schemaProj.files.get(`releases/${r110.produced.name}.rpack`)!;
            const f110 = parseSchemaReleaseFile(f110Text);
            assertEquals(f110.steps.length, 2, '1.1.0 ships both steps in causal order');
            const v110 = await verifyRelease(f110Text);
            assertTrue(v110.ok, '1.1.0 verifies');

            // Status and log work in a schema repo
            const st = await statusOf(ctx, '1.1.0');
            assertTrue(st.lines.join('\n').includes('hhs:comments 1.1.0'), 'status prints schema header');
            assertEquals(st.draft.refusals.length, 0, `the released folder's status is not refused:\n${st.lines.join('\n')}`);
            assertEquals(st.lines[4], `Released as ${r110.produced.name}`, 'status names the release made from the folder');
            const log = await logReleases(ctx);
            assertEquals(log.length, 2, 'log lists 1.0.0 and 1.1.0');

            // Tampering with a step fails verification
            const tampered = JSON.parse(f110Text);
            tampered.steps[1].version = '1.2.0';
            const badVer = await verifyRelease(JSON.stringify(tampered));
            assertFalse(badVer.ok, 'tampered step version fails hash verification');
        },
    },
    {
        name: '[RPACK21] registry publish & import, IMPORT SCHEMA DAG replay, tableMaps, and effective group params',
        invoke: async () => {
            const dev = await devKeys();
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const registry = new MemoryProject();

            // 1. Create and publish hhs:comments 1.0.0 and 1.1.0
            const schemaProj = new MemoryProject();
            const schemaCtx: RpackContext = { project: schemaProj };
            await initProject(schemaProj, 'hhs:comments', '', { kind: 'schema' });
            await newVersion(schemaCtx, '1.0.0');
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), COMMENTS_100);
            await releaseVersion(schemaCtx, '1.0.0');
            await newVersion(schemaCtx, '1.1.0');
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            await releaseVersion(schemaCtx, '1.1.0');

            const pubLines = await publishRelease(schemaProj, registry);
            assertEquals(pubLines.length, 2, 'published both 1.0.0 and 1.1.0 to registry');
            assertTrue((await registry.list('hhs/comments')).length === 2, 'registry stores under hhs/comments/');

            // 2. Initialize catalog repo `blog` and import hhs:comments 1.0.0
            const catProj = new MemoryProject();
            const catCtx: RpackContext = { project: catProj, vault: dev.vault };
            await initProject(catProj, 'blog', 'dev');
            await newVersion(catCtx, '1.0.0');

            const imp100 = await importSchema(catProj, registry, '1.0.0', 'hhs:comments', '1.0.0');
            assertEquals(imp100.file.manifest.version, '1.0.0', 'imported 1.0.0');
            const cfgAfterImp100 = parseRpackConfig(catProj.files.get('rpack.json')!);
            assertEquals(cfgAfterImp100.imports?.['hhs:comments']?.['1.0.0'], imp100.file.manifest.release,
                'rpack.json locks step hash of hhs:comments 1.0.0');

            const blog100 = `IMPORT SCHEMA hhs:comments VERSION '1.0.0';

CREATE SCHEMA blog:posts AS (
  TABLE articles (
    title string PUB
  )
);

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP posts USING SCHEMA blog:posts,
  TABLEGROUP post_comments USING SCHEMA hhs:comments
    BIND target => posts (items => articles)
    WITH PARAMS (:maxLen = 500)
);
`;
            await catProj.write(workPath('1.0.0', SOURCE_FILE), blog100);
            const rel100 = await releaseVersion(catCtx, '1.0.0', unlock);
            const rel100File = parseReleaseFile(catProj.files.get(`releases/${rel100.produced.name}.rpack`)!);
            assertTrue((await verifyRelease(rel100File)).ok, 'blog 1.0.0 release verifies');
            const commentsObj100 = rel100File.objects.find((o) => o.create['name'] === 'hhs:comments')!;
            assertEquals(commentsObj100.entries.length, 0, 'hhs:comments at 1.0.0 is at genesis in blog 1.0.0');

            // 3. Start blog 1.1.0: writeFresh preserves IMPORT SCHEMA and renders BIND tableMaps + WITH PARAMS
            await newVersion(catCtx, '1.1.0');
            const fresh110 = catProj.files.get(workPath('1.1.0', SOURCE_FILE))!;
            assertTrue(fresh110.includes("IMPORT SCHEMA hhs:comments VERSION '1.0.0';"),
                `fresh 1.1.0 keeps IMPORT SCHEMA: ${fresh110}`);
            assertTrue(fresh110.includes('items => articles') && fresh110.includes(':maxLen = 500'),
                `fresh 1.1.0 keeps tableMaps and WITH PARAMS: ${fresh110}`);

            // Upgrade hhs:comments to 1.1.0 via rpack import (omitting version picks highest = 1.1.0)
            const imp110 = await importSchema(catProj, registry, '1.1.0', 'hhs:comments');
            assertEquals(imp110.file.manifest.version, '1.1.0', 'import without version picks highest (1.1.0)');
            const updated110 = catProj.files.get(workPath('1.1.0', SOURCE_FILE))!;
            assertTrue(updated110.includes("IMPORT SCHEMA hhs:comments VERSION '1.1.0';"),
                'rpack import updates IMPORT SCHEMA VERSION in target-catalog.sql in place');

            // Effective parameter comparison: removing :maxLen = 500 (differs from default 200) is refused!
            await catProj.write(workPath('1.1.0', SOURCE_FILE), updated110.replace('\n    WITH PARAMS (:maxLen = 500)', ''));
            const bRemovedOverride = await buildVersion(catCtx, '1.1.0');
            assertTrue(bRemovedOverride.draft.refusals.some((r) => r.message.includes('param :maxLen is removed from WITH PARAMS') && (r.hint ?? '').includes("write the default's value")),
                `removing non-default group param override is refused with hint: ${JSON.stringify(bRemovedOverride.draft.refusals)}`);

            // Updating :maxLen = 1000 and setting :allowAnon = false (same as default false) succeeds
            await catProj.write(workPath('1.1.0', SOURCE_FILE),
                updated110.replace(':maxLen = 500', ':maxLen = 1000, :allowAnon = false'));
            const rel110 = await releaseVersion(catCtx, '1.1.0', unlock);
            const rel110File = parseReleaseFile(catProj.files.get(`releases/${rel110.produced.name}.rpack`)!);
            assertTrue((await verifyRelease(rel110File)).ok, 'blog 1.1.0 release verifies');
            const commentsObj110 = rel110File.objects.find((o) => o.create['name'] === 'hhs:comments')!;
            assertEquals(commentsObj110.id, commentsObj100.id, 'DAG-preserving replay keeps the same RSchema object ID across releases');
            assertEquals(commentsObj110.entries.length, 1, 'blog 1.1.0 appends only the 1.1.0 schema step entry');

            // 4. Publish blog 1.0.0 and 1.1.0 to registry and resolve via resolveCatalogInRegistry
            await publishRelease(catProj, registry);
            const resolvedLatest = await resolveCatalogInRegistry(registry, 'blog');
            assertEquals(resolvedLatest?.file.manifest.version, '1.1.0', 'resolveCatalogInRegistry picks 1.1.0');
            const resolvedDescendant = await resolveCatalogInRegistry(registry, 'blog', {
                catalogId: rel100File.manifest.catalog,
                afterRelease: rel100File.manifest.release,
                maxMajor: 1,
            });
            assertEquals(resolvedDescendant?.file.manifest.version, '1.1.0', 'resolveCatalogInRegistry finds descendant for rhost update --pull');

            // Refusal when trying to resolve a schema pack as a catalog
            let schemaAsCatalogThrew = false;
            try {
                await resolveCatalogInRegistry(registry, 'hhs:comments');
            } catch (err) {
                schemaAsCatalogThrew = (err as Error).message.includes("'hhs:comments' is a schema, not a catalog");
            }
            assertTrue(schemaAsCatalogThrew, 'resolveCatalogInRegistry refuses a schema pack');
        },
    },
    {
        name: '[RPACK22] switching between IMPORT SCHEMA and inline CREATE SCHEMA',
        invoke: async () => {
            const dev = await devKeys();
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const registry = new MemoryProject();

            // Publish hhs:comments 1.0.0 and 2.0.0
            const schemaProj = new MemoryProject();
            await initProject(schemaProj, 'hhs:comments', '', { kind: 'schema' });
            await newVersion({ project: schemaProj }, '1.0.0');
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), COMMENTS_100);
            await releaseVersion({ project: schemaProj }, '1.0.0');
            await newVersion({ project: schemaProj }, '2.0.0');
            await schemaProj.write(workPath('2.0.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            await releaseVersion({ project: schemaProj }, '2.0.0');
            await publishRelease(schemaProj, registry);

            // Release blog 1.0.0 with imported hhs:comments 1.0.0
            const catProj = new MemoryProject();
            const catCtx: RpackContext = { project: catProj, vault: dev.vault };
            await initProject(catProj, 'blog', 'dev');
            await newVersion(catCtx, '1.0.0');
            await importSchema(catProj, registry, '1.0.0', 'hhs:comments', '1.0.0');
            await catProj.write(workPath('1.0.0', SOURCE_FILE), `IMPORT SCHEMA hhs:comments VERSION '1.0.0';

CREATE SCHEMA blog:posts AS (
  TABLE items (
    title string PUB
  )
);

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP target USING SCHEMA blog:posts,
  TABLEGROUP comments USING SCHEMA hhs:comments BIND target => target
);
`);
            await releaseVersion(catCtx, '1.0.0', unlock);

            // In 1.1.0, switch hhs:comments from IMPORT SCHEMA to inline CREATE SCHEMA and add a column
            await newVersion(catCtx, '1.1.0');
            const inline110 = `${COMMENTS_100.replace('len integer READONLY\n', 'len integer READONLY,\n    edited boolean NULL\n')}
CREATE SCHEMA blog:posts AS (
  TABLE items (
    title string PUB
  )
);

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP target USING SCHEMA blog:posts,
  TABLEGROUP comments USING SCHEMA hhs:comments BIND target => target
);
`;
            await catProj.write(workPath('1.1.0', SOURCE_FILE), inline110);
            const b110 = await buildVersion(catCtx, '1.1.0');
            assertTrue(b110.draft.refusals.some((r) => r.message.includes('was imported')),
                `switching an imported schema to inline CREATE SCHEMA is refused: ${JSON.stringify(b110.draft.refusals)}`);
        },
    },
    {
        name: '[RPACK23] multi-version import, local patches, and branch merge',
        invoke: async () => {
            const dev = await devKeys();
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const registry = new MemoryProject();
            const schemaProj = new MemoryProject();
            const schemaCtx: RpackContext = { project: schemaProj };
            const notes100 = `CREATE SCHEMA hhs:notes AS (
  TABLE notes (
    body string PUB
  ) ALLOW insert IF true
);
`;
            const notes110 = `CREATE SCHEMA hhs:notes AS (
  TABLE notes (
    body string PUB,
    pinned boolean NULL
  ) ALLOW insert IF false
);
`;
            const notes101 = `CREATE SCHEMA hhs:notes AS (
  TABLE notes (
    body string PUB,
    flagged boolean NULL
  ) ALLOW insert IF true
);
`;
            await initProject(schemaProj, 'hhs:notes', '', { kind: 'schema' });
            await newVersion(schemaCtx, '1.0.0');
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), notes100);
            await releaseVersion(schemaCtx, '1.0.0');
            await newVersion(schemaCtx, '1.1.0');
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), notes110);
            await releaseVersion(schemaCtx, '1.1.0');
            await newVersion(schemaCtx, '1.0.1', { base: ['1.0.0'] });
            await schemaProj.write(workPath('1.0.1', SCHEMA_SOURCE_FILE), notes101);
            await releaseVersion(schemaCtx, '1.0.1');
            await publishRelease(schemaProj, registry);

            const catalogOf = (name: string, versions: string, alter = '') => `${versions}
${alter}

CREATE CATALOG ${name} CREATORS ($dev) AS (
  TABLEGROUP notes USING SCHEMA hhs:notes
);
`;

            const blog = new MemoryProject();
            const blogCtx: RpackContext = { project: blog, vault: dev.vault };
            await initProject(blog, 'blog', 'dev');
            await newVersion(blogCtx, '1.0.0');
            await importSchema(blog, registry, '1.0.0', 'hhs:notes', '1.0.0');
            await blog.write(workPath('1.0.0', SOURCE_FILE), catalogOf('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.0';`,
                `ALTER SCHEMA hhs:notes AS (\n  ADD COLUMN notes.modNote string NULL,\n  SET ALLOW RULES notes (\n    ALLOW insert IF false\n  )\n);`));
            const patched = await buildVersion(blogCtx, '1.0.0');
            assertEquals(patched.draft.refusals.length, 0, `local patch builds: ${JSON.stringify(patched.draft.refusals)}`);
            const update = blog.files.get(workPath('1.0.0', 'build/update.sql'))!;
            assertTrue(update.includes("VERSION '1001.0.0'"), `patch version is written: ${update}`);
            assertTrue(update.includes('imported from hhs:notes 1.0.0'), `upstream step is annotated: ${update}`);
            assertTrue(update.includes('Local patch for catalog 1.0.0'), `patch step is annotated: ${update}`);
            await releaseVersion(blogCtx, '1.0.0', unlock);

            await newVersion(blogCtx, '1.1.0');
            await importSchema(blog, registry, '1.1.0', 'hhs:notes', '1.1.0');
            await blog.write(workPath('1.1.0', SOURCE_FILE), catalogOf('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`,
                `ALTER SCHEMA hhs:notes VERSION '9.0.0' AS (\n  ADD COLUMN notes.extra string NULL\n);`));
            await expectRpackError(
                () => buildVersion(blogCtx, '1.1.0'),
                (err) => assertTrue(err.lines.join('\n').includes('remove VERSION'), `version on a local patch: ${err.lines.join('\n')}`),
                'VERSION on a local ALTER SCHEMA',
            );
            await blog.write(workPath('1.1.0', SOURCE_FILE), catalogOf('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`,
                `ALTER SCHEMA hhs:notes AS (\n  ADD COLUMN notes.modNote string NULL,\n  SET ALLOW RULES notes (\n    ALLOW insert IF false\n  )\n);`));
            const warned = await buildVersion(blogCtx, '1.1.0');
            assertEquals(warned.draft.refusals.length, 0, `kept patch builds: ${JSON.stringify(warned.draft.refusals)}`);
            assertTrue(warned.draft.warnings.some((w) => w.includes('restrictions')),
                `an upstream restriction change overridden by the patch warns: ${warned.draft.warnings.join('; ')}`);

            await blog.write(workPath('1.1.0', SOURCE_FILE), catalogOf('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`,
                `ALTER SCHEMA hhs:notes AS (\n  ADD COLUMN notes.modNote string NULL,\n  DROP COLUMN notes.body,\n  ADD COLUMN notes.body integer NULL\n);`));
            await blog.write(workPath('1.1.0', UPGRADE_MANUAL_FILE), `ALTER SCHEMA hhs:notes AS (\n  DROP COLUMN notes.body\n);\n`);
            const reset = await buildVersion(blogCtx, '1.1.0');
            assertEquals(reset.draft.refusals.length, 0, `column reset builds: ${JSON.stringify(reset.draft.refusals)}`);

            const merge = new MemoryProject();
            const mergeCtx: RpackContext = { project: merge, vault: dev.vault };
            await initProject(merge, 'merge', 'dev');
            await newVersion(mergeCtx, '1.0.0');
            await importSchema(merge, registry, '1.0.0', 'hhs:notes', '1.0.0');
            await importSchema(merge, registry, '1.0.0', 'hhs:notes', '1.1.0');
            const collapsed = merge.files.get(workPath('1.0.0', SOURCE_FILE))!;
            assertTrue(collapsed.includes("VERSION '1.1.0'") && !collapsed.includes("'1.0.0'"),
                `importing a descendant replaces its ancestor: ${collapsed}`);
            await merge.write(workPath('1.0.0', SOURCE_FILE), catalogOf('merge', `IMPORT SCHEMA hhs:notes VERSION '1.0.0', '1.1.0';`));
            await expectRpackError(
                () => buildVersion(mergeCtx, '1.0.0'),
                (err) => assertTrue(err.lines.join('\n').includes('not concurrent'), `antichain: ${err.lines.join('\n')}`),
                'importing an ancestor and its descendant',
            );
            await importSchema(merge, registry, '1.0.0', 'hhs:notes', '1.0.1');
            const both = merge.files.get(workPath('1.0.0', SOURCE_FILE))!;
            assertTrue(both.includes("'1.0.1'") && both.includes("'1.1.0'"), `concurrent imports stay: ${both}`);
            await merge.write(workPath('1.0.0', SOURCE_FILE), catalogOf('merge', `IMPORT SCHEMA hhs:notes VERSION '1.0.1', '1.1.0';`));
            const merged = await releaseVersion(mergeCtx, '1.0.0', unlock);
            const mergedReport = await verifyRelease(merge.files.get(`releases/${merged.produced.name}.rpack`)!);
            assertTrue(mergedReport.ok, `merged import verifies: ${mergedReport.problems.join('; ')}`);
            const heads = mergedReport.summary?.schemas.find((s) => s.name === 'hhs:notes')?.versions ?? [];
            assertTrue(heads.includes('1.0.1') && heads.includes('1.1.0'), `two upstream heads: ${heads.join(', ')}`);
        },
    },
    {
        name: '[RPACK25] import edge cases: merge, sibling history, pins, and slot warnings',
        invoke: async () => {
            const dev = await devKeys();
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const registry = new MemoryProject();
            const schemaProj = new MemoryProject();
            const schemaCtx: RpackContext = { project: schemaProj };
            const notes = (columns: string, allow: string) => `CREATE SCHEMA hhs:notes AS (
  TABLE notes (
    body string PUB${columns}
  ) ALLOW insert IF ${allow}
);
`;
            await initProject(schemaProj, 'hhs:notes', '', { kind: 'schema' });
            await newVersion(schemaCtx, '1.0.0');
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), notes('', 'true'));
            await releaseVersion(schemaCtx, '1.0.0');
            await newVersion(schemaCtx, '1.1.0');
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), notes(',\n    pinned boolean NULL', 'true'));
            const released110 = await releaseVersion(schemaCtx, '1.1.0');
            await newVersion(schemaCtx, '1.2.0');
            await schemaProj.write(workPath('1.2.0', SCHEMA_SOURCE_FILE), notes(',\n    pinned boolean NULL', 'false'));
            await releaseVersion(schemaCtx, '1.2.0');
            await newVersion(schemaCtx, '1.0.1', { base: ['1.0.0'] });
            await schemaProj.write(workPath('1.0.1', SCHEMA_SOURCE_FILE), notes(',\n    flagged boolean NULL', 'true'));
            await releaseVersion(schemaCtx, '1.0.1');
            await newVersion(schemaCtx, '1.0.2', { base: ['1.0.1'] });
            await schemaProj.write(workPath('1.0.2', SCHEMA_SOURCE_FILE), notes(',\n    flagged boolean NULL,\n    extra string NULL', 'true'));
            await releaseVersion(schemaCtx, '1.0.2');
            await publishRelease(schemaProj, registry);

            const posts = (extra = '') => `CREATE SCHEMA blog:posts AS (
  TABLE items (
    title string PUB${extra}
  )
);
`;
            const catalog = (name: string, before: string, groups: string) => `${before}
CREATE CATALOG ${name} CREATORS ($dev) AS (
  ${groups}
);
`;

            const wide = new MemoryProject();
            const wideCtx: RpackContext = { project: wide, vault: dev.vault };
            await initProject(wide, 'blog', 'dev');
            await newVersion(wideCtx, '1.0.0');
            await wide.write(workPath('1.0.0', SOURCE_FILE), catalog('blog', posts(), 'TABLEGROUP posts USING SCHEMA blog:posts'));
            await releaseVersion(wideCtx, '1.0.0', unlock);
            await newVersion(wideCtx, '1.1.0');
            await wide.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', posts(',\n    tag string NULL'), 'TABLEGROUP posts USING SCHEMA blog:posts'));
            await releaseVersion(wideCtx, '1.1.0', unlock);
            await newVersion(wideCtx, '1.0.1', { base: ['1.0.0'] });
            await importSchema(wide, registry, '1.0.1', 'hhs:notes', '1.0.0');
            await wide.write(workPath('1.0.1', SOURCE_FILE), catalog('blog',
                `${posts()}\nIMPORT SCHEMA hhs:notes VERSION '1.0.0';`,
                'TABLEGROUP posts USING SCHEMA blog:posts,\n  TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(wideCtx, '1.0.1', unlock);
            await newVersion(wideCtx, '1.2.0', { base: ['1.1.0', '1.0.1'] });
            const mergedSource = wide.files.get(workPath('1.2.0', SOURCE_FILE))!;
            assertTrue(mergedSource.includes("IMPORT SCHEMA hhs:notes VERSION '1.0.0';"), `the lower parent's import is kept:\n${mergedSource}`);
            assertTrue(!mergedSource.includes('CREATE SCHEMA hhs:notes'), `the imported schema is not inlined:\n${mergedSource}`);
            const mergedBuild = await buildVersion(wideCtx, '1.2.0');
            assertEquals(mergedBuild.draft.refusals.length, 0, `the merge builds: ${JSON.stringify(mergedBuild.draft.refusals)}`);

            const packs = await readLockedImports(wide, parseRpackConfig(wide.files.get('rpack.json')!));
            const patched = `IMPORT SCHEMA hhs:notes VERSION '1.0.0';

ALTER SCHEMA hhs:notes AS (
  ADD COLUMN notes.modNote string NULL
);

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP notes USING SCHEMA hhs:notes
);
`;
            const patchedModel = (await readSource(patched, dev.keys, 'dev', SOURCE_FILE, { imports: packs })).model;
            patchedModel.schemas.get('hhs:notes')!.origin = undefined;
            const blank = await writeSource('', patchedModel, dev.keys, 'dev', {
                imports: packs,
                importedVersions: new Map([['hhs:notes', ['1.0.0']]]),
            });
            assertTrue(blank.includes("IMPORT SCHEMA hhs:notes VERSION '1.0.0';"), `blank text keeps the import:\n${blank}`);
            assertTrue(blank.includes('ADD COLUMN notes.modNote'), `blank text keeps the patch:\n${blank}`);
            assertTrue(!blank.includes('CREATE SCHEMA hhs:notes'), `blank text does not inline the import:\n${blank}`);

            const bothAlters = `IMPORT SCHEMA hhs:notes VERSION '1.0.0';

-- keep me
ALTER SCHEMA hhs:notes AS (
  ADD COLUMN notes.modNote string NULL
);

CREATE SCHEMA blog:posts AS (
  TABLE items (
    title string PUB
  )
);

-- drop me
ALTER SCHEMA hhs:notes AS (
  ADD COLUMN notes.flagged boolean NULL
);

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP notes USING SCHEMA hhs:notes,
  TABLEGROUP posts USING SCHEMA blog:posts
);
`;
            const oneAlter = bothAlters.replace(/\n-- drop me\nALTER SCHEMA hhs:notes AS \(\n  ADD COLUMN notes\.flagged boolean NULL\n\);\n/, '\n');
            const kept = await writeSource(bothAlters, (await readSource(oneAlter, dev.keys, 'dev', SOURCE_FILE, { imports: packs })).model, dev.keys, 'dev', { imports: packs });
            assertTrue(kept.includes('CREATE SCHEMA blog:posts'), `the statement between the alters survives:\n${kept}`);
            assertTrue(kept.includes('ADD COLUMN notes.modNote') && !kept.includes('flagged'), `only the still-needed alter remains:\n${kept}`);
            const bare = oneAlter.replace(/\n-- keep me\nALTER SCHEMA hhs:notes AS \(\n  ADD COLUMN notes\.modNote string NULL\n\);\n/, '\n');
            const cleared = await writeSource(bothAlters, (await readSource(bare, dev.keys, 'dev', SOURCE_FILE, { imports: packs })).model, dev.keys, 'dev', { imports: packs });
            assertTrue(!cleared.includes('ALTER SCHEMA') && !cleared.includes('keep me') && !cleared.includes('drop me'), `a patch the merge no longer needs is removed with its comment:\n${cleared}`);
            assertTrue(cleared.includes('CREATE SCHEMA blog:posts'), `removing the alters leaves the schema between them:\n${cleared}`);

            const pastFile = parseSchemaReleaseFile(schemaProj.files.get(`releases/${released110.produced.name}.rpack`)!);
            const doctored = new Map(packs);
            doctored.set('hhs:notes@1.0.0', pastFile);
            const named = await readSource(`IMPORT SCHEMA hhs:notes VERSION '1.0.0';

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP notes USING SCHEMA hhs:notes
);
`, dev.keys, 'dev', SOURCE_FILE, { imports: doctored });
            const namedSchema = named.model.schemas.get('hhs:notes')!;
            assertTrue(namedSchema.origin?.kind === 'imported' && namedSchema.origin.steps.every((s) => s.version === '1.0.0'),
                `steps past the named version are dropped: ${namedSchema.origin?.kind === 'imported' ? namedSchema.origin.steps.map((s) => s.version).join(', ') : namedSchema.origin?.kind}`);
            assertTrue(!Object.keys(namedSchema.tables.get('notes')!.columns).includes('pinned'), 'a later step does not change the imported tables');

            const side = new MemoryProject();
            const sideCtx: RpackContext = { project: side, vault: dev.vault };
            await initProject(side, 'blog', 'dev');
            await newVersion(sideCtx, '1.0.0');
            await importSchema(side, registry, '1.0.0', 'hhs:notes', '1.0.0');
            await side.write(workPath('1.0.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(sideCtx, '1.0.0', unlock);
            await newVersion(sideCtx, '1.1.0');
            await importSchema(side, registry, '1.1.0', 'hhs:notes', '1.1.0');
            await side.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(sideCtx, '1.1.0', unlock);
            await newVersion(sideCtx, '1.0.1', { base: ['1.0.0'] });
            await importSchema(side, registry, '1.0.1', 'hhs:notes', '1.0.1');
            await side.write(workPath('1.0.1', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.1';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const sibling = await buildVersion(sideCtx, '1.0.1');
            assertEquals(sibling.draft.refusals.length, 0, `a sibling release's import does not block another branch: ${JSON.stringify(sibling.draft.refusals)}`);

            await newVersion(sideCtx, '1.2.0', { base: ['1.0.0'] });
            await side.write(workPath('1.2.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const repin = await buildVersion(sideCtx, '1.2.0');
            assertEquals(repin.draft.refusals.length, 0, `repin builds: ${JSON.stringify(repin.draft.refusals)}`);
            const repinPin = repin.draft.pins.get('hhs:notes');
            assertTrue(repinPin?.kind === 'entries' && repinPin.entries.length > 0, `an already-published head is pinned: ${JSON.stringify(repinPin)}`);
            const repinned = await releaseVersion(sideCtx, '1.2.0', unlock);
            const repinReport = await verifyRelease(side.files.get(`releases/${repinned.produced.name}.rpack`)!);
            const repinVersions = repinReport.summary?.schemas.find((s) => s.name === 'hhs:notes')?.versions ?? [];
            assertTrue(repinVersions.includes('1.1.0'), `the release pins 1.1.0: ${repinVersions.join(', ')}`);

            await newVersion(sideCtx, '1.3.0', { base: ['1.0.0'] });
            await importSchema(side, registry, '1.3.0', 'hhs:notes', '1.0.1');
            await side.write(workPath('1.3.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0', '1.0.1';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const covered = await buildVersion(sideCtx, '1.3.0');
            assertEquals(covered.draft.refusals.length, 0, `covering build: ${JSON.stringify(covered.draft.refusals)}`);
            const coveredPin = covered.draft.pins.get('hhs:notes');
            assertTrue(coveredPin?.kind === 'update' && coveredPin.from !== undefined && coveredPin.from.length > 0,
                `queued upstream steps start from the already-published head: ${JSON.stringify(coveredPin)}`);
            const coveredRelease = await releaseVersion(sideCtx, '1.3.0', unlock);
            const coveredReport = await verifyRelease(side.files.get(`releases/${coveredRelease.produced.name}.rpack`)!);
            const coveredVersions = coveredReport.summary?.schemas.find((s) => s.name === 'hhs:notes')?.versions ?? [];
            assertTrue(coveredVersions.includes('1.1.0') && coveredVersions.includes('1.0.1'), `both heads are pinned: ${coveredVersions.join(', ')}`);

            const hist = new MemoryProject();
            const histCtx: RpackContext = { project: hist, vault: dev.vault };
            await initProject(hist, 'blog', 'dev');
            await newVersion(histCtx, '1.0.0');
            await importSchema(hist, registry, '1.0.0', 'hhs:notes', '1.0.2');
            await hist.write(workPath('1.0.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.2';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(histCtx, '1.0.0', unlock);
            await newVersion(histCtx, '1.1.0');
            await importSchema(hist, registry, '1.1.0', 'hhs:notes', '1.1.0');
            await hist.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const intermediate = await buildVersion(histCtx, '1.1.0');
            assertTrue(intermediate.draft.refusals.some((r) => r.message.includes("do not have version '1.0.1'")),
                `an intermediate parent version is named: ${JSON.stringify(intermediate.draft.refusals)}`);

            const locked = new MemoryProject();
            const lockedCtx: RpackContext = { project: locked, vault: dev.vault };
            await initProject(locked, 'blog', 'dev');
            await newVersion(lockedCtx, '1.0.0');
            await importSchema(locked, registry, '1.0.0', 'hhs:notes', '1.0.1');
            await locked.write(workPath('1.0.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.1';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(lockedCtx, '1.0.0', unlock);
            await newVersion(lockedCtx, '1.1.0');
            await importSchema(locked, registry, '1.1.0', 'hhs:notes', '1.1.0');
            await locked.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.1.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const lockedMiss = await buildVersion(lockedCtx, '1.1.0');
            assertTrue(lockedMiss.draft.refusals.some((r) => r.message.includes("do not have version '1.0.1'"))
                && lockedMiss.draft.refusals.every((r) => !r.message.includes('defined inline')),
                `a locked parent version is not called inline: ${JSON.stringify(lockedMiss.draft.refusals)}`);

            const allow = (rule: string) => `ALTER SCHEMA hhs:notes AS (
  SET ALLOW RULES notes (
    ALLOW insert IF ${rule}
  )
);`;
            const warn = new MemoryProject();
            const warnCtx: RpackContext = { project: warn, vault: dev.vault };
            await initProject(warn, 'blog', 'dev');
            await newVersion(warnCtx, '1.0.0');
            await importSchema(warn, registry, '1.0.0', 'hhs:notes', '1.0.0');
            await warn.write(workPath('1.0.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.0';\n\n${allow('false')}`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(warnCtx, '1.0.0', unlock);
            await newVersion(warnCtx, '1.1.0');
            await importSchema(warn, registry, '1.1.0', 'hhs:notes', '1.2.0');
            await warn.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.2.0';\n\n${allow('true')}`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const edited = await buildVersion(warnCtx, '1.1.0');
            assertEquals(edited.draft.refusals.length, 0, `edited patch builds: ${JSON.stringify(edited.draft.refusals)}`);
            const restrictionWarnings = edited.draft.warnings.filter((w) => w.includes('overridden by the local ALTER SCHEMA patch') && w.includes('restrictions'));
            assertTrue(restrictionWarnings.some((w) => w.includes('1.2.0') && !w.includes('1.1.0')),
                `the warning names only the step that changed restrictions: ${edited.draft.warnings.join('; ')}`);
            await warn.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.2.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const removed = await buildVersion(warnCtx, '1.1.0');
            assertTrue(!removed.draft.warnings.some((w) => w.includes('overridden by the local ALTER SCHEMA patch')),
                `removing the override does not warn: ${removed.draft.warnings.join('; ')}`);

            const added = new MemoryProject();
            const addedCtx: RpackContext = { project: added, vault: dev.vault };
            await initProject(added, 'blog', 'dev');
            await newVersion(addedCtx, '1.0.0');
            await importSchema(added, registry, '1.0.0', 'hhs:notes', '1.0.0');
            await added.write(workPath('1.0.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.0.0';`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            await releaseVersion(addedCtx, '1.0.0', unlock);
            await newVersion(addedCtx, '1.1.0');
            await importSchema(added, registry, '1.1.0', 'hhs:notes', '1.2.0');
            await added.write(workPath('1.1.0', SOURCE_FILE), catalog('blog', `IMPORT SCHEMA hhs:notes VERSION '1.2.0';\n\n${allow('true')}`, 'TABLEGROUP notes USING SCHEMA hhs:notes'));
            const introduced = await buildVersion(addedCtx, '1.1.0');
            assertTrue(introduced.draft.warnings.some((w) => w.includes('overridden by the local ALTER SCHEMA patch') && w.includes('restrictions') && w.includes('1.2.0')),
                `an override added with the upstream change warns: ${introduced.draft.warnings.join('; ')}`);
        },
    },
    {
        name: '[RPACK28] a released schema folder: status and build work in it, and the step it makes decides whether it changed since release',
        invoke: async () => {
            const project = new MemoryProject();
            const ctx: RpackContext = { project };
            await initProject(project, 'hhs:comments', '', { kind: 'schema' });
            await newVersion(ctx, '1.0.0');
            await project.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), COMMENTS_100);
            const r100 = (await releaseVersion(ctx, '1.0.0')).produced.name;

            // The first version, when releases/ holds its release alone
            const first = await statusOf(ctx, '1.0.0');
            assertEquals(first.draft.refusals.length, 0, `the first version's status is not refused:\n${first.lines.join('\n')}`);
            assertEquals(first.lines[4], `Released as ${r100}`, 'status names the release');
            const built = await buildVersion(ctx, '1.0.0');
            assertEquals(built.matches, true, `build makes the release:\n${built.lines.join('\n')}`);
            assertEquals(built.lines[1], `work/1.0.0/ still makes ${r100}`, 'and says so');

            // ...and with a release built on it
            await newVersion(ctx, '1.1.0');
            await project.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            const r110 = (await releaseVersion(ctx, '1.1.0')).produced.name;
            const under = await statusOf(ctx, '1.0.0');
            assertEquals(under.draft.refusals.length, 0, `1.0.0 under 1.1.0 is not refused:\n${under.lines.join('\n')}`);
            assertEquals(under.lines[4], `Released as ${r100}`, '1.0.0 is as released with 1.1.0 built on it');
            assertEquals((await statusOf(ctx, '1.1.0')).lines[4], `Released as ${r110}`, '1.1.0 is as released');

            // Comments and layout are not part of the step
            await project.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), `-- the comments schema\n${COMMENTS_100.replace('body string PUB,', 'body   string   PUB,')}`);
            assertEquals((await statusOf(ctx, '1.0.0')).lines[4], `Released as ${r100}`, 'a comment and spacing leave the folder as released');
            assertEquals((await buildVersion(ctx, '1.0.0')).matches, true, 'and build still makes the release');

            // A column is
            await project.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110.replace('pinned boolean NULL', 'pinned boolean NULL,\n    lang string NULL'));
            const changed = await statusOf(ctx, '1.1.0');
            assertEquals(changed.draft.refusals.length, 0, `the changed folder still drafts:\n${changed.lines.join('\n')}`);
            assertEquals(changed.lines[4], `Released as ${r110}; changed since release`, 'a new column is a change');
            const rebuilt = await buildVersion(ctx, '1.1.0');
            assertEquals(rebuilt.matches, false, 'build makes another release');
            assertEquals(rebuilt.lines[1], `work/1.1.0/ no longer makes ${r110}: rpack release --force re-releases 1.1.0`, 'and names the way out');

            // ...and so is the note
            await project.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            assertEquals((await statusOf(ctx, '1.1.0')).lines[4], `Released as ${r110}`, 'the source as released again');
            const versionPath = workPath('1.1.0', 'version.json');
            const versionText = project.files.get(versionPath)!;
            await project.write(versionPath, JSON.stringify({ ...JSON.parse(versionText), note: 'pins' }));
            assertEquals((await statusOf(ctx, '1.1.0')).lines[4], `Released as ${r110}; changed since release`, 'a new note is a change');
            await project.write(versionPath, versionText);

            // A source that can't be read keeps the header
            await project.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), 'CREATE SCHEMA hhs:comments AS (');
            await expectRpackError(() => statusOf(ctx, '1.0.0'), (err) => {
                assertEquals(err.lines[4], `Released as ${r100}`, `the failure names the release:\n${err.lines.join('\n')}`);
                assertEquals(err.lines[6], 'Error assembling 1.0.0', `then that it could not be assembled:\n${err.lines.join('\n')}`);
            }, 'an unreadable source in a released folder');
        },
    },
    {
        name: '[RPACK29] schema re-release: --force replaces a release and makes the ones built on it again; no writes nothing, and one that does not build fails it',
        invoke: async () => {
            const project = new MemoryProject();
            const ctx: RpackContext = { project };
            const snapshot = () => JSON.stringify([...project.files.entries()].sort());
            const baseOf = (folder: string) => (JSON.parse(project.files.get(workPath(folder, 'version.json'))!).base as string[]).join(',');
            await initProject(project, 'hhs:comments', '', { kind: 'schema' });
            await newVersion(ctx, '1.0.0');
            await project.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), COMMENTS_100);
            const r100 = (await releaseVersion(ctx, '1.0.0')).produced.name;
            await newVersion(ctx, '1.1.0');
            await project.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            const r110 = (await releaseVersion(ctx, '1.1.0')).produced.name;
            await newVersion(ctx, '1.2.0');
            assertEquals(baseOf('1.2.0'), r110, 'the unreleased 1.2.0 is on 1.1.0');

            // A released folder needs --force, and an unchanged one has nothing to re-release
            const asReleased = snapshot();
            await expectThrows(() => releaseVersion(ctx, '1.0.0'), `1.0.0 is released as ${r100}; rpack release --force re-releases it`, 'a released folder without --force');
            await expectThrows(() => releaseVersion(ctx, '1.0.0', undefined, { force: true }), `nothing to re-release: work/1.0.0/ makes ${r100} as it is`,
                'an unchanged folder has nothing to re-release');
            assertEquals(snapshot(), asReleased, 'neither writes anything');

            // 1.0.0 gets a column, and 1.1.0's source takes it too
            const withLang = (source: string) => source.replace('len integer READONLY', 'len integer READONLY,\n    lang string NULL');
            await project.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), withLang(COMMENTS_100));
            await project.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), withLang(COMMENTS_110));
            const edited = snapshot();

            await expectThrows(() => releaseVersion(ctx, '1.0.0', undefined, { force: true }), '--yes', 'without a confirm, releases built on it need --yes');
            let preview: ReleasePreview | undefined;
            await expectThrows(() => releaseVersion(ctx, '1.0.0', undefined, { force: true, confirm: async (p) => { preview = p; return false; } }), 'aborted', 'no aborts');
            assertEquals(snapshot(), edited, 'no writes nothing');
            const shown = formatReleasePreview(preview!).join('\n');
            assertTrue(shown.startsWith(`Re-releasing 1.0.0 replaces ${r100}.\n`), `the preview names the release it replaces:\n${shown}`);
            assertTrue(shown.includes(`It also re-releases 1 release built on it:\n  1.1.0  replaces ${r110}  (changed since release)\n`), `and the one built on it:\n${shown}`);
            assertTrue(shown.includes('ADD COLUMN comments.pinned boolean NULL') && !shown.includes('COLUMN comments.lang'),
                `with the rules it has on the new 1.0.0:\n${shown}`);
            assertTrue(shown.includes('work/1.2.0/ is written against the old 1.1.0; its version.json will name the new one: check it.'), `and the unreleased folder on it:\n${shown}`);
            assertTrue(shown.includes(`Warning: copies of ${r100} and ${r110} that were published or imported are not replaced.`), `and warns about the copies:\n${shown}`);
            assertEquals(preview!.catalog, 'hhs:comments', 'the question can name the schema');
            assertEquals(preview!.rebuilt.length, 1, 'and how many are built on it');

            // Yes re-releases both
            const done = await releaseVersion(ctx, '1.0.0', undefined, { force: true, confirm: async () => true });
            const n100 = done.produced.name;
            const n110 = done.rebuilt[0]!.name;
            assertTrue(n100 !== r100 && n110 !== r110, 'both releases are new');
            assertEquals((await project.list('releases')).sort().join(','), [`${n100}.rpack`, `${n110}.rpack`].sort().join(','), 'the old files are gone');
            for (const name of [n100, n110]) assertTrue((await verifyRelease(project.files.get(`releases/${name}.rpack`)!)).ok, `${name} verifies`);
            const steps110 = parseSchemaReleaseFile(project.files.get(`releases/${n110}.rpack`)!).steps.map((s) => s.hash).join(',');
            assertEquals(steps110, [done.produced.hash, done.rebuilt[0]!.hash].join(','), '1.1.0 ships the new 1.0.0 step and its own');
            const config = parseRpackConfig(project.files.get('rpack.json')!);
            assertEquals([config.released.get('1.0.0'), config.released.get('1.1.0')].join(','), [n100, n110].join(','), 'rpack.json names the new releases');
            assertEquals(baseOf('1.1.0'), n100, "1.1.0's version.json names the new 1.0.0");
            assertEquals(baseOf('1.2.0'), n110, "the unreleased folder's names the new 1.1.0");
            assertEquals(project.files.get(workPath('1.0.0', '.released/target-schema.sql')), withLang(COMMENTS_100), '.released/ holds the source as it is now');
            assertEquals((await statusOf(ctx, '1.0.0')).lines[4], `Released as ${n100}`, '1.0.0 is as released');
            assertEquals((await statusOf(ctx, '1.1.0')).lines[4], `Released as ${n110}`, 'and so is 1.1.0');
            assertEquals((await statusOf(ctx, '1.2.0')).lines[3], `Base: 1.1.0 (${n110.slice(n110.lastIndexOf('-') + 1)})`, 'the unreleased folder is on the new 1.1.0');
            const said = done.lines.join('\n');
            assertTrue(said.includes(`removed releases/${r100}.rpack, releases/${r110}.rpack`), `the result names the removed files:\n${said}`);
            assertTrue(said.includes(`Warning: copies of ${r100} and ${r110} that were published or imported are not replaced.`), `and warns about the copies:\n${said}`);

            // 1.0.0 now holds all of 1.1.0, so 1.1.0 has nothing to release on it
            await project.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), withLang(COMMENTS_110));
            const stable = snapshot();
            await expectRpackError(() => releaseVersion(ctx, '1.0.0', undefined, { force: true, yes: true }), (err) => {
                assertEquals(err.message, "1.1.0 doesn't build on the new 1.0.0; nothing was written", 'a release built on it that does not build fails it');
                assertTrue(err.lines.join('\n').includes('nothing to release'), `with the reason:\n${err.lines.join('\n')}`);
            }, 'a dependent that does not build');
            assertEquals(snapshot(), stable, 'and nothing is written');
        },
    },
    {
        name: '[RPACK30] after a schema re-release: publish and import refuse and name --force; with it publish replaces the registry\'s files and import the lock and the copy, and the re-released catalog is published over the old one',
        invoke: async () => {
            const dev = await devKeys();
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const registry = new MemoryProject();
            const tagOf = (name: string) => name.slice(name.lastIndexOf('-') + 1);
            const listed = async (project: MemoryProject, dir: string) => (await project.list(dir)).sort().join(',');
            const files = (...names: string[]) => names.map((n) => `${n}.rpack`).sort().join(',');

            // hhs:comments 1.0.0 and 1.1.0, published
            const schemaProj = new MemoryProject();
            const schemaCtx: RpackContext = { project: schemaProj };
            await initProject(schemaProj, 'hhs:comments', '', { kind: 'schema' });
            await newVersion(schemaCtx, '1.0.0');
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), COMMENTS_100);
            const old100 = (await releaseVersion(schemaCtx, '1.0.0')).produced.name;
            await newVersion(schemaCtx, '1.1.0');
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), COMMENTS_110);
            const old110 = (await releaseVersion(schemaCtx, '1.1.0')).produced.name;
            await publishRelease(schemaProj, registry);

            // blog 1.0.0 imports 1.0.0 and blog 1.1.0 imports 1.1.0, both released and published
            const catProj = new MemoryProject();
            const catCtx: RpackContext = { project: catProj, vault: dev.vault };
            await initProject(catProj, 'blog', 'dev');
            await newVersion(catCtx, '1.0.0');
            await importSchema(catProj, registry, '1.0.0', 'hhs:comments', '1.0.0');
            await catProj.write(workPath('1.0.0', SOURCE_FILE), `IMPORT SCHEMA hhs:comments VERSION '1.0.0';

CREATE SCHEMA blog:posts AS (
  TABLE articles (
    title string PUB
  )
);

CREATE CATALOG blog CREATORS ($dev) AS (
  TABLEGROUP posts USING SCHEMA blog:posts,
  TABLEGROUP post_comments USING SCHEMA hhs:comments
    BIND target => posts (items => articles)
);
`);
            const blog100 = (await releaseVersion(catCtx, '1.0.0', unlock)).produced.name;
            await newVersion(catCtx, '1.1.0');
            await importSchema(catProj, registry, '1.1.0', 'hhs:comments', '1.1.0');
            const blog110 = (await releaseVersion(catCtx, '1.1.0', unlock)).produced.name;
            await publishRelease(catProj, registry);
            assertEquals(await listed(registry, 'blog'), files(blog100, blog110), 'the registry holds both blog releases');

            // The schema's 1.0.0 is re-released with a column, and 1.1.0 on it
            const withLang = (source: string) => source.replace('len integer READONLY', 'len integer READONLY,\n    lang string NULL');
            await schemaProj.write(workPath('1.0.0', SCHEMA_SOURCE_FILE), withLang(COMMENTS_100));
            await schemaProj.write(workPath('1.1.0', SCHEMA_SOURCE_FILE), withLang(COMMENTS_110));
            const redone = await releaseVersion(schemaCtx, '1.0.0', undefined, { force: true, yes: true });
            const new100 = redone.produced.name;
            const new110 = redone.rebuilt[0]!.name;
            const warned = redone.lines.join('\n');
            assertTrue(warned.includes(`Warning: copies of ${old100} and ${old110} that were published or imported are not replaced.`)
                && warned.includes('rpack publish --force replaces it there') && warned.includes('rpack import --force replaces its copy and its lock'),
                `the result warns about the copies and names the commands:\n${warned}`);

            // The registry holds the old ones: publish refuses, naming every file and the flag
            await expectThrows(() => publishRelease(schemaProj, registry),
                `'hhs:comments' 1.0.0 and 1.1.0 are already in the registry with a different hash `
                + `(hhs/comments/${old100}.rpack, hhs/comments/${old110}.rpack): rpack publish --force replaces them`, 'publishing over the old releases');
            assertEquals(await listed(registry, 'hhs/comments'), files(old100, old110), 'the registry is as it was');
            const published = (await publishRelease(schemaProj, registry, undefined, { force: true })).join('\n');
            assertEquals(await listed(registry, 'hhs/comments'), files(new100, new110), '--force replaces both');
            assertTrue(published.includes(`removed hhs/comments/${old100}.rpack from the registry`) && published.includes(`removed hhs/comments/${old110}.rpack from the registry`),
                `and says what it removed:\n${published}`);
            assertTrue(published.includes('A catalog that imported one keeps it until rpack import --force.'), `and who still has them:\n${published}`);

            // The catalog locks the old ones: import refuses, and leaves imports/ as it was
            await expectThrows(() => importSchema(catProj, registry, '1.0.0', 'hhs:comments', '1.0.0'),
                `rpack.json already locks hhs:comments 1.0.0 at #${tagOf(old100)}, not #${tagOf(new100)}: rpack import --force replaces the lock and imports/${old100}.rpack`,
                'importing over the lock');
            assertEquals(await listed(catProj, 'imports'), files(old100, old110), 'a refused import leaves imports/ as it was');

            // --force replaces that one version, and warns about the other one, made on the old 1.0.0
            const imported = await importSchema(catProj, registry, '1.0.0', 'hhs:comments', '1.0.0', { force: true });
            assertEquals(imported.file.manifest.release, redone.produced.hash, 'the new 1.0.0 is imported');
            assertEquals(await listed(catProj, 'imports'), files(new100, old110), 'its copy replaces the old one');
            assertEquals(parseRpackConfig(catProj.files.get('rpack.json')!).imports?.['hhs:comments']?.['1.0.0'], redone.produced.hash, 'and so does its lock');
            const said = imported.lines.join('\n');
            assertTrue(said.includes(`replaced the lock at #${tagOf(old100)} and removed imports/${old100}.rpack`), `the import says what it replaced:\n${said}`);
            assertTrue(said.includes(`Warning: imports/${old110}.rpack was made on another 1.0.0: rpack import --force hhs:comments 1.1.0 takes its new release.`),
                `and warns about the copy made on the old 1.0.0:\n${said}`);
            assertTrue(said.includes('Warning: a release of blog made on the old hhs:comments 1.0.0 has to be re-released'), `and about the catalog's releases:\n${said}`);

            // With one lock replaced and the other not, the catalog's re-release is refused whole
            const mixed = catProj.files.size;
            await expectThrows(() => releaseVersion(catCtx, '1.0.0', unlock, { force: true, yes: true }), "1.1.0 doesn't build on the new 1.0.0; nothing was written",
                'a catalog on a mix of the old and the new schema');
            assertEquals(catProj.files.size, mixed, 'and nothing is written');
            const second = (await importSchema(catProj, registry, '1.1.0', 'hhs:comments', '1.1.0', { force: true })).lines.join('\n');
            assertTrue(!second.includes('was made on another'), `with both replaced, no copy is of the old history:\n${second}`);
            assertEquals(await listed(catProj, 'imports'), files(new100, new110), 'imports/ holds the new releases');

            // The catalog is re-released on the new schema: a new catalog, which the registry refuses beside the old one
            const again = await releaseVersion(catCtx, '1.0.0', unlock, { force: true, yes: true });
            const newBlog100 = again.produced.name;
            const newBlog110 = again.rebuilt[0]!.name;
            assertTrue(again.produced.catalog !== parseReleaseFile(registry.files.get(`blog/${blog100}.rpack`)!).manifest.catalog, 'the re-released blog is another catalog');
            const refusal = await expectThrows(() => publishRelease(catProj, registry), "'blog' in the registry is another catalog (#", 'publishing the new catalog beside the old one');
            assertTrue(refusal.endsWith(`, in blog/${blog100}.rpack, blog/${blog110}.rpack: rpack publish --force replaces them`), `the refusal names its files and the flag: ${refusal}`);
            const over = (await publishRelease(catProj, registry, undefined, { force: true })).join('\n');
            assertEquals(await listed(registry, 'blog'), files(newBlog100, newBlog110), '--force replaces the old catalog');
            assertTrue(over.includes("Hosts of the removed catalog take none of the new one's releases."), `and says what that means for its hosts:\n${over}`);
            for (const name of [newBlog100, newBlog110]) assertTrue((await verifyRelease(registry.files.get(`blog/${name}.rpack`)!)).ok, `${name} verifies`);
        },
    },
];
