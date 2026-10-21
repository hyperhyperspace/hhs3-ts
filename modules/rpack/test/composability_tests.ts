import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    buildVersion, importSchema, initProject, logReleases, newVersion, publishRelease, releaseVersion, RpackError, statusOf,
    type RpackContext,
} from "../src/commands.js";
import { isSchemaReleaseFile, parseAnyReleaseFile, parseReleaseFile, parseSchemaReleaseFile } from "../src/format.js";
import { MemoryProject, parseRpackConfig, SCHEMA_SOURCE_FILE, SOURCE_FILE, UPGRADE_MANUAL_FILE, workPath } from "../src/project.js";
import { readLockedImports, resolveCatalogInRegistry } from "../src/registry.js";
import { readSource } from "../src/source.js";
import { verifyRelease } from "../src/verify.js";
import { writeSource } from "../src/write_source.js";
import { devKeys, PASSPHRASE } from "./fixtures.js";

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
];
