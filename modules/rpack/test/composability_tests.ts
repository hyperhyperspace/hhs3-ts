import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    buildVersion, importSchema, initProject, logReleases, newVersion, publishRelease, releaseVersion, RpackError, statusOf,
    type RpackContext,
} from "../src/commands.js";
import { isSchemaReleaseFile, parseAnyReleaseFile, parseReleaseFile, parseSchemaReleaseFile } from "../src/format.js";
import { MemoryProject, parseRpackConfig, SCHEMA_SOURCE_FILE, SOURCE_FILE, workPath } from "../src/project.js";
import { resolveCatalogInRegistry } from "../src/registry.js";
import { verifyRelease } from "../src/verify.js";
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
            const rel110 = await releaseVersion(catCtx, '1.1.0', unlock);
            const file110 = parseAnyReleaseFile(catProj.files.get(`releases/${rel110.produced.name}.rpack`)!);
            assertTrue((await verifyRelease(file110)).ok, 'switching from IMPORT SCHEMA to inline CREATE SCHEMA releases and verifies');

            // In 1.2.0, trying to switch back to IMPORT SCHEMA 2.0.0 fails because 1.1.0 has an inline entry not in the pack
            await newVersion(catCtx, '1.2.0');
            await importSchema(catProj, registry, '1.2.0', 'hhs:comments', '2.0.0');
            await catProj.write(workPath('1.2.0', SOURCE_FILE), `IMPORT SCHEMA hhs:comments VERSION '2.0.0';

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
            const b120 = await buildVersion(catCtx, '1.2.0');
            assertTrue(b120.draft.refusals.some((r) => r.message.includes('cannot switch to IMPORT SCHEMA')),
                `switching diverged inline schema to IMPORT SCHEMA is refused: ${JSON.stringify(b120.draft.refusals)}`);
        },
    },
];
