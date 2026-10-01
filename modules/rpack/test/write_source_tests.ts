import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import type { KeyDirectory } from "../src/keys.js";
import { modelDifferences, type CatalogModel } from "../src/model.js";
import { readSource } from "../src/source.js";
import { writeSource } from "../src/write_source.js";
import { EDITOR_SOURCE, devKeys } from "./fixtures.js";

const COMMENTS = ['-- The editor app.', '-- the pages of a document', '-- :admin becomes the first manager.'];

async function modelOf(keys: KeyDirectory, source: string): Promise<CatalogModel> {
    return (await readSource(source, keys, 'dev')).model;
}

// Writes `from` towards `target`'s model; the result must read back to it.
async function rewrite(keys: KeyDirectory, from: string, target: string): Promise<string> {
    const wanted = await modelOf(keys, target);
    const written = await writeSource(from, wanted, keys, 'dev');
    assertEquals(modelDifferences(await modelOf(keys, written), wanted).join('; '), '', 'the written source reads back to the target');
    return written;
}

const TAGS = 'CREATE SCHEMA hhs:tags CREATORS ($dev) AS (\n  TABLE tags (label string PUB)\n);';

export const writeSourceTests = [
    {
        name: '[RPACK09] writing source: verbatim where unchanged, each edit kind, params, removals with comments, a full render',
        invoke: async () => {
            const { keys } = await devKeys();

            assertEquals(await writeSource(EDITOR_SOURCE, await modelOf(keys, EDITOR_SOURCE), keys, 'dev'), EDITOR_SOURCE, 'an unchanged model keeps the text byte for byte');

            const edited = (what: string, source: string) => ({ what, source });
            const cases = [
                edited('a new column', EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    tag string NULL PUB')),
                edited('a dropped table', EDITOR_SOURCE.replace(/,\n\n  TABLE blocks \([\s\S]*?\$author\n/, '\n')),
                edited('a redefined column', EDITOR_SOURCE.replace('    title string,', "    title string(200) DEFAULT '',")),
                edited('a column without its FK', EDITOR_SOURCE.replace('pageId string READONLY REFERENCES pages', 'pageId string READONLY')),
                edited('changed options', EDITOR_SOURCE.replace('ALLOW delete IF false', 'ALLOW delete IF true').replace(') CONCURRENT DELETES', ') NO CONCURRENT DELETES')),
                edited('options on a table without them', EDITOR_SOURCE.replace(/\n  \) IDENTITY PROVIDER,/, '\n  ) IDENTITY PROVIDER NO CONCURRENT DELETES,')),
                edited('a new table', EDITOR_SOURCE.replace(/\$author\n\);\n\n-- :admin/, "$author,\n\n  TABLE comments (\n    blockId string REFERENCES blocks,\n    text string\n  )\n);\n\n-- :admin")),
                edited('a new schema and group', EDITOR_SOURCE.replace('-- :admin', `${TAGS}\n\n-- :admin`)
                    .replace(/\n\);\n$/, ',\n  TABLEGROUP tags USING SCHEMA hhs:tags BIND doc => doc\n);\n')),
                edited('a new param', EDITOR_SOURCE.replace('PARAMS (:admin identity)', 'PARAMS (:admin identity, :moderator identity)')),
            ];
            for (const { what, source } of cases) {
                const written = await rewrite(keys, EDITOR_SOURCE, source);
                for (const comment of COMMENTS) assertTrue(written.includes(comment), `${what}: the comment '${comment}' survives`);
            }

            const column = await rewrite(keys, EDITOR_SOURCE, cases[0]!.source);
            assertTrue(column.includes('    deleted boolean,\n    tag string NULL PUB\n  )'), `a new column goes after the last one, indented like it:\n${column}`);
            const param = await rewrite(keys, EDITOR_SOURCE, cases[8]!.source);
            assertTrue(param.includes('PARAMS (:admin identity, :moderator identity)'), 'a new param joins PARAMS');
            const schema = await rewrite(keys, EDITOR_SOURCE, cases[7]!.source);
            assertTrue(schema.indexOf('CREATE SCHEMA hhs:tags') < schema.indexOf('-- :admin becomes the first manager.'), 'a new schema goes before the catalog and its comments');
            assertTrue(schema.includes('    USING IDENTITIES user.identities,\n  TABLEGROUP tags USING SCHEMA hhs:tags\n    BIND doc => doc\n);'), `a new group goes last:\n${schema}`);

            const bare = 'CREATE SCHEMA s CREATORS ($dev) AS (\n  TABLE t (v string)\n);\n\nCREATE CATALOG editor CREATORS ($dev) AS (\n  TABLEGROUP g USING SCHEMA s\n);\n';
            const withParam = await rewrite(keys, bare, bare.replace('CREATORS ($dev) AS (\n  TABLEGROUP', 'CREATORS ($dev) PARAMS (:admin identity) AS (\n  TABLEGROUP'));
            assertTrue(withParam.includes('CREATE CATALOG editor CREATORS ($dev) PARAMS (:admin identity) AS ('), `a new PARAMS clause:\n${withParam}`);

            const drafts = EDITOR_SOURCE.replace('  TABLE blocks (', '  -- drafts, soon gone\n  TABLE drafts (body string),\n\n  TABLE blocks (');
            const removed = await rewrite(keys, drafts, EDITOR_SOURCE);
            assertTrue(!removed.includes('drafts') && !removed.includes('soon gone'), 'a removed table takes the comment above it');
            assertEquals(removed, EDITOR_SOURCE, 'and leaves the rest as it was');
            const lastColumn = await rewrite(keys, cases[0]!.source, EDITOR_SOURCE);
            assertEquals(lastColumn, EDITOR_SOURCE, 'removing a last column takes the comma before it');

            const all = EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    tag string NULL PUB')
                .replace('ALLOW delete IF false', 'ALLOW delete IF true')
                .replace('-- :admin', `${TAGS}\n\n-- :admin`)
                .replace('PARAMS (:admin identity)', 'PARAMS (:admin identity, :moderator identity)')
                .replace(/\n\);\n$/, ',\n  TABLEGROUP tags USING SCHEMA hhs:tags BIND doc => doc\n);\n');
            const merged = await rewrite(keys, EDITOR_SOURCE, all);
            for (const comment of COMMENTS) assertTrue(merged.includes(comment), `several edits at once keep '${comment}'`);

            const full = await writeSource('', await modelOf(keys, all), keys, 'dev');
            assertTrue(full.includes('CREATE SCHEMA hhs:tags CREATORS ($dev) AS (') && full.includes('CREATE CATALOG editor CREATORS ($dev) PARAMS (:admin identity, :moderator identity) AS ('),
                `from empty text, the whole target is rendered:\n${full}`);
        },
    },
    {
        name: '[RPACK15] writing source: FILES items are kept, added, replaced and removed in the catalog body',
        invoke: async () => {
            const { keys } = await devKeys();
            const withFiles = (source: string, item: string) => source.replace(/\n\);\n$/, `,\n  ${item}\n);\n`);
            const media = withFiles(EDITOR_SOURCE, '-- uploads\n  FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF true');

            assertEquals(await writeSource(media, await modelOf(keys, media), keys, 'dev'), media, 'an unchanged FILES is kept verbatim');
            const added = await rewrite(keys, EDITOR_SOURCE, media);
            assertTrue(added.includes('    USING IDENTITIES user.identities,\n  FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF true\n);'),
                `a new FILES goes last:\n${added}`);
            const replaced = await rewrite(keys, media, media.replace('ALLOW WRITE IF true', 'ALLOW WRITE IF false'));
            assertTrue(replaced.includes('-- uploads\n  FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF false'), `a changed FILES is replaced in place:\n${replaced}`);
            const removed = await rewrite(keys, media, EDITOR_SOURCE);
            assertEquals(removed, EDITOR_SOURCE, 'a removed FILES takes its comment and the comma before it');
            const group = await rewrite(keys, media, withFiles(media, 'TABLEGROUP more USING SCHEMA hhs:doc BIND user => user USING IDENTITIES user.identities'));
            assertTrue(group.indexOf('TABLEGROUP more') > group.indexOf('FILES media'), `a new group goes after the last item, a FILES here:\n${group}`);
            const full = await writeSource('', await modelOf(keys, media), keys, 'dev');
            assertTrue(full.includes('  FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF true\n);'), `a full render includes the FILES:\n${full}`);
        },
    },
];
