import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { readNext, readSource, SourceError } from "../src/source.js";
import { DOC_SCHEMA, EDITOR_CATALOG, EDITOR_SOURCE, USER_SCHEMA, devKeys, expectThrows, modelText, schemaView } from "./fixtures.js";

async function issuesOf(fn: () => Promise<unknown>): Promise<string> {
    try {
        await fn();
    } catch (err) {
        if (err instanceof SourceError) return err.message;
        throw err;
    }
    throw new Error('expected a SourceError');
}

export const sourceTests = [
    {
        name: '[RPACK08] reading the source: the model, real keys, clause agreement with positions, upgrade-manual',
        invoke: async () => {
            const { vault, keys } = await devKeys();
            const dev = keys.get('dev')!;
            const alice = keys.get('alice')!;

            const read = await readSource(EDITOR_SOURCE, keys, 'dev');
            const model = read.model;
            assertEquals([...model.schemas.keys()].join(','), 'hhs:user,hhs:doc', 'schemas by name, in source order');
            assertEquals([...model.schemas.get('hhs:doc')!.tables.keys()].join(','), 'pages,blocks', 'tables by name');
            assertEquals([...model.groups.keys()].join(','), 'user,doc', 'groups by name');
            assertEquals(JSON.stringify(model.groups.get('doc')!.bindings), '{"user":"user"}', 'bindings by group name');
            assertEquals(model.groups.get('doc')!.schema, 'hhs:doc', "a group's schema by name");
            assertEquals([...model.params.keys()].join(','), 'admin', 'params');
            assertEquals(model.creators.map((c) => c.keyId).join(','), dev.keyId, 'the catalog creators are the real keys');
            assertEquals(model.schemas.get('hhs:user')!.creators[0]!.publicKey, dev.publicKey, 'schema creators carry the real public key');
            const text = modelText(model);
            assertTrue(read.standIns.length > 0 && read.standIns.every((s) => !text.includes(s)), 'no stand-in key is left in the model');
            assertEquals(read.locate('table hhs:doc pages')?.line, 20, 'units are located in target-catalog.sql');

            const rows = EDITOR_SOURCE.replace("caps (label = 'manager', grantee = :admin)",
                "caps (label = 'manager', grantee = :admin),\n      caps (label = 'writer', grantee = $alice)");
            const withRows = modelText((await readSource(rows, keys, 'dev')).model);
            assertTrue(withRows.includes(alice.keyId), "a $label in a row is the label's real key");

            const schemaVersion = EDITOR_SOURCE.replace('CREATE SCHEMA hhs:doc CREATORS ($dev)', "CREATE SCHEMA hhs:doc CREATORS ($dev) VERSION '9.9.9'");
            const catalogAgreed = schemaVersion
                .replace('CREATE CATALOG editor CREATORS ($dev)', "CREATE CATALOG editor CREATORS ($dev) VERSION '1.0.0'")
                .replace(/;\s*$/, " NOTE 'initial release';\n");
            const agreed = await readSource(catalogAgreed, keys, 'dev', 'target-catalog.sql', { version: '1.0.0', note: 'initial release' });
            assertEquals(agreed.model.name, 'editor', 'a schema VERSION is kept, and a matching catalog VERSION and NOTE pass');
            assertEquals(JSON.stringify([...agreed.schemaVersions]), '[["hhs:doc","9.9.9"]]', 'schemaVersions holds only the stated schema versions');
            assertEquals(read.schemaVersions.size, 0, 'a source that states none has none');
            const byDev = `${USER_SCHEMA}\n${DOC_SCHEMA}\n${EDITOR_CATALOG.replace(/;$/, ' BY $dev;')}`;
            assertEquals((await readSource(byDev, keys, 'dev')).model.name, 'editor', 'BY $dev matches the signer');
            const byPrefix = `${USER_SCHEMA}\n${DOC_SCHEMA}\n${EDITOR_CATALOG.replace(/;$/, ` BY #${dev.keyId};`)}`;
            assertEquals((await readSource(byPrefix, keys, 'dev')).model.name, 'editor', 'BY #keyId matches the signer');

            const catalogVersion = await issuesOf(() => readSource(
                EDITOR_SOURCE.replace('CREATE CATALOG editor CREATORS ($dev)', "CREATE CATALOG editor CREATORS ($dev) VERSION '1.2.0'"),
                keys, 'dev', 'target-catalog.sql', { version: '1.0.0' },
            ));
            assertTrue(catalogVersion.includes("CREATE CATALOG: VERSION '1.2.0' does not match the release version '1.0.0'"), `a catalog VERSION mismatch names both: ${catalogVersion}`);
            const noted = EDITOR_SOURCE.replace(/;\s*$/, " NOTE 'initial release';\n");
            const note = await issuesOf(() => readSource(noted, keys, 'dev', 'target-catalog.sql', { note: 'other' }));
            assertTrue(note.includes("NOTE 'initial release' does not match version.json's note 'other'"), `a NOTE mismatch names both: ${note}`);
            assertEquals((await readSource(noted, keys, 'dev')).model.name, 'editor', 'a NOTE is allowed when version.json has no note');
            const atLatest = EDITOR_SOURCE.replace('TABLEGROUP doc USING SCHEMA hhs:doc', 'TABLEGROUP doc USING SCHEMA hhs:doc AT LATEST');
            assertEquals((await readSource(atLatest, keys, 'dev')).model.groups.get('doc')!.schema, 'hhs:doc', 'AT LATEST is the frontier the fresh instance already pins');
            const atSet = await issuesOf(() => readSource(EDITOR_SOURCE.replace('TABLEGROUP doc USING SCHEMA hhs:doc', 'TABLEGROUP doc USING SCHEMA hhs:doc AT {v1}'), keys, 'dev'));
            assertTrue(atSet.includes('TABLEGROUP doc: remove AT {v1}'), `AT names a version set: ${atSet}`);
            const atHash = await issuesOf(() => readSource(EDITOR_SOURCE.replace('TABLEGROUP user USING SCHEMA hhs:user', 'TABLEGROUP user USING SCHEMA hhs:user AT #abc'), keys, 'dev'));
            assertTrue(atHash.includes('TABLEGROUP user: remove AT #abc'), `AT names a hash: ${atHash}`);
            const by = await issuesOf(() => readSource(`${USER_SCHEMA}\n${DOC_SCHEMA}\n${EDITOR_CATALOG.replace(/;$/, ' BY $alice;')}`, keys, 'dev'));
            assertTrue(by.includes("BY $alice does not match rpack.json's key 'dev'"), `BY names the signer: ${by}`);
            const nobody = await issuesOf(() => readSource(`${USER_SCHEMA}\n${DOC_SCHEMA}\n${EDITOR_CATALOG.replace(/;$/, ' BY NOBODY;')}`, keys, 'dev'));
            assertTrue(nobody.includes("BY NOBODY does not match rpack.json's key 'dev'"), `BY NOBODY mismatches: ${nobody}`);
            const alter = await issuesOf(() => readSource(`${EDITOR_SOURCE}\nALTER SCHEMA hhs:doc AS (DROP COLUMN pages.title);`, keys, 'dev'));
            assertTrue(alter.includes('ALTER SCHEMA does not belong in target-catalog.sql'), `ALTER is named: ${alter}`);
            assertTrue(alter.includes('holds only CREATE SCHEMA statements and one CREATE CATALOG'), `ALTER still says what the file holds: ${alter}`);
            const database = await issuesOf(() => readSource(`${EDITOR_SOURCE}\nCREATE DATABASE app USING CATALOG editor BY $dev;`, keys, 'dev'));
            assertTrue(database.includes('CREATE DATABASE app does not belong in target-catalog.sql'), `CREATE DATABASE is named: ${database}`);
            const unknown = await issuesOf(() => readSource(EDITOR_SOURCE.replace('CREATE SCHEMA hhs:doc CREATORS ($dev)', 'CREATE SCHEMA hhs:doc CREATORS ($bob)'), keys, 'dev'));
            assertTrue(unknown.includes('$bob is not a key in your keystore'), `an unknown label is refused: ${unknown}`);
            await expectThrows(() => readSource(EDITOR_SOURCE, keys, 'carol'), "rpack.json's key 'carol' is not in your keystore", 'an unknown release key');
            const notCreator = await issuesOf(() => readSource(EDITOR_SOURCE, keys, 'alice'));
            assertTrue(notCreator.includes('must be one of its CREATORS'), `the release key must be a catalog creator: ${notCreator}`);

            const { view, close } = await schemaView(vault, DOC_SCHEMA, 'hhs:doc');
            try {
                const views = (name: string) => (name === 'hhs:doc' ? view : undefined);
                const next = readNext('-- reset the title\nALTER SCHEMA hhs:doc AS (\n  DROP COLUMN pages.title,\n  ADD COLUMN pages.title integer DEFAULT 0\n);\n', views);
                const rules = next.rules.get('hhs:doc')!;
                assertEquals(rules.map((r) => r.rule.rule).join(','), 'drop-column,add-column', 'upgrade-manual rules, in order');
                assertEquals(rules[1]!.where.line, 4, 'each rule knows its line');
                assertEquals(readNext('-- nothing yet\n', views).rules.size, 0, 'a blank file has no rules');

                const wrong = await issuesOf(async () => readNext("ALTER SCHEMA hhs:doc VERSION '2.0.0' AS (DROP COLUMN pages.title);\nALTER SCHEMA hhs:tags AS (DROP TABLE t);\nINSERT INTO doc.pages (title) VALUES ('x');", views));
                assertTrue(wrong.includes('upgrade-manual.sql:1:1: remove VERSION'), `VERSION is refused: ${wrong}`);
                assertTrue(wrong.includes('schema hhs:tags is not in the release\'s parents'), `an unknown schema is refused: ${wrong}`);
                assertTrue(wrong.includes('holds only ALTER SCHEMA statements'), `other statements are refused: ${wrong}`);
            } finally {
                await close();
            }
        },
    },
];
