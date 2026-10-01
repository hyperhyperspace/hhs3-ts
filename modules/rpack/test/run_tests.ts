import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { createBasicCrypto, HASH_SHA256, type B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    canonicalEntryOrder, exportRelease, installRelease, parseReleaseFile, releaseFileName, releaseTag,
    serializeReleaseFile, verifyRelease, type ReleaseEntry, type ReleaseFile,
} from "../src/index.js";
import { diffTests } from "./diff_tests.js";
import { releaseTests } from "./release_tests.js";
import { sourceTests } from "./source_tests.js";
import { writeSourceTests } from "./write_source_tests.js";

const EDITOR_SQL = resolve('../rdb/examples/editor.sql');
const sha256 = createBasicCrypto().hash(HASH_SHA256);

const SECOND_RELEASE = `
    ALTER SCHEMA hhs:doc VERSION '1.1.0' AS (ADD COLUMN pages.tag string NULL);
    ALTER CATALOG editor VERSION '1.1.0' AS (UPDATE SCHEMA hhs:doc TO LATEST ON doc) NOTE 'tags' BY $admin;
`;

function nameRef(text: string) {
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

async function newRuntime(): Promise<RdbRuntime> {
    const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
    await runtime.session.createKey('admin', 'pw');
    runtime.session.selectAuthor('admin');
    return runtime;
}

type Source = { runtime: RdbRuntime; catalogId: B64Hash };

// The developer's replica: editor.sql run with a fresh admin key.
async function editorSource(): Promise<Source> {
    const runtime = await newRuntime();
    await runtime.execute(await readFile(EDITOR_SQL, 'utf8'));
    const root = await runtime.workspace.roots.resolveCatalog(nameRef('editor'));
    return { runtime, catalogId: root.id };
}

async function catalogOf(runtime: RdbRuntime, id: B64Hash): Promise<RCatalogImpl> {
    return (await runtime.workspace.replica.getObject(id)) as unknown as RCatalogImpl;
}

async function releaseHash(runtime: RdbRuntime, catalogId: B64Hash, version: string): Promise<B64Hash> {
    const catalog = await catalogOf(runtime, catalogId);
    const frontier = await (await catalog.getScopedDag()).getFrontier();
    const matches = (await catalog.getIndex()).findReleasesByVersion(version, frontier);
    if (matches.length !== 1) throw new Error(`expected one release ${version}, got ${matches.length}`);
    return matches[0];
}

async function exportVersion(source: Source, version: string): Promise<ReleaseFile> {
    return exportRelease(source.runtime.workspace.replica, source.catalogId, await releaseHash(source.runtime, source.catalogId, version));
}

function clone(file: ReleaseFile): ReleaseFile {
    return JSON.parse(JSON.stringify(file)) as ReleaseFile;
}

function catalogObject(file: ReleaseFile) {
    return file.objects[file.objects.length - 1];
}

function schemaObject(file: ReleaseFile, name: string) {
    const object = file.objects.find((o) => o.create['name'] === name);
    if (object === undefined) throw new Error(`no schema '${name}' in the file`);
    return object;
}

async function problemOf(input: string | ReleaseFile): Promise<string> {
    const report = await verifyRelease(input);
    assertFalse(report.ok, 'the release file should not verify');
    return report.problems.join('; ');
}

const tests = [
    {
        name: '[RPACK01] export 1.0.0: manifest, objects in dependency order, deterministic bytes',
        invoke: async () => {
            const source = await editorSource();
            try {
                const file = await exportVersion(source, '1.0.0');
                assertEquals(file.format, 1, 'format 1');
                assertEquals(file.manifest.name, 'editor', 'catalog name');
                assertEquals(file.manifest.version, '1.0.0', 'release version');
                assertEquals(file.manifest.release, source.catalogId, 'the genesis is the first release');
                assertEquals(file.manifest.parents.length, 0, 'the genesis has no parents');
                assertEquals(file.manifest.note, 'initial release', 'release note');

                assertEquals(file.objects.length, 3, 'two schemas and the catalog');
                const schemaIds = file.objects.slice(0, 2).map((o) => o.id);
                assertEquals(schemaIds.join(','), [...schemaIds].sort().join(','), 'schemas are sorted by id');
                assertEquals(catalogObject(file).id, source.catalogId, 'the catalog comes last');
                assertEquals(file.objects.map((o) => o.entries.length).join(','), '0,0,0', 'nothing beyond the creates');

                const again = await exportVersion(source, '1.0.0');
                assertEquals(serializeReleaseFile(again), serializeReleaseFile(file), 'exporting twice gives the same bytes');

                const name = releaseFileName(file.manifest.name, file.manifest.version, file.manifest.release);
                assertTrue(/^editor-1\.0\.0-[0-9a-f]{8}\.rpack$/.test(name), `file name shape: ${name}`);
            } finally {
                await source.runtime.close();
            }
        },
    },
    {
        name: '[RPACK02] install into another replica, then deploy a database from it',
        invoke: async () => {
            const source = await editorSource();
            const target = await newRuntime();
            try {
                const file = parseReleaseFile(serializeReleaseFile(await exportVersion(source, '1.0.0')));
                const report = await installRelease(target.workspace.replica, file);
                assertEquals(report.createdRoots.length, 3, 'the two schemas and the catalog are created');
                assertEquals(report.appliedEntries, 0, 'no entries beyond the creates');

                const catalog = await catalogOf(target, source.catalogId);
                assertTrue(catalog !== undefined, 'the catalog is in the target replica');
                assertEquals((await catalog.getIndex()).releaseState(source.catalogId).version, '1.0.0', 'the release is installed');

                const again = await installRelease(target.workspace.replica, file);
                assertEquals(again.createdRoots.length, 0, 'a second install creates nothing');

                await target.execute(`
                    CREATE DATABASE app USING CATALOG editor AT '1.0.0'
                      CREATORS ($admin) WITH PARAMS (:admin = $admin) BY $admin;
                `);
                assertEquals(target.workspace.roots.list('group').length, 2, 'the target admin deploys the installed release');
            } finally {
                await source.runtime.close();
                await target.close();
            }
        },
    },
    {
        name: '[RPACK03] a second release ships only its past; installing it applies only the new entries',
        invoke: async () => {
            const source = await editorSource();
            const target = await newRuntime();
            try {
                const first = serializeReleaseFile(await exportVersion(source, '1.0.0'));
                await source.runtime.execute(SECOND_RELEASE);

                const second = await exportVersion(source, '1.1.0');
                assertEquals(second.manifest.parents.join(','), source.catalogId, '1.1.0 builds on the genesis');
                assertEquals(second.manifest.note, 'tags', 'release note');
                assertEquals(schemaObject(second, 'hhs:doc').entries.length, 1, 'the doc schema update ships');
                assertEquals(schemaObject(second, 'hhs:user').entries.length, 0, 'the user schema is unchanged');
                const releaseEntries = catalogObject(second).entries;
                assertEquals(releaseEntries.length, 1, 'the catalog ships the 1.1.0 release');
                assertEquals((releaseEntries[0].payload as { action: string }).action, 'release', 'a release entry');

                await installRelease(target.workspace.replica, parseReleaseFile(first));
                const upgrade = await installRelease(target.workspace.replica, second);
                assertEquals(upgrade.createdRoots.length, 0, 'the objects already exist');
                assertEquals(upgrade.appliedEntries, 2, 'the schema update and the release are applied');
                assertEquals(upgrade.skippedEntries, 0, 'nothing is skipped');
                const index = await (await catalogOf(target, source.catalogId)).getIndex();
                assertEquals(index.releaseState(second.manifest.release).version, '1.1.0', 'the target has 1.1.0');

                const repeat = await installRelease(target.workspace.replica, second);
                assertEquals(repeat.appliedEntries, 0, 'installing again applies nothing');
                assertEquals(repeat.skippedEntries, 2, 'installing again skips both entries');

                const firstAgain = serializeReleaseFile(await exportVersion(source, '1.0.0'));
                assertEquals(firstAgain, first, '1.0.0 exports the same bytes after 1.1.0 exists: later work stays out');
            } finally {
                await source.runtime.close();
                await target.close();
            }
        },
    },
    {
        name: '[RPACK04] verify: a good file passes with a summary; tampering and malformed files fail with the cause',
        invoke: async () => {
            const source = await editorSource();
            try {
                await source.runtime.execute(SECOND_RELEASE);
                const good = await exportVersion(source, '1.1.0');

                const report = await verifyRelease(serializeReleaseFile(good));
                assertTrue(report.ok, `a good file verifies: ${report.problems.join('; ')}`);
                const summary = report.summary!;
                assertEquals(summary.version, '1.1.0', 'summary version');
                assertEquals(summary.catalog.releases, 2, 'two releases in the past');
                assertEquals(summary.catalog.declares, 0, 'no declares');
                const doc = summary.schemas.find((s) => s.name === 'hhs:doc')!;
                const user = summary.schemas.find((s) => s.name === 'hhs:user')!;
                assertEquals(doc.versions.join(','), '1.1.0', 'the doc schema ships at 1.1.0');
                assertEquals(user.versions.join(','), '1.0.0', 'the user schema ships at 1.0.0');

                const editedStale = clone(good);
                (catalogObject(editedStale).entries[0].payload as json.LiteralMap)['note'] = 'tampered';
                assertTrue((await problemOf(editedStale)).includes('hash mismatch'), 'a payload edit breaks the recorded hash');

                const editedRehashed = clone(good);
                const entry = catalogObject(editedRehashed).entries[0];
                (entry.payload as json.LiteralMap)['note'] = 'tampered';
                entry.hash = dag.createEntry(entry.payload, {}, position(...entry.prevs), sha256).hash;
                const rehashed = await problemOf(editedRehashed);
                assertTrue(rehashed.includes('signature') && rehashed.includes('could not be verified'),
                    `a payload edit with a fixed hash fails the signature check: ${rehashed}`);

                const wrongHash = clone(good);
                schemaObject(wrongHash, 'hhs:doc').entries[0].hash = source.catalogId;
                assertTrue((await problemOf(wrongHash)).includes('hash mismatch'), 'a wrong recorded hash is caught');

                const missingSchema = clone(good);
                missingSchema.objects = missingSchema.objects.filter((o) => o.create['name'] !== 'hhs:doc');
                const missing = await problemOf(missingSchema);
                assertTrue(missing.includes('create was rejected') && missing.includes('is not present'),
                    `a missing schema stops the catalog: ${missing}`);

                const wrongVersion = clone(good);
                wrongVersion.manifest.version = '9.9.9';
                assertTrue((await problemOf(wrongVersion)).includes("does not match the release version"), 'the manifest version is checked');

                assertTrue((await problemOf('not json')).includes('not valid JSON'), 'text that is not JSON');
                assertTrue((await problemOf('{"format": 1, "objects": []}')).includes('manifest is malformed'), 'a file without a manifest');
                assertTrue((await problemOf('{"format": 2}')).includes('unsupported release file format'), 'an unknown format');
            } finally {
                await source.runtime.close();
            }
        },
    },
    {
        name: '[RPACK05] canonical entry order and the sorted-key serializer are deterministic',
        invoke: async () => {
            const e = (hash: string, prevs: string[]): ReleaseEntry => ({ hash, prevs, payload: { h: hash } });
            const entries = [e('d', ['a']), e('c', ['a', 'b']), e('b', ['r']), e('a', ['r'])];
            const order = (list: ReleaseEntry[]) => canonicalEntryOrder(list).map((x) => x.hash).join('');
            assertEquals(order(entries), 'abcd', 'the smallest ready hash goes first');
            assertEquals(order([...entries].reverse()), 'abcd', 'the input order does not matter');

            const text = serializeReleaseFile({ b: 1, a: { d: [], c: {} }, '10': true, '2': false } as unknown as ReleaseFile);
            assertEquals(text, '{\n  "10": true,\n  "2": false,\n  "a": {\n    "c": {},\n    "d": []\n  },\n  "b": 1\n}\n',
                'keys are sorted as strings, whatever their insertion order');

            assertEquals(releaseTag(sha256.hashToB64(new Uint8Array([1, 2, 3]))).length, 8, 'a tag is 8 hex digits');
        },
    },
];

tests.push(...diffTests, ...sourceTests, ...writeSourceTests, ...releaseTests);

async function main() {
    const filters = process.argv.slice(2);

    console.log('Running tests for Hyper Hyper Space v3 rpack module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    console.log('rpack');

    for (const test of tests) {
        let match = true;
        for (const filter of filters) {
            match = match && test.name.indexOf(filter) >= 0;
        }

        if (match) {
            testing.exitIfFailed(await testing.run(test.name, test.invoke));
        } else {
            await testing.skip(test.name);
        }
    }

    console.log();
}

main();
