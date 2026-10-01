import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    buildVersion, initProject, logReleases, newVersion, releaseVersion, RpackError, setBase, statusOf, type RpackContext,
} from "../src/commands.js";
import type { ReleaseDraft } from "../src/draft.js";
import { formatReleaseConfirm, formatReleasePreview, formatStatus, type ReleasePreview } from "../src/format_draft.js";
import { folderVersion, formatVersionFile, MemoryProject, parseRpackConfig, parseVersionFile } from "../src/project.js";
import { verifyRelease } from "../src/verify.js";
import { stripCatalogVersion } from "../src/write_source.js";
import { EDITOR_SOURCE, PASSPHRASE, devKeys, expectThrows, type DevKeys } from "./fixtures.js";

const TAGS = 'CREATE SCHEMA hhs:tags CREATORS ($dev) AS (\n  TABLE tags (label string PUB)\n);';

const withTags = (source: string) => source
    .replace('-- :admin', `${TAGS}\n\n-- :admin`)
    .replace(/\n\);\n$/, ',\n  TABLEGROUP tags USING SCHEMA hhs:tags BIND doc => doc\n);\n');

export const V110 = withTags(EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    summary string NULL'));

export const V200 = V110
    .replace("    title string,\n", "    title string(200) DEFAULT '',\n")
    .replace(/,\n\n  TABLE blocks \([\s\S]*?\$author\n/, '\n');

export const RESET = 'ALTER SCHEMA hhs:doc AS (\n  DROP COLUMN pages.title\n);\n';

const withTag = (source: string) => source.replace('summary string NULL', 'summary string NULL,\n    tag string NULL');

const at = (folder: string, file: string) => `work/${folder}/${file}`;

function configOf(project: MemoryProject) {
    return parseRpackConfig(project.files.get('rpack.json')!);
}

async function setNote(project: MemoryProject, folder: string, note: string): Promise<void> {
    const path = at(folder, 'version.json');
    await project.write(path, formatVersionFile({ ...parseVersionFile(project.files.get(path)!, path), note }));
}

const noteOf = (text: string) => JSON.parse(text).manifest.note as string | undefined;

function snapshot(project: MemoryProject): string {
    return [...project.files].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([path, text]) => `${path}\n${text}`).join('\0');
}

async function rpackError(fn: () => Promise<unknown>, what: string): Promise<RpackError> {
    try {
        await fn();
    } catch (err) {
        if (err instanceof RpackError) return err;
        throw err;
    }
    throw new Error(`${what}: expected an RpackError`);
}

// The count line `status` should print for a draft that is not refused.
function statusCounts(draft: ReleaseDraft): string {
    let addedSchemas = 0, addedTables = 0, addedColumns = 0, droppedColumns = 0;
    let foreignKeys = 0, restrictions = 0, concurrentDeletes = 0, droppedTables = 0;
    for (const step of draft.schemas) {
        if (step.kind === 'create') {
            addedSchemas += 1;
            addedTables += step.schema.tables.size;
            continue;
        }
        for (const { rule } of step.rules) {
            switch (rule.rule) {
                case 'add-table': addedTables += 1; break;
                case 'drop-table': droppedTables += 1; break;
                case 'add-column': addedColumns += 1; break;
                case 'drop-column': droppedColumns += 1; break;
                case 'set-fks': foreignKeys += 1; break;
                case 'set-restrictions': restrictions += 1; break;
                case 'set-concurrent-deletes': concurrentDeletes += 1; break;
            }
        }
    }
    const phrase = (n: number, verb: string, one: string, many: string) => n === 0 ? undefined : `${verb} ${n} ${n === 1 ? one : many}`;
    const phrases = [
        phrase(addedSchemas, 'added', 'schema', 'schemas'),
        phrase(addedTables, 'added', 'table', 'tables'),
        phrase(addedColumns, 'added', 'column', 'columns'),
        phrase(foreignKeys, 'changed', 'foreign key', 'foreign keys'),
        phrase(restrictions, 'changed', 'restriction', 'restrictions'),
        phrase(concurrentDeletes, 'changed', 'concurrent delete', 'concurrent deletes'),
        phrase(droppedTables, 'dropped', 'table', 'tables'),
        phrase(droppedColumns, 'dropped', 'column', 'columns'),
        phrase(draft.add.length, 'added', 'group', 'groups'),
        phrase(draft.changes.length, 'set', 'group', 'groups'),
        phrase(draft.params.length, 'added', 'param', 'params'),
    ].filter((p) => p !== undefined);
    return phrases.length > 0 ? phrases.join(', ') : 'nothing to release';
}

// The release chain: 1.0.0, 1.1.0, 2.0.0 (a drop and a reset), 1.1.1 on
// 1.1.0, and 2.0.1 merging 2.0.0 and 1.1.1, each from its own work folder.
// Returns each release file by name.
export async function releaseChain(dev: DevKeys, project: MemoryProject, check = true): Promise<Map<string, string>> {
    const ctx: RpackContext = { project, vault: dev.vault };
    const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);

    await initProject(project, 'editor', 'dev');
    const n100 = await newVersion(ctx, '1.0.0');
    if (check) {
        assertEquals(n100.parents.length, 0, 'the first release has no base');
        assertEquals(n100.lines.join('\n'), 'Created work/1.0.0/ for editor 1.0.0, the first release.', 'new prints one line');
        assertEquals(project.files.get(at('1.0.0', 'version.json')), '{\n  "base": []\n}\n', 'version.json lists no release, and no note');
        assertTrue(project.files.get(at('1.0.0', 'target-catalog.sql'))!.includes('CREATE CATALOG editor'), 'the folder starts from the skeleton');
        assertEquals(project.files.get('.gitignore'), 'work/*/build/\nwork/*/stage/\nwork/*/.stage.*/\n', 'init ignores the generated folders');
    }
    await project.write(at('1.0.0', 'target-catalog.sql'), EDITOR_SOURCE);
    await setNote(project, '1.0.0', 'initial');
    const first = await releaseVersion(ctx, '1.0.0', unlock);
    const r100 = first.produced.name;
    if (check) {
        assertEquals(first.lines.join('\n'), `released ${r100}: wrote releases/${r100}.rpack and work/1.0.0/.released/`, 'release prints only that it released');
        assertEquals(noteOf(project.files.get(`releases/${r100}.rpack`)!), 'initial', "the release file is written, with version.json's note");
        assertEquals(project.files.get(at('1.0.0', '.released/target-catalog.sql')), EDITOR_SOURCE, '.released/ keeps the signed source');
        assertEquals(configOf(project).released.get('1.0.0'), r100, 'rpack.json maps the folder to its release');
        assertEquals(project.files.get(at('1.0.0', 'target-catalog.sql')), EDITOR_SOURCE, 'the working files stay');
    }

    await newVersion(ctx, '1.1.0');
    if (check) assertEquals(project.files.get(at('1.1.0', 'target-catalog.sql')), EDITOR_SOURCE, "1.1.0 starts from 1.0.0's source");
    await project.write(at('1.1.0', 'target-catalog.sql'), V110);
    const r110 = (await releaseVersion(ctx, '1.1.0', unlock)).produced.name;

    await newVersion(ctx, '2.0.0');
    await project.write(at('2.0.0', 'target-catalog.sql'), V200);
    await project.write(at('2.0.0', 'upgrade-manual.sql'), RESET);
    const plan200 = await buildVersion(ctx, '2.0.0');
    if (check) {
        assertEquals(plan200.lines.join('\n'), 'Generated build/update.sql', `build prints that it wrote the file:\n${plan200.lines.join('\n')}`);
        const upgrade = project.files.get(at('2.0.0', 'build/update.sql'))!;
        assertTrue(upgrade.startsWith('-- Generated by `rpack build`. Do not edit.\n'), `build writes build/update.sql:\n${upgrade}`);
        assertTrue(upgrade.includes('DROP COLUMN pages.title,    -- upgrade-manual.sql:2'), `the hand-written rule comes first:\n${upgrade}`);
        assertTrue(upgrade.includes("ADD COLUMN pages.title string(200) DEFAULT ''"), 'the diff adds the column back');
        assertEquals(plan200.draft.warnings.length, 0, 'a major release drops without warnings');
        const status = await statusOf(ctx, '2.0.0');
        assertEquals(project.files.get(at('2.0.0', 'build/update.sql')), upgrade, 'status leaves build/update.sql');
        assertEquals(status.lines[0], 'Catalog "editor"', 'status names the catalog');
        assertEquals(status.lines[2], 'Version: 2.0.0', 'status names the version');
        assertTrue(/^Base: 1\.1\.0 \([0-9a-f]{8}\)$/.test(status.lines[3]!), `status names the base:\n${status.lines[3]}`);
        assertEquals(status.lines[5], statusCounts(plan200.draft), `the count matches the draft:\n${status.lines.join('\n')}`);
        assertTrue(status.lines[5]!.includes('dropped 1 table') && status.lines[5]!.includes('added 1 column') && status.lines[5]!.includes('dropped 1 column'),
            `2.0.0 drops blocks and resets title:\n${status.lines[5]}`);
        assertEquals(status.lines.length, 6, 'a draft without warnings has no warning line');
        const saved = project.files.get(at('2.0.0', 'target-catalog.sql'))!;
        await project.write(at('2.0.0', 'target-catalog.sql'), 'NOT SQL\n');
        const failure = await rpackError(() => statusOf(ctx, '2.0.0'), 'unreadable target-catalog.sql should fail status');
        const lines = failure.lines;
        assertEquals(lines[0], 'Catalog "editor"', 'an error keeps the catalog');
        assertEquals(lines[2], 'Version: 2.0.0', 'an error keeps the version');
        assertTrue(/^Base: 1\.1\.0 \([0-9a-f]{8}\)$/.test(lines[3] ?? ''), `an error keeps the base:\n${lines[3]}`);
        assertEquals(lines[4], '', 'a blank line separates the header from the error');
        assertEquals(lines[5], 'Error assembling 2.0.0', 'the failure says the version could not be assembled');
        assertTrue(lines.some((l: string) => /^target-catalog\.sql:\d+:\d+: /.test(l)), `the issue names the file and line:\n${lines.join('\n')}`);
        assertEquals(failure.message, '', 'the report is the lines, not the message');
        const buildFailure = await rpackError(() => buildVersion(ctx, '2.0.0'), 'unreadable target-catalog.sql should fail build');
        assertEquals(buildFailure.lines.join('\n'), ['Error assembling 2.0.0', ...lines.slice(6)].join('\n'), 'build names the version, then the source error');
        const withAge = saved.replace('name string NULL PUB', 'name string NULL PUB,\n    age integer');
        await project.write(at('2.0.0', 'target-catalog.sql'), withAge);
        const rowFailure = await rpackError(() => statusOf(ctx, '2.0.0'), 'a WITH ROWS row missing a NOT NULL column should fail status');
        const sourceLines = withAge.split('\n');
        const rowLine = sourceLines.findIndex((l) => l.includes('identities (keyId = :admin')) + 1;
        const rowColumn = sourceLines[rowLine - 1]!.indexOf('identities') + 1;
        const expected = `target-catalog.sql:${rowLine}:${rowColumn}: TABLEGROUP user: identities row 1 in WITH ROWS doesn't set age, `
            + 'which is NOT NULL with no DEFAULT: set it in the row, make age NULL, or give it a DEFAULT';
        assertEquals(rowFailure.lines[5], 'Error assembling 2.0.0', 'the failure says the version could not be assembled');
        assertEquals(rowFailure.lines[6], expected, `the row is located in the source:\n${rowFailure.lines.join('\n')}`);
        await project.write(at('2.0.0', 'target-catalog.sql'), saved);
    }
    await releaseVersion(ctx, '2.0.0', unlock);
    if (check) {
        assertEquals(project.files.get(at('2.0.0', 'upgrade-manual.sql')), RESET, 'release leaves upgrade-manual.sql');
        assertEquals(project.files.get(at('2.0.0', '.released/upgrade-manual.sql')), RESET, 'and keeps it in .released/');
        assertEquals(project.files.get(at('2.0.0', '.released/update.sql')), project.files.get(at('2.0.0', 'build/update.sql')), '.released/ keeps update.sql');
        assertEquals(project.files.has(at('2.0.0', '.released/test-data.sql')), false, '.released/ keeps only the signed inputs and update.sql');
    }

    const n111 = await newVersion(ctx, '1.1.1');
    if (check) {
        assertEquals(n111.parents.map((p) => p.version).join(','), '1.1.0', '1.1.1 starts from 1.1.0');
        assertEquals(project.files.get(at('1.1.1', 'version.json')), `{\n  "base": [\n    "${r110}"\n  ]\n}\n`, 'version.json names it, one release per line');
        assertEquals(project.files.get(at('1.1.1', 'target-catalog.sql')), V110, "target-catalog.sql is 1.1.0's source");
    }
    await project.write(at('1.1.1', 'target-catalog.sql'), withTag(V110));
    await releaseVersion(ctx, '1.1.1', unlock);
    if (check) {
        const log = (await logReleases(ctx)).join('\n');
        assertTrue(/1\.1\.1 +[0-9a-f]{8}  after 1\.1\.0  \(no higher release includes it\)/.test(log), `log shows the open lower release:\n${log}`);
    }

    const n201 = await newVersion(ctx, '2.0.1');
    if (check) {
        assertEquals(n201.parents.map((p) => p.version).join(','), '1.1.1,2.0.0', '2.0.1 starts from 2.0.0 and 1.1.1');
        assertEquals(project.files.get(at('2.0.1', 'target-catalog.sql')), withTag(V200), "target-catalog.sql is 2.0.0's source with 1.1.1's column written in");
        const plan = await buildVersion(ctx, '2.0.1');
        assertEquals(plan.draft.schemas.length, 0, 'a release of the merge as it is has no schema rules');
        assertEquals(plan.draft.changes.map((c) => c.group).join(','), 'doc', 'only the group the parents disagree on');
    }
    await releaseVersion(ctx, '2.0.1', unlock);

    const files = new Map<string, string>();
    for (const name of await project.list('releases')) files.set(name, project.files.get(`releases/${name}`)!);
    return files;
}

export const releaseTests = [
    {
        name: '[RPACK10] end to end: work folders, build, release, a lower release and its merge, log, set base, refusals, deterministic bytes',
        invoke: async () => {
            assertEquals(folderVersion('2.0.4'), '2.0.4', 'a folder named by its version');
            assertEquals(folderVersion('2.0.4-b3c1'), '2.0.4', 'or by its version and a suffix');
            assertEquals(folderVersion('next'), undefined, 'anything else is not a version folder');

            const dev = await devKeys();
            const project = new MemoryProject();
            const files = await releaseChain(dev, project);
            const ctx: RpackContext = { project, vault: dev.vault };
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);

            assertEquals(files.size, 5, 'five releases');
            for (const [name, text] of files) {
                const report = await verifyRelease(text);
                assertTrue(report.ok, `${name} verifies: ${report.problems.join('; ')}`);
            }
            const f201 = [...files.keys()].find((n) => n.startsWith('editor-2.0.1-'))!;
            const r201 = f201.replace(/\.rpack$/, '');
            const manifest = JSON.parse(files.get(f201)!).manifest as { parents: string[] };
            assertEquals(manifest.parents.length, 2, '2.0.1 has two parents');
            assertEquals(Object.keys(JSON.parse(project.files.get('rpack.json')!).released).join(','), '1.0.0,1.1.0,1.1.1,2.0.0,2.0.1',
                'rpack.json lists the released folders by version');

            const log = (await logReleases(ctx)).join('\n');
            assertTrue(!log.includes('no higher release includes it'), `every lower release is included now:\n${log}`);
            assertTrue(!/^work/m.test(log), `every folder is released:\n${log}`);

            await expectThrows(() => newVersion(ctx, '2.0.1'), 'Release 2.0.1 already exists', 'a released version');
            const n220 = await newVersion(ctx, '2.2.0');
            assertEquals(n220.parents.map((p) => p.version).join(','), '2.0.1', '2.2.0 starts from the highest release');
            await expectThrows(() => newVersion(ctx, '2.2.0'), 'work/2.2.0/ already exists', 'an existing folder');
            await expectThrows(() => newVersion(ctx, '2.3.0', { base: ['1.1.0', '1.1.1'] }), 'is in the past of another release', 'the base takes maximal releases');
            await expectThrows(() => newVersion(ctx, '1.5.0', { base: ['2.0.0'] }), 'is not above its parent', 'the version must be above every parent');

            await newVersion(ctx, '2.1.0', { base: ['2.0.0'] });
            const baseline = project.files.get(at('2.1.0', 'target-catalog.sql'))!;
            assertEquals(baseline, V200, "a folder on 2.0.0 starts from 2.0.0's source");
            await project.write(at('2.1.0', 'target-catalog.sql'), baseline.replace("title string(200) DEFAULT ''", 'title integer'));
            await project.write(at('2.1.0', 'build/update.sql'), '-- keep\n');
            const refused = await statusOf(ctx, '2.1.0');
            assertEquals(refused.lines[2], 'Version: 2.1.0', 'a refused status still names the version');
            assertEquals(refused.lines[5], 'refused', 'a redefined column is refused');
            assertTrue(refused.lines.some((l) => l.includes("can't be redefined in place")), `the refusal names the column:\n${refused.lines.join('\n')}`);
            assertTrue(!refused.lines.some((l) => l.startsWith('added ') || l.startsWith('dropped ') || l.startsWith('changed ') || l === 'nothing to release'),
                'a refusal does not print a count');
            const refusedPlan = await buildVersion(ctx, '2.1.0');
            assertTrue(refusedPlan.draft.refusals.length > 0 && refusedPlan.lines.includes('refused'), 'the build is refused, and says so');
            assertEquals(project.files.get(at('2.1.0', 'build/update.sql')), '-- keep\n', 'a refusal leaves build/update.sql');
            const more = baseline.replace('deleted boolean', 'deleted boolean,\n    more string NULL');
            await project.write(at('2.1.0', 'target-catalog.sql'), more);
            const narrow = await buildVersion(ctx, '2.1.0');
            assertTrue(narrow.draft.warnings.some((w) => w.startsWith('1.1.1') && w.includes('not in its past'))
                && narrow.draft.warnings.some((w) => w.startsWith('2.0.1') && w.includes('(`rpack set base 2.0.1` includes it)')),
            `lower releases left out are warned about: ${narrow.draft.warnings.join('; ')}`);
            assertTrue(narrow.lines[0] === 'Generated build/update.sql' && narrow.lines.includes('warnings'), `build lists the warnings:\n${narrow.lines.join('\n')}`);

            const stopped = await expectThrows(() => setBase(ctx, '2.1.0', ['2.0.1']), 'differs from 2.0.0', 'work stops a change of base');
            assertTrue(stopped.includes('--force'), `the refusal offers --force: ${stopped}`);
            const staging = project.files.get(at('2.1.0', 'staging.json'));
            await project.write(at('2.1.0', 'target-catalog.sql'), baseline);
            await project.write(at('2.1.0', 'staging.json'), '{ "sync": {} }\n');
            await expectThrows(() => setBase(ctx, '2.1.0', ['2.0.1']), 'staging.json', 'a staging edit stops a change of base');
            await project.write(at('2.1.0', 'target-catalog.sql'), more);
            await project.write(at('2.1.0', 'staging.json'), staging ?? '{}\n');
            await setNote(project, '2.1.0', 'wider');
            const moved = await setBase(ctx, '2.1.0', ['2.0.1'], { force: true });
            assertEquals(moved.join('\n'), `2.1.0 is now after 2.0.1 (${r201.slice(-8)}).`, `set base says where the folder is now, in one line:\n${moved.join('\n')}`);
            assertEquals(project.files.get(at('2.1.0', 'version.json')), formatVersionFile({ base: [r201], note: 'wider' }), 'version.json names the new base and keeps the note');
            assertEquals(project.files.get(at('2.1.0', 'target-catalog.sql')), withTag(V200), "--force starts over from 2.0.1's source");
            assertTrue((await setBase(ctx, '2.1.0', ['2.0.1']))[0]!.includes('is already after 2.0.1'), 'the same base changes nothing');
            const worked = (await logReleases(ctx)).join('\n');
            assertTrue(/^work +2\.1\.0  after 2\.0\.1$/m.test(worked) && /^work +2\.2\.0  after 2\.0\.1$/m.test(worked), `log lists the unreleased folders:\n${worked}`);
            const v220 = project.files.get(at('2.2.0', 'version.json'))!;
            await project.write(at('2.2.0', 'version.json'), v220.replace('"base"', '"bse": [],\n  "base"'));
            await expectThrows(() => statusOf(ctx, '2.2.0'), "work/2.2.0/version.json: unknown field 'bse'", 'an unknown field in version.json is refused');
            await project.write(at('2.2.0', 'version.json'), v220);

            await expectThrows(() => releaseVersion(ctx, '2.0.0', unlock), 'rpack release --force', 'a released folder is not released again without --force');
            const r110 = configOf(project).released.get('1.1.0')!;
            const releasedStatus = await statusOf(ctx, '1.1.0');
            assertEquals(releasedStatus.lines[4], `Released as ${r110}`, `a released folder names its release:\n${releasedStatus.lines.join('\n')}`);
            const rebuilt = await buildVersion(ctx, '1.1.0');
            assertTrue(rebuilt.matches === true && rebuilt.lines[1] === `It is the update.sql ${r110} was released with`,
                `building a released folder again gives its update.sql:\n${rebuilt.lines.join('\n')}`);

            const again = await releaseChain(dev, new MemoryProject(), false);
            assertEquals([...again.keys()].join(','), [...files.keys()].join(','), 'the same inputs give the same release names');
            for (const [name, text] of files) assertEquals(again.get(name), text, `${name}: the same bytes`);
        },
    },
    {
        name: "[RPACK12] new removes a CREATE CATALOG VERSION naming a parent, and keeps schema VERSIONs",
        invoke: async () => {
            const dev = await devKeys();
            const project = new MemoryProject();
            const ctx: RpackContext = { project, vault: dev.vault };
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const withDoc = EDITOR_SOURCE.replace('CREATE SCHEMA hhs:doc CREATORS ($dev)', "CREATE SCHEMA hhs:doc CREATORS ($dev) VERSION '1.0.0'");
            const catalogAt = (source: string, v: string) => source.replace('CREATE CATALOG editor CREATORS ($dev)', `CREATE CATALOG editor CREATORS ($dev) VERSION '${v}'`);

            await initProject(project, 'editor', 'dev');
            await newVersion(ctx, '1.0.0');
            await project.write(at('1.0.0', 'target-catalog.sql'), catalogAt(withDoc, '1.0.0'));
            const r100 = (await releaseVersion(ctx, '1.0.0', unlock)).produced.name;
            assertEquals(project.files.get(at('1.0.0', '.released/target-catalog.sql')), catalogAt(withDoc, '1.0.0'), 'the first release has no parent to be stale');

            const next = await newVersion(ctx, '1.0.1');
            assertEquals(next.lines[0], `Created work/1.0.1/ for editor 1.0.1, after 1.0.0 (${r100.slice(-8)}).`, 'the removal is not announced');
            assertEquals(project.files.get(at('1.0.1', 'target-catalog.sql')), withDoc, 'only the catalog clause goes; comments and the schema VERSION stay');
            assertEquals(project.files.get(at('1.0.0', '.released/target-catalog.sql')), catalogAt(withDoc, '1.0.0'), '.released/ is left as it was');

            await project.write(at('1.0.1', 'target-catalog.sql'), withDoc.replace('grantee identity PUB READONLY', 'grantee identity PUB READONLY,\n    memo string NULL'));
            await releaseVersion(ctx, '1.0.1', unlock);

            await newVersion(ctx, '1.0.2', { base: ['1.0.0'] });
            assertEquals(project.files.get(at('1.0.2', 'target-catalog.sql')), withDoc, "from .released/: the parent's source without its catalog VERSION");

            assertEquals(stripCatalogVersion(catalogAt(withDoc, '1.0.3'), new Set(['1.0.0'])).text, catalogAt(withDoc, '1.0.3'), 'a catalog VERSION naming no parent is kept');
            const body = ' AS (\n  TABLEGROUP g USING SCHEMA s\n);\n';
            const ownLine = `CREATE CATALOG editor CREATORS ($dev)\n  VERSION '1.0.0'\n  PARAMS (:admin identity)${body}`;
            assertEquals(stripCatalogVersion(ownLine, new Set(['1.0.0'])).text, `CREATE CATALOG editor CREATORS ($dev)\n  PARAMS (:admin identity)${body}`,
                'a clause alone on its line goes with the line');
        },
    },
    {
        name: '[RPACK13] release confirm: no writes nothing, yes releases, warnings follow the status count',
        invoke: async () => {
            const dev = await devKeys();
            const project = new MemoryProject();
            const ctx: RpackContext = { project, vault: dev.vault };
            let unlocked = false;
            const unlock = async (label: string) => {
                unlocked = true;
                return dev.vault.unlock(label, PASSPHRASE);
            };
            // The count line of a status that was not refused: the last line, or the one above the warning count.
            const summaryOf = (draft: ReleaseDraft) => {
                const status = formatStatus(draft);
                return status[status.length - (draft.warnings.length > 0 ? 2 : 1)]!;
            };

            await initProject(project, 'editor', 'dev');
            await newVersion(ctx, '1.0.0');
            await project.write(at('1.0.0', 'target-catalog.sql'), EDITOR_SOURCE);
            const first = await releaseVersion(ctx, '1.0.0', unlock, {
                confirm: async (preview) => {
                    const draft = preview.draft;
                    assertEquals(draft.warnings.length, 0, 'the first release has no warnings');
                    assertEquals(formatReleaseConfirm(draft).join('\n'), summaryOf(draft), 'with no warnings the confirm is the status count');
                    assertEquals(formatReleasePreview(preview).join('\n'), summaryOf(draft), 'a release that replaces nothing previews just that');
                    return true;
                },
            });
            assertTrue(unlocked, 'yes unlocks the key');
            assertTrue(project.files.has(`releases/${first.produced.name}.rpack`), 'yes writes the release');

            unlocked = false;
            await newVersion(ctx, '1.1.0');
            await project.write(at('1.1.0', 'target-catalog.sql'), EDITOR_SOURCE.replace('PARAMS (:admin identity)', 'PARAMS (:admin identity, :moderator identity)'));
            const before = snapshot(project);
            const aborted = await rpackError(() => releaseVersion(ctx, '1.1.0', unlock, {
                confirm: async ({ draft }) => {
                    assertTrue(draft.warnings.length > 0, 'a new param is a warning');
                    assertEquals(formatReleaseConfirm(draft).join('\n'), [summaryOf(draft), '', 'Warning:', ...draft.warnings].join('\n'),
                        'warnings follow a blank line and one heading');
                    return false;
                },
            }), 'no aborts');
            assertTrue(aborted.message === 'aborted' && aborted.lines.length === 0, `no aborts (${aborted})`);
            assertTrue(!unlocked, 'a no does not unlock the key');
            assertEquals(snapshot(project), before, 'a no writes nothing');

            const yes = await releaseVersion(ctx, '1.1.0', unlock, { confirm: async () => true });
            assertTrue(unlocked, 'a later yes unlocks');
            assertTrue(project.files.has(`releases/${yes.produced.name}.rpack`), 'yes writes the release');
        },
    },
    {
        name: '[RPACK17] a read-only FILES: a source still listing it says to leave it out; new writes sources without it',
        invoke: async () => {
            const dev = await devKeys();
            const project = new MemoryProject();
            const ctx: RpackContext = { project, vault: dev.vault };
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const media = EDITOR_SOURCE
                .replace('  TABLE caps (', '  TABLE uploaders (\n    grantee identity PUB READONLY\n  ),\n\n  TABLE caps (')
                .replace(/\n\);\n$/, ',\n  FILES media\n    USING IDENTITIES user.identities\n    ALLOW WRITE IF EXISTS user.uploaders WHERE user.uploaders.grantee = $author\n);\n');
            const stillListed = media.replace('  TABLE uploaders (\n    grantee identity PUB READONLY\n  ),\n\n', '');

            await initProject(project, 'editor', 'dev');
            await newVersion(ctx, '1.0.0');
            await project.write(at('1.0.0', 'target-catalog.sql'), media);
            await releaseVersion(ctx, '1.0.0', unlock);

            await newVersion(ctx, '1.1.0');
            await project.write(at('1.1.0', 'target-catalog.sql'), stillListed);
            const hint = 'media is already released: remove it from target-catalog.sql to leave it read-only, and add a new FILES in its place';
            const text = (await rpackError(() => statusOf(ctx, '1.1.0'), 'a source still listing the FILES')).lines.join('\n');
            assertTrue(text.includes('FILES media: ALLOW WRITE IF reads user.uploaders: schema hhs:user has no table uploaders') && text.includes(hint),
                `status says how to leave the FILES read-only:\n${text}`);

            await project.write(at('1.1.0', 'target-catalog.sql'), EDITOR_SOURCE);
            const second = await releaseVersion(ctx, '1.1.0', unlock);
            assertTrue(second.draft.warnings.some((w) => w.startsWith('FILES media becomes read-only in 1.1.0')), `the release warns: ${second.draft.warnings.join('; ')}`);

            await newVersion(ctx, '1.0.1', { base: ['1.0.0'] });
            assertEquals(project.files.get(at('1.0.1', 'target-catalog.sql')), media, "a parent where the FILES is writable writes it");
            await newVersion(ctx, '1.2.0', { base: ['1.1.0'] });
            assertEquals(project.files.get(at('1.2.0', 'target-catalog.sql')), EDITOR_SOURCE, 'a parent where it is read-only leaves it out');
        },
    },
    {
        name: '[RPACK18] re-release: --force replaces a release, an unchanged folder has nothing to re-release, unreleased folders on it are repointed',
        invoke: async () => {
            const dev = await devKeys();
            const project = new MemoryProject();
            const ctx: RpackContext = { project, vault: dev.vault };
            const unlock = (label: string) => dev.vault.unlock(label, PASSPHRASE);
            const v110 = EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    summary string NULL');

            await initProject(project, 'editor', 'dev');
            await newVersion(ctx, '1.0.0');
            await project.write(at('1.0.0', 'target-catalog.sql'), EDITOR_SOURCE);
            await setNote(project, '1.0.0', 'initial');
            const r100 = (await releaseVersion(ctx, '1.0.0', unlock)).produced.name;
            await newVersion(ctx, '1.1.0');
            await project.write(at('1.1.0', 'target-catalog.sql'), v110);
            await setNote(project, '1.1.0', 'summaries');
            const old = (await releaseVersion(ctx, '1.1.0', unlock)).produced.name;
            await newVersion(ctx, '1.2.0');
            assertEquals(project.files.get(at('1.2.0', 'version.json')), formatVersionFile({ base: [old] }), "an unreleased folder on 1.1.0, without 1.1.0's note");

            await expectThrows(() => releaseVersion(ctx, '1.1.0', unlock), 'rpack release --force', 'a released folder needs --force');
            await expectThrows(() => releaseVersion(ctx, '1.1.0', unlock, { force: true }), 'nothing to re-release', 'an unchanged folder has nothing to re-release');
            assertEquals((await buildVersion(ctx, '1.1.0')).matches, true, 'its build is the released update.sql');
            await setNote(project, '1.1.0', 'summaries and covers');
            assertEquals((await statusOf(ctx, '1.1.0')).lines[4], `Released as ${old}; changed since release`, 'a new note is a change');
            const noteOnly = await buildVersion(ctx, '1.1.0');
            assertEquals(noteOnly.matches, false, 'and the build is no longer the release');
            assertEquals(noteOnly.lines[1], `It is the update.sql ${old} was released with, but version.json's note differs: rpack release --force re-releases 1.1.0`,
                `build says only the note differs:\n${noteOnly.lines.join('\n')}`);

            const v110b = v110.replace('summary string NULL', 'summary string NULL,\n    cover string NULL');
            await project.write(at('1.1.0', 'target-catalog.sql'), v110b);
            const changed = await statusOf(ctx, '1.1.0');
            assertEquals(changed.lines[4], `Released as ${old}; changed since release`, `status marks the change:\n${changed.lines.join('\n')}`);
            assertEquals((await buildVersion(ctx, '1.1.0')).matches, false, 'the build differs from the released update.sql');
            const log = (await logReleases(ctx)).join('\n');
            assertTrue(log.includes('(work/1.1.0/ changed since release)'), `log marks the release:\n${log}`);

            let previewed: ReleasePreview | undefined;
            const redone = await releaseVersion(ctx, '1.1.0', unlock, { force: true, confirm: async (p) => { previewed = p; return true; } });
            const now = redone.produced.name;
            assertTrue(now !== old && now.startsWith('editor-1.1.0-'), `a new release of 1.1.0 (${now})`);
            assertEquals(previewed?.replaces?.name, old, 'the preview names the replaced release');
            assertEquals(previewed!.repointed.join(','), '1.2.0', 'and the unreleased folder on it');
            const text = formatReleasePreview(previewed!).join('\n');
            assertTrue(text.includes(`Re-releasing 1.1.0 replaces ${old}.`) && text.includes('work/1.2.0/ is written against the old 1.1.0'),
                `the preview says what is replaced:\n${text}`);
            assertEquals((await project.list('releases')).join(','), [`${r100}.rpack`, `${now}.rpack`].sort().join(','), 'the replaced file is gone');
            assertEquals(project.files.get(at('1.1.0', '.released/target-catalog.sql')), v110b, '.released/ holds the new source');
            assertEquals(configOf(project).released.get('1.1.0'), now, 'rpack.json names the new release');
            assertEquals(project.files.get(at('1.2.0', 'version.json')), formatVersionFile({ base: [now] }), 'the unreleased folder is repointed');
            assertTrue(redone.lines.some((l) => l.startsWith('work/1.2.0/version.json now names the new releases')), `and the release says so:\n${redone.lines.join('\n')}`);
            assertEquals(noteOf(project.files.get(`releases/${now}.rpack`)!), 'summaries and covers', "the new release has the folder's note");
            assertTrue((await verifyRelease(project.files.get(`releases/${now}.rpack`)!)).ok, 'the new file verifies');
            assertEquals((await statusOf(ctx, '1.1.0')).lines[4], `Released as ${now}`, 'the folder is as released again');
            assertTrue(/^Base: 1\.1\.0 \([0-9a-f]{8}\)$/.test((await statusOf(ctx, '1.2.0')).lines[3]!), 'the repointed folder builds on the new release');

            const catalogOf = (name: string) => JSON.parse(project.files.get(`releases/${name}`)!).manifest.catalog as string;
            const oldCatalog = catalogOf(`${r100}.rpack`);
            await project.write(at('1.0.0', 'target-catalog.sql'), EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    pinned boolean NULL'));
            let firstPreview: ReleasePreview | undefined;
            const genesis = await releaseVersion(ctx, '1.0.0', unlock, { force: true, confirm: async (p) => { firstPreview = p; return true; } });
            assertTrue(formatReleasePreview(firstPreview!).join('\n').includes('the re-release starts a new catalog'), 'the first release warns of a new catalog');
            assertEquals(genesis.rebuilt.map((p) => p.version).join(','), '1.1.0', '1.1.0 is re-released on it');
            assertEquals(noteOf(genesis.rebuilt[0]!.text), 'summaries and covers', "a dependent is re-released with its folder's note");
            assertEquals(project.files.get(at('1.1.0', 'version.json')), formatVersionFile({ base: [genesis.produced.name], note: 'summaries and covers' }),
                "the dependent's version.json names the new base and keeps its note");
            assertEquals(noteOf(genesis.produced.text), 'initial', "1.0.0 keeps its folder's note");
            const catalogs = new Set((await project.list('releases')).map(catalogOf));
            assertTrue(catalogs.size === 1 && !catalogs.has(oldCatalog), 'every file is of the new catalog');
        },
    },
    {
        name: '[RPACK19] re-release with releases built on it: the preview shows how each builds, no writes nothing, yes re-releases them all; one that does not build fails it',
        invoke: async () => {
            const dev = await devKeys();
            const project = new MemoryProject();
            const ctx: RpackContext = { project, vault: dev.vault };
            let unlocked = false;
            const unlock = async (label: string) => {
                unlocked = true;
                return dev.vault.unlock(label, PASSPHRASE);
            };
            const s110 = EDITOR_SOURCE.replace('deleted boolean', 'deleted boolean,\n    summary string NULL');
            const s120 = s110.replace('grantee identity PUB READONLY', 'grantee identity PUB READONLY,\n    memo string NULL');
            const s130 = s120.replace('name string NULL PUB', 'name string NULL PUB,\n    bio string NULL');

            await initProject(project, 'editor', 'dev');
            const old: string[] = [];
            for (const [version, source] of [['1.0.0', EDITOR_SOURCE], ['1.1.0', s110], ['1.2.0', s120], ['1.3.0', s130]] as const) {
                await newVersion(ctx, version);
                await project.write(at(version, 'target-catalog.sql'), source);
                old.push((await releaseVersion(ctx, version, unlock)).produced.name);
            }
            await newVersion(ctx, '1.4.0');

            await project.write(at('1.1.0', 'target-catalog.sql'), withTags(s110));
            const before = snapshot(project);
            const broken = await rpackError(() => releaseVersion(ctx, '1.1.0', unlock, { force: true, yes: true }), 'a dependent that does not build');
            assertEquals(broken.message, "1.2.0 doesn't build on the new 1.1.0; nothing was written", `the failure names the dependent: ${broken.message}`);
            assertTrue(broken.lines.some((l) => l.includes('schema hhs:tags is missing')), `with its refusal:\n${broken.lines.join('\n')}`);
            assertEquals(snapshot(project), before, 'and nothing is written');

            const extra = s110.replace('summary string NULL', 'summary string NULL,\n    extra string NULL');
            await project.write(at('1.1.0', 'target-catalog.sql'), extra);
            const edited130 = s130.replace('bio string NULL', 'bio string NULL,\n    avatar string NULL');
            await project.write(at('1.3.0', 'target-catalog.sql'), edited130);

            unlocked = false;
            await expectThrows(() => releaseVersion(ctx, '1.1.0', unlock, { force: true }), '--yes', 'without a confirm, dependents need --yes');
            assertTrue(!unlocked, 'refused before the key is unlocked');

            const snap = snapshot(project);
            let preview: ReleasePreview | undefined;
            await expectThrows(() => releaseVersion(ctx, '1.1.0', unlock, { force: true, confirm: async (p) => { preview = p; return false; } }), 'aborted', 'no aborts');
            assertEquals(snapshot(project), snap, 'a no writes nothing');
            assertEquals(preview!.rebuilt.map((r) => r.draft.version).join(','), '1.2.0,1.3.0', 'the preview has both dependents, by version');
            assertTrue(preview!.rebuilt[0]!.draft.warnings.some((w) => w.includes('drops column pages.extra')),
                `1.2.0 drops the new column: ${preview!.rebuilt[0]!.draft.warnings.join('; ')}`);
            assertEquals(preview!.rebuilt.map((r) => r.edited).join(','), 'false,true', "1.3.0's working edits are marked");
            assertEquals(preview!.repointed.join(','), '1.4.0', 'the unreleased folder on 1.3.0');
            const text = formatReleasePreview(preview!).join('\n');
            assertTrue(text.includes('It also re-releases 2 releases built on it:') && text.includes('(changed since release)') && text.includes('drops column pages.extra'),
                `the preview shows each dependent's build:\n${text}`);

            const done = await releaseVersion(ctx, '1.1.0', unlock, { force: true, confirm: async () => true });
            const config = configOf(project);
            const names = ['1.0.0', '1.1.0', '1.2.0', '1.3.0'].map((v) => config.released.get(v)!);
            assertEquals(names[0], old[0], '1.0.0 is kept');
            for (let i = 1; i < 4; i++) assertTrue(names[i] !== old[i], `${old[i]} is replaced by ${names[i]}`);
            assertEquals(done.rebuilt.map((p) => p.name).join(','), names.slice(2).join(','), 'the dependents are re-released, by version');
            assertEquals((await project.list('releases')).join(','), names.map((n) => `${n}.rpack`).sort().join(','), 'one release per version: the replaced files are gone');
            assertEquals(project.files.get(at('1.2.0', 'version.json')), formatVersionFile({ base: [names[1]!] }), '1.2.0 is on the new 1.1.0');
            assertEquals(project.files.get(at('1.3.0', 'version.json')), formatVersionFile({ base: [names[2]!] }), '1.3.0 on the new 1.2.0');
            assertEquals(project.files.get(at('1.4.0', 'version.json')), formatVersionFile({ base: [names[3]!] }), 'the unreleased folder on the new 1.3.0');
            assertEquals(project.files.get(at('1.3.0', '.released/target-catalog.sql')), edited130, "1.3.0's working edits ship");
            assertTrue(project.files.get(at('1.2.0', '.released/update.sql'))!.includes('DROP COLUMN pages.extra'), "1.2.0's update.sql drops the column");
            for (const name of names) {
                const report = await verifyRelease(project.files.get(`releases/${name}.rpack`)!);
                assertTrue(report.ok, `${name} verifies: ${report.problems.join('; ')}`);
            }
            const log = (await logReleases(ctx)).join('\n');
            assertTrue(!log.includes('no higher release includes it') && !log.includes('changed since release'), `the chain is whole and as released:\n${log}`);
            assertTrue(/^1\.3\.0 +[0-9a-f]{8}  after 1\.2\.0$/m.test(log), `1.3.0 follows the new 1.2.0:\n${log}`);
        },
    },
];
