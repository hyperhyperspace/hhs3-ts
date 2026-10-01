// `rpack stage`: a staging app in a version folder's stage/, with one host,
// built by replaying every release in the staged release's past. Each step
// ships that release's file, deploys it through rhost, and adds the test data
// of the folder it was released from. A released folder that still makes its
// release replays that release file, with no key; any other folder's release
// is signed in memory and deployed last. The build happens in a side folder
// and is renamed into place, so a failure leaves the previous app, or nothing.
//
//   stage/
//     app.json  keys.json  catalogs/
//     hosts/default/        host.json, rdb/replica.rdb, db/data.sqlite

import { promises as fs } from "node:fs";
import { join } from "node:path";

import { createBasicCrypto, HASH_SHA256, type B64Hash, type OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    catalogStatus, deriveTableId, formatOpVoidDetail,
    type CatalogParamDecl, type OpVerdictChange, type RDbImpl, type RTableChanges, type RTableGroupImpl,
} from "@hyper-hyper-space/hhs3_rdb";
import { lex, parseScript } from "@hyper-hyper-space/hhs3_rdb_lang";
import { executeText, LanguageError, RdbRuntime, useDatabase, type KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    ConfigError, DEFAULT_HOST, parseAppConfig, parseSyncConfig, type AppConfig, type Host, type SyncConfig,
} from "@hyper-hyper-space/hhs3_rhost";
import { APP_CONFIG_FILE, KeyStore, openApp } from "@hyper-hyper-space/hhs3_rhost_node";
import {
    CONFIG_FILE, RELEASES_DIR, SOURCE_FILE, STAGE_DIR, STAGING_FILE, TEST_DATA_FILE, VERSION_FILE, WORK_DIR,
    describeFolder, exportRelease, parseReleaseFile, parseRpackConfig, produceVersion, Released, releaseTag, serializeReleaseFile, sortReleases, workPath,
    type ReleaseDraft, type ReleaseInfo, type RpackConfig,
} from "@hyper-hyper-space/hhs3_rpack";

import { NodeProject } from "./node_project.js";

const CATALOGS_DIR = 'catalogs';
const KEYS_FILE = 'keys.json';
const HOST = DEFAULT_HOST;
const ADMIN = 'admin';
const STAGING_FIELDS = ['sync', 'allow', 'projection', 'autoDeploy', 'params'];
const ROW_KINDS = new Set(['insert', 'update', 'delete', 'bundle']);
const hashSuite = createBasicCrypto().hash(HASH_SHA256);

export type StageOptions = {
    // Unlocks the release key, asked only when the folder's release is signed
    // here and extends existing releases.
    unlock?: (label: string) => Promise<OwnIdentity>;
};

type Step = {
    version: string;
    // editor-1.0.0-8b21d0aa, or the planned release's name.
    name: string;
    file: string;
    samplesPath: string;
    samples: string;
    params: Map<string, CatalogParamDecl>;
};

export async function stage(project: NodeProject, devVault: KeyVault, folder: string, options: StageOptions = {}): Promise<string[]> {
    const configText = await project.read(CONFIG_FILE);
    if (configText === undefined) throw new Error(`there is no ${CONFIG_FILE} here; start a catalog repository with rpack init`);
    const config = parseRpackConfig(configText);
    const info = await describeFolder({ project, vault: devVault }, folder);

    const files = new Map<string, string>();
    for (const name of (await project.list(RELEASES_DIR)).filter((n) => n.endsWith('.rpack'))) {
        const text = await project.read(`${RELEASES_DIR}/${name}`);
        if (text === undefined) continue;
        const m = parseReleaseFile(text).manifest;
        files.set(`${m.name}-${m.version}-${releaseTag(m.release)}`, text);
    }
    const released = await Released.open([...files.values()].map(parseReleaseFile), config.name);
    const folderDir = join(project.dir, WORK_DIR, folder);
    const building = join(folderDir, `.${STAGE_DIR}.building`);
    try {
        const replayOnly = info.release !== undefined && info.unchanged;
        const heads = replayOnly ? [info.release!] : info.base.map((name) => {
            try {
                return released.resolve(name);
            } catch {
                throw new Error(`${workPath(folder, VERSION_FILE)} names ${name}, which isn't in ${RELEASES_DIR}/`);
            }
        });
        const steps = await replaySteps(project, released, files, config, heads);
        clash(steps);

        await fs.rm(building, { recursive: true, force: true });
        await fs.mkdir(join(building, CATALOGS_DIR), { recursive: true });
        const first = !replayOnly && info.base.length === 0;
        const staging = parseStaging(await readJson(project, workPath(folder, STAGING_FILE)));
        const labels = await standInLabels(project, config.key, folder, steps, !replayOnly, first);
        await createStandIns(join(building, KEYS_FILE), labels);

        if (!replayOnly) {
            const vault = first ? await KeyStore.open(join(building, KEYS_FILE), hashSuite) : devVault;
            const unlock = first ? (label: string) => vault.unlock(label, '') : options.unlock;
            if (unlock === undefined) throw new Error(`rpack stage needs the passphrase of '${config.key}'`);
            const produced = await produceVersion({ project, vault }, folder, unlock);
            const samplesPath = workPath(folder, TEST_DATA_FILE);
            steps.push(plannedStep(produced.draft, produced.produced.name, produced.produced.text, samplesPath, (await project.read(samplesPath)) ?? ''));
            clash(steps);
        }

        const lines = await replay(building, steps, staging, labels);
        await swapIn(building, join(folderDir, STAGE_DIR), join(folderDir, `.${STAGE_DIR}.old`));
        const what = replayOnly ? info.release!.name : `${folder} as it would be released`;
        return [`staged ${what} in ${workPath(folder, STAGE_DIR)}/: ${steps.map((s) => s.version).join(', ')}`, ...lines];
    } catch (err) {
        await fs.rm(building, { recursive: true, force: true });
        throw err;
    } finally {
        await released.close();
    }
}

// The releases `heads` cover, each with the file it ships and the rows of the
// folder it was released from (none when no folder is its).
async function replaySteps(project: NodeProject, released: Released, files: Map<string, string>, config: RpackConfig, heads: ReleaseInfo[]): Promise<Step[]> {
    if (heads.length === 0) return [];
    const index = await (await released.catalog()).getIndex();
    const folderOf = new Map([...config.released].map(([folder, name]) => [name, folder]));
    const steps: Step[] = [];
    for (const info of await covered(released, heads)) {
        const folder = folderOf.get(info.name);
        const samplesPath = folder !== undefined ? workPath(folder, TEST_DATA_FILE) : '';
        steps.push({
            version: info.version,
            name: info.name,
            file: files.get(info.name) ?? serializeReleaseFile(await exportRelease(released.ctx, released.catalogId!, info.hash)),
            samplesPath,
            samples: folder !== undefined ? (await project.read(samplesPath)) ?? '' : '',
            params: index.releaseState(info.hash).params,
        });
    }
    return steps;
}

// `heads` and every release in their past, in version order.
async function covered(released: Released, heads: ReleaseInfo[]): Promise<ReleaseInfo[]> {
    const want = new Set(heads.map((h) => h.hash));
    const out: ReleaseInfo[] = [];
    for (const release of released.releases()) {
        if (want.has(release.hash)) { out.push(release); continue; }
        for (const head of heads) {
            if (await released.isBelow(release.hash, head.hash)) { out.push(release); break; }
        }
    }
    return sortReleases(out);
}

function clash(steps: Step[]): void {
    const seen = new Map<string, string>();
    for (const step of steps) {
        const other = seen.get(step.version);
        if (other !== undefined) {
            throw new Error(`two releases have version ${step.version} (${other}, ${step.name}); a staging host can ship only one`);
        }
        seen.set(step.version, step.name);
    }
}

function plannedStep(draft: ReleaseDraft, name: string, file: string, samplesPath: string, samples: string): Step {
    const params = new Map(draft.base?.fold.params ?? []);
    for (const decl of draft.params) params.set(decl.name, decl);
    return { version: draft.version, name, file, samplesPath, samples, params };
}

// Every $label the rows use, plus the catalog's labels when a first release
// is staged from source. `admin` is the staging database's admin.
async function standInLabels(project: NodeProject, key: string, folder: string, steps: Step[], planned: boolean, first: boolean): Promise<string[]> {
    const labels = new Set<string>([ADMIN]);
    const add = async (path: string) => {
        for (const label of variablesIn((await project.read(path)) ?? '', path)) labels.add(label);
    };
    for (const step of steps) for (const label of variablesIn(step.samples, step.samplesPath)) labels.add(label);
    if (planned) await add(workPath(folder, TEST_DATA_FILE));
    if (first) {
        labels.add(key);
        await add(workPath(folder, SOURCE_FILE));
    }
    return [...labels].sort();
}

function variablesIn(text: string, file: string): string[] {
    if (text.trim().length === 0) return [];
    const parsed = lex(text);
    if (!parsed.ok) {
        const diagnostic = parsed.diagnostics[0]!;
        const at = diagnostic.span !== undefined ? `${file}:${diagnostic.span.line}:${diagnostic.span.column}: ` : `${file}: `;
        throw new Error(`${at}${diagnostic.message}`);
    }
    const labels = new Set<string>();
    for (const token of parsed.value) {
        if (token.kind !== 'variable') continue;
        const name = token.text.slice(1);
        if (name !== 'me' && name !== 'author') labels.add(name);
    }
    return [...labels];
}

async function createStandIns(path: string, labels: string[]): Promise<void> {
    const keys = await KeyStore.open(path, hashSuite);
    for (const label of labels) await keys.create(label, '');
}

async function readJson(project: NodeProject, path: string): Promise<unknown> {
    const text = await project.read(path);
    if (text === undefined || text.trim().length === 0) return {};
    try {
        return JSON.parse(text) as unknown;
    } catch (err) {
        throw new Error(`${path} is not valid JSON: ${err instanceof Error ? err.message : String(err)}`);
    }
}

// staging.json: the staging host's `sync`, and the `allow`, `projection`,
// `autoDeploy` and `params` of its app.json.
type Staging = {
    sync: SyncConfig;
    app: { allow?: unknown; projection?: unknown; autoDeploy?: unknown };
    params: { [name: string]: unknown };
};

function parseStaging(value: unknown): Staging {
    if (!isObject(value)) throw new ConfigError(`${STAGING_FILE}: the file must be an object`);
    for (const key of Object.keys(value)) {
        if (!STAGING_FIELDS.includes(key)) throw new ConfigError(`${STAGING_FILE}: '${key}' is not a known field`);
    }
    const params = value['params'] ?? {};
    if (!isObject(params)) throw new ConfigError(`${STAGING_FILE}: params must be an object`);
    const app: Staging['app'] = {};
    if (value['allow'] !== undefined) app.allow = value['allow'];
    if (value['projection'] !== undefined) app.projection = value['projection'];
    if (value['autoDeploy'] !== undefined) app.autoDeploy = value['autoDeploy'];
    return { sync: parseSyncConfig(value['sync'] ?? { scope: 'localhost' }, STAGING_FILE), app, params };
}

// Every param staging.json sets must be one the staged release declares.
function checkStagingParams(staging: Staging, steps: Step[]): void {
    const last = steps[steps.length - 1];
    if (last === undefined) return;
    const undeclared = Object.keys(staging.params).filter((name) => !last.params.has(name)).sort();
    if (undeclared.length > 0) {
        throw new ConfigError(`${STAGING_FILE} sets ${undeclared.map((n) => `:${n}`).join(', ')}, which ${last.version} doesn't declare`);
    }
}

// One step's app.json: staging.json's sections, the params the step's
// release declares, and staging's own releases and keystore.
function appConfig(staging: Staging, step: Step): AppConfig {
    const params: { [name: string]: unknown } = {};
    const missing: string[] = [];
    for (const [name, decl] of [...step.params].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
        const configured = staging.params[name];
        if (configured !== undefined) params[name] = configured;
        else if (decl.type === 'identity') params[name] = '$me';
        else missing.push(name);
    }
    if (missing.length > 0) {
        throw new Error(`staging ${step.version} needs ${missing.map((n) => `:${n}`).join(', ')}; set ${missing.length === 1 ? 'it' : 'them'} in ${STAGING_FILE}`);
    }
    return parseAppConfig({
        releases: `${CATALOGS_DIR}/`,
        keystore: KEYS_FILE,
        params,
        autoDeploy: staging.app.autoDeploy ?? 'none',
        ...(staging.app.allow !== undefined ? { allow: staging.app.allow } : {}),
        projection: staging.app.projection ?? { path: 'db/data.sqlite' },
    }, STAGING_FILE);
}

function isObject(value: unknown): value is { [key: string]: unknown } {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

// The staging host signs with the stand-in admin key, which has no
// passphrase.
async function replay(dir: string, steps: Step[], staging: Staging, labels: string[]): Promise<string[]> {
    checkStagingParams(staging, steps);
    let report: string[] = [];
    let last: Step | undefined;
    for (let i = 0; i < steps.length; i++) {
        const step = steps[i]!;
        last = step;
        await fs.writeFile(join(dir, CATALOGS_DIR, `${step.name}.rpack`), step.file);
        await fs.writeFile(join(dir, APP_CONFIG_FILE), JSON.stringify(appConfig(staging, step), undefined, 2) + '\n');
        const app = await openApp(dir);
        try {
            const host = i === 0 ? await app.create(HOST, { key: ADMIN, sync: staging.sync }) : await app.host(HOST);
            const before = i === 0 ? new Map<B64Hash, Version>() : await frontiers(host);
            if (i > 0) await host.deploy();
            if (i === steps.length - 1) report = await deployReport(host.db, before);
            await runRows(host.runtime, host.db.getId(), labels, step);
        } finally {
            await app.close();
        }
    }
    const app = await openApp(dir);
    try {
        await (await app.host(HOST)).project();
    } finally {
        await app.close();
    }
    const lines = [`deploying ${last?.version ?? '?'}:`, ...report.map((line) => `  ${line}`)];
    if (last !== undefined && last.samples.trim().length > 0) lines.push(`rows: ${last.samplesPath}`);
    return lines;
}

async function frontiers(host: Host): Promise<Map<B64Hash, Version>> {
    const out = new Map<B64Hash, Version>();
    for (const id of await host.db.getMemberGroups()) {
        const group = (await host.runtime.workspace.replica.getObject(id)) as RTableGroupImpl;
        out.set(id, await (await group.getScopedDag()).getFrontier());
    }
    return out;
}

async function runRows(runtime: RdbRuntime, database: B64Hash, labels: string[], step: Step): Promise<void> {
    if (step.samples.trim().length === 0) return;
    const parsed = parseScript(step.samples);
    if (!parsed.ok) {
        const diagnostic = parsed.diagnostics[0]!;
        throw new Error(`${where(step, diagnostic.span)}${diagnostic.message}`);
    }
    for (const statement of parsed.value.statements) {
        if (!ROW_KINDS.has(statement.kind)) {
            throw new Error(`${where(step, statement.span)}samples hold only row writes (insert, update, delete, bundle)`);
        }
    }
    const session = runtime.session;
    for (const label of labels) await session.unlockKey(label, '');
    session.selectAuthor(ADMIN);
    // A new identity has to reach the groups bound to it before they can
    // verify that key's signature, the way the REPL propagates refs.
    session.setRefAutoUpdate('auto');
    await useDatabase(session, database);
    try {
        await executeText(session, step.samples);
    } catch (err) {
        if (err instanceof LanguageError) {
            const diagnostic = err.diagnostics[0]!;
            throw new Error(`${where(step, diagnostic.span)}${diagnostic.message}`);
        }
        throw err;
    }
}

function where(step: Step, span: { line: number; column: number } | undefined): string {
    return span === undefined ? `${step.samplesPath}: ` : `${step.samplesPath}:${span.line}:${span.column}: `;
}

async function deployReport(db: RDbImpl, before: Map<B64Hash, Version>): Promise<string[]> {
    const status = await catalogStatus(db);
    const members: { name: string; id: B64Hash; group: RTableGroupImpl }[] = [];
    for (const id of await db.getMemberGroups()) {
        const group = (await db.getContext().getObject(id)) as RTableGroupImpl;
        members.push({ name: group.getName(), id, group });
    }
    members.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    const width = Math.max(1, ...members.map((m) => m.name.length));
    const lines: string[] = [];
    for (const member of members) {
        const earlier = before.get(member.id);
        if (earlier === undefined) {
            lines.push(`${member.name.padEnd(width)}  created`);
            continue;
        }
        const end = await (await member.group.getScopedDag()).getFrontier();
        const delta = await member.group.computeDelta(earlier, end);
        const names = await tableNames(member.group, earlier, end);
        const parts: string[] = [];
        const tables = [...delta.tableChanges].sort(([a], [b]) => compareNames(names.get(a), names.get(b)));
        for (const [tableId, changes] of tables) {
            const summary = summarizeRows(changes);
            if (summary !== undefined) parts.push(`${names.get(tableId) ?? tableId}: ${summary}`);
        }
        // A dropped table's rows are not walked: the schema change names the
        // drop, and the rows are counted at the version before it.
        const startView = await member.group.getView(earlier, earlier);
        for (const change of delta.schemaChanges.tableChanges) {
            if (!change.existedBefore || change.existsAfter) continue;
            const n = (await (await startView.getTableView(change.table)).liveRowIds()).length;
            if (n > 0) parts.push(`${change.table}: ${n} ${n === 1 ? 'row' : 'rows'} gone`);
        }
        parts.sort();
        const verdicts = delta.opVerdictChanges.map(verdictLine).filter((line): line is string => line !== undefined);
        const detail = parts.length === 0 && verdicts.length === 0 ? 'no change' : parts.join('; ');
        lines.push(`${member.name.padEnd(width)}  ${detail}`);
        for (const verdict of verdicts) lines.push(`${''.padEnd(width)}  ${verdict}`);
    }
    if (members.length === 0 && status.unresolved !== undefined) lines.push(`the database does not resolve: ${status.unresolved.kind}`);
    return lines;
}

function compareNames(a: string | undefined, b: string | undefined): number {
    const x = a ?? '';
    const y = b ?? '';
    return x < y ? -1 : x > y ? 1 : 0;
}

async function tableNames(group: RTableGroupImpl, start: Version, end: Version): Promise<Map<B64Hash, string>> {
    const names = new Map<B64Hash, string>();
    for (const at of [start, end]) {
        const view = await group.getView(at, at);
        for (const table of view.getSchemaView().getTableNames()) names.set(deriveTableId(group.getId(), table), table);
    }
    return names;
}

function summarizeRows(changes: RTableChanges): string | undefined {
    let gone = 0;
    let changed = 0;
    let back = 0;
    for (const row of changes.rowChanges) {
        if (row.liveBefore && !row.liveAfter) gone += 1;
        else if (!row.liveBefore && row.liveAfter) back += 1;
        else if (row.columnChanges.length > 0) changed += 1;
    }
    const parts: string[] = [];
    if (gone > 0) parts.push(`${gone} ${gone === 1 ? 'row' : 'rows'} gone`);
    if (changed > 0) parts.push(`${changed} ${changed === 1 ? 'row' : 'rows'} changed`);
    if (back > 0) parts.push(`${back} ${back === 1 ? 'row' : 'rows'} back`);
    return parts.length === 0 ? undefined : parts.join(', ');
}

function verdictLine(flip: OpVerdictChange): string | undefined {
    if (flip.voidBefore === flip.voidAfter) return undefined;
    const what = flip.voidAfter ? 'voided' : 'reinstated';
    const table = flip.table !== undefined ? ` ${flip.table}` : '';
    const why = flip.reason !== undefined ? `: ${formatOpVoidDetail(flip.reason)}` : '';
    return `${what} ${flip.kind}${table}${why}`;
}

async function swapIn(building: string, finalDir: string, aside: string): Promise<void> {
    await fs.rm(aside, { recursive: true, force: true });
    if (await exists(finalDir)) await fs.rename(finalDir, aside);
    await fs.rename(building, finalDir);
    await fs.rm(aside, { recursive: true, force: true });
}

async function exists(path: string): Promise<boolean> {
    try {
        await fs.access(path);
        return true;
    } catch {
        return false;
    }
}
