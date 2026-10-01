// The rpack commands over a catalog repository: init, new, set base, status,
// build, release and log. Each version is prepared in its own work folder
// (work/<version>/); rpack.json maps the released folders to their releases.
// The CLI (rdb_tools) finds the folder, parses arguments and prints the lines.

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { compareSemver, isValidSemver } from "@hyper-hyper-space/hhs3_rdb";
import type { KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";

import { hintReadOnlyFiles } from "./catalog_diff.js";
import { formatDraft, formatStatus, formatStatusError, formatUpgrade, type RebuiltRelease, type ReleasePreview } from "./format_draft.js";
import { parseReleaseFile, releaseTag, type ReleaseFile } from "./format.js";
import { KeyDirectory } from "./keys.js";
import { modelDifferences } from "./model.js";
import { draftRelease, type ReleaseDraft } from "./draft.js";
import { produceRelease, type ProducedRelease } from "./produce.js";
import {
    BUILD_DIR, CONFIG_FILE, GITIGNORE, RELEASES_DIR, SOURCE_FILE, STAGING_FILE, TEST_DATA_FILE, UPDATE_FILE, UPGRADE_MANUAL_FILE, VERSION_FILE, WORK_DIR,
    compareFolders, folderVersion, formatRpackConfig, formatVersionFile, parseRpackConfig, parseVersionFile, releasedPath, workPath,
    type RpackConfig, type RpackProject, type VersionFile,
} from "./project.js";
import { describeRelease, Released, sortReleases, type ReleaseInfo } from "./released.js";
import { formatWhere, isBlankSql, readSource, SourceError } from "./source.js";
import { stripCatalogVersion, writeSource } from "./write_source.js";

export class RpackError extends Error {
    constructor(message: string, readonly lines: string[] = []) {
        super(message);
        this.name = 'RpackError';
    }
}

export type RpackContext = {
    project: RpackProject;
    // The developer keystore: public records by label, and the release key.
    vault: KeyVault;
};

type Unlock = (label: string) => Promise<OwnIdentity>;

// A file in releases/, under the name its release has.
type Stored = { path: string; file: ReleaseFile };

type Repo = {
    config: RpackConfig;
    stored: Map<string, Stored>;
    released: Released;
    keys: KeyDirectory;
};

// A work folder: its version, version.json's base and note, and the release
// rpack.json says was made from it.
type Folder = {
    name: string;
    version: string;
    base: string[];
    note?: string;
    release?: ReleaseInfo;
};

// A folder's signed inputs.
type Inputs = { source: string; manual: string };

async function readConfig(project: RpackProject): Promise<RpackConfig> {
    const text = await project.read(CONFIG_FILE);
    if (text === undefined) throw new RpackError(`There is no ${CONFIG_FILE} here; start a catalog repository with rpack init`);
    return parseRpackConfig(text);
}

async function writeConfig(project: RpackProject, config: RpackConfig): Promise<void> {
    await project.write(CONFIG_FILE, formatRpackConfig(config));
}

async function readStored(project: RpackProject): Promise<Map<string, Stored>> {
    const stored = new Map<string, Stored>();
    for (const name of (await project.list(RELEASES_DIR)).filter((n) => n.endsWith('.rpack')).sort()) {
        const path = `${RELEASES_DIR}/${name}`;
        let file: ReleaseFile;
        try {
            file = parseReleaseFile((await project.read(path))!);
        } catch (err) {
            throw new RpackError(`${path}: ${err instanceof Error ? err.message : String(err)}`);
        }
        const m = file.manifest;
        const release = `${m.name}-${m.version}-${releaseTag(m.release)}`;
        if (!stored.has(release)) stored.set(release, { path, file });
    }
    return stored;
}

async function openRepo(ctx: RpackContext): Promise<Repo> {
    const config = await readConfig(ctx.project);
    const stored = await readStored(ctx.project);
    const released = await Released.open([...stored.values()].map((s) => s.file), config.name);
    return { config, stored, released, keys: KeyDirectory.fromVault(ctx.vault) };
}

async function withRepo<T>(ctx: RpackContext, run: (repo: Repo) => Promise<T>): Promise<T> {
    const repo = await openRepo(ctx);
    try {
        return await run(repo);
    } finally {
        await repo.released.close();
    }
}

function tryResolve(released: Released, selector: string): ReleaseInfo | undefined {
    try {
        return released.resolve(selector);
    } catch {
        return undefined;
    }
}

function resolveSelector(released: Released, selector: string): ReleaseInfo {
    try {
        return released.resolve(selector);
    } catch (err) {
        throw new RpackError(err instanceof Error ? err.message : String(err));
    }
}

// The name a base entry stands for; an entry no release matches stays as written.
function canonicalName(released: Released, entry: string): string {
    return tryResolve(released, entry)?.name ?? entry;
}

function folderOf(config: RpackConfig, release: string): string | undefined {
    for (const [folder, name] of config.released) if (name === release) return folder;
    return undefined;
}

async function readFolder(ctx: RpackContext, repo: Repo, name: string): Promise<Folder> {
    const version = folderVersion(name);
    if (version === undefined) throw new RpackError(`${workPath(name)}/ is not a version folder: its name must be a version such as 1.2.0`);
    const path = workPath(name, VERSION_FILE);
    const text = await ctx.project.read(path);
    if (text === undefined) throw new RpackError(`There is no ${path}; rpack new ${version} starts ${workPath(name)}/`);
    const file = parseVersionFile(text, path);
    const folder: Folder = { name, version, ...file };
    const releaseName = repo.config.released.get(name);
    if (releaseName !== undefined) {
        const release = repo.released.releases().find((r) => r.name === releaseName);
        if (release === undefined) throw new RpackError(`${CONFIG_FILE} says ${workPath(name)}/ is released as ${releaseName}, which isn't in ${RELEASES_DIR}/`);
        folder.release = release;
    }
    return folder;
}

function resolveBase(released: Released, folder: { name: string; base: string[] }): ReleaseInfo[] {
    return folder.base.map((entry) => {
        const release = tryResolve(released, entry);
        if (release === undefined) throw new RpackError(`${workPath(folder.name, VERSION_FILE)} names ${entry}, which isn't in ${RELEASES_DIR}/`);
        return release;
    });
}

function describeBase(released: Released, entries: string[]): string {
    if (entries.length === 0) return 'None, this is the first release';
    return entries.map((entry) => {
        const release = tryResolve(released, entry);
        return release !== undefined ? describeRelease(release) : entry;
    }).join(' + ');
}

function describeParents(parents: ReleaseInfo[]): string {
    return parents.length === 0 ? 'the first release' : `after ${parents.map(describeRelease).join(' + ')}`;
}

function highestOf(releases: ReleaseInfo[]): ReleaseInfo {
    return sortReleases(releases)[releases.length - 1]!;
}

function parentKey(list: ReleaseInfo[]): string {
    return list.map((r) => r.hash).sort().join(',');
}

function checkVersion(version: string): void {
    if (!isValidSemver(version)) throw new RpackError(`'${version}' is not a version (major.minor.patch)`);
}

async function checkParents(released: Released, version: string, parents: ReleaseInfo[]): Promise<void> {
    const maximal = new Set(await released.maximal(parents.map((p) => p.hash)));
    for (const parent of parents) {
        if (!maximal.has(parent.hash)) throw new RpackError(`${describeRelease(parent)} is in the past of another release in the base`);
        if (compareSemver(version, parent.version) <= 0) throw new RpackError(`${version} is not above its parent ${describeRelease(parent)}`);
    }
    if (parents.length === 0 && released.releases().length > 0) {
        throw new RpackError(`No release is below ${version}; every release after the first has parents`);
    }
}

const skeleton = (name: string, key: string) => `-- The ${name} catalog as this version should leave it: CREATE SCHEMA
-- statements, then one CREATE CATALOG. AT LATEST may be written on a group; a
-- hash or a version set may not. A catalog VERSION, NOTE, or BY may appear
-- when it agrees with the release. A schema VERSION is the version a release
-- that changes the schema gives it; without one, the schema takes the release's.

CREATE SCHEMA ${name}:main CREATORS ($${key}) AS (
  TABLE items (
    name string
  )
);

CREATE CATALOG ${name} CREATORS ($${key}) AS (
  TABLEGROUP main USING SCHEMA ${name}:main
);
`;

const EMPTY_STAGING = '{}\n';

export async function initProject(project: RpackProject, name: string, key: string): Promise<string[]> {
    if (await project.read(CONFIG_FILE) !== undefined) throw new RpackError(`There is a ${CONFIG_FILE} here already`);
    const lines: string[] = [];
    if (await project.read('.gitignore') === undefined) {
        await project.write('.gitignore', GITIGNORE);
        lines.push('Wrote .gitignore');
    }
    for (const dir of [RELEASES_DIR, WORK_DIR]) if ((await project.list(dir)).length === 0) await project.writeFolder(dir, {});
    await writeConfig(project, { name, key, released: new Map() });
    lines.push(`Wrote ${CONFIG_FILE}; next: rpack new 1.0.0`);
    return lines;
}

async function freshStagingText(project: RpackProject, config: RpackConfig, parents: ReleaseInfo[]): Promise<string> {
    if (parents.length === 0) return EMPTY_STAGING;
    const folder = folderOf(config, highestOf(parents).name);
    return (folder !== undefined ? await project.read(workPath(folder, STAGING_FILE)) : undefined) ?? EMPTY_STAGING;
}

// A folder's files as a release of `parents` starts them: the parents' merged
// catalog, written over the highest parent's released source, the highest
// parent folder's staging config, and empty upgrade-manual.sql and
// test-data.sql. With no parents, the skeleton. A CREATE CATALOG VERSION that
// names a parent was right for that release and is wrong for this one, so it
// is dropped; the release supplies the version.
async function writeFresh(project: RpackProject, repo: Repo, folder: string, parents: ReleaseInfo[]): Promise<void> {
    const { config, released, keys } = repo;
    const at = (file: string) => workPath(folder, file);
    let source: string;
    if (parents.length === 0) {
        source = skeleton(config.name, config.key);
    } else {
        const base = await released.base(parents.map((p) => p.hash));
        if (base.clashes.length > 0) throw new RpackError(`The base can't be written as one ${SOURCE_FILE}: ${base.clashes.join('; ')}`);
        const highestFolder = folderOf(config, highestOf(parents).name);
        const text = highestFolder !== undefined ? await project.read(releasedPath(highestFolder, SOURCE_FILE)) : undefined;
        const written = await writeSource(text ?? '', base.model, keys, config.key);
        source = stripCatalogVersion(written, new Set(parents.map((p) => p.version))).text;
    }
    await project.write(at(SOURCE_FILE), source);
    await project.write(at(UPGRADE_MANUAL_FILE), '');
    await project.write(at(TEST_DATA_FILE), '');
    await project.write(at(STAGING_FILE), await freshStagingText(project, config, parents));
}

async function writeVersionFile(project: RpackProject, folder: string, file: VersionFile): Promise<void> {
    await project.write(workPath(folder, VERSION_FILE), formatVersionFile(file));
}

function noteOption(note: string | undefined): { note?: string } {
    return note !== undefined ? { note } : {};
}

// A folder's version.json with another base; the note stays.
function rebased(folder: { note?: string }, base: string[]): VersionFile {
    return { base, ...noteOption(folder.note) };
}

// How a folder differs from the fresh start of `current`. Empty when writing
// it fresh on another base would lose nothing.
async function workingDifferences(project: RpackProject, repo: Repo, folder: Folder, current: ReleaseInfo[]): Promise<string[]> {
    const { config, released, keys } = repo;
    const at = (file: string) => workPath(folder.name, file);
    const reasons: string[] = [];
    for (const file of [UPGRADE_MANUAL_FILE, TEST_DATA_FILE]) {
        if (!isBlankSql(await project.read(at(file)))) reasons.push(`${at(file)} isn't empty`);
    }
    const text = await project.read(at(SOURCE_FILE));
    if (current.length === 0) {
        if (!isBlankSql(text) && text !== skeleton(config.name, config.key)) reasons.push(`${at(SOURCE_FILE)} holds work no release has`);
    } else if (!isBlankSql(text)) {
        try {
            const model = (await readSource(text!, keys, config.key, SOURCE_FILE, { version: folder.version })).model;
            const left = modelDifferences(model, (await released.base(current.map((r) => r.hash))).model);
            if (left.length > 0) {
                reasons.push(`${at(SOURCE_FILE)} differs from ${current.map(describeRelease).join(' + ')}: ${left.slice(0, 3).join('; ')}${left.length > 3 ? '; ...' : ''}`);
            }
        } catch (err) {
            if (err instanceof SourceError) reasons.push(`${at(SOURCE_FILE)} can't be read (${err.message})`);
            else throw err;
        }
    }
    if ((await project.read(at(STAGING_FILE)) ?? '') !== await freshStagingText(project, config, current)) {
        reasons.push(`${at(STAGING_FILE)} differs from the base's copy`);
    }
    return reasons;
}

export type NewResult = { folder: string; parents: ReleaseInfo[]; lines: string[] };

// Starts work/<version>/ on `base` (release selectors), or by default on the
// releases below the version that no other one below it includes.
export async function newVersion(ctx: RpackContext, version: string, options: { base?: string[] } = {}): Promise<NewResult> {
    checkVersion(version);
    return withRepo(ctx, async (repo) => {
        const folder = version;
        const same = repo.released.releases().filter((r) => r.version === version);
        if (same.length > 0) throw new RpackError(`Release ${version} already exists (${same.map((r) => r.name).join(', ')})`);
        if ((await ctx.project.list(workPath(folder))).length > 0) throw new RpackError(`${workPath(folder)}/ already exists`);
        const parents = options.base !== undefined && options.base.length > 0
            ? options.base.map((s) => resolveSelector(repo.released, s))
            : await repo.released.defaultParents(version);
        await checkParents(repo.released, version, parents);
        await writeFresh(ctx.project, repo, folder, parents);
        await writeVersionFile(ctx.project, folder, { base: parents.map((p) => p.name) });
        return { folder, parents, lines: [`Created ${workPath(folder)}/ for ${repo.config.name} ${version}, ${describeParents(parents)}.`] };
    });
}

// Puts a folder on another base and writes it fresh there. Refuses over work
// unless `force`. A released folder then holds a pending re-release.
export async function setBase(ctx: RpackContext, folderName: string, selectors: string[], options: { force?: boolean } = {}): Promise<string[]> {
    if (selectors.length === 0) throw new RpackError('Set base needs at least one release');
    return withRepo(ctx, async (repo) => {
        const folder = await readFolder(ctx, repo, folderName);
        const parents = selectors.map((s) => resolveSelector(repo.released, s));
        await checkParents(repo.released, folder.version, parents);
        const describe = describeParents(parents);
        const currentNames = folder.base.map((entry) => canonicalName(repo.released, entry)).sort();
        if (currentNames.join(',') === parents.map((p) => p.name).sort().join(',')) {
            return [`${workPath(folder.name)}/ is already ${describe}`];
        }
        if (options.force !== true) {
            const reasons = await workingDifferences(ctx.project, repo, folder, resolveBase(repo.released, folder));
            if (reasons.length > 0) throw new RpackError(`${reasons[0]}; pass --force to start ${folder.name} over on the new base`);
        }
        await writeFresh(ctx.project, repo, folder.name, parents);
        await writeVersionFile(ctx.project, folder.name, rebased(folder, parents.map((p) => p.name)));
        const lines = [`${folder.version} is now ${describe}.`];
        if (folder.release !== undefined) lines.push(`${folder.name} is released as ${folder.release.name}: rpack release --force re-releases it on the new base`);
        return lines;
    });
}

// Where a folder's release is drafted: every release, or for a released
// folder, the release files without it and the releases built on it. That is
// what releases/ would hold after a re-release, so the old release doesn't
// count as the version already existing.
type Surface = { released: Released; replaced: ReleaseInfo[]; close(): Promise<void> };

async function surfaceFor(repo: Repo, folder: Folder): Promise<Surface> {
    if (folder.release === undefined) return { released: repo.released, replaced: [], close: async () => {} };
    const replaced = [folder.release, ...await repo.released.descendants(folder.release.hash)];
    const names = new Set(replaced.map((r) => r.name));
    const files = [...repo.stored].filter(([name]) => !names.has(name)).map(([, s]) => s.file);
    const scratch = await Released.open(files, repo.config.name);
    return { released: scratch, replaced, close: () => scratch.close() };
}

async function withSurface<T>(repo: Repo, folder: Folder, run: (surface: Surface) => Promise<T>): Promise<T> {
    const surface = await surfaceFor(repo, folder);
    try {
        return await run(surface);
    } finally {
        await surface.close();
    }
}

async function readInputs(project: RpackProject, folder: string): Promise<Inputs> {
    const path = workPath(folder, SOURCE_FILE);
    const source = await project.read(path);
    if (source === undefined) throw new RpackError(`There is no ${path}`);
    return { source, manual: (await project.read(workPath(folder, UPGRADE_MANUAL_FILE))) ?? '' };
}

async function draftOn(
    repo: Repo, released: Released, version: string, parents: ReleaseInfo[], inputs: Inputs, note: string | undefined,
): Promise<ReleaseDraft> {
    const { config, keys } = repo;
    let source;
    try {
        source = await readSource(inputs.source, keys, config.key, SOURCE_FILE, { version, ...(note !== undefined ? { note } : {}) });
    } catch (err) {
        if (!(err instanceof SourceError) || parents.length === 0) throw err;
        const hinted = hintReadOnlyFiles(err.issues, await released.base(parents.map((p) => p.hash)));
        throw hinted === undefined ? err : new SourceError(hinted);
    }
    return draftRelease({ catalog: config.name, version, parents, released, source, nextText: inputs.manual, signer: keys.get(config.key)! });
}

// A released folder's draft makes the update its release was made with: the
// same parents, and the same update.sql.
async function sameUpdate(project: RpackProject, repo: Repo, folder: Folder, draft: ReleaseDraft): Promise<boolean> {
    if (folder.release === undefined || draft.refusals.length > 0) return false;
    if (parentKey(draft.parents) !== [...folder.release.parents].sort().join(',')) return false;
    return formatUpgrade(draft, repo.keys.labels()) === await project.read(releasedPath(folder.name, UPDATE_FILE));
}

// ...and, with the same note, still makes its release.
async function matchesRelease(project: RpackProject, repo: Repo, folder: Folder, draft: ReleaseDraft): Promise<boolean> {
    return folder.note === folder.release?.note && await sameUpdate(project, repo, folder, draft);
}

// A released folder's signed inputs, base or note, byte for byte, differ from
// what its release was made from.
async function editedSinceRelease(project: RpackProject, repo: Repo, folder: Folder): Promise<boolean> {
    if (folder.note !== folder.release!.note) return true;
    for (const file of [SOURCE_FILE, UPGRADE_MANUAL_FILE]) {
        const working = await project.read(workPath(folder.name, file));
        const signed = await project.read(releasedPath(folder.name, file));
        if ((working ?? '') !== (signed ?? '')) return true;
    }
    const base = folder.base.map((entry) => canonicalName(repo.released, entry)).sort().join(',');
    const parents = folder.release!.parents.map((h) => repo.released.get(h)?.name ?? h).sort().join(',');
    return base !== parents;
}

// Where assembly broke: a source location, or a message with none.
function assemblyDetail(err: unknown): string[] {
    return err instanceof SourceError
        ? err.issues.map((issue) => `${formatWhere(issue.where)}${issue.message}`)
        : [err instanceof Error ? err.message : String(err)];
}

function releasedLine(folder: Folder, matches?: boolean): string | undefined {
    if (folder.release === undefined) return undefined;
    return `Released as ${folder.release.name}${matches === false ? '; changed since release' : ''}`;
}

export type DraftResult = { draft: ReleaseDraft; lines: string[] };

// The folder's release, counted: catalog, version, base, the release made
// from it, then the draft's shape. A failure before the draft exists still
// carries the header and where the source broke.
export async function statusOf(ctx: RpackContext, folderName: string): Promise<DraftResult> {
    return withRepo(ctx, async (repo) => {
        const folder = await readFolder(ctx, repo, folderName);
        return withSurface(repo, folder, async (surface) => {
            let draft: ReleaseDraft;
            try {
                const parents = resolveBase(surface.released, folder);
                draft = await draftOn(repo, surface.released, folder.version, parents, await readInputs(ctx.project, folder.name), folder.note);
            } catch (err) {
                const base = describeBase(repo.released, folder.base);
                throw new RpackError('', formatStatusError(repo.config.name, folder.version, base, assemblyDetail(err), releasedLine(folder)));
            }
            const matches = folder.release !== undefined ? await matchesRelease(ctx.project, repo, folder, draft) : undefined;
            return { draft, lines: formatStatus(draft, releasedLine(folder, matches)) };
        });
    });
}

export type BuildResult = DraftResult & {
    // For a released folder: whether build/update.sql is the one it was released with.
    matches?: boolean;
};

// Writes work/<version>/build/update.sql. A refusal writes nothing and
// returns the draft's refusals as the lines.
export async function buildVersion(ctx: RpackContext, folderName: string): Promise<BuildResult> {
    return withRepo(ctx, async (repo) => {
        const folder = await readFolder(ctx, repo, folderName);
        return withSurface(repo, folder, async (surface) => {
            let draft: ReleaseDraft;
            try {
                const parents = resolveBase(surface.released, folder);
                draft = await draftOn(repo, surface.released, folder.version, parents, await readInputs(ctx.project, folder.name), folder.note);
            } catch (err) {
                throw new RpackError('', [`Error assembling ${folder.version}`, ...assemblyDetail(err)]);
            }
            const labels = repo.keys.labels();
            if (draft.refusals.length > 0) return { draft, lines: formatDraft(draft, labels) };
            await ctx.project.write(workPath(folder.name, `${BUILD_DIR}/${UPDATE_FILE}`), formatUpgrade(draft, labels));
            const lines = [`Generated ${BUILD_DIR}/${UPDATE_FILE}`];
            const result: BuildResult = { draft, lines };
            if (folder.release !== undefined) {
                const update = await sameUpdate(ctx.project, repo, folder, draft);
                result.matches = update && folder.note === folder.release.note;
                const name = folder.release.name;
                const again = `rpack release --force re-releases ${folder.version}`;
                lines.push(result.matches
                    ? `It is the update.sql ${name} was released with`
                    : update
                        ? `It is the update.sql ${name} was released with, but ${VERSION_FILE}'s note differs: ${again}`
                        : `It differs from the update.sql ${name} was released with: ${again}`);
            }
            if (draft.warnings.length > 0) {
                lines.push('', 'warnings');
                for (const warning of draft.warnings) lines.push(`  ${warning}`);
            }
            return result;
        });
    });
}

export type FolderInfo = {
    folder: string;
    version: string;
    // version.json's base, by name.
    base: string[];
    release?: ReleaseInfo;
    // A released folder that still makes its release, so staging can replay
    // the release file instead of signing a new one.
    unchanged: boolean;
};

export async function describeFolder(ctx: RpackContext, folderName: string): Promise<FolderInfo> {
    return withRepo(ctx, async (repo) => {
        const folder = await readFolder(ctx, repo, folderName);
        const info: FolderInfo = {
            folder: folder.name,
            version: folder.version,
            base: folder.base.map((entry) => canonicalName(repo.released, entry)),
            unchanged: false,
        };
        if (folder.release === undefined) return info;
        info.release = folder.release;
        if (!await editedSinceRelease(ctx.project, repo, folder)) {
            info.unchanged = true;
            return info;
        }
        info.unchanged = await withSurface(repo, folder, async (surface) => {
            try {
                const parents = resolveBase(surface.released, folder);
                const draft = await draftOn(repo, surface.released, folder.version, parents, await readInputs(ctx.project, folder.name), folder.note);
                return await matchesRelease(ctx.project, repo, folder, draft);
            } catch {
                return false;
            }
        });
        return info;
    });
}

export type ProducedVersion = { produced: ProducedRelease; draft: ReleaseDraft };

// The folder's release, signed, without writing anything: staging keeps the
// file inside its own host folder. A released folder is signed as its
// re-release would be.
export async function produceVersion(ctx: RpackContext, folderName: string, unlock: Unlock): Promise<ProducedVersion> {
    return withRepo(ctx, async (repo) => {
        const folder = await readFolder(ctx, repo, folderName);
        return withSurface(repo, folder, async (surface) => {
            const parents = resolveBase(surface.released, folder);
            const draft = await draftOn(repo, surface.released, folder.version, parents, await readInputs(ctx.project, folder.name), folder.note);
            if (draft.refusals.length > 0) throw new RpackError('the release is refused', formatDraft(draft, repo.keys.labels()));
            const produced = await produceRelease(draft, surface.released, await unlock(repo.config.key), noteOption(folder.note));
            return { produced, draft };
        });
    });
}

export type ReleaseOptions = {
    // Re-release a released folder.
    force?: boolean;
    // Shown what the release would do before anything is written; false aborts.
    confirm?: (preview: ReleasePreview) => Promise<boolean>;
    // Go ahead without `confirm`, even when a re-release rebuilds releases built on it.
    yes?: boolean;
};

export type ReleaseResult = {
    produced: ProducedRelease;
    draft: ReleaseDraft;
    // A re-release's dependents, made again on it, by version.
    rebuilt: ProducedRelease[];
    lines: string[];
};

async function writeReleased(project: RpackProject, folder: string, inputs: Inputs, upgrade: string): Promise<void> {
    await project.writeFolder(releasedPath(folder), {
        [SOURCE_FILE]: inputs.source,
        [UPGRADE_MANUAL_FILE]: inputs.manual,
        [UPDATE_FILE]: upgrade,
    });
}

// Signs the folder's release, with version.json's note, then writes
// releases/<name>.rpack, work/<version>/.released/ and rpack.json's entry, in
// that order. The working files are never written. A released folder needs
// `force`: see `rerelease`.
export async function releaseVersion(ctx: RpackContext, folderName: string, unlock: Unlock, options: ReleaseOptions = {}): Promise<ReleaseResult> {
    return withRepo(ctx, async (repo) => {
        const folder = await readFolder(ctx, repo, folderName);
        if (folder.release !== undefined) {
            if (options.force !== true) {
                throw new RpackError(`${folder.name} is released as ${folder.release.name}; rpack release --force re-releases it, and rpack new <version> starts another version`);
            }
            return rerelease(ctx, repo, folder, unlock, options);
        }
        const { config, released, keys } = repo;
        const parents = resolveBase(released, folder);
        const inputs = await readInputs(ctx.project, folder.name);
        const draft = await draftOn(repo, released, folder.version, parents, inputs, folder.note);
        if (draft.refusals.length > 0) throw new RpackError('the release is refused', formatDraft(draft, keys.labels()));
        const confirm = options.yes === true ? undefined : options.confirm;
        if (confirm !== undefined && !await confirm({ catalog: config.name, draft, rebuilt: [], repointed: [] })) throw new RpackError('aborted');
        const produced = await produceRelease(draft, released, await unlock(config.key), noteOption(folder.note));

        await ctx.project.write(`${RELEASES_DIR}/${produced.name}.rpack`, produced.text);
        await writeReleased(ctx.project, folder.name, inputs, formatUpgrade(draft, keys.labels()));
        config.released.set(folder.name, produced.name);
        await writeConfig(ctx.project, config);
        return {
            produced, draft, rebuilt: [],
            lines: [`released ${produced.name}: wrote ${RELEASES_DIR}/${produced.name}.rpack and ${releasedPath(folder.name)}/`],
        };
    });
}

// The unreleased folders whose version.json names one of `replaced`, with the
// base by name.
async function unreleasedOn(project: RpackProject, repo: Repo, replaced: Set<string>): Promise<{ folder: string; file: VersionFile }[]> {
    const out: { folder: string; file: VersionFile }[] = [];
    for (const folder of await listFolders(project)) {
        if (repo.config.released.has(folder)) continue;
        const file = await readVersionFile(project, folder);
        const base = file.base.map((entry) => canonicalName(repo.released, entry));
        if (base.some((name) => replaced.has(name))) out.push({ folder, file: { ...file, base } });
    }
    return out;
}

async function readVersionFile(project: RpackProject, folder: string): Promise<VersionFile> {
    const path = workPath(folder, VERSION_FILE);
    return parseVersionFile((await project.read(path))!, path);
}

async function listFolders(project: RpackProject): Promise<string[]> {
    const out: string[] = [];
    for (const name of await project.list(WORK_DIR)) {
        if (folderVersion(name) === undefined) continue;
        if (await project.read(workPath(name, VERSION_FILE)) === undefined) continue;
        out.push(name);
    }
    return out.sort(compareFolders);
}

type Made = { folder: string; produced: ProducedRelease; draft: ReleaseDraft; inputs: Inputs; version: VersionFile };

// Replaces a released folder's release with one made from its working files.
// Everything is made in a scratch replica without the release and the
// releases built on it (its dependents): the new release first, then each
// dependent again, by version, from its own folder's working files and note on
// the remapped base. A dependent without a folder, or one that doesn't build,
// fails the whole re-release. Then, in order: the new release files, each
// folder's .released/, the dependents' and unreleased folders' version.json,
// rpack.json, and last the replaced release files are removed.
async function rerelease(ctx: RpackContext, repo: Repo, folder: Folder, unlock: Unlock, options: ReleaseOptions): Promise<ReleaseResult> {
    const { config, keys } = repo;
    const labels = keys.labels();
    const old = folder.release!;
    return withSurface(repo, folder, async (surface) => {
        const scratch = surface.released;
        const dependents = surface.replaced.slice(1);
        const folders = new Map<string, string>();
        for (const d of dependents) {
            const f = folderOf(config, d.name);
            if (f === undefined) throw new RpackError(`${d.name} is built on ${old.name} and has no work folder, so it can't be re-released on the new ${old.version}`);
            folders.set(d.name, f);
        }

        const inputs = await readInputs(ctx.project, folder.name);
        const draft = await draftOn(repo, scratch, folder.version, resolveBase(scratch, folder), inputs, folder.note);
        if (draft.refusals.length > 0) throw new RpackError('the re-release is refused', formatDraft(draft, labels));
        if (await matchesRelease(ctx.project, repo, folder, draft)) {
            throw new RpackError(`nothing to re-release: ${workPath(folder.name)}/ makes ${old.name} as it is`);
        }

        const repointed = await unreleasedOn(ctx.project, repo, new Set(surface.replaced.map((r) => r.name)));
        const confirm = options.yes === true ? undefined : options.confirm;
        if (dependents.length > 0 && confirm === undefined && options.yes !== true) {
            throw new RpackError(`Re-releasing ${folder.version} re-releases ${dependents.map((d) => d.version).join(', ')} too, built on it: `
                + 'run it on a terminal to review that, or pass --yes');
        }
        const preview: ReleasePreview = { catalog: config.name, draft, replaces: old, rebuilt: [], repointed: repointed.map((u) => u.folder) };
        if (dependents.length === 0 && confirm !== undefined && !await confirm(preview)) throw new RpackError('aborted');

        const key = await unlock(config.key);
        const produced = await produceRelease(draft, scratch, key, noteOption(folder.note));
        const renamed = new Map([[old.name, produced.name]]);
        const made: Made[] = [{ folder: folder.name, produced, draft, inputs, version: rebased(folder, folder.base) }];
        const rebuilt: RebuiltRelease[] = [];
        for (const d of dependents) {
            const dFolder = await readFolder(ctx, repo, folders.get(d.name)!);
            const base = dFolder.base.map((entry) => {
                const name = canonicalName(repo.released, entry);
                return renamed.get(name) ?? name;
            });
            const dInputs = await readInputs(ctx.project, dFolder.name);
            let dDraft: ReleaseDraft;
            try {
                dDraft = await draftOn(repo, scratch, dFolder.version, resolveBase(scratch, { name: dFolder.name, base }), dInputs, dFolder.note);
            } catch (err) {
                throw new RpackError(`${d.version} can't be re-released on the new ${old.version}; nothing was written`, [`Error assembling ${d.version}`, ...assemblyDetail(err)]);
            }
            if (dDraft.refusals.length > 0) {
                throw new RpackError(`${d.version} doesn't build on the new ${old.version}; nothing was written`, formatDraft(dDraft, labels));
            }
            const edited = await editedSinceRelease(ctx.project, repo, dFolder);
            const dProduced = await produceRelease(dDraft, scratch, key, noteOption(dFolder.note));
            renamed.set(d.name, dProduced.name);
            rebuilt.push({ folder: dFolder.name, replaces: d, draft: dDraft, edited });
            made.push({ folder: dFolder.name, produced: dProduced, draft: dDraft, inputs: dInputs, version: rebased(dFolder, base) });
        }
        if (dependents.length > 0) {
            preview.rebuilt = rebuilt;
            if (confirm !== undefined && !await confirm(preview)) throw new RpackError('aborted');
        }

        for (const m of made) await ctx.project.write(`${RELEASES_DIR}/${m.produced.name}.rpack`, m.produced.text);
        for (const m of made) await writeReleased(ctx.project, m.folder, m.inputs, formatUpgrade(m.draft, labels));
        for (const m of made.slice(1)) await writeVersionFile(ctx.project, m.folder, m.version);
        for (const u of repointed) {
            await writeVersionFile(ctx.project, u.folder, { ...u.file, base: u.file.base.map((name) => renamed.get(name) ?? name) });
        }
        for (const m of made) config.released.set(m.folder, m.produced.name);
        await writeConfig(ctx.project, config);
        const removed: string[] = [];
        const kept = new Set(made.map((m) => m.produced.name));
        for (const r of surface.replaced) {
            const stored = repo.stored.get(r.name);
            if (stored === undefined || kept.has(r.name)) continue;
            await ctx.project.remove(stored.path);
            removed.push(stored.path);
        }

        const lines = [`re-released ${produced.name}: wrote ${RELEASES_DIR}/${produced.name}.rpack and ${releasedPath(folder.name)}/`];
        for (const m of made.slice(1)) lines.push(`re-released ${m.produced.name} on it, from ${workPath(m.folder)}/`);
        if (removed.length > 0) lines.push(`removed ${removed.join(', ')}`);
        for (const u of repointed) {
            lines.push(`${workPath(u.folder, VERSION_FILE)} now names the new releases; ${u.folder} was written against the old ${old.version}: check it`);
        }
        return { produced, draft, rebuilt: made.slice(1).map((m) => m.produced), lines };
    });
}

// The releases by version, each with its tag and parents, and the lower ones
// no higher release includes; then the unreleased work folders.
export async function logReleases(ctx: RpackContext): Promise<string[]> {
    return withRepo(ctx, async (repo) => {
        const { config, released } = repo;
        const releases = released.releases();
        const folders = await listFolders(ctx.project);
        const versionOf = (hash: B64Hash) => released.get(hash)?.version ?? hash.slice(0, 8);
        const lines: string[] = [];
        const width = Math.max(4, ...releases.map((r) => r.version.length));
        const changed = new Map<string, string>();
        for (const name of folders) {
            const releaseName = config.released.get(name);
            if (releaseName === undefined) continue;
            const folder = await readFolder(ctx, repo, name);
            if (await editedSinceRelease(ctx.project, repo, folder)) changed.set(releaseName, name);
        }
        for (const release of releases) {
            const after = release.parents.length === 0 ? 'the first release' : `after ${release.parents.map(versionOf).join(' + ')}`;
            const higher = releases.filter((r) => compareSemver(r.version, release.version) > 0);
            let open = false;
            if (higher.length > 0) {
                open = true;
                for (const h of higher) if (await released.isBelow(release.hash, h.hash)) { open = false; break; }
            }
            const edited = changed.get(release.name);
            lines.push(`${release.version.padEnd(width)}  ${release.tag}  ${after}${release.note !== undefined ? `  "${release.note}"` : ''}`
                + `${open ? '  (no higher release includes it)' : ''}${edited !== undefined ? `  (${workPath(edited)}/ changed since release)` : ''}`);
        }
        for (const name of folders) {
            if (config.released.has(name)) continue;
            const { base } = await readVersionFile(ctx.project, name);
            const after = base.length === 0 ? 'the first release' : `after ${base.map((entry) => tryResolve(released, entry)?.version ?? entry).join(' + ')}`;
            lines.push(`${'work'.padEnd(width)}  ${name}  ${after}`);
        }
        if (lines.length === 0) lines.push('No releases or work folders yet; rpack new 1.0.0 starts one');
        return lines;
    });
}
