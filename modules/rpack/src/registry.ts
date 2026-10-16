// The .rpack registry: publishing schema and catalog releases, importing
// schema releases into a catalog repository's imports/ folder, and resolving
// catalog releases for rhost.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { compareSemver, isValidSchemaName } from "@hyper-hyper-space/hhs3_rdb";
import { parseScript, renderIdent, renderLiteral } from "@hyper-hyper-space/hhs3_rdb_lang";

import {
    isSchemaReleaseFile, parseAnyReleaseFile, parseSchemaReleaseFile, releaseFileName, releaseName, releaseTag, serializeReleaseFile,
    type AnyReleaseFile, type ReleaseFile, type SchemaReleaseFile,
} from "./format.js";
import {
    CONFIG_FILE, IMPORTS_DIR, RELEASES_DIR, SOURCE_FILE, folderVersion, formatRpackConfig, parseRpackConfig, workPath,
    type RpackConfig, type RpackProject,
} from "./project.js";
import { Released } from "./released.js";
import { verifyRelease } from "./verify.js";

export function registryPath(name: string): string {
    if (!isValidSchemaName(name)) {
        throw new Error(`invalid package name '${name}'`);
    }
    return name.split(':').join('/');
}

type StoredEntry = {
    path: string;
    fileName: string;
    text: string;
    file: AnyReleaseFile;
    releaseName: string;
};

async function readDirectoryReleases(project: RpackProject, dir: string): Promise<StoredEntry[]> {
    const names = (await project.list(dir)).filter((n) => n.endsWith('.rpack')).sort();
    const out: StoredEntry[] = [];
    for (const fileName of names) {
        const path = `${dir}/${fileName}`;
        const text = await project.read(path);
        if (text === undefined) continue;
        const file = parseAnyReleaseFile(text);
        const m = file.manifest;
        out.push({
            path,
            fileName,
            text,
            file,
            releaseName: releaseName(m.name, m.version, m.release),
        });
    }
    return out;
}

function matchSelector(entries: StoredEntry[], pkgName: string, selector: string): StoredEntry {
    let text = selector.endsWith('.rpack') ? selector.slice(0, -'.rpack'.length) : selector;
    const slash = text.lastIndexOf('/');
    if (slash !== -1) text = text.slice(slash + 1);
    if (text.startsWith('#')) {
        const prefix = text.slice(1);
        const matches = entries.filter((e) => e.file.manifest.release.startsWith(prefix));
        if (matches.length === 1) return matches[0];
        if (matches.length === 0) throw new Error(`no release of '${pkgName}' matches '${selector}'`);
        throw new Error(`'${selector}' matches several releases of '${pkgName}'`);
    }
    const sanitizedPrefix = `${pkgName.replace(/:/g, '_')}-`;
    if (text.startsWith(sanitizedPrefix)) text = text.slice(sanitizedPrefix.length);
    else if (text.startsWith(`${pkgName}-`)) text = text.slice(pkgName.length + 1);

    const dash = text.indexOf('-');
    const wantedVer = dash === -1 ? text : text.slice(0, dash);
    const tagPrefix = dash === -1 ? '' : text.slice(dash + 1);
    const matches = entries.filter((e) =>
        e.file.manifest.version === wantedVer
        && releaseTag(e.file.manifest.release).startsWith(tagPrefix));
    if (matches.length === 1) return matches[0];
    if (matches.length === 0) throw new Error(`no release of '${pkgName}' matches '${selector}'`);
    throw new Error(`'${selector}' matches several releases of '${pkgName}': ${matches.map((m) => `${m.file.manifest.version}-${releaseTag(m.file.manifest.release)}`).join(', ')}; add the tag`);
}

function pickHighestVersion(entries: StoredEntry[], pkgName: string): StoredEntry {
    const sorted = [...entries].sort((a, b) =>
        compareSemver(a.file.manifest.version, b.file.manifest.version)
        || (a.file.manifest.release < b.file.manifest.release ? -1 : a.file.manifest.release > b.file.manifest.release ? 1 : 0));
    const highest = sorted[sorted.length - 1]!;
    const sameHighest = sorted.filter((e) => e.file.manifest.version === highest.file.manifest.version);
    if (sameHighest.length > 1) {
        throw new Error(`several releases of '${pkgName}' have version ${highest.file.manifest.version}: ${sameHighest.map((m) => `${m.file.manifest.version}-${releaseTag(m.file.manifest.release)}`).join(', ')}; specify the version-tag`);
    }
    return highest;
}

// Publishes one or all released versions of `project` into `registry`.
export async function publishToRegistry(
    project: RpackProject,
    registry: RpackProject,
    selector?: string,
): Promise<string[]> {
    const configText = await project.read(CONFIG_FILE);
    if (configText === undefined) {
        throw new Error(`There is no ${CONFIG_FILE} here; start a repository with rpack init`);
    }
    const config = parseRpackConfig(configText);
    const stored = await readDirectoryReleases(project, RELEASES_DIR);
    if (stored.length === 0) {
        throw new Error(`No releases in ${RELEASES_DIR}/ to publish`);
    }

    let toPublish: StoredEntry[];
    if (selector !== undefined) {
        toPublish = [matchSelector(stored, config.name, selector)];
    } else {
        const releasedNames = new Set(config.released.values());
        toPublish = stored.filter((s) => releasedNames.has(s.releaseName));
        if (toPublish.length === 0) toPublish = stored;
    }

    const expectedKind = config.kind === 'schema' ? 'schema' : 'catalog';
    for (const entry of toPublish) {
        const entryKind = isSchemaReleaseFile(entry.file) ? 'schema' : 'catalog';
        if (entryKind !== expectedKind) {
            throw new Error(`${entry.path} is a ${entryKind} release, but ${CONFIG_FILE} is a ${expectedKind} repository`);
        }
        const report = await verifyRelease(entry.file);
        if (!report.ok) {
            throw new Error(`${entry.path} failed verification: ${report.problems.join('; ')}`);
        }
    }

    const regDir = registryPath(config.name);
    const existing = await readDirectoryReleases(registry, regDir);
    for (const ex of existing) {
        const exKind = isSchemaReleaseFile(ex.file) ? 'schema' : 'catalog';
        if (exKind !== expectedKind) {
            throw new Error(`'${config.name}' in the registry is a ${exKind}, not a ${expectedKind}`);
        }
        if (!isSchemaReleaseFile(ex.file) && !isSchemaReleaseFile(toPublish[0].file)) {
            if (ex.file.manifest.catalog !== toPublish[0].file.manifest.catalog) {
                throw new Error(`'${config.name}' in the registry has catalog #${ex.file.manifest.catalog.slice(0, 8)}, which differs from #${toPublish[0].file.manifest.catalog.slice(0, 8)}`);
            }
        }
    }

    const lines: string[] = [];
    for (const entry of toPublish) {
        const m = entry.file.manifest;
        const clash = existing.find((e) => e.file.manifest.version === m.version && e.file.manifest.release !== m.release);
        if (clash !== undefined) {
            throw new Error(`'${config.name}' ${m.version} is already in the registry with a different hash (#${releaseTag(clash.file.manifest.release)})`);
        }
        const outName = releaseFileName(m.name, m.version, m.release);
        const dest = `${regDir}/${outName}`;
        await registry.write(dest, serializeReleaseFile(entry.file));
        lines.push(`published ${entry.releaseName} to ${dest}`);
    }
    return lines;
}

// Imports a schema release from `registry` into `project`'s `imports/`, locks
// its step hash in `rpack.json`, and inserts or updates `IMPORT SCHEMA` in
// `work/<folderName>/target-catalog.sql`.
export async function importSchemaFromRegistry(
    project: RpackProject,
    registry: RpackProject,
    folderName: string,
    schemaName: string,
    versionSelector?: string,
): Promise<{ file: SchemaReleaseFile; lines: string[] }> {
    const configText = await project.read(CONFIG_FILE);
    if (configText === undefined) {
        throw new Error(`There is no ${CONFIG_FILE} here; start a catalog repository with rpack init`);
    }
    const config = parseRpackConfig(configText);
    if (config.kind === 'schema') {
        throw new Error('rpack import runs in a catalog repository, not a schema repository');
    }
    if (folderVersion(folderName) === undefined) {
        throw new Error(`${workPath(folderName)}/ is not a version folder`);
    }
    const sourcePath = workPath(folderName, SOURCE_FILE);
    const sourceText = await project.read(sourcePath);
    if (sourceText === undefined) {
        throw new Error(`There is no ${sourcePath}; start ${workPath(folderName)}/ with rpack new`);
    }

    const regDir = registryPath(schemaName);
    const entries = await readDirectoryReleases(registry, regDir);
    if (entries.length === 0) {
        throw new Error(`schema '${schemaName}' is not in the registry`);
    }
    for (const entry of entries) {
        if (!isSchemaReleaseFile(entry.file)) {
            throw new Error(`'${schemaName}' is a catalog, not a schema`);
        }
    }

    const chosen = versionSelector !== undefined
        ? matchSelector(entries, schemaName, versionSelector)
        : pickHighestVersion(entries, schemaName);
    const schemaFile = chosen.file as SchemaReleaseFile;
    const report = await verifyRelease(schemaFile);
    if (!report.ok) {
        throw new Error(`schema '${schemaName}' ${schemaFile.manifest.version} in the registry is invalid: ${report.problems.join('; ')}`);
    }

    const importFileName = releaseFileName(schemaFile.manifest.name, schemaFile.manifest.version, schemaFile.manifest.release);
    const importPath = `${IMPORTS_DIR}/${importFileName}`;
    await project.write(importPath, serializeReleaseFile(schemaFile));

    if (config.imports === undefined) config.imports = {};
    if (config.imports[schemaName] === undefined) config.imports[schemaName] = {};
    const existingLock = config.imports[schemaName][schemaFile.manifest.version];
    if (existingLock !== undefined && existingLock !== schemaFile.manifest.release) {
        throw new Error(`${CONFIG_FILE} already locks ${schemaName} ${schemaFile.manifest.version} at #${releaseTag(existingLock)}, not #${releaseTag(schemaFile.manifest.release)}`);
    }
    config.imports[schemaName][schemaFile.manifest.version] = schemaFile.manifest.release;
    await project.write(CONFIG_FILE, formatRpackConfig(config));

    const updatedSource = upsertImportStatement(sourceText, schemaName, schemaFile.manifest.version);
    if (updatedSource !== sourceText) {
        await project.write(sourcePath, updatedSource);
    }

    return {
        file: schemaFile,
        lines: [
            `imported ${schemaName} ${schemaFile.manifest.version} (${releaseTag(schemaFile.manifest.release)}) into ${importPath} and ${sourcePath}`,
        ],
    };
}

function upsertImportStatement(text: string, schemaName: string, version: string): string {
    const stmtText = `IMPORT SCHEMA ${renderIdent(schemaName)} VERSION ${renderLiteral(version)};`;
    const parsed = parseScript(text, { catalogVersionOptional: true });
    if (parsed.ok) {
        for (const stmt of parsed.value.statements) {
            if (stmt.kind === 'import-schema' && stmt.name === schemaName) {
                return text.slice(0, stmt.versionSpan.start) + `VERSION ${renderLiteral(version)}` + text.slice(stmt.versionSpan.end);
            }
        }
        const firstSchemaOrCatalog = parsed.value.statements.find(
            (s) => s.kind === 'import-schema' || s.kind === 'create-schema' || s.kind === 'create-catalog',
        );
        if (firstSchemaOrCatalog !== undefined) {
            const lineStart = text.lastIndexOf('\n', firstSchemaOrCatalog.span.start - 1) + 1;
            return text.slice(0, lineStart) + `${stmtText}\n\n` + text.slice(lineStart);
        }
    }
    return `${stmtText}\n\n${text}`;
}

// Reads `imports/*.rpack` in a catalog repository, indexed by manifest (`name`
// and `name@version`), and verifies every entry against `config.imports`.
export async function readLockedImports(
    project: RpackProject,
    config: RpackConfig,
): Promise<Map<string, SchemaReleaseFile>> {
    const out = new Map<string, SchemaReleaseFile>();
    const names = (await project.list(IMPORTS_DIR)).filter((n) => n.endsWith('.rpack')).sort();
    const bySchema = new Map<string, SchemaReleaseFile[]>();

    for (const fileName of names) {
        const path = `${IMPORTS_DIR}/${fileName}`;
        const text = await project.read(path);
        if (text === undefined) continue;
        const file = parseSchemaReleaseFile(text);
        const { name, version, release } = file.manifest;
        const lockedHash = config.imports?.[name]?.[version];
        if (lockedHash === undefined) {
            throw new Error(`${path}: ${name} ${version} is not locked in ${CONFIG_FILE}'s imports`);
        }
        if (lockedHash !== release) {
            throw new Error(`${path}: step hash #${releaseTag(release)} does not match ${CONFIG_FILE}'s lock #${releaseTag(lockedHash)} for ${name} ${version}`);
        }
        const report = await verifyRelease(file);
        if (!report.ok) {
            throw new Error(`${path}: ${report.problems.join('; ')}`);
        }
        out.set(`${name}@${version}`, file);
        const list = bySchema.get(name) ?? [];
        list.push(file);
        bySchema.set(name, list);
    }

    for (const [name, list] of bySchema) {
        const sorted = [...list].sort((a, b) => compareSemver(a.manifest.version, b.manifest.version));
        out.set(name, sorted[sorted.length - 1]!);
    }

    for (const [schema, versions] of Object.entries(config.imports ?? {})) {
        for (const [ver, expectedHash] of Object.entries(versions)) {
            const loaded = out.get(`${schema}@${ver}`);
            if (loaded === undefined) {
                throw new Error(`${CONFIG_FILE} locks ${schema} ${ver} (#${releaseTag(expectedHash)}), which is missing from ${IMPORTS_DIR}/`);
            }
        }
    }

    return out;
}

export type ResolvedRegistryCatalog = {
    file: ReleaseFile;
    fileName: string;
    text: string;
};

// Resolves a catalog release in `registry` by name (and optional `version`
// selector). When `catalogId` and `afterRelease` are given (`rhost update --pull`),
// only releases of `catalogId` that strictly descend from `afterRelease` are
// considered, returning `undefined` when the registry has no newer descendant.
export async function resolveCatalogInRegistry(
    registry: RpackProject,
    catalogName: string,
    options: { version?: string; catalogId?: B64Hash; afterRelease?: B64Hash; maxMajor?: number } = {},
): Promise<ResolvedRegistryCatalog | undefined> {
    const regDir = registryPath(catalogName);
    const entries = await readDirectoryReleases(registry, regDir);
    if (entries.length === 0) {
        if (options.afterRelease !== undefined) return undefined;
        throw new Error(`catalog '${catalogName}' is not in the registry`);
    }
    for (const entry of entries) {
        if (isSchemaReleaseFile(entry.file)) {
            throw new Error(`'${catalogName}' is a schema, not a catalog`);
        }
    }

    let candidates = entries;
    if (options.catalogId !== undefined) {
        candidates = candidates.filter((e) => !isSchemaReleaseFile(e.file) && e.file.manifest.catalog === options.catalogId);
    }
    if (options.maxMajor !== undefined) {
        candidates = candidates.filter((e) => {
            const major = Number(e.file.manifest.version.split('.')[0]);
            return Number.isSafeInteger(major) && major <= options.maxMajor!;
        });
    }

    if (options.afterRelease !== undefined) {
        if (candidates.length === 0) return undefined;
        const released = await Released.open(candidates.map((c) => c.file as ReleaseFile), catalogName);
        try {
            if (released.get(options.afterRelease) === undefined) return undefined;
            const descendants = new Set((await released.descendants(options.afterRelease)).map((d) => d.hash));
            candidates = candidates.filter((c) => descendants.has(c.file.manifest.release));
            if (candidates.length === 0) return undefined;
        } finally {
            await released.close();
        }
    }

    const chosen = options.version !== undefined
        ? matchSelector(candidates, catalogName, options.version)
        : pickHighestVersion(candidates, catalogName);
    const file = chosen.file as ReleaseFile;
    const report = await verifyRelease(file);
    if (!report.ok) {
        throw new Error(`catalog '${catalogName}' ${file.manifest.version} in the registry is invalid: ${report.problems.join('; ')}`);
    }
    return {
        file,
        fileName: releaseFileName(file.manifest.name, file.manifest.version, file.manifest.release),
        text: serializeReleaseFile(file),
    };
}
