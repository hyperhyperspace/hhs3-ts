// A catalog repository's files, through an injected interface: rdb_tools
// implements it over the file system, tests in memory.
//
//   rpack.json                 name, key, and the released folders
//   releases/<name>.rpack      the signed releases
//   work/<version>/            one folder per version:
//     version.json             the releases it starts from, and its note
//     target-catalog.sql       the desired state
//     upgrade-manual.sql       ALTER SCHEMA steps the diff cannot infer
//     test-data.sql            rows staging inserts after deploying this release
//     staging.json             how staging hosts project and sync
//     .released/               the signed inputs and update.sql, as released
//     build/  stage/           generated, gitignored
//
// A folder is named by its version (2.0.4), or by its version and a suffix
// (2.0.4-b3c1) when two folders prepare the same version.

import { compareSemver, isValidSemver } from "@hyper-hyper-space/hhs3_rdb";

export const CONFIG_FILE = 'rpack.json';
export const SOURCE_FILE = 'target-catalog.sql';
export const UPGRADE_MANUAL_FILE = 'upgrade-manual.sql';
export const UPDATE_FILE = 'update.sql';
export const TEST_DATA_FILE = 'test-data.sql';
export const STAGING_FILE = 'staging.json';
export const VERSION_FILE = 'version.json';
export const RELEASES_DIR = 'releases';
export const WORK_DIR = 'work';
// Inside a work folder.
export const RELEASED_DIR = '.released';
export const BUILD_DIR = 'build';
export const STAGE_DIR = 'stage';

// The generated folders, and a stage being built or swapped in.
export const GITIGNORE = `${WORK_DIR}/*/${BUILD_DIR}/\n${WORK_DIR}/*/${STAGE_DIR}/\n${WORK_DIR}/*/.${STAGE_DIR}.*/\n`;

export type RpackConfig = {
    name: string;
    key: string;
    // Work folder -> the release made from it (editor-2.0.4-8b21d0aa).
    released: Map<string, string>;
};

export class ConfigError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ConfigError';
    }
}

const FOLDER_NAME = /^(\d+\.\d+\.\d+)(-[0-9A-Za-z][0-9A-Za-z._-]*)?$/;

// The version a work folder prepares, or undefined when the name isn't one.
export function folderVersion(folder: string): string | undefined {
    const m = FOLDER_NAME.exec(folder);
    return m !== null && isValidSemver(m[1]) ? m[1] : undefined;
}

export function compareFolders(a: string, b: string): number {
    return compareSemver(folderVersion(a)!, folderVersion(b)!) || (a < b ? -1 : a > b ? 1 : 0);
}

export function workPath(folder: string, file?: string): string {
    return file === undefined ? `${WORK_DIR}/${folder}` : `${WORK_DIR}/${folder}/${file}`;
}

export function releasedPath(folder: string, file?: string): string {
    return workPath(folder, file === undefined ? RELEASED_DIR : `${RELEASED_DIR}/${file}`);
}

export function parseRpackConfig(text: string): RpackConfig {
    let value: unknown;
    try {
        value = JSON.parse(text);
    } catch (err) {
        throw new ConfigError(`${CONFIG_FILE} is not valid JSON: ${err instanceof Error ? err.message : String(err)}`);
    }
    if (!isObject(value)) throw new ConfigError(`${CONFIG_FILE} must be a JSON object`);
    const stringField = (field: string): string => {
        const v = value[field];
        if (typeof v !== 'string' || v.length === 0) throw new ConfigError(`${CONFIG_FILE}: '${field}' must be a non-empty string`);
        return v;
    };
    const config: RpackConfig = { name: stringField('name'), key: stringField('key'), released: new Map() };
    const released = value['released'] ?? {};
    if (!isObject(released)) throw new ConfigError(`${CONFIG_FILE}: 'released' must map work folders to release names`);
    for (const [folder, name] of Object.entries(released)) {
        if (folderVersion(folder) === undefined) {
            throw new ConfigError(`${CONFIG_FILE}: '${folder}' in 'released' is not a work folder name such as '1.2.0'`);
        }
        if (typeof name !== 'string' || name.length === 0) {
            throw new ConfigError(`${CONFIG_FILE}: 'released.${folder}' must be a release name`);
        }
        config.released.set(folder, name);
    }
    return config;
}

// Released folders in version order, one per line.
export function formatRpackConfig(config: RpackConfig): string {
    const folders = [...config.released.keys()].sort(compareFolders);
    const released = folders.length === 0
        ? '{}'
        : `{\n${folders.map((f) => `    ${JSON.stringify(f)}: ${JSON.stringify(config.released.get(f))}`).join(',\n')}\n  }`;
    return `{\n  "name": ${JSON.stringify(config.name)},\n  "key": ${JSON.stringify(config.key)},\n  "released": ${released}\n}\n`;
}

// A work folder's version.json: the releases it starts from ([] for the first
// release), and the note its release carries.
export type VersionFile = { base: string[]; note?: string };

const VERSION_FIELDS = ['base', 'note'];

export function parseVersionFile(text: string, path: string): VersionFile {
    let value: unknown;
    try {
        value = JSON.parse(text);
    } catch (err) {
        throw new ConfigError(`${path} is not valid JSON: ${err instanceof Error ? err.message : String(err)}`);
    }
    if (!isObject(value)) throw new ConfigError(`${path} must be a JSON object`);
    for (const field of Object.keys(value)) {
        if (!VERSION_FIELDS.includes(field)) throw new ConfigError(`${path}: unknown field '${field}'; it holds ${VERSION_FIELDS.join(' and ')}`);
    }
    const base = value['base'];
    if (!Array.isArray(base) || !base.every((b) => typeof b === 'string' && b.length > 0)) {
        throw new ConfigError(`${path}: 'base' must be a list of release names, [] for the first release`);
    }
    const file: VersionFile = { base: [...base] as string[] };
    const note = value['note'];
    if (note !== undefined) {
        if (typeof note !== 'string' || note.length === 0) throw new ConfigError(`${path}: 'note' must be a non-empty string`);
        file.note = note;
    }
    return file;
}

// One base entry per line.
export function formatVersionFile(file: VersionFile): string {
    const base = file.base.length === 0 ? '[]' : `[\n${file.base.map((name) => `    ${JSON.stringify(name)}`).join(',\n')}\n  ]`;
    const note = file.note !== undefined ? `,\n  "note": ${JSON.stringify(file.note)}` : '';
    return `{\n  "base": ${base}${note}\n}\n`;
}

function isObject(value: unknown): value is { [key: string]: unknown } {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export interface RpackProject {
    // A file's text, or undefined when it doesn't exist. Paths are relative to
    // the repository, with '/' separators.
    read(path: string): Promise<string | undefined>;
    // The names in a folder, or [] when it doesn't exist.
    list(dir: string): Promise<string[]>;
    // Replaces a file whole (on a file system: written aside, then renamed).
    write(path: string, text: string): Promise<void>;
    // Writes a folder whole, replacing any folder of that name.
    writeFolder(path: string, files: { [name: string]: string }): Promise<void>;
    // Removes a file or a folder. A missing path is not an error.
    remove(path: string): Promise<void>;
}

export class MemoryProject implements RpackProject {
    readonly files = new Map<string, string>();

    constructor(files: { [path: string]: string } = {}) {
        for (const [path, text] of Object.entries(files)) this.files.set(path, text);
    }

    async read(path: string): Promise<string | undefined> {
        return this.files.get(path);
    }

    async list(dir: string): Promise<string[]> {
        const prefix = `${dir}/`;
        const names = new Set<string>();
        for (const path of this.files.keys()) {
            if (path.startsWith(prefix)) names.add(path.slice(prefix.length).split('/')[0]);
        }
        return [...names].sort();
    }

    async write(path: string, text: string): Promise<void> {
        this.files.set(path, text);
    }

    async writeFolder(path: string, files: { [name: string]: string }): Promise<void> {
        for (const existing of [...this.files.keys()]) if (existing.startsWith(`${path}/`)) this.files.delete(existing);
        for (const [name, text] of Object.entries(files)) this.files.set(`${path}/${name}`, text);
    }

    async remove(path: string): Promise<void> {
        const prefix = `${path}/`;
        for (const existing of [...this.files.keys()]) {
            if (existing === path || existing.startsWith(prefix)) this.files.delete(existing);
        }
    }
}
