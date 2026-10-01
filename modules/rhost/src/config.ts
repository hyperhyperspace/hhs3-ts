// app.json (what the app ships, the same on every device) and host.json (what
// the user set up for one host: its database, key and network, plus the
// sections it overrides).

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import { validateIndexSpec, type IndexDecl, type IndexSpec } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { parseAllowSource, type AllowSource, type SyncScope } from "./sync.js";

export type AutoDeploy = 'minor' | 'major' | 'none';

// How a host meets its peers. Without `listen`, a free port is picked when the
// host starts.
export type SyncConfig = {
    scope: SyncScope;
    tracker?: string;
    trackerKey?: string;
    listen?: string;
};

// The key a host signs with. `label` is its name in the keystore when the
// host was set up; the host unlocks it by `keyId`, so relabeling it changes
// nothing. `publicKey` is base64 of the serialized key, so the host is known
// without the keystore. `passphrase` is 'prompt', 'env:<VAR>', or absent for a
// key stored without one.
export type HostKeyConfig = { label: string; keyId: string; publicKey: string; passphrase?: string };

// A FILES member mounted as a folder; `path` resolves in the host folder.
export type FilesMountConfig = { name: string; path: string };

// `path` resolves in the host folder. `indexes` and `indexPub` are the
// projection index spec, shared by every host. Omitting both leaves an already
// installed spec alone. `files` mounts FILES members as folders.
export type ProjectionConfig = { path: string; indexPub?: boolean; indexes?: IndexDecl[]; files?: FilesMountConfig[] };

// The spec to reconcile, or undefined when the config names none. `indexPub`
// alone (true) is a spec whose only indexes are the generated `pub__` ones.
export function projectionIndexSpec(projection: ProjectionConfig): IndexSpec | undefined {
    if (projection.indexes === undefined && projection.indexPub !== true) return undefined;
    const spec: IndexSpec = { indexes: projection.indexes ?? [] };
    if (projection.indexPub === true) spec.indexPub = true;
    return spec;
}

export type ParamsConfig = { [name: string]: json.Literal };

// `keystore` is where the platform keeps keys, when not in its default place
// (on Node, a path relative to the app folder). `allow` names the catalog
// columns whose keys may sync with this app's hosts; empty or absent is anyone.
export type AppConfig = {
    releases: string;
    keystore?: string;
    params?: ParamsConfig;
    autoDeploy?: AutoDeploy;
    allow?: string[];
    projection: ProjectionConfig;
};

export type HostRecord = {
    database: B64Hash;
    catalog: string;
    created: boolean;
    key: HostKeyConfig;
    sync: SyncConfig;
    params?: ParamsConfig;
    autoDeploy?: AutoDeploy;
    projection?: ProjectionConfig;
};

export type EffectiveConfig = {
    releases: string;
    key: HostKeyConfig;
    params: ParamsConfig;
    autoDeploy: AutoDeploy;
    sync: SyncConfig;
    allow: AllowSource[];
    projection: ProjectionConfig;
};

export class ConfigError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ConfigError';
    }
}

type Obj = { [key: string]: unknown };

function isObject(value: unknown): value is Obj {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

class Checker {
    constructor(private readonly source: string) {}

    fail(field: string, problem: string): never {
        throw new ConfigError(`${this.source}: ${field} ${problem}`);
    }

    object(value: unknown, field: string, allowed: string[]): Obj {
        if (value === undefined && field !== '') this.fail(field, 'is missing');
        if (!isObject(value)) this.fail(field === '' ? 'the file' : field, 'must be an object');
        for (const key of Object.keys(value)) {
            if (!allowed.includes(key)) this.fail(field === '' ? `'${key}'` : `${field}.${key}`, 'is not a known field');
        }
        return value;
    }

    string(value: unknown, field: string): string {
        if (value === undefined) this.fail(field, 'is missing');
        if (typeof value !== 'string' || value.length === 0) this.fail(field, 'must be a non-empty string');
        return value;
    }

    optionalString(value: unknown, field: string): string | undefined {
        return value === undefined ? undefined : this.string(value, field);
    }
}

const PASSPHRASE_SOURCE = /^env:[A-Za-z_][A-Za-z0-9_]*$/;

function checkKey(c: Checker, value: unknown, field: string): HostKeyConfig {
    const o = c.object(value, field, ['label', 'keyId', 'publicKey', 'passphrase']);
    const key: HostKeyConfig = {
        label: c.string(o['label'], `${field}.label`),
        keyId: c.string(o['keyId'], `${field}.keyId`),
        publicKey: c.string(o['publicKey'], `${field}.publicKey`),
    };
    const passphrase = c.optionalString(o['passphrase'], `${field}.passphrase`);
    if (passphrase !== undefined) {
        if (passphrase !== 'prompt' && !PASSPHRASE_SOURCE.test(passphrase)) {
            c.fail(`${field}.passphrase`, `must be 'prompt' or 'env:<VAR>', got '${passphrase}'`);
        }
        key.passphrase = passphrase;
    }
    return key;
}

function checkParams(c: Checker, value: unknown, field: string): ParamsConfig {
    const o = c.object(value, field, Object.keys(isObject(value) ? value : {}));
    const out: ParamsConfig = {};
    for (const [name, entry] of Object.entries(o)) out[name] = checkLiteral(c, entry, `${field}.${name}`);
    return out;
}

function checkAutoDeploy(c: Checker, value: unknown, field: string): AutoDeploy {
    if (value !== 'minor' && value !== 'major' && value !== 'none') {
        c.fail(field, `must be 'minor', 'major' or 'none', got ${JSON.stringify(value)}`);
    }
    return value;
}

function checkSync(c: Checker, value: unknown, field: string): SyncConfig {
    const o = c.object(value, field, ['scope', 'tracker', 'trackerKey', 'listen']);
    const scope = o['scope'];
    if (scope === undefined) c.fail(`${field}.scope`, 'is missing');
    if (scope !== 'internet' && scope !== 'localhost') {
        c.fail(`${field}.scope`, `must be 'internet' or 'localhost', got ${JSON.stringify(scope)}`);
    }
    const out: SyncConfig = { scope };
    const tracker = c.optionalString(o['tracker'], `${field}.tracker`);
    const trackerKey = c.optionalString(o['trackerKey'], `${field}.trackerKey`);
    const listen = c.optionalString(o['listen'], `${field}.listen`);
    if (tracker !== undefined) out.tracker = tracker;
    if (trackerKey !== undefined) out.trackerKey = trackerKey;
    if (listen !== undefined) {
        if (listenUrl(listen) === undefined) c.fail(`${field}.listen`, `must be an address such as ws://0.0.0.0:7400, got '${listen}'`);
        out.listen = listen;
    }
    return out;
}

function listenUrl(listen: string): URL | undefined {
    try {
        return new URL(listen);
    } catch {
        return undefined;
    }
}

// The port a `listen` address names, if it names one.
export function listenPort(listen: string | undefined): string | undefined {
    if (listen === undefined) return undefined;
    const port = listenUrl(listen)?.port;
    return port === undefined || port === '' ? undefined : port;
}

function checkAllow(c: Checker, value: unknown, field: string): string[] {
    if (!Array.isArray(value)) c.fail(field, 'must be an array of strings');
    return value.map((entry, i) => {
        const text = c.string(entry, `${field}[${i}]`);
        try {
            parseAllowSource(text);
        } catch (e) {
            c.fail(`${field}[${i}]`, (e as Error).message);
        }
        return text;
    });
}

function checkProjection(c: Checker, value: unknown, field: string): ProjectionConfig {
    const o = c.object(value, field, ['path', 'indexes', 'indexPub', 'files']);
    const out: ProjectionConfig = { path: checkHostPath(c, o['path'], `${field}.path`) };
    if (o['indexPub'] !== undefined) {
        if (typeof o['indexPub'] !== 'boolean') c.fail(`${field}.indexPub`, 'must be true or false');
        out.indexPub = o['indexPub'];
    }
    if (o['indexes'] !== undefined) out.indexes = checkIndexDecls(c, o['indexes'], `${field}.indexes`);
    const spec = projectionIndexSpec(out);
    if (spec !== undefined) {
        const reason = validateIndexSpec(spec);
        if (reason !== undefined) c.fail(field, reason);
    }
    if (o['files'] !== undefined) out.files = checkFilesMounts(c, o['files'], `${field}.files`, out.path);
    return out;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Relative, '/'-separated, no '.' or '..' segments.
function relativePathReason(path: string): string | undefined {
    if (path.startsWith('/') || path.includes('\\') || /^[A-Za-z]:/.test(path)) return 'must be a relative path';
    if (path.split('/').some((s) => s === '' || s === '.' || s === '..')) return "must not have empty, '.' or '..' segments";
    return undefined;
}

function nests(a: string, b: string): boolean {
    return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
}

// What rhost keeps in every host folder: its record, its replica, and its
// lock, socket and log.
const RESERVED_HOST_PATHS = ['host.json', 'rdb', 'run'];

// A path in the host folder: relative, and clear of the reserved ones.
function checkHostPath(c: Checker, value: unknown, field: string): string {
    const path = c.string(value, field);
    const reason = relativePathReason(path);
    if (reason !== undefined) c.fail(field, `${reason}, got '${path}'`);
    const reserved = RESERVED_HOST_PATHS.find((r) => nests(path, r));
    if (reserved !== undefined) c.fail(field, `'${path}' overlaps '${reserved}', which rhost keeps for itself`);
    return path;
}

// Unique identifier names; unique host paths, none inside another, and none
// at or around the projection's own path.
function checkFilesMounts(c: Checker, value: unknown, field: string, projectionPath: string): FilesMountConfig[] {
    if (!Array.isArray(value)) c.fail(field, 'must be an array');
    const out: FilesMountConfig[] = [];
    value.forEach((entry, i) => {
        const at = `${field}[${i}]`;
        const o = c.object(entry, at, ['name', 'path']);
        const name = c.string(o['name'], `${at}.name`);
        if (!IDENTIFIER.test(name)) c.fail(`${at}.name`, `must be an identifier, got '${name}'`);
        const path = checkHostPath(c, o['path'], `${at}.path`);
        if (nests(path, projectionPath)) c.fail(`${at}.path`, `'${path}' overlaps the projection path '${projectionPath}'`);
        for (const [j, other] of out.entries()) {
            if (other.name === name) c.fail(`${at}.name`, `'${name}' is already mounted by ${field}[${j}]`);
            if (nests(path, other.path)) c.fail(`${at}.path`, `'${path}' overlaps ${field}[${j}].path '${other.path}'`);
        }
        out.push({ name, path });
    });
    return out;
}

function checkIndexDecls(c: Checker, value: unknown, field: string): IndexDecl[] {
    if (!Array.isArray(value)) c.fail(field, 'must be an array');
    return value.map((entry, i) => {
        const at = `${field}[${i}]`;
        const o = c.object(entry, at, ['name', 'group', 'table', 'columns', 'options']);
        const decl: IndexDecl = {
            name: c.string(o['name'], `${at}.name`),
            group: c.string(o['group'], `${at}.group`),
            table: c.string(o['table'], `${at}.table`),
            columns: checkColumnNames(c, o['columns'], `${at}.columns`),
        };
        if (o['options'] !== undefined) decl.options = checkLiteral(c, o['options'], `${at}.options`);
        return decl;
    });
}

function checkColumnNames(c: Checker, value: unknown, field: string): string[] {
    if (value === undefined) c.fail(field, 'is missing');
    if (!Array.isArray(value)) c.fail(field, 'must be an array of column names');
    return value.map((entry, i) => c.string(entry, `${field}[${i}]`));
}

function checkLiteral(c: Checker, value: unknown, field: string): json.Literal {
    if (typeof value === 'boolean' || typeof value === 'string') return value;
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) c.fail(field, 'must be a JSON value');
        return value;
    }
    if (Array.isArray(value)) return value.map((entry, i) => checkLiteral(c, entry, `${field}[${i}]`));
    if (isObject(value)) {
        const out: { [key: string]: json.Literal } = {};
        for (const [key, entry] of Object.entries(value)) out[key] = checkLiteral(c, entry, `${field}.${key}`);
        return out;
    }
    c.fail(field, 'must be a JSON value');
}

export function parseAppConfig(value: unknown, source = 'app.json'): AppConfig {
    const c = new Checker(source);
    const o = c.object(value, '', ['releases', 'keystore', 'params', 'autoDeploy', 'allow', 'projection']);
    const config: AppConfig = {
        releases: c.string(o['releases'], 'releases'),
        projection: checkProjection(c, o['projection'], 'projection'),
    };
    const keystore = c.optionalString(o['keystore'], 'keystore');
    if (keystore !== undefined) config.keystore = keystore;
    if (o['params'] !== undefined) config.params = checkParams(c, o['params'], 'params');
    if (o['autoDeploy'] !== undefined) config.autoDeploy = checkAutoDeploy(c, o['autoDeploy'], 'autoDeploy');
    if (o['allow'] !== undefined) config.allow = checkAllow(c, o['allow'], 'allow');
    return config;
}

export function parseHostRecord(value: unknown, source = 'host.json'): HostRecord {
    const c = new Checker(source);
    const o = c.object(value, '', ['database', 'catalog', 'created', 'key', 'sync', 'params', 'autoDeploy', 'projection']);
    const created = o['created'];
    if (created === undefined) return c.fail('created', 'is missing');
    if (typeof created !== 'boolean') return c.fail('created', 'must be true or false');
    const record: HostRecord = {
        database: c.string(o['database'], 'database'),
        catalog: c.string(o['catalog'], 'catalog'),
        created,
        key: checkKey(c, o['key'], 'key'),
        sync: checkSync(c, o['sync'], 'sync'),
    };
    if (o['params'] !== undefined) record.params = checkParams(c, o['params'], 'params');
    if (o['autoDeploy'] !== undefined) record.autoDeploy = checkAutoDeploy(c, o['autoDeploy'], 'autoDeploy');
    if (o['projection'] !== undefined) record.projection = checkProjection(c, o['projection'], 'projection');
    return record;
}

// The network settings given to create or join, checked as host.json would
// check them.
export function parseSyncConfig(value: unknown, source: string): SyncConfig {
    return checkSync(new Checker(source), value, 'sync');
}

export function checkPassphraseSource(passphrase: string, source: string): void {
    if (passphrase !== 'prompt' && !PASSPHRASE_SOURCE.test(passphrase)) {
        new Checker(source).fail('passphrase', `must be 'prompt' or 'env:<VAR>', got '${passphrase}'`);
    }
}

// app.json with each section the host overrides replaced. The key and the
// network settings are the host's own; the allow list is always the app's.
export function effectiveConfig(app: AppConfig, record: HostRecord): EffectiveConfig {
    return {
        releases: app.releases,
        key: record.key,
        params: record.params ?? app.params ?? {},
        autoDeploy: record.autoDeploy ?? app.autoDeploy ?? 'minor',
        sync: record.sync,
        allow: (app.allow ?? []).map(parseAllowSource),
        projection: record.projection ?? app.projection,
    };
}
