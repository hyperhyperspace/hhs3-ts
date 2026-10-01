import { existsSync, promises as fs, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { HostSetup } from "@hyper-hyper-space/hhs3_rhost";
import { exportRelease, releaseFileName, serializeReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import { createNodeSyncMeshFactory, initApp, KeyStore } from "../src/index.js";

const EDITOR_SQL = join(process.cwd(), '../rdb/examples/editor.sql');
const SECOND_RELEASE = `
    ALTER SCHEMA hhs:doc VERSION '1.1.0' AS (ADD COLUMN pages.tag string NULL);
    ALTER CATALOG editor VERSION '1.1.0' AS (UPDATE SCHEMA hhs:doc TO LATEST ON doc) NOTE 'tags' BY $admin;
`;
export const hashSuite = createBasicCrypto().hash(HASH_SHA256);

function nameRef(text: string) {
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

export type ReleasePaths = { v100: string; v110: string };

let releases: Promise<ReleasePaths> | undefined;

// The editor catalog at 1.0.0 and 1.1.0, written as release files once.
export function editorReleases(): Promise<ReleasePaths> {
    releases ??= (async () => {
        const dir = await fs.mkdtemp(join(tmpdir(), 'rhost-releases-'));
        process.once('exit', () => { rmSync(dir, { recursive: true, force: true }); });
        const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
        try {
            await runtime.session.createKey('admin', 'pw');
            runtime.session.selectAuthor('admin');
            await runtime.execute(await fs.readFile(EDITOR_SQL, 'utf8'));
            const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
            const write = async (version: string): Promise<string> => {
                const catalog = (await runtime.workspace.replica.getObject(catalogId)) as unknown as RCatalogImpl;
                const frontier = await (await catalog.getScopedDag()).getFrontier();
                const [release] = (await catalog.getIndex()).findReleasesByVersion(version, frontier);
                const file = await exportRelease(runtime.workspace.replica, catalogId, release!);
                const path = join(dir, releaseFileName('editor', version, release!));
                await fs.writeFile(path, serializeReleaseFile(file));
                return path;
            };
            const v100 = await write('1.0.0');
            await runtime.execute(SECOND_RELEASE);
            const v110 = await write('1.1.0');
            return { v100, v110 };
        } finally {
            await runtime.close();
        }
    })();
    return releases;
}

export async function withDir<T>(prefix: string, fn: (dir: string) => Promise<T>): Promise<T> {
    const dir = await fs.mkdtemp(join(tmpdir(), prefix));
    try {
        return await fn(dir);
    } finally {
        await fs.rm(dir, { recursive: true, force: true });
    }
}

export async function userKeystore(dir: string, labels: string[]): Promise<string> {
    const path = join(dir, 'user-keys.json');
    const keys = await KeyStore.open(path, hashSuite);
    for (const label of labels) await keys.create(label, 'pw');
    return path;
}

// An app folder whose app.json points at a keystore beside it, with the key
// 'me' (passphrase 'pw').
export async function newApp(dir: string, releaseFiles: string[]): Promise<string> {
    const app = join(dir, 'app');
    await userKeystore(dir, ['me']);
    await initApp(app, { releases: releaseFiles, params: { admin: '$me' } });
    await editConfig(app, (config) => { config['keystore'] = '../user-keys.json'; });
    return app;
}

export async function editConfig(app: string, edit: (config: Record<string, unknown>) => void): Promise<void> {
    const path = join(app, 'app.json');
    const config = JSON.parse(await fs.readFile(path, 'utf8')) as Record<string, unknown>;
    edit(config);
    await fs.writeFile(path, JSON.stringify(config, undefined, 2) + '\n');
}

export function hostSetup(overrides: Partial<HostSetup> = {}): HostSetup {
    return { key: 'me', passphrase: 'prompt', sync: { scope: 'localhost' }, ...overrides };
}

export function appOptions(dir: string) {
    return {
        passphrase: async () => 'pw',
        meshFactory: createNodeSyncMeshFactory({ folderRoot: join(dir, 'mesh') }),
    };
}

export async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 15_000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (await check()) return;
    throw new Error(`timed out waiting for ${what}`);
}

export function readApp<T>(path: string, query: (db: Database.Database) => T): T {
    const db = new Database(path, { readonly: true, fileMustExist: true });
    db.pragma('busy_timeout = 5000');
    try {
        return query(db);
    } finally {
        db.close();
    }
}

export function tableHas(path: string, table: string): boolean {
    return existsSync(path) && readApp(path, (db) => db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?").get(table) !== undefined);
}
