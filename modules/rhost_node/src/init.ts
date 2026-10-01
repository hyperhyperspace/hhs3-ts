// `rhost init`: a new app folder from the release files the app ships, and
// the prompts `rhost create`, `join` and `deploy` share with it. Anything the
// flags leave out is asked on the terminal: each param the releases declare,
// and for a new host, its key and sync scope.

import { promises as fs } from "node:fs";
import { basename, join, resolve } from "node:path";

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { CatalogParamDecl, RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import { parseParamText, shippedReleases, type AppConfig, type SyncScope } from "@hyper-hyper-space/hhs3_rhost";
import { installRelease, parseReleaseFile, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import type { KeyStore, StoredKeyRecord } from "./keystore.js";
import { APP_CONFIG_FILE } from "./open_app.js";
import { NON_INTERACTIVE, type Prompter } from "./prompter.js";

export const MAX_LISTED_KEYS = 8;

export type InitFlags = {
    releases: string[];
    params?: { [name: string]: string };
};

export type InitResult = {
    config: AppConfig;
    releases: string[];
};

async function exists(path: string): Promise<boolean> {
    try {
        await fs.access(path);
        return true;
    } catch {
        return false;
    }
}

export type KeyChoice =
    | { kind: 'existing'; record: StoredKeyRecord }
    | { kind: 'create'; label: string; passphrase: string };

// A key from the user's keystore: `given` by label or `#<key id>`, or picked
// from a numbered list (the first eight keys, "Other" to type a label, and
// "Create new key pair"). `command` and `question` word the prompt.
export async function chooseKey(
    given: string | undefined,
    userKeys: KeyStore,
    keystorePath: string,
    prompter: Prompter,
    wording: { command: string; question: string } = { command: 'rhost create', question: 'Which key should this host sign with?' },
): Promise<KeyChoice> {
    if (given !== undefined) {
        try {
            return { kind: 'existing', record: userKeys.resolveRecord(given) };
        } catch {
            throw new Error(`no key '${given}' in ${keystorePath}`);
        }
    }
    if (!prompter.interactive) throw new Error(`${wording.command} needs --key <label> (there is no terminal to ask on)`);

    const keys = userKeys.list();
    const listed = keys.slice(0, MAX_LISTED_KEYS);
    const other = keys.length > MAX_LISTED_KEYS;
    prompter.say(`${wording.question} (from ${keystorePath})`);
    listed.forEach((key, i) => prompter.say(`  ${i + 1}. ${key.label} (${key.keyId.slice(0, 8)})`));
    const otherChoice = other ? listed.length + 1 : undefined;
    const createChoice = listed.length + (other ? 2 : 1);
    if (otherChoice !== undefined) prompter.say(`  ${otherChoice}. Other`);
    prompter.say(`  ${createChoice}. Create new key pair`);

    for (;;) {
        const answer = Number(await prompter.ask(`key [1-${createChoice}]: `));
        if (Number.isSafeInteger(answer) && answer >= 1 && answer <= listed.length) {
            return { kind: 'existing', record: listed[answer - 1]! };
        }
        if (answer === otherChoice) {
            const label = await prompter.ask('key label: ');
            try {
                return { kind: 'existing', record: userKeys.resolveRecord(label) };
            } catch {
                prompter.say(`no key '${label}' in ${keystorePath}`);
                continue;
            }
        }
        if (answer === createChoice) {
            const label = (await prompter.ask('label for the new key [me]: ')) || 'me';
            for (;;) {
                const passphrase = await prompter.secret(`passphrase (${label}): `);
                const repeat = await prompter.secret(`repeat (${label}): `);
                if (passphrase === repeat) return { kind: 'create', label, passphrase };
                prompter.say('passphrases do not match');
            }
        }
        prompter.say(`choose a number from 1 to ${createChoice}`);
    }
}

// The sync scope of a new host: `given`, or asked.
export async function chooseScope(given: SyncScope | undefined, prompter: Prompter, command = 'rhost create'): Promise<SyncScope> {
    if (given !== undefined) return given;
    if (!prompter.interactive) throw new Error(`${command} needs --scope internet|localhost (there is no terminal to ask on)`);
    prompter.say('Sync over:');
    prompter.say('  1. internet');
    prompter.say('  2. localhost');
    for (;;) {
        const answer = await prompter.ask('scope [1-2]: ');
        if (answer === '1' || answer === 'internet') return 'internet';
        if (answer === '2' || answer === 'localhost') return 'localhost';
        prompter.say('choose 1 or 2');
    }
}

// The params the shipped releases declare, read from a scratch replica.
async function declaredParams(files: ReleaseFile[]): Promise<Map<string, CatalogParamDecl>> {
    const decls = new Map<string, CatalogParamDecl>();
    for (const file of files) {
        const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
        try {
            await installRelease(runtime.workspace.replica, file);
            const catalog = (await runtime.workspace.replica.getObject(file.manifest.catalog)) as unknown as RCatalogImpl;
            const params = (await catalog.getIndex()).releaseState(file.manifest.release).params;
            for (const [name, decl] of params) decls.set(name, decl);
        } finally {
            await runtime.close();
        }
    }
    return decls;
}

// Asks for one param until the answer parses. An identity defaults to $me.
export async function askParam(name: string, decl: { type: string }, prompter: Prompter): Promise<json.Literal> {
    for (;;) {
        const suggestion = decl.type === 'identity' ? ' [$me]' : '';
        const answer = await prompter.ask(`param ${name} (${decl.type})${suggestion}: `);
        const text = answer === '' && decl.type === 'identity' ? '$me' : answer;
        try {
            return parseParamText(name, decl, text);
        } catch (e) {
            prompter.say((e as Error).message);
        }
    }
}

async function chooseParams(flags: InitFlags, decls: Map<string, CatalogParamDecl>, prompter: Prompter): Promise<{ [name: string]: json.Literal }> {
    const given = flags.params ?? {};
    const undeclared = Object.keys(given).filter((name) => !decls.has(name)).sort();
    if (undeclared.length > 0) {
        throw new Error(`the shipped releases declare no param ${undeclared.map((n) => `'${n}'`).join(', ')}`);
    }
    const params: { [name: string]: json.Literal } = {};
    for (const name of [...decls.keys()].sort()) {
        const decl = decls.get(name)!;
        const flag = given[name];
        if (flag !== undefined) {
            params[name] = parseParamText(name, decl, flag);
            continue;
        }
        if (!prompter.interactive) throw new Error(`rhost init needs --param ${name}=<value> (there is no terminal to ask on)`);
        params[name] = await askParam(name, decl, prompter);
    }
    return params;
}

// Writes app.json and copies the release files into catalogs/. Hosts are
// added with `rhost create` or `rhost join`, each with its own key.
export async function initApp(dir: string, flags: InitFlags, prompter: Prompter = NON_INTERACTIVE): Promise<InitResult> {
    const appDir = resolve(dir);
    const configPath = join(appDir, APP_CONFIG_FILE);
    if (await exists(configPath)) throw new Error(`${configPath} already exists`);
    if (flags.releases.length === 0) throw new Error('rhost init needs --release <file>');

    const files: ReleaseFile[] = [];
    for (const path of flags.releases) {
        try {
            files.push(parseReleaseFile(await fs.readFile(path, 'utf8')));
        } catch (e) {
            throw new Error(`${path}: ${(e as Error).message}`);
        }
    }
    const shipped = [...shippedReleases(files).values()];
    const params = await chooseParams(flags, await declaredParams(shipped), prompter);

    await fs.mkdir(join(appDir, 'catalogs'), { recursive: true });
    const copied: string[] = [];
    for (const path of flags.releases) {
        const name = basename(path);
        await fs.copyFile(path, join(appDir, 'catalogs', name));
        copied.push(name);
    }

    const config: AppConfig = {
        releases: 'catalogs/',
        ...(Object.keys(params).length > 0 ? { params } : {}),
        autoDeploy: 'minor',
        projection: { path: 'db/data.sqlite' },
    };
    await fs.writeFile(configPath, JSON.stringify(config, undefined, 2) + '\n');
    return { config, releases: copied };
}
