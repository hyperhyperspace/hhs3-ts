#!/usr/bin/env node
// rkeys: the keys in a keystore file, without a workspace.
//
//   rkeys [--keystore <path>] <command>
//     list
//     create <label> [--passphrase-stdin]
//     export <label>... [--out <file>]
//     import <file|-> [<label>...]
//
// The keystore is ~/.rdb/keys.json (RDB_KEYSTORE, RDB_HOME), or --keystore.
// An export is a keystore file holding the chosen records, still encrypted:
// it works as --keystore, and import takes any keystore file, a host's
// keys.json too. Neither asks for a passphrase.

import { promises as fs } from "node:fs";
import { resolve } from "node:path";
import { stderr, stdin, stdout } from "node:process";

import { createBasicCrypto, HASH_SHA256, keyIdFromPublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { decodePublicKey, defaultKeystorePath, KeyStore, type StoredKeyRecord } from "@hyper-hyper-space/hhs3_rhost_node";

import { ttyPrompter } from "../src/host/tty_prompter.js";

const USAGE = [
    'Usage: rkeys [--keystore <path>] <command>',
    '  list',
    '  create <label> [--passphrase-stdin]',
    '  export <label>... [--out <file>]',
    '  import <file|-> [<label>...]',
].join('\n');

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

class UsageError extends Error {}

type Parsed = { positional: string[]; values: Map<string, string[]>; switches: Set<string> };

function parseArgs(args: string[], valueFlags: string[], switchFlags: string[]): Parsed {
    const parsed: Parsed = { positional: [], values: new Map(), switches: new Set() };
    for (let i = 0; i < args.length; i++) {
        const arg = args[i]!;
        if (valueFlags.includes(arg)) {
            const value = args[i + 1];
            if (value === undefined || value.startsWith('--')) throw new UsageError(`${arg} requires a value`);
            parsed.values.set(arg, [...(parsed.values.get(arg) ?? []), value]);
            i += 1;
        } else if (switchFlags.includes(arg)) {
            parsed.switches.add(arg);
        } else if (arg.startsWith('--')) {
            throw new UsageError(`Unknown flag ${arg}`);
        } else {
            parsed.positional.push(arg);
        }
    }
    return parsed;
}

function single(parsed: Parsed, flag: string): string | undefined {
    const values = parsed.values.get(flag);
    if (values === undefined) return undefined;
    if (values.length > 1) throw new UsageError(`${flag} is given twice`);
    return values[0];
}

function noArguments(args: string[]): void {
    if (args.length > 0) throw new UsageError(`Unexpected argument '${args[0]}'`);
}

function print(line: string, out: NodeJS.WritableStream = stdout): void {
    out.write(line + '\n');
}

async function readStdin(): Promise<string> {
    const chunks: Buffer[] = [];
    for await (const chunk of stdin) chunks.push(chunk as Buffer);
    return Buffer.concat(chunks).toString('utf8').replace(/\r?\n$/, '');
}

// `--keystore <path>` may come before or after the command.
function takeKeystore(argv: string[]): string {
    const at = argv.indexOf('--keystore');
    if (at < 0) return defaultKeystorePath();
    const path = argv[at + 1];
    if (path === undefined || path.startsWith('--')) throw new UsageError('--keystore requires a path');
    argv.splice(at, 2);
    if (argv.includes('--keystore')) throw new UsageError('--keystore is given twice');
    return resolve(path);
}

type Store = { keys: KeyStore; path: string };

async function listCommand(store: Store, args: string[]): Promise<number> {
    noArguments(parseArgs(args, [], []).positional);
    const keys = store.keys.list();
    const width = Math.max(0, ...keys.map((key) => key.label.length));
    for (const key of keys) print(`${key.label.padEnd(width)}  ${key.keyId}`);
    return 0;
}

async function newPassphrase(label: string, fromStdin: boolean): Promise<string> {
    if (fromStdin) return readStdin();
    const prompter = ttyPrompter();
    try {
        if (!prompter.interactive) {
            throw new Error(`Rkeys create needs a passphrase for '${label}': run it on a terminal, or pass --passphrase-stdin`);
        }
        for (;;) {
            const passphrase = await prompter.secret(`passphrase (${label}): `);
            const repeat = await prompter.secret(`repeat (${label}): `);
            if (passphrase === repeat) return passphrase;
            prompter.say('passphrases do not match');
        }
    } finally {
        prompter.close();
    }
}

async function createCommand(store: Store, args: string[]): Promise<number> {
    const parsed = parseArgs(args, [], ['--passphrase-stdin']);
    const [label, ...rest] = parsed.positional;
    if (label === undefined) throw new UsageError('Create needs a label');
    noArguments(rest);
    if (store.keys.list().some((key) => key.label === label)) {
        throw new Error(`Key label '${label}' already exists in ${store.path}`);
    }
    const passphrase = await newPassphrase(label, parsed.switches.has('--passphrase-stdin'));
    const identity = await store.keys.create(label, passphrase);
    print(`created ${label} ${identity.keyId}`);
    return 0;
}

async function exportCommand(store: Store, args: string[]): Promise<number> {
    const parsed = parseArgs(args, ['--out'], []);
    if (parsed.positional.length === 0) throw new UsageError('Export needs a key label');
    const records: StoredKeyRecord[] = [];
    for (const ref of parsed.positional) {
        const record = store.keys.resolveRecord(ref);
        if (!records.includes(record)) records.push(record);
    }
    const text = JSON.stringify({ version: 1, keys: records }, undefined, 2) + '\n';

    const out = single(parsed, '--out');
    if (out === undefined) {
        stdout.write(text);
        return 0;
    }
    try {
        await fs.writeFile(out, text, { mode: 0o600, flag: 'wx' });
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'EEXIST') throw new Error(`${out} already exists`);
        throw e;
    }
    print(`wrote ${out}`);
    return 0;
}

function isKeyRecord(value: unknown): value is StoredKeyRecord {
    if (typeof value !== 'object' || value === null) return false;
    const record = value as Partial<StoredKeyRecord>;
    return typeof record.label === 'string'
        && typeof record.keyId === 'string'
        && typeof record.publicKey?.suite === 'string'
        && typeof record.publicKey?.key === 'string'
        && typeof record.kdf === 'object' && record.kdf !== null
        && typeof record.aead === 'object' && record.aead !== null;
}

function parseKeystoreFile(text: string, source: string): StoredKeyRecord[] {
    let data: unknown;
    try {
        data = JSON.parse(text);
    } catch {
        throw new Error(`${source} is not JSON`);
    }
    const file = data as { version?: unknown; keys?: unknown };
    if (typeof data !== 'object' || data === null || file.version !== 1 || !Array.isArray(file.keys)) {
        throw new Error(`${source} is not a keystore file (version 1, with a keys list)`);
    }
    file.keys.forEach((record, i) => {
        if (!isKeyRecord(record)) throw new Error(`${source}: entry ${i + 1} is not a key record`);
    });
    return file.keys as StoredKeyRecord[];
}

// The records `refs` name, by label or key id prefix, as KeyStore resolves them.
function pick(records: StoredKeyRecord[], refs: string[], source: string): StoredKeyRecord[] {
    const picked: StoredKeyRecord[] = [];
    for (const ref of refs) {
        const normalized = ref.startsWith('#') ? ref.slice(1) : ref;
        const byLabel = records.filter((record) => record.label === normalized);
        const matches = byLabel.length === 1 ? byLabel : records.filter((record) => record.keyId.startsWith(normalized));
        if (matches.length === 0) throw new Error(`No key '${ref}' in ${source}`);
        if (matches.length > 1) throw new Error(`Ambiguous key prefix '${ref}' in ${source}`);
        if (!picked.includes(matches[0]!)) picked.push(matches[0]!);
    }
    return picked;
}

function matchesPublicKey(record: StoredKeyRecord): boolean {
    try {
        return keyIdFromPublicKey(decodePublicKey(record.publicKey), hashSuite) === record.keyId;
    } catch {
        return false;
    }
}

async function importCommand(store: Store, args: string[]): Promise<number> {
    const parsed = parseArgs(args, [], []);
    const [file, ...refs] = parsed.positional;
    if (file === undefined) throw new UsageError('Import needs a keystore file, or - for stdin');
    const source = file === '-' ? 'stdin' : file;
    const records = parseKeystoreFile(file === '-' ? await readStdin() : await fs.readFile(file, 'utf8'), source);
    const picked = refs.length === 0 ? records : pick(records, refs, source);

    // Every record is checked before anything is written: a refusal imports none.
    const held = [...store.keys.list()];
    const problems: string[] = [];
    const plan: { record: StoredKeyRecord; present: boolean }[] = [];
    for (const record of picked) {
        if (!matchesPublicKey(record)) {
            problems.push(`Key '${record.label}' in ${source} does not match its public key`);
            continue;
        }
        const sameLabel = held.find((key) => key.label === record.label);
        if (sameLabel !== undefined) {
            if (sameLabel.keyId === record.keyId) plan.push({ record, present: true });
            else problems.push(`Key label '${record.label}' already exists in ${store.path} with another key`);
            continue;
        }
        const sameKey = held.find((key) => key.keyId === record.keyId);
        if (sameKey !== undefined) {
            problems.push(`Key '${record.label}' is already in ${store.path} as '${sameKey.label}'`);
            continue;
        }
        held.push(record);
        plan.push({ record, present: false });
    }
    if (problems.length > 0) {
        for (const problem of problems) print(problem, stderr);
        print('Nothing was imported', stderr);
        return 1;
    }

    for (const { record, present } of plan) {
        if (present) {
            print(`${record.label} already present`);
        } else {
            await store.keys.importRecord(record);
            print(`imported ${record.label}`);
        }
    }
    return 0;
}

async function main(): Promise<void> {
    const argv = process.argv.slice(2);
    const path = takeKeystore(argv);
    const [command, ...args] = argv;
    if (command === undefined) throw new UsageError('A command is required');
    const store: Store = { keys: await KeyStore.open(path, hashSuite), path };
    if (command === 'list') process.exitCode = await listCommand(store, args);
    else if (command === 'create') process.exitCode = await createCommand(store, args);
    else if (command === 'export') process.exitCode = await exportCommand(store, args);
    else if (command === 'import') process.exitCode = await importCommand(store, args);
    else throw new UsageError(`Unknown command '${command}'`);
}

main().catch((e) => {
    if (e instanceof UsageError) {
        stderr.write(e.message + '\n');
        stderr.write(USAGE + '\n');
    } else {
        stderr.write((e instanceof Error ? e.message : String(e)) + '\n');
    }
    process.exitCode = 1;
});
