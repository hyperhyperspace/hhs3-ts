#!/usr/bin/env node
// rpack: the developer's release tool for catalogs.
//
//   rpack [-C <dir>] [--keystore <path>] <command>
//     init <name> [--key <label>]
//     new <version> [--base <release> [+ <release>]...]
//     set base <release> [+ <release>]... [--force]
//     status
//     build
//     stage [--passphrase-stdin]
//     release [--passphrase-stdin] [--force] [--yes]
//     log
//     verify <file>
//     export <store.db> <catalog> [<version>] [--out <dir>]
//
// Commands run where they're started (or at -C): the catalog repository is the
// nearest folder up that holds rpack.json, and status, build, set base, stage
// and release work on the version folder (work/<version>/) they're started
// in. init makes the start folder a repository. Keys come from the developer
// keystore (~/.rdb/keys.json, RDB_KEYSTORE, or --keystore). `export` writes
// the release file of a release made by hand in the rdb REPL.

import { access, mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { stderr, stdin, stdout } from "node:process";

import { createBasicCrypto, HASH_SHA256, type B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { NameOrHashRef } from "@hyper-hyper-space/hhs3_rdb_lang";
import { chooseKey, defaultKeystorePath, KeyStore } from "@hyper-hyper-space/hhs3_rhost_node";
import {
    buildVersion, CONFIG_FILE, exportRelease, formatReleasePreview, formatVerifyReport, initProject, logReleases, newVersion, releaseFileName,
    releaseTag, releaseVersion, RpackError, serializeReleaseFile, setBase, SourceError, statusOf, verifyRelease, VERSION_FILE, WORK_DIR,
    type RpackContext,
} from "@hyper-hyper-space/hhs3_rpack";

import { ttyPrompter } from "../src/host/tty_prompter.js";
import { locate } from "../src/rpack/locate.js";
import { NodeProject } from "../src/rpack/node_project.js";
import { stage as stageHost } from "../src/rpack/stage.js";
import { Workspace } from "../src/workspace/workspace.js";

const USAGE = [
    'Usage: rpack [-C <dir>] [--keystore <path>] <command>',
    '  init <name> [--key <label>]',
    '  new <version> [--base <release> [+ <release>]...]',
    '  set base <release> [+ <release>]... [--force]',
    '  status',
    '  build',
    '  stage [--passphrase-stdin]',
    '  release [--passphrase-stdin] [--force] [--yes]',
    '  log',
    '  verify <file>',
    '  export <store.db> <catalog> [<version>] [--out <dir>]',
].join('\n');

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

// `2.0.4 + 1.0.7`, as one argument or several. A #hash may hold a '+'.
function baseSelectors(args: string[]): string[] {
    return args
        .flatMap((arg) => arg.split(/\s+/))
        .flatMap((token) => (token.startsWith('#') ? [token] : token.split('+')))
        .filter((token) => token !== '');
}

function print(lines: string[], out: NodeJS.WritableStream = stdout): void {
    for (const line of lines) out.write(line + '\n');
}

type Global = { start: string; keystore: string };

async function openKeys(global: Global): Promise<KeyStore> {
    return KeyStore.open(global.keystore, createBasicCrypto().hash(HASH_SHA256));
}

async function repository(global: Global): Promise<{ root: string; folder?: string }> {
    const located = await locate(global.start);
    if (located === undefined) throw new RpackError(`There is no ${CONFIG_FILE} here or above; start a catalog repository with rpack init`);
    return located;
}

async function context(global: Global): Promise<RpackContext> {
    return { project: new NodeProject((await repository(global)).root), vault: await openKeys(global) };
}

// The repository and the version folder a command runs in.
async function inFolder(global: Global, command: string): Promise<{ ctx: RpackContext; project: NodeProject; folder: string }> {
    const located = await repository(global);
    if (located.folder === undefined) {
        throw new RpackError(`Rpack ${command} runs inside a version folder: cd ${WORK_DIR}/<version>, or pass -C ${WORK_DIR}/<version>`);
    }
    const project = new NodeProject(located.root);
    return { ctx: { project, vault: await openKeys(global) }, project, folder: located.folder };
}

async function readStdin(): Promise<string> {
    const chunks: Buffer[] = [];
    for await (const chunk of stdin) chunks.push(chunk as Buffer);
    return Buffer.concat(chunks).toString('utf8').replace(/\r?\n$/, '');
}

function unlocker(ctx: RpackContext, parsed: Parsed, prompter: ReturnType<typeof ttyPrompter>, command: string) {
    return async (label: string) => {
        const passphrase = parsed.switches.has('--passphrase-stdin')
            ? await readStdin()
            : prompter.interactive
                ? await prompter.secret(`passphrase (${label}): `)
                : (() => { throw new RpackError(`Rpack ${command} needs the passphrase of '${label}': run it on a terminal, or pass --passphrase-stdin`); })();
        return ctx.vault.unlock(label, passphrase);
    };
}

async function initCommand(global: Global, args: string[]): Promise<number> {
    const parsed = parseArgs(args, ['--key'], []);
    const [name, ...rest] = parsed.positional;
    if (name === undefined) throw new UsageError('Init needs a name');
    noArguments(rest);
    const keys = await openKeys(global);
    const prompter = ttyPrompter();
    try {
        const choice = await chooseKey(single(parsed, '--key'), keys, global.keystore, prompter,
            { command: 'rpack init', question: 'Which key signs the releases?' });
        let label: string;
        if (choice.kind === 'existing') {
            label = choice.record.label;
        } else {
            await keys.create(choice.label, choice.passphrase);
            label = choice.label;
        }
        print(await initProject(new NodeProject(global.start), name, label));
        return 0;
    } finally {
        prompter.close();
    }
}

async function newCommand(global: Global, args: string[]): Promise<number> {
    const parsed = parseArgs(args, ['--base'], []);
    const [version, ...rest] = parsed.positional;
    if (version === undefined) throw new UsageError('New needs a version');
    const given = parsed.values.get('--base');
    if (given === undefined) noArguments(rest);
    const base = given !== undefined ? baseSelectors([...given, ...rest]) : [];
    print((await newVersion(await context(global), version, { base })).lines);
    return 0;
}

async function setCommand(global: Global, args: string[]): Promise<number> {
    const parsed = parseArgs(args, [], ['--force']);
    const [what, ...rest] = parsed.positional;
    if (what !== 'base') throw new UsageError(what === undefined ? 'Set needs what to set: base' : `Unknown setting '${what}'`);
    const selectors = baseSelectors(rest);
    if (selectors.length === 0) throw new UsageError('Set base needs a release');
    const { ctx, folder } = await inFolder(global, 'set base');
    print(await setBase(ctx, folder, selectors, { force: parsed.switches.has('--force') }));
    return 0;
}

async function statusCommand(global: Global, args: string[]): Promise<number> {
    noArguments(args);
    const { ctx, folder } = await inFolder(global, 'status');
    const { draft, lines } = await statusOf(ctx, folder);
    print(lines);
    return draft.refusals.length > 0 ? 1 : 0;
}

async function buildCommand(global: Global, args: string[]): Promise<number> {
    noArguments(args);
    const { ctx, folder } = await inFolder(global, 'build');
    const { draft, lines } = await buildVersion(ctx, folder);
    print(lines);
    return draft.refusals.length > 0 ? 1 : 0;
}

async function releaseCommand(global: Global, args: string[]): Promise<number> {
    if (args.includes('--note')) throw new RpackError(`A release's note is the "note" in its folder's ${VERSION_FILE}`);
    const parsed = parseArgs(args, [], ['--passphrase-stdin', '--force', '--yes']);
    noArguments(parsed.positional);
    const { ctx, folder } = await inFolder(global, 'release');
    const prompter = ttyPrompter();
    try {
        const result = await releaseVersion(ctx, folder, unlocker(ctx, parsed, prompter, 'release'), {
            force: parsed.switches.has('--force'),
            yes: parsed.switches.has('--yes'),
            ...(prompter.interactive ? {
                confirm: async (preview) => {
                    print([...formatReleasePreview(preview), '']);
                    const name = `${preview.catalog}-${preview.draft.version}`;
                    const n = preview.rebuilt.length;
                    const question = preview.replaces === undefined
                        ? `Release ${name}? y/n `
                        : `Re-release ${name}${n > 0 ? ` and the ${n} built on it` : ''}? y/n `;
                    const answer = (await prompter.ask(question)).toLowerCase();
                    return answer === 'y' || answer === 'yes';
                },
            } : {}),
        });
        print(result.lines);
        return 0;
    } finally {
        prompter.close();
    }
}

async function stageCommand(global: Global, args: string[]): Promise<number> {
    const parsed = parseArgs(args, [], ['--passphrase-stdin']);
    noArguments(parsed.positional);
    const { ctx, project, folder } = await inFolder(global, 'stage');
    const prompter = ttyPrompter();
    try {
        print(await stageHost(project, ctx.vault, folder, { unlock: unlocker(ctx, parsed, prompter, 'stage') }));
        return 0;
    } finally {
        prompter.close();
    }
}

async function logCommand(global: Global, args: string[]): Promise<number> {
    noArguments(args);
    print(await logReleases(await context(global)));
    return 0;
}

function ref(text: string): NameOrHashRef {
    const span = { start: 0, end: text.length, line: 1, column: 1 };
    return text.startsWith('#')
        ? { kind: 'hash', prefix: text.slice(1), span }
        : { kind: 'name', text, parts: text.split('.'), span };
}

async function verifyCommand(args: string[]): Promise<number> {
    const [file, ...rest] = args;
    if (file === undefined) throw new UsageError('Verify needs a file');
    noArguments(rest);
    const report = await verifyRelease(await readFile(file, 'utf8'));
    print(formatVerifyReport(report), report.ok ? stdout : stderr);
    return report.ok ? 0 : 1;
}

async function exportCommand(args: string[]): Promise<number> {
    const parsed = parseArgs(args, ['--out'], []);
    const outDir = single(parsed, '--out') ?? 'releases';
    const [storePath, catalogName, selector, ...rest] = parsed.positional;
    if (storePath === undefined || catalogName === undefined) throw new UsageError('Export needs a store and a catalog');
    noArguments(rest);

    // Workspace.open would create a missing store; exporting from one is a mistake.
    try {
        await access(storePath);
    } catch {
        stderr.write(`No store at '${storePath}'\n`);
        return 1;
    }

    const workspace = await Workspace.open({ path: storePath });
    try {
        const root = await workspace.roots.resolveCatalog(ref(catalogName));
        if (root.catalog === undefined) throw new Error(`catalog '${catalogName}' is not loaded`);
        const catalog = root.catalog;
        const index = await catalog.getIndex();
        const frontier = await (await catalog.getScopedDag()).getFrontier();

        let candidates: B64Hash[];
        if (selector === undefined) {
            candidates = index.maximalReleasesAt(frontier);
        } else if (selector.startsWith('#')) {
            candidates = index.releasesAt(frontier).filter((h) => h.startsWith(selector.slice(1)));
        } else {
            const m = /^(\d+\.\d+\.\d+)(?:-([0-9a-f]+))?$/.exec(selector);
            if (m === null) throw new Error(`'${selector}' is not a version, version-tag or #hash`);
            candidates = index.findReleasesByVersion(m[1], frontier);
            if (m[2] !== undefined) candidates = candidates.filter((h) => releaseTag(h).startsWith(m[2]));
        }

        const label = (h: B64Hash) => `${index.releaseState(h).version}-${releaseTag(h)}`;
        if (candidates.length === 0) {
            stderr.write(`No release of '${catalog.getName()}' matches ${selector === undefined ? 'the frontier' : `'${selector}'`}\n`);
            return 1;
        }
        if (candidates.length > 1) {
            stderr.write(`Several releases match; name one: ${candidates.map(label).join(', ')}\n`);
            return 1;
        }

        const release = candidates[0];
        const file = await exportRelease(workspace.replica, root.id, release);
        await mkdir(outDir, { recursive: true });
        const path = join(outDir, releaseFileName(file.manifest.name, file.manifest.version, release));
        await writeFile(path, serializeReleaseFile(file));
        stdout.write(`wrote ${path}\n`);
        return 0;
    } finally {
        await workspace.close();
    }
}

async function main(): Promise<void> {
    const argv = process.argv.slice(2);
    const global: Global = { start: process.cwd(), keystore: defaultKeystorePath() };
    while (argv[0] === '-C' || argv[0] === '--keystore') {
        const value = argv[1];
        if (value === undefined) throw new UsageError(`${argv[0]} requires a value`);
        if (argv[0] === '-C') global.start = resolve(value);
        else global.keystore = resolve(value);
        argv.splice(0, 2);
    }
    const [command, ...args] = argv;
    if (command === 'init') process.exitCode = await initCommand(global, args);
    else if (command === 'new') process.exitCode = await newCommand(global, args);
    else if (command === 'set') process.exitCode = await setCommand(global, args);
    else if (command === 'status') process.exitCode = await statusCommand(global, args);
    else if (command === 'build') process.exitCode = await buildCommand(global, args);
    else if (command === 'release') process.exitCode = await releaseCommand(global, args);
    else if (command === 'stage') process.exitCode = await stageCommand(global, args);
    else if (command === 'log') process.exitCode = await logCommand(global, args);
    else if (command === 'verify') process.exitCode = await verifyCommand(args);
    else if (command === 'export') process.exitCode = await exportCommand(args);
    else throw new UsageError(command === undefined ? 'A command is required' : `Unknown command '${command}'`);
}

main().catch((e) => {
    if (e instanceof UsageError) {
        stderr.write((e.message.length > 0 ? e.message : 'Error') + '\n');
        stderr.write(USAGE + '\n');
    } else if (e instanceof RpackError) {
        print(e.lines, stderr);
        if (e.message.length > 0) stderr.write(e.message + '\n');
    } else if (e instanceof SourceError) {
        stderr.write(e.message + '\n');
    } else {
        stderr.write((e instanceof Error ? e.message : String(e)) + '\n');
    }
    process.exitCode = 1;
});
