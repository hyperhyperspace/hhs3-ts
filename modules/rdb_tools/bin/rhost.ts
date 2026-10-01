#!/usr/bin/env node
// rhost: the hosts of one app on this device. Each host runs in its own
// background process (`start`), or in the foreground (`serve`).

import { spawn } from "node:child_process";
import { closeSync, promises as fs, writeSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { stderr, stdin, stdout } from "node:process";
import type { Readable } from "node:stream";

import { formatOpVoidDetail, type OpVoidDetail } from "@hyper-hyper-space/hhs3_rdb";
import {
    DEFAULT_HOST, formatReleases, parseParamText,
    type CreateOptions, type HostSetup, type HostStatus, type ParamsConfig, type Rhost, type SyncConfig, type SyncScope,
    type UpdateReport,
} from "@hyper-hyper-space/hhs3_rhost";
import { formatFilesMount } from "@hyper-hyper-space/hhs3_rdb_repl";
import { isHostStatus, type ClientEvent, type ClientParam } from "@hyper-hyper-space/hhs3_rhost_client";
import { hostKey, openClient } from "@hyper-hyper-space/hhs3_rhost_client_node";
import {
    APP_CONFIG_FILE, HOST_FILE, LOG_FILE, NON_INTERACTIVE, NodePlatform,
    askParam, checkHostName, chooseKey, chooseScope, hostDir, initApp, nodePlatform, openApp, readAppConfig, readLock,
    type Prompter,
} from "@hyper-hyper-space/hhs3_rhost_node";

import { ttyPrompter } from "../src/host/tty_prompter.js";

const USAGE = [
    'Usage: rhost [--app <dir>] <command>',
    '  init --release <file>... [--param <name>=<value>]...',
    '  create [<host>] [--catalog <name>] [--name <name>] [<host options>]',
    '  join <database-id> [<host>] [<host options>]',
    '  remove <host>',
    '  update [<host>] [--yes]',
    '  deploy [<host>] [--version <version>|<version>-<tag>|#<hash>] [--param <name>=<value>]...',
    '  status [<host>]',
    '  events [<host>] [--after <id>]',
    '  whoami [<host>]',
    '  serve [<host>] [--passphrase-stdin]',
    '  start [<host>]',
    '  stop [<host>]',
    '',
    'Host options (create, join); --key and --scope are asked on a terminal:',
    '  --key <label>|#<key id>    the key the host signs with, from the keystore',
    '  --scope internet|localhost',
    '  --passphrase-env <VAR>     the key\'s passphrase comes from $VAR (default: asked)',
    '  --tracker <url>  --tracker-key <key id>  --listen <address>',
].join('\n');

const HOST_FLAGS = ['--key', '--passphrase-env', '--scope', '--tracker', '--tracker-key', '--listen'];
const PARAM_FORMS = '$me, $<key label>, or a JSON value';

const START_TIMEOUT_MS = 60_000;
const STOP_TIMEOUT_MS = 10_000;
const STATUS_FD_ENV = 'RHOST_STATUS_FD';

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
            throw new UsageError(`unknown flag ${arg}`);
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

function atMost(parsed: Parsed, n: number): void {
    if (parsed.positional.length > n) throw new UsageError(`unexpected argument '${parsed.positional[n]}'`);
}

// Each `--param <name>=<value>`, as typed.
function paramFlags(parsed: Parsed): Map<string, string> {
    const params = new Map<string, string>();
    for (const entry of parsed.values.get('--param') ?? []) {
        const eq = entry.indexOf('=');
        if (eq <= 0) throw new UsageError(`--param takes <name>=<value>, got '${entry}'`);
        const name = entry.slice(0, eq);
        if (params.has(name)) throw new UsageError(`--param ${name} is given twice`);
        params.set(name, entry.slice(eq + 1));
    }
    return params;
}

function print(line = ''): void {
    stdout.write(line + '\n');
}

async function exists(path: string): Promise<boolean> {
    try {
        await fs.access(path);
        return true;
    } catch {
        return false;
    }
}

// The app folder for this command, and the host whose folder it is run from.
// `app.json` here is the app. A `host.json` here, in `<app>/hosts/<name>/`
// with `app.json` in `<app>`, is that host.
async function locate(cwd: string): Promise<{ dir: string; host?: string }> {
    const here = resolve(cwd);
    if (await exists(join(here, APP_CONFIG_FILE))) return { dir: here };
    const app = resolve(here, '..', '..');
    if (basename(dirname(here)) === 'hosts' && await exists(join(here, HOST_FILE)) && await exists(join(app, APP_CONFIG_FILE))) {
        return { dir: app, host: basename(here) };
    }
    return { dir: here };
}

function reportIssue(report: { source?: string; message?: string; kind?: string }): void {
    stderr.write(`[${report.source ?? 'issue'}] ${report.message ?? report.kind ?? 'unknown issue'}\n`);
}

function formatParamValue(value: unknown): string {
    return typeof value === 'string' ? value : JSON.stringify(value);
}

// ":moderator (identity), :color (string)"
function formatParams(params: ClientParam[]): string {
    return params.map((p) => `:${p.name} (${p.type})`).join(', ');
}

function paramFlagsFor(params: ClientParam[]): string {
    return params.map((p) => `--param ${p.name}=<value>`).join(' ');
}

// What to do about a deploy that waits for params.
function paramHint(host: string, missing: ClientParam[] | undefined): string | undefined {
    if (missing === undefined || missing.length === 0) return undefined;
    return `set ${missing.length === 1 ? 'it' : 'them'} in app.json's params, or run: rhost deploy ${host} ${paramFlagsFor(missing)}`;
}

function formatSync(sync: SyncConfig): string {
    return [
        sync.scope,
        ...(sync.tracker !== undefined ? [`tracker ${sync.tracker}`] : []),
        ...(sync.trackerKey !== undefined ? [`tracker key ${sync.trackerKey}`] : []),
        ...(sync.listen !== undefined ? [`listening on ${sync.listen}`] : []),
    ].join(', ');
}

// --- init ---

async function initCommand(dir: string, args: string[], prompter: Prompter): Promise<number> {
    const parsed = parseArgs(args, ['--release', '--param'], []);
    atMost(parsed, 0);
    const releases = parsed.values.get('--release') ?? [];
    if (releases.length === 0) throw new UsageError('init needs at least one --release <file>');
    const result = await initApp(dir, { releases, params: Object.fromEntries(paramFlags(parsed)) }, prompter);
    print(`initialized ${resolve(dir)}`);
    print(`  releases  ${result.releases.join(', ')}`);
    const params = Object.entries(result.config.params ?? {});
    if (params.length > 0) print(`  params    ${params.map(([name, value]) => `${name}=${formatParamValue(value)}`).join(', ')}`);
    print('add a host with: rhost create, or rhost join <database-id>');
    return 0;
}

// --- create, join, remove ---

function nodePlatformOf(app: Rhost): NodePlatform {
    if (!(app.platform instanceof NodePlatform)) throw new Error('rhost needs the Node platform');
    return app.platform;
}

// A name for a new host, checked before anything is asked.
async function newHostName(app: Rhost, given: string | undefined): Promise<string> {
    const name = given ?? DEFAULT_HOST;
    checkHostName(name);
    if ((await app.hosts()).includes(name)) throw new Error(`host '${name}' already exists`);
    return name;
}

// The key, passphrase source and network settings of a new host, from the
// flags or asked. A key pair created here goes into the app's keystore, and
// its passphrase is not asked again to unlock it.
async function hostSetup(app: Rhost, parsed: Parsed, prompter: Prompter, command: string): Promise<HostSetup> {
    const scope = single(parsed, '--scope');
    if (scope !== undefined && scope !== 'internet' && scope !== 'localhost') {
        throw new UsageError(`--scope is internet or localhost, got '${scope}'`);
    }
    const env = single(parsed, '--passphrase-env');
    const platform = nodePlatformOf(app);
    const keys = await platform.keyVault();
    const choice = await chooseKey(single(parsed, '--key'), keys, platform.keystoreLocation, prompter,
        { command, question: 'Which key should this host sign with?' });
    let keyId: string;
    if (choice.kind === 'create') {
        keyId = (await keys.create(choice.label, choice.passphrase)).keyId;
        platform.rememberPassphrase(keyId, choice.passphrase);
        prompter.say(`created key ${choice.label} (${keyId.slice(0, 8)}) in ${platform.keystoreLocation}`);
    } else {
        keyId = choice.record.keyId;
    }
    const sync: SyncConfig = { scope: await chooseScope(scope as SyncScope | undefined, prompter, command) };
    if (env === undefined && choice.kind === 'existing' && !prompter.interactive) {
        throw new Error(`${command} needs --passphrase-env <VAR> to unlock '${choice.record.label}' (there is no terminal to ask on)`);
    }
    const tracker = single(parsed, '--tracker');
    const trackerKey = single(parsed, '--tracker-key');
    const listen = single(parsed, '--listen');
    if (tracker !== undefined) sync.tracker = tracker;
    if (trackerKey !== undefined) sync.trackerKey = trackerKey;
    if (listen !== undefined) sync.listen = listen;
    return { key: `#${keyId}`, passphrase: env !== undefined ? `env:${env}` : 'prompt', sync };
}

function printHostSetup(app: Rhost, name: string, setup: { key: { label: string; keyId: string }; sync: SyncConfig }): void {
    print(`  key       ${setup.key.label} (${setup.key.keyId.slice(0, 8)}, in ${app.platform.keystoreLocation ?? 'the keystore'})`);
    print(`  sync      ${formatSync(setup.sync)}`);
    print(`start it with: rhost start ${name}`);
}

async function createCommand(app: Rhost, args: string[], prompter: Prompter): Promise<number> {
    const parsed = parseArgs(args, ['--catalog', '--name', ...HOST_FLAGS], []);
    atMost(parsed, 1);
    const name = await newHostName(app, parsed.positional[0]);
    const options: CreateOptions = await hostSetup(app, parsed, prompter, 'rhost create');
    const catalog = single(parsed, '--catalog');
    const dbName = single(parsed, '--name');
    if (catalog !== undefined) options.catalog = catalog;
    if (dbName !== undefined) options.name = dbName;
    const host = await app.create(name, options);
    const deployed = formatReleases((await host.status()).deployed);
    print(`created ${host.name}: database ${host.db.getName() ?? ''} #${host.db.getId()}, ${host.record.catalog} ${deployed}`);
    printHostSetup(app, host.name, host.record);
    return 0;
}

async function joinCommand(app: Rhost, args: string[], prompter: Prompter): Promise<number> {
    const parsed = parseArgs(args, HOST_FLAGS, []);
    atMost(parsed, 2);
    const id = parsed.positional[0];
    if (id === undefined) throw new UsageError('join needs a database id');
    const name = await newHostName(app, parsed.positional[1]);
    const setup = await hostSetup(app, parsed, prompter, 'rhost join');
    const host = await app.join(id.startsWith('#') ? id.slice(1) : id, name, setup);
    print(`joined ${host.name}: database #${host.db.getId()}, catalog ${host.record.catalog}`);
    printHostSetup(app, host.name, host.record);
    return 0;
}

async function removeCommand(dir: string, app: Rhost, args: string[]): Promise<number> {
    const parsed = parseArgs(args, [], []);
    atMost(parsed, 1);
    const name = parsed.positional[0];
    if (name === undefined) throw new UsageError('remove needs a host name');
    if (!(await app.hosts()).includes(name)) throw new Error(`no host '${name}'`);
    if (await readLock(dir, name) !== undefined) {
        const line = await stopProcess(dir, name);
        print(line);
    }
    await app.remove(name);
    print(`removed ${name}: this device's copy is gone; the database lives on in its other replicas`);
    return 0;
}

// --- update, status ---

async function confirmAll(what: string, names: string[], yes: boolean, prompter: Prompter): Promise<boolean> {
    if (yes || names.length <= 1) return true;
    if (!prompter.interactive) {
        throw new Error(`this app has several hosts (${names.join(', ')}); pass --yes to ${what} them all, or name one`);
    }
    const answer = (await prompter.ask(`${what} all ${names.length} hosts (${names.join(', ')})? [y/N] `)).toLowerCase();
    return answer === 'y' || answer === 'yes';
}

function formatUpdate(report: UpdateReport): string[] {
    const parts = [
        `${report.host}: shipping ${report.shipped.name} ${report.shipped.version}`,
        `installed ${report.appliedEntries} entries (${report.skippedEntries} present)`,
        `adopting ${report.adoptionRange}`,
        report.deployed !== undefined ? `deployed ${report.deployed.version}` : `not deployed: ${report.notDeployed ?? ''}`,
    ];
    const hint = paramHint(report.host, report.missingParams);
    return [parts.join('; '), ...(hint !== undefined ? [`  ${hint}`] : [])];
}

async function updateCommand(app: Rhost, args: string[], prompter: Prompter, here?: string): Promise<number> {
    const parsed = parseArgs(args, [], ['--yes']);
    atMost(parsed, 1);
    const name = parsed.positional[0] ?? here;
    const names = name !== undefined ? [name] : await app.hosts();
    if (names.length === 0) {
        print('(no hosts)');
        return 0;
    }
    if (!await confirmAll('update', names, parsed.switches.has('--yes'), prompter)) {
        print('nothing updated');
        return 0;
    }
    let code = 0;
    for (const n of names) {
        try {
            for (const line of formatUpdate((await app.update(n))[0]!)) print(line);
        } catch (err) {
            print(`${n}: ${(err as Error).message}`);
            code = 1;
        }
    }
    return code;
}

function formatStatus(status: HostStatus, pid: number | undefined): string[] {
    const lines = [
        `host      ${status.host} (${status.created ? 'created here' : 'joined'})`,
        `database  ${status.name ?? ''} #${status.database}`,
        `catalog   ${status.catalog}${status.shipped !== undefined
            ? `, shipping ${status.shipped.version} (tag ${status.shipped.tag})` : ', not shipped by this app'}`,
        `deployed  ${formatReleases(status.deployed)}`,
        `adopting  ${status.adoptionRange}`,
    ];
    if (status.held.length > 0) lines.push(`held      ${formatReleases(status.held)}`);
    if (status.members.length > 0) lines.push(`members   ${status.members.map((m) => `${m.name} ${m.state}`).join(', ')}`);
    if (status.unresolved !== undefined) lines.push(`unresolved ${JSON.stringify(status.unresolved)}`);
    const flags = [
        ...(status.upgradeRequired ? ['upgrade required'] : []),
        ...(status.hostBehind ? ['host behind'] : []),
    ];
    if (flags.length > 0) lines.push(`flags     ${flags.join(', ')}`);
    if (status.notDeployed !== undefined) {
        lines.push(`behind    ${status.notDeployed}`);
        const hint = paramHint(status.host, status.missingParams);
        if (hint !== undefined) lines.push(`          ${hint}`);
    }
    lines.push(`running   ${pid !== undefined ? `pid ${pid}` : 'stopped'}`);
    if (status.peers !== undefined) lines.push(`peers     ${status.peers}`);
    for (const mount of status.files ?? []) lines.push(`files     ${formatFilesMount(mount)}`);
    if (status.lastError !== undefined) lines.push(`error     ${status.lastError}`);
    return lines;
}

// The status of a host: from its process over run/rhost.sock while it runs,
// from its store otherwise.
async function hostStatus(dir: string, app: Rhost, name: string): Promise<HostStatus> {
    if (await readLock(dir, name) !== undefined) {
        const client = openClient(hostDir(dir, name));
        try {
            const status = await client.status();
            if (isHostStatus(status)) return status;
        } finally {
            await client.close();
        }
    }
    return (await app.status(name))[0]!;
}

async function statusCommand(dir: string, app: Rhost, args: string[], here?: string): Promise<number> {
    const parsed = parseArgs(args, [], []);
    atMost(parsed, 1);
    const name = parsed.positional[0] ?? here;
    const names = name !== undefined ? [name] : await app.hosts();
    if (names.length === 0) {
        print('(no hosts)');
        return 0;
    }
    let code = 0;
    for (const [i, n] of names.entries()) {
        if (i > 0) print();
        try {
            const status = await hostStatus(dir, app, n);
            for (const line of formatStatus(status, await readLock(dir, n))) print(line);
        } catch (err) {
            print(`host      ${n}`);
            print(`error     ${(err as Error).message}`);
            code = 1;
        }
    }
    return code;
}

// --- deploy, events, whoami ---

// `default`, or the only host there is, without opening the app.
async function defaultHost(dir: string): Promise<string> {
    const names = await nodePlatform(dir).hosts.list();
    if (names.includes(DEFAULT_HOST)) return DEFAULT_HOST;
    if (names.length === 1) return names[0]!;
    if (names.length === 0) throw new Error('this app has no hosts; create or join one');
    throw new Error(`this app has several hosts (${names.join(', ')}); name one`);
}

async function deployCommand(app: Rhost, args: string[], prompter: Prompter, here?: string): Promise<number> {
    const parsed = parseArgs(args, ['--version', '--param'], []);
    atMost(parsed, 1);
    const texts = paramFlags(parsed);
    const name = parsed.positional[0] ?? here ?? await app.defaultHost();
    const host = await app.host(name);
    const version = single(parsed, '--version');
    const target = version !== undefined ? { release: version } : {};

    // Typed by what the release declares; a name it doesn't need is refused
    // by paramNeeds below, whatever its value.
    const declared = new Map((await host.paramNeeds(target)).missing.map((p) => [p.name, p]));
    const params: ParamsConfig = {};
    for (const [n, text] of texts) params[n] = parseParamText(n, declared.get(n) ?? { type: '' }, text);
    const needs = await host.paramNeeds({ ...target, params });
    if (needs.missing.length > 0) {
        if (!prompter.interactive) {
            throw new Error(`deploying ${needs.version} needs ${formatParams(needs.missing)}; `
                + `pass ${paramFlagsFor(needs.missing)} (${PARAM_FORMS})`);
        }
        prompter.say(`${needs.version} needs ${formatParams(needs.missing)}, which app.json's params don't set (${PARAM_FORMS})`);
        for (const p of needs.missing) params[p.name] = await askParam(p.name, p, prompter);
    }
    const report = await host.deploy({ ...target, ...(Object.keys(params).length > 0 ? { params } : {}) });
    print(`deployed ${report.deployed.name} ${report.deployed.version} (tag ${report.deployed.tag}) on ${name}`);
    return 0;
}

function formatReason(reason: unknown): string {
    if (typeof reason !== 'object' || reason === null) return '';
    const r = reason as { source?: string; failure?: { reason?: string }; detail?: unknown };
    if (r.source === 'validation') return r.failure?.reason ?? '';
    if (r.source === 'void') {
        try {
            return formatOpVoidDetail(r.detail as OpVoidDetail);
        } catch {
            return JSON.stringify(r.detail);
        }
    }
    return '';
}

function formatEvent(event: ClientEvent): string {
    const where = event.table === undefined ? '' : ` ${event.table}${event.localId !== undefined ? `#${event.localId}` : ''}`;
    const reason = formatReason(event.reason);
    return `#${event.id} ${event.origin}/${event.direction} ${event.kind}${where}${reason === '' ? '' : ` - ${reason}`}`;
}

async function eventsCommand(dir: string, args: string[], here?: string): Promise<number> {
    const parsed = parseArgs(args, ['--after'], []);
    atMost(parsed, 1);
    await readAppConfig(dir);
    const name = parsed.positional[0] ?? here ?? await defaultHost(dir);
    const after = single(parsed, '--after');
    if (after !== undefined && !/^\d+$/.test(after)) throw new UsageError(`--after takes an event id, got '${after}'`);
    const client = openClient(hostDir(dir, name));
    try {
        const events = await client.events.since(after === undefined ? 0 : Number(after));
        if (events.length === 0) print('(no events)');
        for (const event of events) print(formatEvent(event));
    } finally {
        await client.close();
    }
    return 0;
}

// From host.json alone: no keystore, no passphrase.
async function whoamiCommand(dir: string, args: string[], here?: string): Promise<number> {
    const parsed = parseArgs(args, [], []);
    atMost(parsed, 1);
    await readAppConfig(dir);
    const name = parsed.positional[0] ?? here ?? await defaultHost(dir);
    const key = await hostKey(hostDir(dir, name));
    print(`host        ${name}`);
    print(`key         ${key.label}`);
    print(`key id      ${key.keyId}`);
    print(`public key  ${key.publicKey}`);
    return 0;
}

// --- serve, start, stop ---

function statusFd(): number | undefined {
    const value = process.env[STATUS_FD_ENV];
    if (value === undefined) return undefined;
    const fd = Number(value);
    return Number.isSafeInteger(fd) ? fd : undefined;
}

function tellParent(fd: number | undefined, line: string): void {
    if (fd === undefined) return;
    try {
        writeSync(fd, line + '\n');
        closeSync(fd);
    } catch {
        // the parent is gone
    }
}

async function serveCommand(app: Rhost, args: string[], here?: string): Promise<number> {
    const parsed = parseArgs(args, [], ['--passphrase-stdin']);
    atMost(parsed, 1);
    const fd = statusFd();
    let name: string | undefined;
    try {
        name = parsed.positional[0] ?? here ?? await app.defaultHost();
        const host = await app.host(name);
        let lastError: string | undefined;
        host.onStatus((status) => {
            if (status.lastError !== undefined && status.lastError !== lastError) stderr.write(`${new Date().toISOString()} ${status.lastError}\n`);
            lastError = status.lastError;
        });
        await app.start(name);
    } catch (err) {
        tellParent(fd, `error ${(err as Error).message}`);
        throw err;
    }
    print(`${new Date().toISOString()} serving ${name} (pid ${process.pid})`);
    tellParent(fd, `ready ${process.pid}`);

    await new Promise<void>((done) => {
        const shutdown = () => { done(); };
        process.once('SIGINT', shutdown);
        process.once('SIGTERM', shutdown);
    });
    await app.close();
    print(`${new Date().toISOString()} stopped ${name}`);
    return 0;
}

function firstLine(stream: Readable, timeoutMs: number): Promise<string | undefined> {
    return new Promise((done) => {
        let text = '';
        const finish = (line: string | undefined) => {
            clearTimeout(timer);
            stream.removeAllListeners('data');
            stream.removeAllListeners('end');
            stream.destroy();
            done(line);
        };
        const timer = setTimeout(() => finish(undefined), timeoutMs);
        stream.setEncoding('utf8');
        stream.on('data', (chunk: string) => {
            text += chunk;
            const nl = text.indexOf('\n');
            if (nl >= 0) finish(text.slice(0, nl));
        });
        stream.on('end', () => finish(text.length > 0 ? text : undefined));
        stream.on('error', () => finish(undefined));
    });
}

async function spawnServe(dir: string, name: string, passphrase: string): Promise<{ pid?: number; error?: string }> {
    const logPath = join(hostDir(dir, name), LOG_FILE);
    await fs.mkdir(dirname(logPath), { recursive: true });
    const log = await fs.open(logPath, 'a');
    try {
        const script = process.argv[1]!;
        const child = spawn(process.execPath, [...process.execArgv, script, '--app', dir, 'serve', name, '--passphrase-stdin'], {
            detached: true,
            stdio: ['pipe', log.fd, log.fd, 'pipe'],
            env: { ...process.env, [STATUS_FD_ENV]: '3' },
        });
        child.stdin!.end(passphrase + '\n');
        const line = await firstLine(child.stdio[3] as Readable, START_TIMEOUT_MS);
        child.unref();
        if (line?.startsWith('ready ') === true) return { pid: child.pid! };
        if (line?.startsWith('error ') === true) return { error: line.slice('error '.length) };
        return { error: `serve did not report back; see ${join('hosts', name, LOG_FILE)}` };
    } finally {
        await log.close();
    }
}

// Each host's key is unlocked here first, so a wrong passphrase is reported
// before anything is spawned; hosts that share a key are asked once.
async function startCommand(dir: string, app: Rhost, args: string[], here?: string): Promise<number> {
    const parsed = parseArgs(args, [], []);
    atMost(parsed, 1);
    const name = parsed.positional[0] ?? here;
    const names = name !== undefined ? [name] : await app.hosts();
    if (names.length === 0) {
        print('(no hosts)');
        return 0;
    }
    let code = 0;
    for (const name of names) {
        try {
            const running = await readLock(dir, name);
            if (running !== undefined) {
                print(`${name} is already running (pid ${running})`);
                continue;
            }
            const record = await app.platform.hosts.read(name);
            if (record === undefined) throw new Error(`no host '${name}'`);
            await app.identity(name, record.key);
            const result = await spawnServe(dir, name, await app.platform.passphrase(record.key));
            if (result.error !== undefined) throw new Error(result.error);
            print(`started ${name} (pid ${result.pid})`);
        } catch (err) {
            print(`${name}: ${(err as Error).message}`);
            code = 1;
        }
    }
    return code;
}

async function stopProcess(dir: string, name: string): Promise<string> {
    const pid = await readLock(dir, name);
    if (pid === undefined) return `${name} is not running`;
    process.kill(pid, 'SIGTERM');
    const deadline = Date.now() + STOP_TIMEOUT_MS;
    while (Date.now() < deadline) {
        if (await readLock(dir, name) === undefined) return `stopped ${name} (pid ${pid})`;
        await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error(`${name} (pid ${pid}) did not stop within ${STOP_TIMEOUT_MS / 1000} seconds`);
}

async function stopCommand(dir: string, args: string[], here?: string): Promise<number> {
    const parsed = parseArgs(args, [], []);
    atMost(parsed, 1);
    await readAppConfig(dir);
    const name = parsed.positional[0] ?? here;
    const names = name !== undefined ? [name] : await nodePlatform(dir).hosts.list();
    let code = 0;
    for (const name of names) {
        try {
            print(await stopProcess(dir, name));
        } catch (err) {
            print((err as Error).message);
            code = 1;
        }
    }
    return code;
}

// --- main ---

async function main(): Promise<number> {
    const args = process.argv.slice(2);
    const appFlag = args.indexOf('--app');
    let dir: string;
    let here: string | undefined;
    if (appFlag >= 0) {
        const value = args[appFlag + 1];
        if (value === undefined || value.startsWith('--')) throw new UsageError('--app requires a folder');
        dir = resolve(value);
        args.splice(appFlag, 2);
    } else {
        const found = await locate(process.cwd());
        dir = found.dir;
        here = found.host;
    }
    const [command, ...rest] = args;
    const prompter = stdin.isTTY === true ? ttyPrompter() : NON_INTERACTIVE;
    try {
        if (command === 'init') return await initCommand(dir, rest, prompter);
        const commands = ['create', 'join', 'remove', 'update', 'deploy', 'status', 'events', 'whoami', 'serve', 'start', 'stop'];
        if (command === undefined || !commands.includes(command)) {
            throw new UsageError(command === undefined ? '' : `unknown command '${command}'`);
        }
        if (command === 'stop') return await stopCommand(dir, rest, here);
        if (command === 'events') return await eventsCommand(dir, rest, here);
        if (command === 'whoami') return await whoamiCommand(dir, rest, here);
        const serving = command === 'serve';
        const app = await openApp(dir, {
            prompter,
            ...(serving && rest.includes('--passphrase-stdin') ? { passphraseStdin: true } : {}),
            ...(serving ? { report: reportIssue } : {}),
        });
        try {
            switch (command) {
                case 'create': return await createCommand(app, rest, prompter);
                case 'join': return await joinCommand(app, rest, prompter);
                case 'remove': return await removeCommand(dir, app, rest);
                case 'update': return await updateCommand(app, rest, prompter, here);
                case 'deploy': return await deployCommand(app, rest, prompter, here);
                case 'status': return await statusCommand(dir, app, rest, here);
                case 'serve': return await serveCommand(app, rest, here);
                default: return await startCommand(dir, app, rest, here);
            }
        } finally {
            await app.close();
        }
    } finally {
        prompter.close();
    }
}

main().then((code) => {
    process.exit(code);
}, (e) => {
    if (e instanceof UsageError) {
        if (e.message !== '') stderr.write(e.message + '\n');
        stderr.write(USAGE + '\n');
    } else {
        stderr.write((e instanceof Error ? e.message : String(e)) + '\n');
    }
    process.exit(1);
});
