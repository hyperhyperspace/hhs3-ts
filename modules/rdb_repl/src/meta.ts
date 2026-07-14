import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RSchema, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    dumpDatabase,
    dumpGroup,
    dumpSchema,
    findLangCommandRefs,
    isLangCommonHelpQuery,
    LANG_COMMAND_SECTIONS,
    LANG_COMMON_REF,
    type LangCommandRef,
    type RenderOptions,
} from "@hyper-hyper-space/hhs3_rdb_lang";
import type { RefAutoUpdateMode } from "@hyper-hyper-space/hhs3_rdb_runtime";
import { formatAliasListing, formatAliasResult, isAliasScope, resolveAliasTarget, type AliasScope } from "./aliases.js";
import { runDeltaCommand } from "./delta/command.js";
import { createDumpRenderOptions } from "./dump/alias_context.js";
import { runDumpOpCommand } from "./dump/op_command.js";
import { createDisplayContext, formatDisplayString, formatSessionRows } from "./format/display.js";
import { formatRows } from "./format/rows.js";
import type { ReplSession } from "./session.js";

export type PassphraseNeed = { kind: 'create' | 'unlock' | 'author'; label: string };
export type MetaCommandResult = {
    handled: boolean;
    output?: string;
    quit?: boolean;
    needsPassphrase?: PassphraseNeed;
};

export async function runMetaCommand(session: ReplSession, line: string): Promise<MetaCommandResult> {
    if (!line.trimStart().startsWith('\\')) return { handled: false };
    const [command, ...args] = line.trim().replace(/;$/, '').trim().slice(1).split(/\s+/);
    switch (command) {
        case 'help': return { handled: true, output: help(args) };
        case 'q':
        case 'quit': return { handled: true, quit: true };
        case 'dbs': return { handled: true, output: roots(session, 'database') };
        case 'schemas': return { handled: true, output: roots(session, 'schema') };
        case 'groups': return { handled: true, output: roots(session, 'group') };
        case 'dt': return { handled: true, output: await listTables(session, args[0]) };
        case 'd': return { handled: true, output: await describeTable(session, args[0]) };
        case 'keys': return { handled: true, output: await listKeys(session) };
        case 'key': return { handled: true, ...await keyCommand(session, args) };
        case 'author': return { handled: true, ...await authorCommand(session, args) };
        case 'whoami': {
            const identity = await session.currentAuthor();
            return { handled: true, output: identity === undefined ? '(no identity selected)' : formatDisplayString(session, identity.keyId, { role: 'hash' }) };
        }
        case 'use': return { handled: true, output: await useCommand(session, args) };
        case 'view': return { handled: true, output: formatView(session) };
        case 'frontier': return { handled: true, output: await frontier(session, args[0]) };
        case 'alias': return { handled: true, output: await alias(session, args) };
        case 'aliases': return { handled: true, output: await aliases(session, args[0]) };
        case 'unalias': return { handled: true, output: unalias(session, args) };
        case 'output': return { handled: true, output: setOutput(session, args[0]) };
        case 'hash-width': return { handled: true, output: setHashWidth(session, args[0]) };
        case 'hash-labels': return { handled: true, output: setHashLabels(session, args[0]) };
        case 'ref-auto-update': return { handled: true, output: setRefAutoUpdate(session, args[0]) };
        case 'dump': return { handled: true, output: await dump(session, args) };
        case 'delta': return { handled: true, output: await runDeltaCommand(session, args) };
        default: return { handled: true, output: `Unknown meta-command \\${command}` };
    }
}

export async function fulfillPassphraseNeed(
    session: ReplSession,
    need: PassphraseNeed,
    passphrase: string,
): Promise<string> {
    if (need.kind === 'create') {
        const identity = await session.createKey(need.label, passphrase);
        return `created ${need.label} ${formatDisplayString(session, identity.keyId, { role: 'hash' })}`;
    }
    await session.unlockKey(need.label, passphrase);
    if (need.kind === 'author') {
        const identity = session.selectAuthor(need.label);
        return `author ${labelFor(session, identity.keyId)}`;
    }
    const identity = session.resolveIdentity(need.label);
    if (identity === undefined) throw new Error(`Key '${need.label}' is locked`);
    return `unlocked ${formatDisplayString(session, identity.keyId, { role: 'hash' })}`;
}

function roots(session: ReplSession, kind: 'database' | 'schema' | 'group'): string {
    return formatSessionRows(session, session.workspace.roots.list(kind).map((root) => ({
        name: root.name ?? '', id: root.id, type: root.type,
    })), undefined, { structuralColumns: new Set(['id']) });
}

async function listTables(session: ReplSession, name?: string): Promise<string> {
    const group = await session.workspace.roots.resolveGroup(ref(name ?? session.currentGroup ?? ''), { aliases: session.aliases });
    if (group.group === undefined) throw new Error('Group is not loaded');
    return formatRows((await group.group.getView()).getSchemaView().getTableNames().map((table) => ({ table })));
}

async function describeTable(session: ReplSession, tableRef?: string): Promise<string> {
    if (tableRef === undefined) throw new Error('Usage: \\d group.table');
    const parts = tableRef.split('.');
    const groupName = parts.length > 1 ? parts[0] : session.currentGroup;
    const tableName = parts.length > 1 ? parts.slice(1).join('.') : parts[0];
    if (groupName === undefined) throw new Error('No current group; use \\d group.table');
    const group = await session.workspace.roots.resolveGroup(ref(groupName), { aliases: session.aliases });
    if (group.group === undefined) throw new Error('Group is not loaded');
    const table = (await group.group.getView()).getSchemaView().getTable(tableName);
    if (table === undefined) throw new Error(`Unknown table '${tableName}'`);
    return formatRows(Object.entries(table.columns).map(([column, def]) => ({
        column, type: def.type, pub: def.pub === true, readonly: def.readonly === true, nullable: def.nullable === true,
    })));
}

async function listKeys(session: ReplSession): Promise<string> {
    if (session.keyVault === undefined) return '(no keystore)';
    const selected = (await session.currentAuthor())?.keyId;
    return formatSessionRows(session, session.keyVault.list().map((key) => ({
        label: key.label, keyId: key.keyId, unlocked: session.isUnlocked(key.keyId), selected: selected === key.keyId,
    })), undefined, { structuralColumns: new Set(['keyId']) });
}

type KeyResult = Pick<MetaCommandResult, 'output' | 'needsPassphrase'>;
async function keyCommand(session: ReplSession, args: string[]): Promise<KeyResult> {
    if (session.keyVault === undefined) throw new Error('No keystore configured');
    const [sub, label, passphrase] = args;
    if (sub === 'create') {
        if (label === undefined) throw new Error('Usage: \\key create <label> [passphrase]');
        if (passphrase === undefined) return { needsPassphrase: { kind: 'create', label } };
        return { output: await fulfillPassphraseNeed(session, { kind: 'create', label }, passphrase) };
    }
    if (sub === 'unlock') {
        if (label === undefined) throw new Error('Usage: \\key unlock <label|#prefix>');
        const record = session.keyVault.resolveRecord(session.resolveKeyRef(label));
        if (passphrase === undefined) return { needsPassphrase: { kind: 'unlock', label: record.label } };
        return { output: await fulfillPassphraseNeed(session, { kind: 'unlock', label: record.label }, passphrase) };
    }
    throw new Error('Usage: \\key create|unlock ...');
}

async function authorCommand(session: ReplSession, args: string[]): Promise<KeyResult> {
    if (session.keyVault === undefined) throw new Error('No keystore configured');
    const [target, passphrase] = args;
    if (target === undefined) {
        const identity = await session.currentAuthor();
        return { output: identity === undefined ? 'nobody' : labelFor(session, identity.keyId) };
    }
    if (target.toLowerCase() === 'nobody') {
        session.clearAuthor();
        return { output: 'author nobody' };
    }
    if (session.resolveIdentity(target) === undefined) {
        if (passphrase === undefined) return { needsPassphrase: { kind: 'author', label: target } };
        await session.unlockKey(target, passphrase);
    }
    return { output: `author ${labelFor(session, session.selectAuthor(target).keyId)}` };
}

function labelFor(session: ReplSession, keyId: string): string {
    return session.keyVault?.list().find((key) => key.keyId === keyId)?.label
        ?? formatDisplayString(session, keyId, { role: 'hash' });
}

async function useCommand(session: ReplSession, args: string[]): Promise<string> {
    const [kind, name] = args;
    if (kind === 'database') {
        const root = await session.workspace.roots.resolveDatabase(ref(name), { aliases: session.aliases });
        session.setCurrentDatabase(root.id);
        return `using database ${formatDisplayString(session, root.id, { role: 'hash' })}`;
    }
    if (kind === 'group') {
        const root = await session.workspace.roots.resolveGroup(ref(name), { aliases: session.aliases });
        session.setCurrentGroup(root.id);
        return `using group ${formatDisplayString(session, root.id, { role: 'hash' })}`;
    }
    throw new Error('Usage: \\use database <name> | \\use group <name>');
}

async function frontier(session: ReplSession, name?: string): Promise<string> {
    const root = await session.workspace.roots.resolveGroup(ref(name ?? session.currentGroup ?? ''), { aliases: session.aliases });
    if (root.group === undefined) throw new Error('Group is not loaded');
    const hashes = [...await (await root.group.getScopedDag()).getFrontier()];
    const ctx = createDisplayContext(session, hashes);
    return `{${hashes.map((hash) => ctx.formatString(hash, { role: 'hash', hashPrefix: true })).join(', ')}}`;
}

async function alias(session: ReplSession, args: string[]): Promise<string> {
    let scope: AliasScope | 'auto' = 'auto';
    let rest = args;
    if (args[0] !== undefined && isAliasScope(args[0])) {
        scope = args[0];
        rest = args.slice(1);
    }
    const [name, target] = rest;
    if (name === undefined || target === undefined) throw new Error('Usage: \\alias [scope] <name> <#prefix>');
    if (!target.startsWith('#')) throw new Error('Alias target must be a #prefix');
    const resolved = await resolveAliasTarget(scope, target.slice(1), session);
    for (const item of resolved) session.aliases.set(item.scope, name, item.hash);
    return (await Promise.all(resolved.map((item) => formatAliasResult(item.scope, name, item.hash, session)))).join('\n');
}

async function aliases(session: ReplSession, arg?: string): Promise<string> {
    const scope = arg !== undefined && arg !== '' && isAliasScope(arg) ? arg : undefined;
    if (arg !== undefined && arg !== '' && scope === undefined) throw new Error(`Unknown alias scope '${arg}'`);
    const output = await formatAliasListing(session, scope);
    return output === '' ? '(no aliases)' : output;
}

function unalias(session: ReplSession, args: string[]): string {
    const [scope, name] = args;
    if (scope === undefined || name === undefined || !isAliasScope(scope)) throw new Error('Usage: \\unalias <scope> <name>');
    if (!session.aliases.delete(scope, name)) throw new Error(`No alias '${name}' in scope '${scope}'`);
    return `removed ${scope} ${name}`;
}

function setOutput(session: ReplSession, mode?: string): string {
    if (mode !== 'table' && mode !== 'json' && mode !== 'vertical') throw new Error('Usage: \\output table|json|vertical');
    session.setOutputMode(mode);
    return `output ${mode}`;
}

function setHashWidth(session: ReplSession, width?: string): string {
    if (width === 'auto' || width === 'full') {
        session.setHashWidth(width);
        return `hash-width ${width}`;
    }
    const numeric = Number(width);
    if (!Number.isInteger(numeric) || numeric <= 0) throw new Error('Usage: \\hash-width auto|full|<N>');
    session.setHashWidth(numeric);
    return `hash-width ${numeric}`;
}

function setHashLabels(session: ReplSession, mode?: string): string {
    if (mode !== 'on' && mode !== 'off') throw new Error('Usage: \\hash-labels on|off');
    session.setHashLabels(mode === 'on');
    return `hash-labels ${mode}`;
}

function setRefAutoUpdate(session: ReplSession, mode?: string): string {
    const normalized: RefAutoUpdateMode = mode === 'on' ? 'auto'
        : mode === 'auto' || mode === 'self' || mode === 'off' ? mode
            : (() => { throw new Error('Usage: \\ref-auto-update auto|self|off'); })();
    session.setRefAutoUpdate(normalized);
    return `ref-auto-update ${normalized}`;
}

async function dump(session: ReplSession, args: string[]): Promise<string> {
    const [kind, name] = args;
    const render = (extra?: RenderOptions) => createDumpRenderOptions(session, extra);
    if (kind === 'op') return runDumpOpCommand(session, args.slice(1));
    if (kind === 'schema') {
        const root = await session.workspace.roots.resolveSchema(ref(name), { aliases: session.aliases });
        if (root.schema === undefined) throw new Error('Schema is not loaded');
        return dumpSchema(root.schema as Parameters<typeof dumpSchema>[0], { render: render() });
    }
    if (kind === 'group') {
        const root = await session.workspace.roots.resolveGroup(ref(name), { aliases: session.aliases });
        if (root.group === undefined) throw new Error('Group is not loaded');
        return dumpGroup(root.group as Parameters<typeof dumpGroup>[0], { render: render() });
    }
    if (kind === 'database') {
        if (name === undefined) throw new Error('Usage: \\dump database <name> [full|schema]');
        const mode = args[2] === 'schema' ? 'schema' : 'full';
        const root = await session.workspace.roots.resolveDatabase(ref(name), { aliases: session.aliases });
        if (root.db === undefined) throw new Error('Database is not loaded');
        return dumpDatabase(root.db as Parameters<typeof dumpDatabase>[0], {
            mode,
            loadSchema: async (id) => await loadRoot(session, id) as RSchema & Parameters<typeof dumpSchema>[0],
            loadGroup: async (id) => await loadRoot(session, id) as RTableGroup & Parameters<typeof dumpGroup>[0],
            render: render({ profile: mode }),
        });
    }
    throw new Error('Usage: \\dump schema|group|database <name> [full|schema] | \\dump op [group] #hash');
}

async function loadRoot(session: ReplSession, id: B64Hash): Promise<RObject> {
    const record = session.workspace.roots.get(id);
    const object = record?.object ?? await session.workspace.replica.getObject(id);
    if (object === undefined) throw new Error(`Object '${id}' is not loaded`);
    return object;
}

function formatView(session: ReplSession): string {
    if (session.defaultView === undefined) return 'VIEW LATEST';
    const at = [...session.defaultView.at];
    const from = [...(session.defaultView.from ?? [])];
    const ctx = createDisplayContext(session, [...at, ...from]);
    const render = (values: string[]) => values.map((hash) => ctx.formatString(hash, { role: 'hash', hashPrefix: true })).join(', ');
    return `VIEW AT {${render(at)}}${from.length === 0 ? '' : ` FROM {${render(from)}}`}`;
}

function ref(text = '') {
    const span = { start: 0, end: text.length, line: 1, column: 1 };
    return text.startsWith('#')
        ? { kind: 'hash' as const, prefix: text.slice(1), span }
        : { kind: 'name' as const, text, parts: text.split('.'), span };
}

function help(args: string[]): string {
    if (args[0] !== 'commands' && args[0] !== 'command') return [
        '\\dbs, \\schemas, \\groups, \\dt [group], \\d group.table',
        '\\key create <label> [passphrase], \\key unlock <label|#prefix> [passphrase], \\keys, \\whoami',
        '\\author [<label|#prefix> [passphrase]|nobody]',
        '\\use database <name>, \\use group <name>, \\view, \\frontier [group]',
        '\\alias [scope] <name> <#prefix>, \\aliases [scope], \\unalias <scope> <name>, \\output table|json|vertical, \\hash-width auto|full|<N>, \\hash-labels on|off, \\ref-auto-update auto|self|off, \\dump schema|group|database <name> [full|schema], \\dump op [group] #hash',
        '\\delta schema|group <name> <start> <end> [bounded|full]',
        '\\quit',
        '\\help commands [filter]  (C-SQL reference)',
    ].join('\n');
    const filter = args.slice(1).join(' ').trim() || undefined;
    if (isLangCommonHelpQuery(filter)) return formatHelpEntry(LANG_COMMON_REF).join('\n');
    const refs = findLangCommandRefs(filter);
    if (refs.length === 0) return `No C-SQL commands match '${filter}'`;
    const lines: string[] = [];
    if (filter === undefined) lines.push('--- common ---', ...formatHelpEntry(LANG_COMMON_REF), '');
    const showSections = refs.length > 1 || filter === undefined;
    for (const section of LANG_COMMAND_SECTIONS) {
        const entries = refs.filter((entry) => entry.section === section);
        if (entries.length === 0) continue;
        if (showSections) lines.push(`--- ${section} ---`);
        for (const entry of entries) lines.push(...formatHelpEntry(entry), '');
    }
    if (lines[lines.length - 1] === '') lines.pop();
    return lines.join('\n');
}

function formatHelpEntry(ref: Pick<LangCommandRef, 'command' | 'syntax' | 'description'>): string[] {
    return [ref.command, ...ref.syntax.split('\n').map((line) => `  ${line}`), `  ${ref.description}`];
}
