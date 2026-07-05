import { dumpDatabase, dumpGroup, dumpSchema, findLangCommandRefs, isLangCommonHelpQuery, LANG_COMMAND_SECTIONS, LANG_COMMON_REF, type LangCommandRef, type RenderOptions } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RSchema, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import { runDumpOpCommand } from "../dump/op_command.js";
import { runDeltaCommand } from "../delta/delta_command.js";
import { createDumpRenderOptions } from "../dump/alias_context.js";
import { createDisplayContext, formatDisplayString, formatSessionRows } from "../format/display.js";
import { formatRows } from "../format/rows.js";
import {
    formatAliasListing,
    formatAliasResult,
    isAliasScope,
    resolveAliasTarget,
    type AliasScope,
} from "../session/aliases.js";
import { WorkspaceSession, type RefAutoUpdateMode } from "../session/session.js";
import type { RootResolveContext } from "../workspace/root_index.js";

export type MetaCommandResult = {
    handled: boolean;
    output?: string;
    quit?: boolean;
    needsPassphrase?: { kind: 'create' | 'unlock' | 'author'; label: string };
};

export async function runMetaCommand(session: WorkspaceSession, line: string): Promise<MetaCommandResult> {
    if (!line.trimStart().startsWith('\\')) return { handled: false };
    const normalized = line.trim().replace(/;$/, '').trim();
    const [command, ...args] = normalized.slice(1).split(/\s+/);

    switch (command) {
        case 'help': return { handled: true, output: helpCommand(args) };
        case 'q':
        case 'quit': return { handled: true, quit: true };
        case 'dbs': return { handled: true, output: formatRoots(session, 'database') };
        case 'schemas': return { handled: true, output: formatRoots(session, 'schema') };
        case 'groups': return { handled: true, output: formatRoots(session, 'group') };
        case 'dt': return { handled: true, output: await listTables(session, args[0]) };
        case 'd': return { handled: true, output: await describeTable(session, args[0]) };
        case 'keys': return { handled: true, output: await listKeys(session) };
        case 'key': return { handled: true, ...await keyCommand(session, args) };
        case 'author': return { handled: true, ...await authorCommand(session, args) };
        case 'whoami': {
            const identity = await session.currentAuthor();
            if (identity === undefined) return { handled: true, output: '(no identity selected)' };
            return { handled: true, output: formatDisplayString(session, identity.keyId, { role: 'hash' }) };
        }
        case 'use': return { handled: true, output: await useCommand(session, args) };
        case 'view': return { handled: true, output: session.defaultView === undefined ? 'VIEW LATEST' : formatView(session) };
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

function formatRoots(session: WorkspaceSession, kind: 'database' | 'schema' | 'group'): string {
    const rows = session.workspace.roots.list(kind).map((root) => ({
        name: root.name ?? '',
        id: root.id,
        type: root.type,
    }));
    return formatSessionRows(session, rows, undefined, { structuralColumns: new Set(['id']) });
}

function rootCtx(session: WorkspaceSession): RootResolveContext {
    return { aliases: session.aliases };
}

async function listTables(session: WorkspaceSession, groupName?: string): Promise<string> {
    const group = await session.workspace.roots.resolveGroup(ref(groupName ?? session.currentGroup ?? ''), rootCtx(session));
    if (group.group === undefined) throw new Error('Group is not loaded');
    const schema = (await group.group.getView()).getSchemaView();
    return formatRows(schema.getTableNames().map((table) => ({ table })));
}

async function describeTable(session: WorkspaceSession, tableRef: string | undefined): Promise<string> {
    if (tableRef === undefined) throw new Error('Usage: \\d group.table');
    const parts = tableRef.split('.');
    const groupName = parts.length > 1 ? parts[0] : session.currentGroup;
    const tableName = parts.length > 1 ? parts.slice(1).join('.') : parts[0];
    if (groupName === undefined) throw new Error('No current group; use \\d group.table');
    const group = await session.workspace.roots.resolveGroup(ref(groupName), rootCtx(session));
    if (group.group === undefined) throw new Error('Group is not loaded');
    const schema = (await group.group.getView()).getSchemaView();
    const table = schema.getTable(tableName);
    if (table === undefined) throw new Error(`Unknown table '${tableName}'`);
    return formatRows(Object.entries(table.columns).map(([name, def]) => ({
        column: name,
        type: def.type,
        pub: def.pub === true,
        readonly: def.readonly === true,
        nullable: def.nullable === true,
    })));
}

async function listKeys(session: WorkspaceSession): Promise<string> {
    if (session.keystore === undefined) return '(no keystore)';
    const selectedKeyId = (await session.currentAuthor())?.keyId;
    const rows = session.keystore.list().map((key) => ({
        label: key.label,
        keyId: key.keyId,
        unlocked: session.isUnlocked(key.keyId),
        selected: selectedKeyId === key.keyId,
    }));
    return formatSessionRows(session, rows, undefined, { structuralColumns: new Set(['keyId']) });
}

type KeyCommandResult = Pick<MetaCommandResult, 'output' | 'needsPassphrase'>;

async function keyCommand(session: WorkspaceSession, args: string[]): Promise<KeyCommandResult> {
    if (session.keystore === undefined) throw new Error('No keystore configured');
    const [sub, label, passphrase] = args;
    if (sub === 'create') {
        if (label === undefined) throw new Error('Usage: \\key create <label> [passphrase]');
        if (passphrase === undefined) return { needsPassphrase: { kind: 'create', label } };
        const identity = await session.createKey(label, passphrase);
        return { output: `created ${label} ${formatDisplayString(session, identity.keyId, { role: 'hash' })}` };
    }
    if (sub === 'unlock') {
        if (label === undefined) throw new Error('Usage: \\key unlock <label|#prefix>');
        const record = session.keystore.resolveRecord(session.resolveKeyRef(label));
        if (passphrase === undefined) return { needsPassphrase: { kind: 'unlock', label: record.label } };
        const identity = await session.unlockKey(label, passphrase);
        return { output: `unlocked ${formatDisplayString(session, identity.keyId, { role: 'hash' })}` };
    }
    throw new Error('Usage: \\key create|unlock ...');
}

// Manage the default author: who writes are signed as when a statement omits a
// BY clause. Selecting a key that is still locked unlocks it first (prompting
// for the passphrase, or accepting one inline for scripts).
async function authorCommand(session: WorkspaceSession, args: string[]): Promise<KeyCommandResult> {
    if (session.keystore === undefined) throw new Error('No keystore configured');
    const [target, passphrase] = args;
    if (target === undefined) {
        const identity = await session.currentAuthor();
        return { output: identity === undefined ? 'nobody' : labelFor(session, identity.keyId) };
    }
    // A key literally labelled "nobody" is selected via its #keyid prefix.
    if (target.toLowerCase() === 'nobody') {
        session.clearAuthor();
        return { output: 'author nobody' };
    }
    // resolveIdentity throws on an unknown/ambiguous key, and returns undefined
    // only when the key is known but still locked.
    if (session.resolveIdentity(target) === undefined) {
        if (passphrase === undefined) return { needsPassphrase: { kind: 'author', label: target } };
        await session.unlockKey(target, passphrase);
    }
    const identity = session.selectAuthor(target);
    return { output: `author ${labelFor(session, identity.keyId)}` };
}

function labelFor(session: WorkspaceSession, keyId: string): string {
    const label = session.keystore?.list().find((key) => key.keyId === keyId)?.label;
    if (label !== undefined) return label;
    return formatDisplayString(session, keyId, { role: 'hash' });
}

async function useCommand(session: WorkspaceSession, args: string[]): Promise<string> {
    const [kind, name] = args;
    if (kind === 'database') {
        const db = await session.workspace.roots.resolveDatabase(ref(name), rootCtx(session));
        session.setCurrentDatabase(db.id);
        return `using database ${formatDisplayString(session, db.id, { role: 'hash' })}`;
    }
    if (kind === 'group') {
        const group = await session.workspace.roots.resolveGroup(ref(name), rootCtx(session));
        session.setCurrentGroup(group.id);
        return `using group ${formatDisplayString(session, group.id, { role: 'hash' })}`;
    }
    throw new Error('Usage: \\use database <name> | \\use group <name>');
}

async function frontier(session: WorkspaceSession, groupName?: string): Promise<string> {
    const group = await session.workspace.roots.resolveGroup(ref(groupName ?? session.currentGroup ?? ''), rootCtx(session));
    if (group.group === undefined) throw new Error('Group is not loaded');
    const hashes = [...(await (await group.group.getScopedDag()).getFrontier())];
    const ctx = createDisplayContext(session, hashes);
    return `{${hashes.map((h) => ctx.formatString(h, { role: 'hash', hashPrefix: true })).join(', ')}}`;
}

async function alias(session: WorkspaceSession, args: string[]): Promise<string> {
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
    for (const r of resolved) session.aliases.set(r.scope, name, r.hash);
    const lines = await Promise.all(resolved.map((r) => formatAliasResult(r.scope, name, r.hash, session)));
    return lines.join('\n');
}

async function aliases(session: WorkspaceSession, scopeArg: string | undefined): Promise<string> {
    const scope = scopeArg !== undefined && scopeArg !== '' && isAliasScope(scopeArg) ? scopeArg : undefined;
    if (scopeArg !== undefined && scopeArg !== '' && scope === undefined) {
        throw new Error(`Unknown alias scope '${scopeArg}'`);
    }
    const rows = await formatAliasListing(session, scope);
    return rows === '' ? '(no aliases)' : rows;
}

function unalias(session: WorkspaceSession, args: string[]): string {
    const [scopeArg, name] = args;
    if (scopeArg === undefined || name === undefined || !isAliasScope(scopeArg)) {
        throw new Error('Usage: \\unalias <scope> <name>');
    }
    if (!session.aliases.delete(scopeArg, name)) throw new Error(`No alias '${name}' in scope '${scopeArg}'`);
    return `removed ${scopeArg} ${name}`;
}

function setOutput(session: WorkspaceSession, mode: string | undefined): string {
    if (mode !== 'table' && mode !== 'json' && mode !== 'vertical') throw new Error('Usage: \\output table|json|vertical');
    session.setOutputMode(mode);
    return `output ${mode}`;
}

function setHashWidth(session: WorkspaceSession, width: string | undefined): string {
    if (width === undefined) throw new Error('Usage: \\hash-width auto|full|<N>');
    if (width === 'auto') {
        session.setHashWidth('auto');
        return 'hash-width auto';
    }
    if (width === 'full') {
        session.setHashWidth('full');
        return 'hash-width full';
    }
    const n = Number(width);
    if (!Number.isInteger(n) || n <= 0) throw new Error('Usage: \\hash-width auto|full|<N>');
    session.setHashWidth(n);
    return `hash-width ${n}`;
}

function setHashLabels(session: WorkspaceSession, mode: string | undefined): string {
    if (mode !== 'on' && mode !== 'off') throw new Error('Usage: \\hash-labels on|off');
    session.setHashLabels(mode === 'on');
    return `hash-labels ${mode}`;
}

function normalizeRefAutoUpdateMode(mode: string | undefined): RefAutoUpdateMode {
    if (mode === undefined) throw new Error('Usage: \\ref-auto-update auto|self|off');
    if (mode === 'on') return 'auto';
    if (mode === 'auto' || mode === 'self' || mode === 'off') return mode;
    throw new Error('Usage: \\ref-auto-update auto|self|off');
}

function setRefAutoUpdate(session: WorkspaceSession, mode: string | undefined): string {
    const normalized = normalizeRefAutoUpdateMode(mode);
    session.setRefAutoUpdate(normalized);
    return `ref-auto-update ${normalized}`;
}

async function dump(session: WorkspaceSession, args: string[]): Promise<string> {
    const [kind, name] = args;
    const dumpRender = (extra?: RenderOptions): RenderOptions => createDumpRenderOptions(session, extra);
    if (kind === 'op') return runDumpOpCommand(session, args.slice(1));
    if (kind === 'schema') {
        const schema = await session.workspace.roots.resolveSchema(ref(name), rootCtx(session));
        if (schema.schema === undefined) throw new Error('Schema is not loaded');
        return dumpSchema(schema.schema as Parameters<typeof dumpSchema>[0], { render: dumpRender() });
    }
    if (kind === 'group') {
        const group = await session.workspace.roots.resolveGroup(ref(name), rootCtx(session));
        if (group.group === undefined) throw new Error('Group is not loaded');
        return dumpGroup(group.group as Parameters<typeof dumpGroup>[0], { render: dumpRender() });
    }
    if (kind === 'database') {
        const dbName = name;
        if (dbName === undefined) throw new Error('Usage: \\dump database <name> [full|schema]');
        const modeArg = args[2];
        const mode = modeArg === 'schema' ? 'schema' : 'full';
        const db = await session.workspace.roots.resolveDatabase(ref(dbName), rootCtx(session));
        if (db.db === undefined) throw new Error('Database is not loaded');
        return dumpDatabase(db.db as Parameters<typeof dumpDatabase>[0], {
            mode,
            loadSchema: (id) => loadSchemaObject(session, id),
            loadGroup: (id) => loadGroupObject(session, id),
            render: dumpRender({ profile: mode }),
        });
    }
    throw new Error('Usage: \\dump schema|group|database <name> [full|schema] | \\dump op [group] #hash');
}

async function loadRootObject(session: WorkspaceSession, id: B64Hash): Promise<RObject> {
    const record = session.workspace.roots.get(id);
    if (record?.object !== undefined) return record.object;
    const object = await session.workspace.replica.getObject(id);
    if (object === undefined) throw new Error(`Object '${id}' is not loaded`);
    return object;
}

async function loadSchemaObject(session: WorkspaceSession, id: B64Hash) {
    return loadRootObject(session, id) as Promise<RSchema & Parameters<typeof dumpSchema>[0]>;
}

async function loadGroupObject(session: WorkspaceSession, id: B64Hash) {
    return loadRootObject(session, id) as Promise<RTableGroup & Parameters<typeof dumpGroup>[0]>;
}

function formatView(session: WorkspaceSession): string {
    if (session.defaultView === undefined) return 'VIEW LATEST';
    const atHashes = [...session.defaultView.at];
    const fromHashes = session.defaultView.from === undefined ? [] : [...session.defaultView.from];
    const truncatable = [...atHashes, ...fromHashes];
    const ctx = createDisplayContext(session, truncatable);
    const at = atHashes.map((h) => ctx.formatString(h, { role: 'hash', hashPrefix: true })).join(', ');
    const from = fromHashes.length === 0
        ? ''
        : ` FROM {${fromHashes.map((h) => ctx.formatString(h, { role: 'hash', hashPrefix: true })).join(', ')}}`;
    return `VIEW AT {${at}}${from}`;
}

function ref(text: string) {
    if (text.startsWith('#')) return { kind: 'hash' as const, prefix: text.slice(1), span: { start: 0, end: text.length, line: 1, column: 1 } };
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

function helpCommand(args: string[]): string {
    const topic = args[0];
    if (topic === 'commands' || topic === 'command') {
        const filter = args.slice(1).join(' ').trim();
        return formatLangHelp(filter === '' ? undefined : filter);
    }
    return helpText();
}

function formatLangHelp(filter?: string): string {
    if (isLangCommonHelpQuery(filter)) {
        return formatHelpEntry(LANG_COMMON_REF).join('\n');
    }

    const refs = findLangCommandRefs(filter);
    if (refs.length === 0) {
        return `No C-SQL commands match '${filter}'`;
    }

    const lines: string[] = [];
    if (filter === undefined) {
        lines.push('--- common ---');
        lines.push(...formatHelpEntry(LANG_COMMON_REF));
        lines.push('');
    }

    const showSections = refs.length > 1 || filter === undefined;
    for (const section of LANG_COMMAND_SECTIONS) {
        const sectionRefs = refs.filter((ref) => ref.section === section);
        if (sectionRefs.length === 0) continue;
        if (showSections) lines.push(`--- ${section} ---`);
        for (const ref of sectionRefs) {
            lines.push(...formatHelpEntry(ref));
            lines.push('');
        }
    }
    if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
    return lines.join('\n');
}

function formatHelpEntry(ref: Pick<LangCommandRef, 'command' | 'syntax' | 'description'>): string[] {
    const syntaxLines = ref.syntax.split('\n');
    return [
        ref.command,
        ...syntaxLines.map((line) => `  ${line}`),
        `  ${ref.description}`,
    ];
}

function helpText(): string {
    return [
        '\\dbs, \\schemas, \\groups, \\dt [group], \\d group.table',
        '\\key create <label> [passphrase], \\key unlock <label|#prefix> [passphrase], \\keys, \\whoami',
        '\\author [<label|#prefix> [passphrase]|nobody]  (set/show default author; unlocks if needed; \\author nobody clears it)',
        '\\use database <name>, \\use group <name>, \\view, \\frontier [group]',
        '\\alias [scope] <name> <#prefix>, \\aliases [scope], \\unalias <scope> <name>, \\output table|json|vertical, \\hash-width auto|full|<N>, \\hash-labels on|off, \\ref-auto-update auto|self|off, \\dump schema|group|database <name> [full|schema], \\dump op [group] #hash',
        '\\delta schema|group <name> <start> <end> [bounded|full]  (schema = spec migrations; group = rows + schema + op void flips + reasons)',
        '\\quit',
        '\\help commands [filter]  (C-SQL reference)',
    ].join('\n');
}
