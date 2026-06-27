import { dumpDatabase, dumpGroup, dumpSchema } from "@hyper-hyper-space/hhs3_rdb_lang";

import { formatRows } from "../format/table.js";
import { WorkspaceSession } from "../session/session.js";

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
        case 'help': return { handled: true, output: helpText() };
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
        case 'whoami': return { handled: true, output: (await session.currentAuthor())?.keyId ?? '(no identity selected)' };
        case 'use': return { handled: true, output: await useCommand(session, args) };
        case 'view': return { handled: true, output: session.defaultView === undefined ? 'VIEW LATEST' : formatView(session) };
        case 'frontier': return { handled: true, output: await frontier(session, args[0]) };
        case 'alias': return { handled: true, output: await alias(session, args) };
        case 'output': return { handled: true, output: setOutput(session, args[0]) };
        case 'dump': return { handled: true, output: await dump(session, args) };
        default: return { handled: true, output: `Unknown meta-command \\${command}` };
    }
}

function formatRoots(session: WorkspaceSession, kind: 'database' | 'schema' | 'group'): string {
    return formatRows(session.workspace.roots.list(kind).map((root) => ({
        name: root.name ?? '',
        id: root.id,
        type: root.type,
    })));
}

async function listTables(session: WorkspaceSession, groupName?: string): Promise<string> {
    const group = await session.workspace.roots.resolveGroup(ref(groupName ?? session.currentGroup ?? ''));
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
    const group = await session.workspace.roots.resolveGroup(ref(groupName));
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
    return formatRows(session.keystore.list().map((key) => ({
        label: key.label,
        keyId: key.keyId,
        unlocked: session.isUnlocked(key.keyId),
        selected: selectedKeyId === key.keyId,
    })));
}

type KeyCommandResult = Pick<MetaCommandResult, 'output' | 'needsPassphrase'>;

async function keyCommand(session: WorkspaceSession, args: string[]): Promise<KeyCommandResult> {
    if (session.keystore === undefined) throw new Error('No keystore configured');
    const [sub, label, passphrase] = args;
    if (sub === 'create') {
        if (label === undefined) throw new Error('Usage: \\key create <label> [passphrase]');
        if (passphrase === undefined) return { needsPassphrase: { kind: 'create', label } };
        const identity = await session.createKey(label, passphrase);
        return { output: `created ${label} ${identity.keyId}` };
    }
    if (sub === 'unlock') {
        if (label === undefined) throw new Error('Usage: \\key unlock <label|#prefix>');
        const record = session.keystore.resolveRecord(label);
        if (passphrase === undefined) return { needsPassphrase: { kind: 'unlock', label: record.label } };
        const identity = await session.unlockKey(label, passphrase);
        return { output: `unlocked ${identity.keyId}` };
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
    return session.keystore?.list().find((key) => key.keyId === keyId)?.label ?? keyId;
}

async function useCommand(session: WorkspaceSession, args: string[]): Promise<string> {
    const [kind, name] = args;
    if (kind === 'database') {
        const db = await session.workspace.roots.resolveDatabase(ref(name));
        session.setCurrentDatabase(db.id);
        return `using database ${db.id}`;
    }
    if (kind === 'group') {
        const group = await session.workspace.roots.resolveGroup(ref(name));
        session.setCurrentGroup(group.id);
        return `using group ${group.id}`;
    }
    throw new Error('Usage: \\use database <name> | \\use group <name>');
}

async function frontier(session: WorkspaceSession, groupName?: string): Promise<string> {
    const group = await session.workspace.roots.resolveGroup(ref(groupName ?? session.currentGroup ?? ''));
    if (group.group === undefined) throw new Error('Group is not loaded');
    return `{${[...(await (await group.group.getScopedDag()).getFrontier())].map((h) => `#${h}`).join(', ')}}`;
}

async function alias(session: WorkspaceSession, args: string[]): Promise<string> {
    const [name, target] = args;
    if (name === undefined || target === undefined) throw new Error('Usage: \\alias <name> <root|#prefix>');
    const id = await session.workspace.roots.resolveHash(ref(target), { kind: 'global' });
    await session.workspace.setAlias(name, id);
    return `${name} => ${id}`;
}

function setOutput(session: WorkspaceSession, mode: string | undefined): string {
    if (mode !== 'table' && mode !== 'json' && mode !== 'vertical') throw new Error('Usage: \\output table|json|vertical');
    session.setOutputMode(mode);
    return `output ${mode}`;
}

async function dump(session: WorkspaceSession, args: string[]): Promise<string> {
    const [kind, name] = args;
    if (kind === 'schema') {
        const schema = await session.workspace.roots.resolveSchema(ref(name));
        if (schema.schema === undefined) throw new Error('Schema is not loaded');
        return dumpSchema(schema.schema as Parameters<typeof dumpSchema>[0]);
    }
    if (kind === 'group') {
        const group = await session.workspace.roots.resolveGroup(ref(name));
        if (group.group === undefined) throw new Error('Group is not loaded');
        return dumpGroup(group.group as Parameters<typeof dumpGroup>[0]);
    }
    if (kind === 'database') {
        const db = await session.workspace.roots.resolveDatabase(ref(name));
        if (db.db === undefined) throw new Error('Database is not loaded');
        return dumpDatabase(db.db as Parameters<typeof dumpDatabase>[0]);
    }
    throw new Error('Usage: \\dump schema|group|database <name>');
}

function formatView(session: WorkspaceSession): string {
    if (session.defaultView === undefined) return 'VIEW LATEST';
    const at = [...session.defaultView.at].map((h) => `#${h}`).join(', ');
    const from = session.defaultView.from === undefined ? '' : ` FROM {${[...session.defaultView.from].map((h) => `#${h}`).join(', ')}}`;
    return `VIEW AT {${at}}${from}`;
}

function ref(text: string) {
    if (text.startsWith('#')) return { kind: 'hash' as const, prefix: text.slice(1), span: { start: 0, end: text.length, line: 1, column: 1 } };
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

function helpText(): string {
    return [
        '\\dbs, \\schemas, \\groups, \\dt [group], \\d group.table',
        '\\key create <label> [passphrase], \\key unlock <label|#prefix> [passphrase], \\keys, \\whoami',
        '\\author [<label|#prefix> [passphrase]|nobody]  (set/show default author; unlocks if needed; \\author nobody clears it)',
        '\\use database <name>, \\use group <name>, \\view, \\frontier [group]',
        '\\alias <name> <root|#prefix>, \\output table|json|vertical, \\dump schema|group|database <name>',
        '\\quit',
    ].join('\n');
}
