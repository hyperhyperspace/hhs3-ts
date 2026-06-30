import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

import { formatRows } from "../format/table.js";
import type { RootIndex } from "../workspace/root_index.js";
import type { WorkspaceSession } from "./session.js";

export type AliasScope = 'key' | 'schema' | 'group' | 'db' | 'version';

export type AliasEntry = {
    scope: AliasScope;
    name: string;
    hash: B64Hash;
};

const ALL_SCOPES: AliasScope[] = ['db', 'schema', 'group', 'key', 'version'];

const SCOPE_KEYWORDS = new Set<string>(ALL_SCOPES);

export class AliasTable {
    private readonly tables = new Map<AliasScope, Map<string, B64Hash>>();

    set(scope: AliasScope, name: string, hash: B64Hash): void {
        let table = this.tables.get(scope);
        if (table === undefined) {
            table = new Map();
            this.tables.set(scope, table);
        }
        table.set(name, hash);
    }

    get(scope: AliasScope, name: string): B64Hash | undefined {
        return this.tables.get(scope)?.get(name);
    }

    delete(scope: AliasScope, name: string): boolean {
        return this.tables.get(scope)?.delete(name) === true;
    }

    list(scope?: AliasScope): AliasEntry[] {
        const scopes = scope === undefined ? ALL_SCOPES : [scope];
        const out: AliasEntry[] = [];
        for (const s of scopes) {
            const table = this.tables.get(s);
            if (table === undefined) continue;
            for (const [name, hash] of table) out.push({ scope: s, name, hash });
        }
        return out;
    }
}

export function isAliasScope(text: string): text is AliasScope {
    return SCOPE_KEYWORDS.has(text);
}

export type AliasTarget = {
    scope: AliasScope;
    hash: B64Hash;
};

export async function resolveAliasTarget(
    scope: AliasScope | 'auto',
    prefix: string,
    session: WorkspaceSession,
): Promise<AliasTarget[]> {
    const normalized = prefix.startsWith('#') ? prefix.slice(1) : prefix;
    const searchScopes = scope === 'auto' ? ALL_SCOPES : [scope];
    const matches: AliasTarget[] = [];

    for (const s of searchScopes) {
        for (const hash of await candidatesForScope(s, session)) {
            if (hash.startsWith(normalized)) matches.push({ scope: s, hash });
        }
    }

    const deduped = dedupeByScope(matches);
    if (deduped.length === 0) throw new Error(`Unknown hash prefix '#${normalized}'`);
    if (deduped.length === 1) return deduped;

    const uniqueHashes = new Set(deduped.map((m) => m.hash));
    if (uniqueHashes.size === 1) return deduped;

    const lines = deduped.map((m) => `  ${m.scope.padEnd(7)} => ${m.hash}`).join('\n');
    throw new Error(
        `Ambiguous alias prefix '#${normalized}' (${deduped.length} matches):\n${lines}\n`
        + 'Retry with a longer prefix or \\alias <scope> <name> #...',
    );
}

function dedupeByScope(matches: AliasTarget[]): AliasTarget[] {
    const seen = new Set<AliasScope>();
    const out: AliasTarget[] = [];
    for (const m of matches) {
        if (seen.has(m.scope)) continue;
        seen.add(m.scope);
        out.push(m);
    }
    return out;
}

async function candidatesForScope(scope: AliasScope, session: WorkspaceSession): Promise<B64Hash[]> {
    if (scope === 'key') {
        if (session.keystore === undefined) return [];
        return session.keystore.list().map((k) => k.keyId);
    }
    if (scope === 'version') return collectVersionOpHashes(session.workspace.roots);
    const kind = scope === 'db' ? 'database' : scope;
    return session.workspace.roots.list(kind).map((r) => r.id);
}

export async function collectVersionOpHashes(roots: RootIndex): Promise<B64Hash[]> {
    const hashes: B64Hash[] = [];
    for (const root of roots.list()) {
        if (root.object === undefined) continue;
        const dag = await root.object.getScopedDag();
        for await (const entry of dag.loadAllEntries()) hashes.push(entry.hash);
    }
    return hashes;
}

export async function aliasLabel(
    scope: AliasScope,
    hash: B64Hash,
    session: WorkspaceSession,
): Promise<string | undefined> {
    if (scope === 'key') {
        return session.keystore?.list().find((k) => k.keyId === hash)?.label;
    }
    if (scope === 'version') return rootNameForVersionHash(hash, session.workspace.roots);
    return session.workspace.roots.get(hash)?.name;
}

export async function rootNameForVersionHash(hash: B64Hash, roots: RootIndex): Promise<string | undefined> {
    for (const root of roots.list()) {
        if (root.object === undefined) continue;
        const dag = await root.object.getScopedDag();
        if (await dag.loadEntry(hash) !== undefined) return root.name;
    }
    return undefined;
}

export async function formatAliasResult(
    scope: AliasScope,
    name: string,
    hash: B64Hash,
    session: WorkspaceSession,
): Promise<string> {
    const label = await aliasLabel(scope, hash, session);
    const suffix = label === undefined ? '' : ` (${label})`;
    return `${scope} ${name} => ${hash}${suffix}`;
}

export async function formatAliasListing(session: WorkspaceSession, scope?: AliasScope): Promise<string> {
    const rows = await Promise.all(session.aliases.list(scope).map(async (entry) => ({
        scope: entry.scope,
        name: entry.name,
        hash: entry.hash,
        label: (await aliasLabel(entry.scope, entry.hash, session)) ?? '',
    })));
    return formatRows(rows);
}
