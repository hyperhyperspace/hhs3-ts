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
): Promise<AliasTarget> {
    const normalized = prefix.startsWith('#') ? prefix.slice(1) : prefix;
    const searchScopes = scope === 'auto' ? ALL_SCOPES : [scope];
    const matches: AliasTarget[] = [];

    for (const s of searchScopes) {
        for (const hash of await candidatesForScope(s, session)) {
            if (hash.startsWith(normalized)) matches.push({ scope: s, hash });
        }
    }

    if (matches.length === 0) throw new Error(`Unknown hash prefix '#${normalized}'`);
    if (matches.length > 1) {
        const lines = matches.map((m) => `  ${m.scope.padEnd(7)} => ${m.hash}`).join('\n');
        throw new Error(
            `Ambiguous alias prefix '#${normalized}' (${matches.length} matches):\n${lines}\n`
            + 'Retry with a longer prefix or \\alias <scope> <name> #...',
        );
    }
    return matches[0];
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

export function aliasLabel(scope: AliasScope, hash: B64Hash, session: WorkspaceSession): string | undefined {
    if (scope === 'key') {
        return session.keystore?.list().find((k) => k.keyId === hash)?.label;
    }
    if (scope === 'version') return undefined;
    return session.workspace.roots.get(hash)?.name;
}

export function formatAliasResult(scope: AliasScope, name: string, hash: B64Hash, session: WorkspaceSession): string {
    const label = aliasLabel(scope, hash, session);
    const suffix = label === undefined ? '' : ` (${label})`;
    return `${scope} ${name} => ${hash}${suffix}`;
}

export function formatAliasListing(session: WorkspaceSession, scope?: AliasScope): string {
    return formatRows(session.aliases.list(scope).map((entry) => ({
        scope: entry.scope,
        name: entry.name,
        hash: entry.hash,
        label: aliasLabel(entry.scope, entry.hash, session) ?? '',
    })));
}
