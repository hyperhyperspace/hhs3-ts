import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { RenderAliasContext, RenderOptions, RenderVersionScope } from "@hyper-hyper-space/hhs3_rdb_lang";
import { decodePublicKey, type AliasScope } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { ReplSession } from "../session.js";

export type DumpAliasHints = {
    keyLabels?: Map<B64Hash, string>;
    keyPublicKeys?: Map<B64Hash, string>;
    schemaNames?: Map<B64Hash, string>;
    groupNames?: Map<B64Hash, string>;
    dbNames?: Map<B64Hash, string>;
};

export class DumpAliasContext implements RenderAliasContext {
    private readonly emitted = new Set<string>();
    private readonly hashToName = new Map<string, string>();
    private readonly usedNames = new Map<AliasScope, Set<string>>();
    private readonly pending: string[] = [];
    private readonly versionCounters = new Map<B64Hash, number>();
    private counters = { key: 0, schema: 0, group: 0, db: 0 };

    constructor(private readonly hints: DumpAliasHints) {}

    key(id: B64Hash, hint?: string): string { return this.ensure('key', id, hint ?? this.hints.keyLabels?.get(id) ?? this.next('key')); }
    schema(id: B64Hash, hint?: string): string { return this.ensure('schema', id, hint ?? this.hints.schemaNames?.get(id) ?? this.next('schema')); }
    group(id: B64Hash, hint?: string): string { return this.ensure('group', id, hint ?? this.hints.groupNames?.get(id) ?? this.next('group')); }
    db(id: B64Hash, hint?: string): string { return this.ensure('db', id, hint ?? this.hints.dbNames?.get(id) ?? this.next('db')); }

    version(hash: B64Hash, scope: RenderVersionScope): string {
        const existing = this.hashToName.get(`version:${hash}`);
        if (existing !== undefined) return existing;
        const n = (this.versionCounters.get(scope.objectId) ?? 0) + 1;
        this.versionCounters.set(scope.objectId, n);
        const name = this.unique('version', `${scope.objectName}_ver${n}`);
        this.register('version', hash, name);
        return name;
    }

    drainDefinitions(): string[] {
        const result = [...this.pending];
        this.pending.length = 0;
        return result;
    }

    lookupKeyAlias(id: B64Hash): string | undefined { return this.hashToName.get(`key:${id}`); }
    lookupPublicKeyAlias(serialized: string): string | undefined {
        for (const [id, publicKey] of this.hints.keyPublicKeys ?? []) {
            if (publicKey === serialized) return this.lookupKeyAlias(id);
        }
        return undefined;
    }

    private ensure(scope: AliasScope, hash: B64Hash, preferred: string): string {
        const existing = this.hashToName.get(`${scope}:${hash}`);
        if (existing !== undefined) return existing;
        const name = this.unique(scope, preferred);
        this.register(scope, hash, name);
        return name;
    }

    private register(scope: AliasScope, hash: B64Hash, name: string): void {
        this.hashToName.set(`${scope}:${hash}`, name);
        const key = `${scope}:${name}`;
        if (this.emitted.has(key)) return;
        this.emitted.add(key);
        this.pending.push(`\\alias ${scope} ${name} #${hash}`);
    }

    private unique(scope: AliasScope, preferred: string): string {
        const names = this.usedNames.get(scope) ?? new Set<string>();
        this.usedNames.set(scope, names);
        if (!names.has(preferred)) {
            names.add(preferred);
            return preferred;
        }
        let i = 2;
        while (names.has(`${preferred}${i}`)) i++;
        const name = `${preferred}${i}`;
        names.add(name);
        return name;
    }

    private next(scope: keyof typeof this.counters): string {
        const number = ++this.counters[scope];
        return scope === 'key' ? `keyId${number}` : `${scope}${number}`;
    }
}

export function createDumpAliasContext(session: ReplSession): DumpAliasContext {
    const hints: DumpAliasHints = {
        keyLabels: new Map(), keyPublicKeys: new Map(), schemaNames: new Map(), groupNames: new Map(), dbNames: new Map(),
    };
    for (const key of session.keyVault?.list() ?? []) {
        hints.keyLabels!.set(key.keyId, key.label);
        hints.keyPublicKeys!.set(key.keyId, serializePublicKeyToBase64(decodePublicKey(key.publicKey)));
    }
    for (const root of session.workspace.roots.list()) {
        if (root.name === undefined) continue;
        if (root.kind === 'schema') hints.schemaNames!.set(root.id, root.name);
        if (root.kind === 'group') hints.groupNames!.set(root.id, root.name);
        if (root.kind === 'database') hints.dbNames!.set(root.id, root.name);
    }
    return new DumpAliasContext(hints);
}

export function createDumpRenderOptions(session: ReplSession, extra?: RenderOptions): RenderOptions {
    return { aliasMode: true, aliases: createDumpAliasContext(session), ...extra };
}
