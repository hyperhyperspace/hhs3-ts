import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { RenderAliasContext, RenderVersionScope } from "@hyper-hyper-space/hhs3_rdb_lang";

import { decodePublicKey } from "../keys/identity.js";
import type { AliasScope } from "../session/aliases.js";
import type { WorkspaceSession } from "../session/session.js";

type DumpAliasHints = {
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
    private keyCounter = 0;
    private schemaCounter = 0;
    private groupCounter = 0;
    private dbCounter = 0;

    constructor(private readonly hints: DumpAliasHints) {}

    key(keyId: B64Hash, hint?: string): string {
        return this.ensure('key', keyId, hint ?? this.hints.keyLabels?.get(keyId) ?? this.nextFallback('key'));
    }

    schema(id: B64Hash, hint?: string): string {
        return this.ensure('schema', id, hint ?? this.hints.schemaNames?.get(id) ?? this.nextFallback('schema'));
    }

    group(id: B64Hash, hint?: string): string {
        return this.ensure('group', id, hint ?? this.hints.groupNames?.get(id) ?? this.nextFallback('group'));
    }

    db(id: B64Hash, hint?: string): string {
        return this.ensure('db', id, hint ?? this.hints.dbNames?.get(id) ?? this.nextFallback('db'));
    }

    version(hash: B64Hash, scope: RenderVersionScope): string {
        const existing = this.hashToName.get(`version:${hash}`);
        if (existing !== undefined) return existing;

        const n = (this.versionCounters.get(scope.objectId) ?? 0) + 1;
        this.versionCounters.set(scope.objectId, n);
        const baseName = `${scope.objectName}_ver${n}`;
        const name = this.uniqueName('version', baseName);
        this.register('version', hash, name);
        return name;
    }

    drainDefinitions(): string[] {
        const out = [...this.pending];
        this.pending.length = 0;
        return out;
    }

    lookupKeyAlias(keyId: B64Hash): string | undefined {
        return this.hashToName.get(`key:${keyId}`);
    }

    lookupPublicKeyAlias(serialized: string): string | undefined {
        for (const [keyId, pubkey] of this.hints.keyPublicKeys ?? []) {
            if (pubkey !== serialized) continue;
            const alias = this.lookupKeyAlias(keyId);
            if (alias !== undefined) return alias;
        }
        return undefined;
    }

    private ensure(scope: AliasScope, hash: B64Hash, preferred: string): string {
        const existing = this.hashToName.get(`${scope}:${hash}`);
        if (existing !== undefined) return existing;
        const name = this.uniqueName(scope, preferred);
        this.register(scope, hash, name);
        return name;
    }

    private register(scope: AliasScope, hash: B64Hash, name: string): void {
        this.hashToName.set(`${scope}:${hash}`, name);
        const emitKey = `${scope}:${name}`;
        if (this.emitted.has(emitKey)) return;
        this.emitted.add(emitKey);
        this.pending.push(`\\alias ${scope} ${name} #${hash}`);
    }

    private uniqueName(scope: AliasScope, preferred: string): string {
        let names = this.usedNames.get(scope);
        if (names === undefined) {
            names = new Set();
            this.usedNames.set(scope, names);
        }
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

    private nextFallback(scope: 'key' | 'schema' | 'group' | 'db'): string {
        switch (scope) {
            case 'key': return `keyId${++this.keyCounter}`;
            case 'schema': return `schema${++this.schemaCounter}`;
            case 'group': return `group${++this.groupCounter}`;
            case 'db': return `db${++this.dbCounter}`;
        }
    }
}

export function createDumpAliasContext(session: WorkspaceSession): DumpAliasContext {
    const keyLabels = new Map<B64Hash, string>();
    const keyPublicKeys = new Map<B64Hash, string>();
    if (session.keystore !== undefined) {
        for (const key of session.keystore.list()) {
            keyLabels.set(key.keyId, key.label);
            keyPublicKeys.set(key.keyId, serializePublicKeyToBase64(decodePublicKey(key.publicKey)));
        }
    }

    const schemaNames = new Map<B64Hash, string>();
    const groupNames = new Map<B64Hash, string>();
    const dbNames = new Map<B64Hash, string>();
    for (const root of session.workspace.roots.list()) {
        if (root.name === undefined) continue;
        if (root.kind === 'schema') schemaNames.set(root.id, root.name);
        if (root.kind === 'group') groupNames.set(root.id, root.name);
        if (root.kind === 'database') dbNames.set(root.id, root.name);
    }

    return new DumpAliasContext({ keyLabels, keyPublicKeys, schemaNames, groupNames, dbNames });
}

export function createDumpRenderOptions(session: WorkspaceSession, extra?: import("@hyper-hyper-space/hhs3_rdb_lang").RenderOptions) {
    const aliases = createDumpAliasContext(session);
    return { aliasMode: true, aliases, ...extra };
}
