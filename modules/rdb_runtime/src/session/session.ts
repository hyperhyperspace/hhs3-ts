import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { AuthorRef, LangValue } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { KeyVault } from "../keys/key_vault.js";
import type { RdbWorkspace } from "../workspace/workspace.js";
import { AliasTable } from "./aliases.js";

export type RefAutoUpdateMode = 'auto' | 'self' | 'off';

export type SessionView = {
    at: Version;
    from?: Version;
};

export type RdbSessionOptions = {
    workspace: RdbWorkspace;
    keyVault?: KeyVault;
    refAutoUpdate?: RefAutoUpdateMode;
    createUuid?: () => string;
};

export class KeyPassphraseRequiredError extends Error {
    constructor(readonly label: string) {
        super(`Key '${label}' is not unlocked`);
        this.name = 'KeyPassphraseRequiredError';
    }
}

export class RdbSession {
    readonly workspace: RdbWorkspace;
    readonly variables = new Map<string, LangValue>();
    readonly aliases = new AliasTable();

    private readonly unlocked = new Map<KeyId, OwnIdentity>();
    private currentAuthorKeyId?: KeyId;
    private readonly createUuidFn: () => string;

    keyVault?: KeyVault;
    currentDatabase?: B64Hash;
    currentGroup?: B64Hash;
    defaultView?: SessionView;
    refAutoUpdate: RefAutoUpdateMode;

    constructor(options: RdbSessionOptions) {
        this.workspace = options.workspace;
        this.keyVault = options.keyVault;
        this.refAutoUpdate = options.refAutoUpdate ?? 'off';
        this.createUuidFn = options.createUuid ?? (() => globalThis.crypto.randomUUID());
    }

    async createKey(label: string, passphrase: string): Promise<OwnIdentity> {
        if (this.keyVault === undefined) throw new Error('No key vault configured');
        const identity = await this.keyVault.create(label, passphrase);
        this.unlocked.set(identity.keyId, identity);
        return identity;
    }

    async unlockKey(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        if (this.keyVault === undefined) throw new Error('No key vault configured');
        const identity = await this.keyVault.unlock(this.resolveKeyRef(labelOrPrefix), passphrase);
        this.unlocked.set(identity.keyId, identity);
        return identity;
    }

    selectAuthor(labelOrPrefix: string): OwnIdentity {
        if (this.keyVault === undefined) throw new Error('No key vault configured');
        const record = this.keyVault.resolveRecord(this.resolveKeyRef(labelOrPrefix));
        const identity = this.unlocked.get(record.keyId);
        if (identity === undefined) throw new Error(`Key '${record.label}' is locked`);
        this.currentAuthorKeyId = identity.keyId;
        return identity;
    }

    clearAuthor(): void {
        this.currentAuthorKeyId = undefined;
    }

    isUnlocked(keyId: KeyId): boolean {
        return this.unlocked.has(keyId);
    }

    setCurrentDatabase(id: B64Hash): void {
        this.currentDatabase = id;
    }

    setCurrentGroup(id: B64Hash): void {
        this.currentGroup = id;
    }

    setDefaultView(view: SessionView): void {
        this.defaultView = view;
    }

    clearDefaultView(): void {
        this.defaultView = undefined;
    }

    setRefAutoUpdate(mode: RefAutoUpdateMode): void {
        this.refAutoUpdate = mode;
    }

    setVariable(name: string, value: LangValue): void {
        this.variables.set(name, value);
    }

    selectedAuthor(): OwnIdentity | undefined {
        return this.currentAuthorKeyId === undefined ? undefined : this.unlocked.get(this.currentAuthorKeyId);
    }

    async currentAuthor(): Promise<OwnIdentity | undefined> {
        return this.selectedAuthor();
    }

    resolveIdentity(labelOrPrefix: string): OwnIdentity | undefined {
        if (this.keyVault === undefined) throw new Error('No key vault configured');
        const record = this.keyVault.resolveRecord(this.resolveKeyRef(labelOrPrefix));
        return this.unlocked.get(record.keyId);
    }

    async resolveAuthor(ref: AuthorRef): Promise<OwnIdentity> {
        const labelOrPrefix = ref.kind === 'variable'
            ? this.resolveKeyRef(ref.name)
            : `#${ref.prefix}`;
        const identity = this.resolveIdentity(labelOrPrefix);
        if (identity !== undefined) return identity;
        const record = this.keyVault!.resolveRecord(labelOrPrefix);
        throw new KeyPassphraseRequiredError(record.label);
    }

    async resolveVariable(name: string): Promise<LangValue> {
        if (name === 'me' || name === 'author') {
            const identity = await this.currentAuthor();
            if (identity !== undefined) return identity;
        }

        const explicit = this.variables.get(name);
        if (explicit !== undefined) return explicit;

        const keyAlias = this.aliases.get('key', name);
        if (keyAlias !== undefined) {
            if (this.keyVault === undefined) throw new Error('No key vault configured');
            const record = this.keyVault.resolvePublic(keyAlias);
            return { keyId: record.keyId, publicKey: record.publicKey };
        }

        if (this.keyVault !== undefined) {
            try {
                const publicKey = this.keyVault.resolvePublic(name);
                return { keyId: publicKey.keyId, publicKey: publicKey.publicKey };
            } catch (_e) {
                // Keep the user-facing error centered on the missing variable.
            }
        }

        throw new Error(`Unknown variable '$${name}'`);
    }

    async resolvePublicKey(labelOrPrefix: string): Promise<{ keyId: KeyId; publicKey: PublicKey }> {
        if (this.keyVault === undefined) throw new Error('No key vault configured');
        const record = this.keyVault.resolvePublic(this.resolveKeyRef(labelOrPrefix));
        return { keyId: record.keyId, publicKey: record.publicKey };
    }

    resolveKeyRef(labelOrPrefix: string): string {
        if (labelOrPrefix.startsWith('#')) return labelOrPrefix;
        const aliased = this.aliases.get('key', labelOrPrefix);
        return aliased ?? labelOrPrefix;
    }

    createUuid(): string {
        return this.createUuidFn();
    }

    createSeed(kind: 'rdb' | 'group', name?: string): string {
        const hash = this.workspace.replica.getHashSuite();
        return hash.hashToB64(new TextEncoder().encode(`${kind}:${name ?? ''}:${this.createUuid()}`));
    }
}
