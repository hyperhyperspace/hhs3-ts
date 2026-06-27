import { randomUUID } from "node:crypto";

import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { AuthorRef, LangValue } from "@hyper-hyper-space/hhs3_rdb_lang";

import { KeyStore } from "../keys/keystore.js";
import { Workspace } from "../workspace/workspace.js";

export type OutputMode = 'table' | 'json' | 'vertical';

export type SessionView = {
    at: Version;
    from?: Version;
};

export type WorkspaceSessionOptions = {
    workspace: Workspace;
    keystore?: KeyStore;
    outputMode?: OutputMode;
};

export class WorkspaceSession {
    readonly workspace: Workspace;
    readonly variables = new Map<string, LangValue>();

    // Session state: which keys have been unlocked this session and which one is
    // the current default author. The keystore is a pure on-disk vault and owns
    // none of this.
    private readonly unlocked = new Map<KeyId, OwnIdentity>();
    private currentAuthorKeyId?: KeyId;

    keystore?: KeyStore;
    currentDatabase?: B64Hash;
    currentGroup?: B64Hash;
    defaultView?: SessionView;
    outputMode: OutputMode;
    stopOnError = true;

    constructor(options: WorkspaceSessionOptions) {
        this.workspace = options.workspace;
        this.keystore = options.keystore;
        this.outputMode = options.outputMode ?? 'table';
    }

    // Create a new key in the vault and add it to the unlocked set. Like
    // unlockKey, this does NOT make it the default author.
    async createKey(label: string, passphrase: string): Promise<OwnIdentity> {
        if (this.keystore === undefined) throw new Error('No keystore configured');
        const identity = await this.keystore.create(label, passphrase);
        this.unlocked.set(identity.keyId, identity);
        return identity;
    }

    // Decrypt a stored key and add it to the unlocked set for this session.
    async unlockKey(labelOrPrefix: string, passphrase: string): Promise<OwnIdentity> {
        if (this.keystore === undefined) throw new Error('No keystore configured');
        const identity = await this.keystore.unlock(labelOrPrefix, passphrase);
        this.unlocked.set(identity.keyId, identity);
        return identity;
    }

    // Set the default author. The key must already be unlocked this session.
    selectAuthor(labelOrPrefix: string): OwnIdentity {
        if (this.keystore === undefined) throw new Error('No keystore configured');
        const record = this.keystore.resolveRecord(labelOrPrefix);
        const identity = this.unlocked.get(record.keyId);
        if (identity === undefined) throw new Error(`Key '${record.label}' is locked`);
        this.currentAuthorKeyId = identity.keyId;
        return identity;
    }

    // Clear the default author (writes become anonymous unless they carry an
    // explicit BY clause).
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

    setOutputMode(mode: OutputMode): void {
        this.outputMode = mode;
    }

    setVariable(name: string, value: LangValue): void {
        this.variables.set(name, value);
    }

    // Synchronous accessor for the current default author (used by the prompt,
    // which renders synchronously).
    selectedAuthor(): OwnIdentity | undefined {
        return this.currentAuthorKeyId === undefined ? undefined : this.unlocked.get(this.currentAuthorKeyId);
    }

    async currentAuthor(): Promise<OwnIdentity | undefined> {
        return this.selectedAuthor();
    }

    // Resolve a key reference to an unlocked identity, or undefined when the key
    // is known but still locked. Throws on an unknown/ambiguous reference.
    resolveIdentity(labelOrPrefix: string): OwnIdentity | undefined {
        if (this.keystore === undefined) throw new Error('No keystore configured');
        const record = this.keystore.resolveRecord(labelOrPrefix);
        return this.unlocked.get(record.keyId);
    }

    async resolveAuthor(ref: AuthorRef): Promise<OwnIdentity> {
        const labelOrPrefix = ref.kind === 'variable' ? ref.name : `#${ref.prefix}`;
        const identity = this.resolveIdentity(labelOrPrefix);
        if (identity === undefined) throw new Error(`Key '${labelOrPrefix}' is not unlocked`);
        return identity;
    }

    async resolveVariable(name: string): Promise<LangValue> {
        if (name === 'me' || name === 'author') {
            const identity = await this.currentAuthor();
            if (identity !== undefined) return identity;
        }

        const explicit = this.variables.get(name);
        if (explicit !== undefined) return explicit;

        if (this.keystore !== undefined) {
            try {
                const publicKey = this.keystore.resolvePublic(name);
                return { keyId: publicKey.keyId, publicKey: publicKey.publicKey };
            } catch (_e) {
                // Keep the user-facing error centered on the missing variable.
            }
        }

        throw new Error(`Unknown variable '$${name}'`);
    }

    async resolvePublicKey(labelOrPrefix: string): Promise<{ keyId: KeyId; publicKey: PublicKey }> {
        if (this.keystore === undefined) throw new Error('No keystore configured');
        const record = this.keystore.resolvePublic(labelOrPrefix);
        return { keyId: record.keyId, publicKey: record.publicKey };
    }

    createUuid(): string {
        return randomUUID();
    }

    createSeed(kind: 'rdb' | 'group', name?: string): string {
        const hash = this.workspace.replica.getHashSuite();
        return hash.hashToB64(new TextEncoder().encode(`${kind}:${name ?? ''}:${randomUUID()}`));
    }
}
