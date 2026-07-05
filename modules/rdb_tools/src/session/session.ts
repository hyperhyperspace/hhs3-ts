import { randomUUID } from "node:crypto";

import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { AuthorRef, LangValue } from "@hyper-hyper-space/hhs3_rdb_lang";

import { KeyStore } from "../keys/keystore.js";
import { AliasTable } from "./aliases.js";
import { Workspace } from "../workspace/workspace.js";

export type OutputMode = 'table' | 'json' | 'vertical';

export type HashWidth = 'auto' | 'full' | number;

export type RefAutoUpdateMode = 'auto' | 'self' | 'off';

export type SessionView = {
    at: Version;
    from?: Version;
};

export type WorkspaceSessionOptions = {
    workspace: Workspace;
    keystore?: KeyStore;
    outputMode?: OutputMode;
    hashWidth?: HashWidth;
    hashLabels?: boolean;
};

export class KeyPassphraseRequiredError extends Error {
    constructor(readonly label: string) {
        super(`Key '${label}' is not unlocked`);
        this.name = 'KeyPassphraseRequiredError';
    }
}

export class WorkspaceSession {
    readonly workspace: Workspace;
    readonly variables = new Map<string, LangValue>();
    readonly aliases = new AliasTable();

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
    hashWidth: HashWidth;
    hashLabels: boolean;
    refAutoUpdate: RefAutoUpdateMode;
    promptForKeys: boolean;
    stopOnError = true;

    constructor(options: WorkspaceSessionOptions) {
        this.workspace = options.workspace;
        this.keystore = options.keystore;
        this.outputMode = options.outputMode ?? 'table';
        this.hashWidth = options.hashWidth ?? parseHashWidthEnv() ?? 'auto';
        this.hashLabels = options.hashLabels ?? parseHashLabelsEnv() ?? false;
        this.refAutoUpdate = parseRefAutoUpdateEnv() ?? 'off';
        this.promptForKeys = parsePromptForKeysEnv() ?? false;
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
        const identity = await this.keystore.unlock(this.resolveKeyRef(labelOrPrefix), passphrase);
        this.unlocked.set(identity.keyId, identity);
        return identity;
    }

    // Set the default author. The key must already be unlocked this session.
    selectAuthor(labelOrPrefix: string): OwnIdentity {
        if (this.keystore === undefined) throw new Error('No keystore configured');
        const record = this.keystore.resolveRecord(this.resolveKeyRef(labelOrPrefix));
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

    setHashWidth(width: HashWidth): void {
        this.hashWidth = width;
    }

    setHashLabels(on: boolean): void {
        this.hashLabels = on;
    }

    setRefAutoUpdate(mode: RefAutoUpdateMode): void {
        this.refAutoUpdate = mode;
    }

    setPromptForKeys(on: boolean): void {
        this.promptForKeys = on;
    }

    enableReplDefaults(): void {
        if (parseHashLabelsEnv() === undefined) this.hashLabels = true;
        if (parseRefAutoUpdateEnv() === undefined) this.refAutoUpdate = 'auto';
    }

    enableScriptDefaults(): void {
        if (parseHashWidthEnv() === undefined) this.hashWidth = 'full';
        if (parseRefAutoUpdateEnv() === undefined) this.refAutoUpdate = 'off';
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
        const record = this.keystore.resolveRecord(this.resolveKeyRef(labelOrPrefix));
        return this.unlocked.get(record.keyId);
    }

    async resolveAuthor(ref: AuthorRef): Promise<OwnIdentity> {
        const labelOrPrefix = ref.kind === 'variable'
            ? this.resolveKeyRef(ref.name)
            : `#${ref.prefix}`;
        const identity = this.resolveIdentity(labelOrPrefix);
        if (identity !== undefined) return identity;
        const record = this.keystore!.resolveRecord(labelOrPrefix);
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
            if (this.keystore === undefined) throw new Error('No keystore configured');
            const record = this.keystore.resolvePublic(keyAlias);
            return { keyId: record.keyId, publicKey: record.publicKey };
        }

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
        const record = this.keystore.resolvePublic(this.resolveKeyRef(labelOrPrefix));
        return { keyId: record.keyId, publicKey: record.publicKey };
    }

    /** Resolve a key reference: key-scoped alias first, then label / #prefix via keystore. */
    resolveKeyRef(labelOrPrefix: string): string {
        if (labelOrPrefix.startsWith('#')) return labelOrPrefix;
        const aliased = this.aliases.get('key', labelOrPrefix);
        return aliased ?? labelOrPrefix;
    }

    createUuid(): string {
        return randomUUID();
    }

    createSeed(kind: 'rdb' | 'group', name?: string): string {
        const hash = this.workspace.replica.getHashSuite();
        return hash.hashToB64(new TextEncoder().encode(`${kind}:${name ?? ''}:${randomUUID()}`));
    }
}

function parseHashWidthEnv(): HashWidth | undefined {
    const raw = process.env.RDB_HASH_WIDTH?.trim();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'auto') return 'auto';
    if (raw === 'full') return 'full';
    const n = Number(raw);
    if (Number.isInteger(n) && n > 0) return n;
    throw new Error(`Invalid RDB_HASH_WIDTH '${raw}' (expected auto, full, or a positive integer)`);
}

function parseHashLabelsEnv(): boolean | undefined {
    const raw = process.env.RDB_HASH_LABELS?.trim().toLowerCase();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'on' || raw === 'true' || raw === '1') return true;
    if (raw === 'off' || raw === 'false' || raw === '0') return false;
    throw new Error(`Invalid RDB_HASH_LABELS '${raw}' (expected on or off)`);
}

function parseRefAutoUpdateEnv(): RefAutoUpdateMode | undefined {
    const raw = process.env.RDB_REF_AUTO_UPDATE?.trim().toLowerCase();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'auto' || raw === 'on' || raw === 'true' || raw === '1') return 'auto';
    if (raw === 'self') return 'self';
    if (raw === 'off' || raw === 'false' || raw === '0') return 'off';
    throw new Error(`Invalid RDB_REF_AUTO_UPDATE '${raw}' (expected auto, self, or off)`);
}

function parsePromptForKeysEnv(): boolean | undefined {
    const raw = process.env.RDB_PROMPT_KEYS?.trim().toLowerCase();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'on' || raw === 'true' || raw === '1') return true;
    if (raw === 'off' || raw === 'false' || raw === '0') return false;
    throw new Error(`Invalid RDB_PROMPT_KEYS '${raw}' (expected on or off)`);
}
