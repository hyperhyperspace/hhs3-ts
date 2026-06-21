import { randomUUID } from "node:crypto";

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { LangValue } from "@hyper-hyper-space/hhs3_rdb_lang";

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

    async currentAuthor(): Promise<OwnIdentity | undefined> {
        return this.keystore?.selected();
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

    createUuid(): string {
        return randomUUID();
    }

    createSeed(kind: 'rdb' | 'schema' | 'group', name?: string): string {
        const hash = this.workspace.replica.getHashSuite();
        return hash.hashToB64(new TextEncoder().encode(`${kind}:${name ?? ''}:${randomUUID()}`));
    }
}
