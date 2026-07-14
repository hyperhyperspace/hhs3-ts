import {
    KeyUnlockDeclinedError,
    MemoryKeyVault,
    openMemWorkspace,
    type AuthInteractionContext,
    type RdbWorkspace,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    ReplSession,
    promptForSession,
    runCommand,
    type PassphraseRequest,
} from "@hyper-hyper-space/hhs3_rdb_repl";

import type { ExecuteResult, ReplClient, ReplInteractions } from "./protocol.js";

export class DirectReplClient implements ReplClient {
    private workspace?: RdbWorkspace;
    private session?: ReplSession;

    async start(): Promise<string> {
        ensureBrowserCrypto();
        if (this.session === undefined) await this.open();
        return promptForSession(this.session!);
    }

    async execute(text: string, interactions: ReplInteractions): Promise<ExecuteResult> {
        const session = this.requireSession();
        const result = await runCommand(session, text, undefined, {
            auth: authContext(session, interactions),
            requestPassphrase: (need) => requestPassphrase(interactions, need),
        });

        return {
            output: result.output,
            exitCode: result.exitCode,
            quit: result.quit === true,
            prompt: promptForSession(session),
        };
    }

    async reset(): Promise<string> {
        await this.close();
        await this.open();
        return promptForSession(this.session!);
    }

    async close(): Promise<void> {
        const workspace = this.workspace;
        this.workspace = undefined;
        this.session = undefined;
        if (workspace !== undefined) await workspace.close();
    }

    private async open(): Promise<void> {
        const workspace = await openMemWorkspace({ backendLabel: 'rdb-web' });
        const session = new ReplSession({
            workspace,
            keyVault: new MemoryKeyVault(workspace.replica.getHashSuite()),
        });
        session.enableReplDefaults();
        this.workspace = workspace;
        this.session = session;
    }

    private requireSession(): ReplSession {
        if (this.session === undefined) throw new Error('RDB runtime has not started');
        return this.session;
    }
}

function authContext(session: ReplSession, interactions: ReplInteractions): AuthInteractionContext {
    return {
        canPrompt: () => true,
        onProgress: interactions.onProgress,
        confirmSignRetry: (authorLabel, op) => interactions.requestConfirmation({
            kind: 'confirm',
            title: `Retry as ${authorLabel}?`,
            detail: `The operation “${op}” requires a different author.`,
        }),
        confirmRefUpdateUnlock: async (observerGroup, authorLabel) => {
            const confirmed = await interactions.requestConfirmation({
                kind: 'confirm',
                title: `Unlock ${authorLabel}?`,
                detail: `Updating references for ${observerGroup} requires this identity.`,
            });
            if (!confirmed) throw new KeyUnlockDeclinedError(authorLabel);
        },
        unlockIdentity: async (label) => {
            const passphrase = await interactions.requestPassphrase({
                kind: 'passphrase',
                title: `Unlock ${label}`,
                detail: 'Enter the passphrase for this in-memory identity.',
                label,
            });
            if (passphrase === undefined) return undefined;
            return session.unlockKey(label, passphrase);
        },
    };
}

function requestPassphrase(
    interactions: ReplInteractions,
    need: PassphraseRequest,
): Promise<string | undefined> {
    const action = need.kind === 'create' ? 'Create' : 'Unlock';
    return interactions.requestPassphrase({
        kind: 'passphrase',
        title: `${action} ${need.label}`,
        detail: need.kind === 'create'
            ? 'Choose a passphrase for this ephemeral identity.'
            : 'Enter the passphrase for this in-memory identity.',
        label: need.label,
    });
}

function ensureBrowserCrypto(): void {
    if (typeof globalThis.crypto?.getRandomValues !== 'function') {
        throw new Error('This REPL requires Web Crypto in a secure browser context.');
    }
    if (typeof globalThis.crypto.randomUUID !== 'function') {
        throw new Error('This browser does not provide crypto.randomUUID().');
    }
}
