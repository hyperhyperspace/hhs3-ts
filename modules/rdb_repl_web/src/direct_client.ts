import {
    KeyUnlockDeclinedError,
    MemoryKeyVault,
    openMemWorkspace,
    type AuthInteractionContext,
    type RdbWorkspace,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import { MemoryTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    ReplSession,
    promptForSession,
    runCommand,
    stopAllProjections,
    stopAllSyncs,
    type PassphraseRequest,
} from "@hyper-hyper-space/hhs3_rdb_repl";

import { createBrowserMesh } from "@hyper-hyper-space/hhs3_mesh_browser";

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
        // Async projection notices (reactive sync throws, ingest-reject warnings)
        // land in the transcript. Rebind per execute: mountRepl reuses one
        // interactions object whose onProgress appends to the transcript, so
        // notices arriving after the command returns still show up.
        session.onProjectionError = interactions.onProgress;
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

    async hasKey(label: string): Promise<boolean> {
        return this.requireSession().keystore?.list().some((key) => key.label === label) === true;
    }

    async reset(): Promise<string> {
        await this.close();
        await this.open();
        return promptForSession(this.session!);
    }

    async close(): Promise<void> {
        const session = this.session;
        const workspace = this.workspace;
        this.workspace = undefined;
        this.session = undefined;
        if (session !== undefined) {
            await stopAllProjections(session);
            await stopAllSyncs(session);
        }
        if (workspace !== undefined) await workspace.close();
    }

    private async open(): Promise<void> {
        const workspace = await openMemWorkspace({ backendLabel: 'rdb-web' });
        const session = new ReplSession({
            workspace,
            keyVault: new MemoryKeyVault(workspace.replica.getHashSuite()),
            // `\project ...` backend for the browser: an ephemeral in-memory
            // target (capture-provisioned so local edits round-trip to rdb).
            // The only legal destination is `to :memory:`.
            projectionTargetFactory: async ({ path }) => {
                if (path !== ':memory:') {
                    throw new Error('this host only supports to :memory:');
                }
                return new MemoryTarget({ captureChanges: true });
            },
            syncMeshFactory: createBrowserMesh,
            // Naive first-cut issue sink for the browser demo: structured
            // reports from the mesh/swarm/sync layers go to the dev console.
            report: (report) => {
                const source = report.source ?? 'issue';
                const detail = report.message ?? report.kind ?? 'unknown issue';
                console.log(`[${source} issue] ${detail}`);
            },
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
            title: `Sign and retry as ${authorLabel}?`,
            detail: `The ${op} operation requires this identity.`,
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

async function requestPassphrase(
    interactions: ReplInteractions,
    need: PassphraseRequest,
): Promise<string | undefined> {
    if (need.kind === 'statement-unlock') {
        const confirmed = await interactions.requestConfirmation({
            kind: 'confirm',
            title: `Sign with ${need.label}?`,
            detail: `This operation needs ${need.label}. Unlock, sign, and retry?`,
        });
        if (!confirmed) throw new KeyUnlockDeclinedError(need.label);
    }

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
