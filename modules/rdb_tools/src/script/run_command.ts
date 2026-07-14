import type { Interface } from "node:readline/promises";
import {
    runCommand as runPortableCommand,
    type CommandRun,
} from "@hyper-hyper-space/hhs3_rdb_repl";

import {
    confirmStatementUnlock,
    fulfillKeyPassphrase,
    KeyUnlockDeclinedError,
    keyDisplayLabel,
    requestPassphrase,
} from "../repl/passphrase.js";
import { canPromptForKeys, closePromptTty, createPromptInterface } from "../repl/prompt_tty.js";
import { keyPassphraseRequiredFromError, runLanguageText, type RunLanguageTextOptions, type ScriptRunResult } from "../session/adapter.js";
import { toAuthInteractionContext } from "../session/auth_bridge.js";
import { WorkspaceSession } from "../session/session.js";

export type { CommandRun };

export type CommandRunOptions = {
    rl?: Interface;
};

export async function runLanguageWithUnlock(
    session: WorkspaceSession,
    text: string,
    options?: RunLanguageTextOptions,
): Promise<ScriptRunResult> {
    while (true) {
        try {
            return await runLanguageText(session, text, options);
        } catch (e) {
            if (e instanceof KeyUnlockDeclinedError) throw e;
            const required = keyPassphraseRequiredFromError(e);
            if (required === undefined) throw e;
            if (!canPromptForKeys(session)) {
                throw new Error('passphrase required; use the REPL or pass it inline with -c');
            }
            const owned = options?.rl === undefined;
            const activeRl = options?.rl ?? createPromptInterface(session);
            if (activeRl === undefined) {
                throw new Error('passphrase required; use the REPL or pass it inline with -c');
            }
            try {
                const displayName = keyDisplayLabel(session, required.label);
                await confirmStatementUnlock(activeRl, displayName);
                await fulfillKeyPassphrase(session, { kind: 'unlock', label: required.label }, activeRl);
            } finally {
                if (owned) {
                    activeRl.close();
                    closePromptTty();
                }
            }
        }
    }
}

export async function runCommand(
    session: WorkspaceSession,
    command: string,
    file?: string,
    options?: CommandRunOptions,
): Promise<CommandRun> {
    return runPortableCommand(session, command, file, {
        auth: toAuthInteractionContext(session, { rl: options?.rl }),
        requestPassphrase: (need) => requestPassphrase(session, need, options?.rl),
    });
}
