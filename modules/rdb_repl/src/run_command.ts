import {
    KeyUnlockDeclinedError,
    type AuthInteractionContext,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    keyPassphraseRequiredFromError,
    LanguageError,
    runLanguageText,
    type ScriptRunResult,
} from "./adapter.js";
import { formatDiagnostics } from "./format/diagnostics.js";
import { renderStatementOutput } from "./format/table.js";
import { fulfillPassphraseNeed, runMetaCommand, type PassphraseNeed } from "./meta.js";
import type { ReplSession } from "./session.js";

export type PassphraseRequest = PassphraseNeed | { kind: 'statement-unlock'; label: string };
export type CommandRun = {
    exitCode: number;
    output: string;
    quit?: boolean;
    needsPassphrase?: PassphraseRequest;
};
export type CommandRunOptions = {
    auth?: AuthInteractionContext;
    requestPassphrase?: (need: PassphraseRequest) => Promise<string | undefined>;
};

export async function runLanguageWithUnlock(
    session: ReplSession,
    text: string,
    options?: CommandRunOptions,
): Promise<ScriptRunResult | PassphraseRequest> {
    while (true) {
        try {
            return await runLanguageText(session, text, options?.auth);
        } catch (error) {
            const required = keyPassphraseRequiredFromError(error);
            if (required === undefined) throw error;
            const need: PassphraseRequest = { kind: 'statement-unlock', label: required.label };
            const passphrase = await options?.requestPassphrase?.(need);
            if (passphrase === undefined) return need;
            await session.unlockKey(required.label, passphrase);
        }
    }
}

export async function runCommand(
    session: ReplSession,
    command: string,
    file?: string,
    options?: CommandRunOptions,
): Promise<CommandRun> {
    try {
        const meta = await runMetaCommand(session, command);
        if (meta.handled) {
            if (meta.needsPassphrase !== undefined) {
                const passphrase = await options?.requestPassphrase?.(meta.needsPassphrase);
                if (passphrase === undefined) {
                    return { exitCode: 1, output: 'passphrase required', needsPassphrase: meta.needsPassphrase };
                }
                return { exitCode: 0, output: await fulfillPassphraseNeed(session, meta.needsPassphrase, passphrase) };
            }
            return { exitCode: 0, output: meta.output ?? '', quit: meta.quit };
        }
        const run = await runLanguageWithUnlock(session, command, options);
        if (!('results' in run)) {
            return { exitCode: 1, output: 'passphrase required', needsPassphrase: run };
        }
        return {
            exitCode: 0,
            output: run.results
                .map((item) => renderStatementOutput(session, item))
                .filter(Boolean)
                .join('\n'),
        };
    } catch (error) {
        if (error instanceof KeyUnlockDeclinedError) return { exitCode: 1, output: 'unlock declined' };
        if (error instanceof LanguageError) {
            return { exitCode: 2, output: formatDiagnostics(error.diagnostics, file, error.hints) };
        }
        return { exitCode: 1, output: error instanceof Error ? error.message : String(error) };
    }
}
