import { stdin as input, stdout as output } from "node:process";
import { createInterface, type Interface } from "node:readline/promises";

import { formatDiagnostics } from "../format/diagnostics.js";
import { renderStatementOutput } from "../format/table.js";
import { runMetaCommand } from "../repl/meta.js";
import {
    confirmStatementUnlock,
    fulfillKeyPassphrase,
    fulfillPassphraseNeed,
    KeyUnlockDeclinedError,
    keyDisplayLabel,
} from "../repl/passphrase.js";
import { keyPassphraseRequiredFromError, LanguageError, runLanguageText, type ScriptRunResult } from "../session/adapter.js";
import { WorkspaceSession } from "../session/session.js";

export type CommandRun = {
    exitCode: number;
    output: string;
};

export type CommandRunOptions = {
    rl?: Interface;
};

export async function runLanguageWithUnlock(
    session: WorkspaceSession,
    text: string,
    options?: { rl?: Interface },
): Promise<ScriptRunResult> {
    while (true) {
        try {
            return await runLanguageText(session, text, options);
        } catch (e) {
            if (e instanceof KeyUnlockDeclinedError) throw e;
            const required = keyPassphraseRequiredFromError(e);
            if (required === undefined) throw e;
            if (!input.isTTY) {
                throw new Error('passphrase required; use the REPL or pass it inline with -c');
            }
            const owned = options?.rl === undefined;
            const activeRl = options?.rl ?? createInterface({ input, output });
            try {
                const displayName = keyDisplayLabel(session, required.label);
                await confirmStatementUnlock(activeRl, displayName);
                await fulfillKeyPassphrase(session, { kind: 'unlock', label: required.label }, activeRl);
            } finally {
                if (owned) activeRl.close();
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
    try {
        const meta = await runMetaCommand(session, command);
        if (meta.handled) {
            if (meta.needsPassphrase !== undefined) {
                const text = await fulfillPassphraseNeed(session, meta.needsPassphrase, options?.rl);
                return { exitCode: 0, output: text };
            }
            return { exitCode: 0, output: meta.output ?? '' };
        }

        const run = await runLanguageWithUnlock(session, command, { rl: options?.rl });
        const rendered = run.results
            .map((item) => renderStatementOutput(session, item))
            .filter((text) => text.length > 0)
            .join('\n');
        return { exitCode: 0, output: rendered };
    } catch (e) {
        if (e instanceof KeyUnlockDeclinedError) {
            return { exitCode: 1, output: 'unlock declined' };
        }
        if (e instanceof LanguageError) {
            return { exitCode: 2, output: formatDiagnostics(e.diagnostics, file, e.hints) };
        }
        return { exitCode: 1, output: e instanceof Error ? e.message : String(e) };
    }
}
