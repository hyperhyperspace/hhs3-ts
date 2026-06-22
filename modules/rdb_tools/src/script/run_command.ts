import { stdin as input, stdout as output } from "node:process";
import { createInterface, type Interface } from "node:readline/promises";

import { formatDiagnostics } from "../format/diagnostics.js";
import { formatJson } from "../format/json.js";
import { formatTableResult } from "../format/table.js";
import { runMetaCommand } from "../repl/meta.js";
import { fulfillKeyPassphrase } from "../repl/passphrase.js";
import { LanguageError, runLanguageText } from "../session/adapter.js";
import { WorkspaceSession } from "../session/session.js";

export type CommandRun = {
    exitCode: number;
    output: string;
};

export type CommandRunOptions = {
    rl?: Interface;
};

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
                if (!input.isTTY) {
                    return { exitCode: 1, output: 'passphrase required; use the REPL or pass it inline with -c' };
                }
                const owned = options?.rl === undefined;
                const rl = options?.rl ?? createInterface({ input, output });
                try {
                    const text = await fulfillKeyPassphrase(session, meta.needsPassphrase, rl);
                    return { exitCode: 0, output: text };
                } finally {
                    if (owned) rl.close();
                }
            }
            return { exitCode: 0, output: meta.output ?? '' };
        }

        const run = await runLanguageText(session, command);
        const rendered = run.results
            .map((item) => session.outputMode === 'json'
                ? formatJson(item.result)
                : formatTableResult(item.result, session.outputMode))
            .filter((text) => text.length > 0)
            .join('\n');
        return { exitCode: 0, output: rendered };
    } catch (e) {
        if (e instanceof LanguageError) {
            return { exitCode: 2, output: formatDiagnostics(e.diagnostics, file) };
        }
        return { exitCode: 1, output: e instanceof Error ? e.message : String(e) };
    }
}
