import { formatDiagnostics } from "../format/diagnostics.js";
import { formatJson } from "../format/json.js";
import { formatTableResult } from "../format/table.js";
import { LanguageError, runLanguageText } from "../session/adapter.js";
import { WorkspaceSession } from "../session/session.js";
import { runMetaCommand } from "../repl/meta.js";

export type CommandRun = {
    exitCode: number;
    output: string;
};

export async function runCommand(session: WorkspaceSession, command: string, file?: string): Promise<CommandRun> {
    try {
        const meta = await runMetaCommand(session, command);
        if (meta.handled) {
            if (meta.needsPassphrase !== undefined) {
                return { exitCode: 1, output: 'passphrase required; use the REPL or pass it inline with -c' };
            }
            return { exitCode: 0, output: meta.output ?? '' };
        }

        const run = await runLanguageText(session, command);
        const output = run.results
            .map((item) => session.outputMode === 'json'
                ? formatJson(item.result)
                : formatTableResult(item.result, session.outputMode))
            .filter((text) => text.length > 0)
            .join('\n');
        return { exitCode: 0, output };
    } catch (e) {
        if (e instanceof LanguageError) {
            return { exitCode: 2, output: formatDiagnostics(e.diagnostics, file) };
        }
        return { exitCode: 1, output: e instanceof Error ? e.message : String(e) };
    }
}
