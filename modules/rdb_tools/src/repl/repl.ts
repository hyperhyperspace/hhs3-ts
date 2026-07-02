import { stdin as input, stdout as output } from "node:process";
import { emitKeypressEvents } from "node:readline";
import { createInterface } from "node:readline/promises";

import { scanStatement } from "@hyper-hyper-space/hhs3_rdb_lang";

import { formatDiagnostics } from "../format/diagnostics.js";
import { renderStatementOutput } from "../format/table.js";
import { LanguageError } from "../session/adapter.js";
import { runLanguageWithUnlock } from "../script/run_command.js";
import { WorkspaceSession } from "../session/session.js";
import { runMetaCommand } from "./meta.js";
import { fulfillKeyPassphrase, KeyUnlockDeclinedError } from "./passphrase.js";
import { promptForSession } from "./prompt.js";

export async function startRepl(session: WorkspaceSession): Promise<void> {
    session.enableReplDefaults();
    const rl = createInterface({ input, output });
    let buffer = '';

    // Event-driven design: readline owns all prompt rendering. We never write the
    // prompt out of band, which is what previously raced readline's terminal-mode
    // line editor and corrupted the display during multi-line paste. Lines are
    // handled in arrival order via a small queue so async work (statement
    // execution, passphrase prompts) is fully serialized.
    let isPasting = false;
    let busy = false;
    const pending: string[] = [];

    if (input.isTTY) {
        emitKeypressEvents(input, rl);
        input.on('keypress', (_s: string, key: { name?: string } | undefined) => {
            if (key?.name === 'paste-start') isPasting = true;
            if (key?.name === 'paste-end') isPasting = false;
        });
        // Enable bracketed paste so the terminal brackets the paste and readline
        // coalesces it; combined with isPasting this suppresses continuation
        // prompts mid-paste.
        output.write('\x1b[?2004h');
    }

    const drawPrompt = (): void => {
        if (isPasting) return;
        rl.setPrompt(promptForSession(session, buffer.length > 0));
        rl.prompt();
    };

    const handleLine = async (line: string): Promise<{ quit: boolean }> => {
        if (buffer.length === 0 && line.trim().length === 0) return { quit: false };

        if (buffer.length === 0 && line.trimStart().startsWith('\\')) {
            try {
                const meta = await runMetaCommand(session, line);
                if (meta.needsPassphrase !== undefined) {
                    output.write(await fulfillKeyPassphrase(session, meta.needsPassphrase, rl) + '\n');
                } else if (meta.output !== undefined && meta.output.length > 0) {
                    output.write(meta.output + '\n');
                }
                if (meta.quit === true) return { quit: true };
            } catch (e) {
                output.write((e instanceof Error ? e.message : String(e)) + '\n');
            }
            return { quit: false };
        }

        buffer += (buffer.length === 0 ? '' : '\n') + line;
        if (!isComplete(buffer)) return { quit: false };

        try {
            const run = await runLanguageWithUnlock(session, buffer, { rl });
            for (const item of run.results) {
                const rendered = renderStatementOutput(session, item);
                if (rendered.length > 0) output.write(rendered + '\n');
            }
        } catch (e) {
            if (e instanceof KeyUnlockDeclinedError) {
                // User declined unlock; return to the main prompt without an error line.
            } else if (e instanceof LanguageError) {
                output.write(formatDiagnostics(e.diagnostics, undefined, e.hints) + '\n');
            } else {
                output.write((e instanceof Error ? e.message : String(e)) + '\n');
            }
        } finally {
            buffer = '';
        }
        return { quit: false };
    };

    await new Promise<void>((resolve) => {
        const pump = async (): Promise<void> => {
            if (busy) return;
            busy = true;
            try {
                while (pending.length > 0) {
                    const line = pending.shift()!;
                    const { quit } = await handleLine(line);
                    if (quit) {
                        rl.close();
                        return;
                    }
                }
            } finally {
                busy = false;
            }
            // Only prompt once the queue is fully drained, so a burst of pasted
            // lines never produces intermediate prompts.
            if (pending.length === 0) drawPrompt();
        };

        rl.on('line', (line) => {
            pending.push(line);
            void pump();
        });

        rl.on('close', () => resolve());

        drawPrompt();
    });

    if (input.isTTY) output.write('\x1b[?2004l');
}

function isComplete(text: string): boolean {
    return scanStatement(text).kind === 'complete';
}
