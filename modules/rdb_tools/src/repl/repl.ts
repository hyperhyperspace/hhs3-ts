import { stdin as input, stdout as output } from "node:process";
import { createInterface } from "node:readline/promises";

import { formatJson } from "../format/json.js";
import { formatTableResult } from "../format/table.js";
import { runLanguageText } from "../session/adapter.js";
import { WorkspaceSession } from "../session/session.js";
import { runMetaCommand } from "./meta.js";
import { fulfillKeyPassphrase } from "./passphrase.js";
import { promptForSession } from "./prompt.js";

export async function startRepl(session: WorkspaceSession): Promise<void> {
    const rl = createInterface({ input, output });
    let buffer = '';

    try {
        while (true) {
            const line = await rl.question(promptForSession(session, buffer.length > 0));
            if (buffer.length === 0 && line.trimStart().startsWith('\\')) {
                try {
                    const meta = await runMetaCommand(session, line);
                    if (meta.needsPassphrase !== undefined) {
                        output.write(await fulfillKeyPassphrase(session, meta.needsPassphrase, rl) + '\n');
                    } else if (meta.output !== undefined && meta.output.length > 0) {
                        output.write(meta.output + '\n');
                    }
                    if (meta.quit === true) break;
                } catch (e) {
                    output.write((e instanceof Error ? e.message : String(e)) + '\n');
                }
                continue;
            }

            buffer += (buffer.length === 0 ? '' : '\n') + line;
            if (!isComplete(buffer)) continue;

            try {
                const run = await runLanguageText(session, buffer);
                for (const item of run.results) {
                    const rendered = session.outputMode === 'json'
                        ? formatJson(item.result)
                        : formatTableResult(item.result, session.outputMode);
                    if (rendered.length > 0) output.write(rendered + '\n');
                }
            } catch (e) {
                output.write((e instanceof Error ? e.message : String(e)) + '\n');
            } finally {
                buffer = '';
            }
        }
    } finally {
        rl.close();
    }
}

function isComplete(text: string): boolean {
    return text.trimEnd().endsWith(';');
}
