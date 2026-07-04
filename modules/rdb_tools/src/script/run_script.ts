import { promises as fs } from "node:fs";
import { stdin as input } from "node:process";
import { text } from "node:stream/consumers";

import { scanStatement } from "@hyper-hyper-space/hhs3_rdb_lang";

import { closePromptTty, createPromptInterface } from "../repl/prompt_tty.js";
import { WorkspaceSession } from "../session/session.js";
import { runCommand } from "./run_command.js";

export type ScriptRun = {
    exitCode: number;
    output: string;
};

export async function runScriptFile(session: WorkspaceSession, path: string): Promise<ScriptRun> {
    return runScript(session, await fs.readFile(path, 'utf8'), path);
}

export async function runScriptStdin(session: WorkspaceSession): Promise<ScriptRun> {
    return runScript(session, await text(input), '<stdin>');
}

export async function runScript(session: WorkspaceSession, text: string, file = '<script>'): Promise<ScriptRun> {
    session.enableScriptDefaults();
    const outputs: string[] = [];
    let buffer = '';
    const rl = createPromptInterface(session);

    try {
        for (const line of text.split(/\r?\n/)) {
            const trimmed = line.trim();
            if (buffer.length === 0 && (trimmed.length === 0 || trimmed.startsWith('--'))) continue;

            if (buffer.length === 0 && trimmed.startsWith('\\')) {
                const result = await runCommand(session, trimmed, file, { rl });
                if (result.output.length > 0) outputs.push(result.output);
                if (result.exitCode !== 0 && session.stopOnError) return { exitCode: result.exitCode, output: outputs.join('\n') };
                continue;
            }

            buffer += (buffer.length === 0 ? '' : '\n') + line;
            if (scanStatement(buffer).kind !== 'complete') continue;

            const result = await runCommand(session, buffer, file, { rl });
            if (result.output.length > 0) outputs.push(result.output);
            buffer = '';
            if (result.exitCode !== 0 && session.stopOnError) return { exitCode: result.exitCode, output: outputs.join('\n') };
        }

        if (buffer.trim().length > 0) {
            const result = await runCommand(session, buffer, file, { rl });
            if (result.output.length > 0) outputs.push(result.output);
            if (result.exitCode !== 0 && session.stopOnError) return { exitCode: result.exitCode, output: outputs.join('\n') };
        }

        return { exitCode: 0, output: outputs.join('\n') };
    } finally {
        rl?.close();
        closePromptTty();
    }
}
