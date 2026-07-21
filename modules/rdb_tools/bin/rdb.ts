#!/usr/bin/env node
import { stderr, stdin, stdout } from "node:process";

import { defaultKeystorePath, KeyStore } from "../src/keys/keystore.js";
import { startRepl } from "../src/repl/repl.js";
import { runCommand } from "../src/script/run_command.js";
import { runScriptFile, runScriptStdin } from "../src/script/run_script.js";
import { WorkspaceSession } from "../src/session/session.js";
import { Workspace } from "../src/workspace/workspace.js";

async function main(): Promise<void> {
    const args = process.argv.slice(2);
    const workspacePath = args.shift();
    if (workspacePath === undefined) {
        stderr.write("Usage: rdb <workspace.db> [-c command] [-f file|-] [-k] [--json]\n");
        process.exitCode = 1;
        return;
    }

    const workspace = await Workspace.open({ path: workspacePath });
    const keystore = await KeyStore.open(defaultKeystorePath(), workspace.replica.getHashSuite());
    const session = new WorkspaceSession({ workspace, keystore });

    try {
        if (args.includes('--json')) session.setOutputMode('json');
        if (args.includes('-k') || args.includes('--prompt-keys')) session.setPromptForKeys(true);
        const c = args.indexOf('-c');
        const f = args.indexOf('-f');

        if (c >= 0) {
            const command = args[c + 1];
            if (command === undefined) throw new Error('-c requires a command');
            session.enableScriptDefaults();
            const result = await runCommand(session, command);
            if (result.output.length > 0) stdout.write(result.output + '\n');
            process.exitCode = result.exitCode;
            return;
        }

        if (f >= 0) {
            const file = args[f + 1];
            if (file === undefined) throw new Error('-f requires a file');
            const result = file === '-'
                ? await runScriptStdin(session)
                : await runScriptFile(session, file);
            if (result.output.length > 0) stdout.write(result.output + '\n');
            process.exitCode = result.exitCode;
            return;
        }

        if (!stdin.isTTY) {
            const result = await runScriptStdin(session);
            if (result.output.length > 0) stdout.write(result.output + '\n');
            process.exitCode = result.exitCode;
            return;
        }

        await startRepl(session);
    } finally {
        await workspace.close();
    }
}

main().catch((e) => {
    stderr.write((e instanceof Error ? e.message : String(e)) + '\n');
    process.exitCode = 1;
});
