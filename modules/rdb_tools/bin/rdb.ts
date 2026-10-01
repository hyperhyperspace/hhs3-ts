#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { stderr, stdin, stdout } from "node:process";
import Database from "better-sqlite3";

import { SqliteTarget } from "@hyper-hyper-space/hhs3_rdb_adapter_sqlite";
import { stopAllProjections, stopAllSyncs } from "@hyper-hyper-space/hhs3_rdb_repl";

import { createNodeSyncMeshFactory, defaultKeystorePath, KeyStore } from "@hyper-hyper-space/hhs3_rhost_node";
import { nodeFilesDirectories } from "../src/files/folders.js";
import { nodeLocalFiles } from "../src/files/local_files.js";
import { startRepl } from "../src/repl/repl.js";
import { runCommand } from "../src/script/run_command.js";
import { runScriptFile, runScriptStdin } from "../src/script/run_script.js";
import { WorkspaceSession } from "../src/session/session.js";
import { Workspace } from "../src/workspace/workspace.js";

async function main(): Promise<void> {
    const args = process.argv.slice(2);
    const workspacePath = args.shift();
    if (workspacePath === undefined) {
        stderr.write("Usage: rdb <workspace.db> [-c command] [-f file|-] [-k] [--json] [--keystore <path>]\n");
        process.exitCode = 1;
        return;
    }

    // `--keystore <path>` is the flag form of RDB_KEYSTORE: sign with another
    // keystore file (an app's own, a staging app's) instead of the user's.
    let keystorePath = defaultKeystorePath();
    const ks = args.indexOf('--keystore');
    if (ks >= 0) {
        const path = args[ks + 1];
        if (path === undefined || path.startsWith('-')) throw new Error('--keystore requires a path');
        keystorePath = path;
        args.splice(ks, 2);
    }

    const workspace = await Workspace.open({ path: workspacePath });
    const keystore = await KeyStore.open(keystorePath, workspace.replica.getHashSuite());
    const session = new WorkspaceSession({ workspace, keystore });

    // Async projection notices (reactive sync throws, ingest-reject warnings) go
    // to stderr, out of statement stdout / JSON dumps. Set once here so every
    // entry point (interactive REPL, -c, -f, piped stdin) reports them.
    session.onProjectionError = (message) => { stderr.write(message + '\n'); };

    // `\project ...` backend: a capture-provisioned SQLite file, separate from
    // the workspace DAG store. `to <path>` is the file (`:memory:` is allowed).
    session.projectionTargetFactory = async ({ path }) => {
        // Pass dbPath so the target uses kernel-driven WAL watching (not polling)
        // to detect local edits waiting in its capture outbox. `:memory:` has no WAL.
        return new SqliteTarget(new Database(path), { captureChanges: true, dbPath: path });
    };

    session.readTextFile = (path) => readFile(path, 'utf8');
    session.localFiles = nodeLocalFiles();
    session.filesDirectoryFactory = nodeFilesDirectories();

    session.syncMeshFactory = createNodeSyncMeshFactory();

    // Naive first-cut issue sink: structured reports from the mesh/swarm/sync
    // layers go to stderr, out of statement stdout / JSON dumps.
    session.report = (report) => {
        const source = report.source ?? 'issue';
        const detail = report.message ?? report.kind ?? 'unknown issue';
        stderr.write(`[${source} issue] ${detail}\n`);
    };

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
        await stopAllProjections(session);
        await stopAllSyncs(session);
        await workspace.close();
    }
}

main().catch((e) => {
    stderr.write((e instanceof Error ? e.message : String(e)) + '\n');
    process.exitCode = 1;
});
