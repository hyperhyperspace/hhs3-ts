import type { Interface } from "node:readline/promises";
import { promptForSession as portablePromptForSession } from "@hyper-hyper-space/hhs3_rdb_repl";

import { promptInputStream, promptOutputStream } from "./prompt_tty.js";
import { WorkspaceSession } from "../session/session.js";

// Arrow keys and bracketed paste markers arrive as escape sequences; they are
// never part of the passphrase.
const ESCAPE_SEQUENCE = /\x1b(?:\[[0-?]*[ -/]*[@-~]|O.)?/g;

export function promptForSession(session: WorkspaceSession, continuation = false): string {
    return portablePromptForSession(session, continuation);
}

export async function promptSecret(rl: Interface, query: string): Promise<string> {
    const promptIn = promptInputStream();
    const output = promptOutputStream();
    // `closed` is not in the typings; Node 24 throws on pause() once it is set.
    const pauseRl = () => { if ((rl as { closed?: boolean }).closed !== true) rl.pause(); };

    return new Promise((resolve, reject) => {
        let password = '';
        let cleaned = false;
        const wasRaw = promptIn.isRaw;
        const keypressListeners = promptIn.listeners('keypress') as Array<(str: string, key: unknown) => void>;

        output.write(query);
        pauseRl();
        promptIn.removeAllListeners('keypress');
        promptIn.setRawMode(true);
        promptIn.resume();
        promptIn.setEncoding('utf8');

        const cleanup = () => {
            if (cleaned) return;
            cleaned = true;
            promptIn.removeListener('data', onData);
            if (promptIn.isTTY) promptIn.setRawMode(wasRaw);
            promptIn.pause();
            for (const listener of keypressListeners) promptIn.on('keypress', listener);
            pauseRl();
        };

        const onData = (chunk: string) => {
            try {
                for (const char of chunk.replace(ESCAPE_SEQUENCE, '')) {
                    if (char === '\n' || char === '\r' || char === '\u0004') {
                        output.write('\n');
                        cleanup();
                        resolve(password);
                        return;
                    }
                    if (char === '\u0003') {
                        output.write('\n');
                        cleanup();
                        reject(new Error('cancelled'));
                        return;
                    }
                    if (char === '\u007f' || char === '\b') {
                        password = password.slice(0, -1);
                        continue;
                    }
                    if (char >= ' ') password += char;
                }
            } catch (e) {
                try { cleanup(); } catch { /* report the first failure */ }
                reject(e);
            }
        };

        promptIn.on('data', onData);
    });
}
