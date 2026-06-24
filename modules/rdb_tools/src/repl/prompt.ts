import type { Interface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";

import { WorkspaceSession } from "../session/session.js";

export function promptForSession(session: WorkspaceSession, continuation = false): string {
    if (continuation) return '... ';
    return `rdb:${groupDisplayName(session)}:${keyDisplayName(session)}> `;
}

function groupDisplayName(session: WorkspaceSession): string {
    if (session.currentGroup === undefined) return '-';
    const name = session.workspace.roots.get(session.currentGroup)?.name;
    return name ?? short(session.currentGroup);
}

function keyDisplayName(session: WorkspaceSession): string {
    const identity = session.selectedAuthor();
    if (identity === undefined) return '-';
    const label = session.keystore?.list().find((key) => key.keyId === identity.keyId)?.label;
    return label ?? short(identity.keyId);
}

function short(value: string): string {
    return value.length <= 10 ? value : value.slice(0, 10);
}

export async function promptSecret(rl: Interface, query: string): Promise<string> {
    if (!input.isTTY) throw new Error('passphrase required; use the REPL or pass it inline with -c');

    return new Promise((resolve, reject) => {
        let password = '';
        const wasRaw = input.isRaw;
        const keypressListeners = input.listeners('keypress') as Array<(str: string, key: unknown) => void>;

        output.write(query);
        rl.pause();
        input.removeAllListeners('keypress');
        input.setRawMode(true);
        input.resume();
        input.setEncoding('utf8');

        const cleanup = () => {
            input.removeListener('data', onData);
            if (input.isTTY) input.setRawMode(wasRaw);
            for (const listener of keypressListeners) input.on('keypress', listener);
            rl.resume();
        };

        const onData = (char: string) => {
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
                if (password.length > 0) password = password.slice(0, -1);
                return;
            }
            if (char.length === 1 && char >= ' ') {
                password += char;
            }
        };

        input.on('data', onData);
    });
}
