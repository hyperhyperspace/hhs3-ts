import { stdout as output } from "node:process";
import type { Interface } from "node:readline/promises";

import { formatDisplayString } from "../format/display.js";
import { promptInputStream } from "./prompt_tty.js";
import { WorkspaceSession } from "../session/session.js";

export function promptForSession(session: WorkspaceSession, continuation = false): string {
    if (continuation) return '... ';
    return `rdb:${groupDisplayName(session)}:${keyDisplayName(session)}> `;
}

function groupDisplayName(session: WorkspaceSession): string {
    if (session.currentGroup === undefined) return '-';
    const name = session.workspace.roots.get(session.currentGroup)?.name;
    return name ?? formatDisplayString(session, session.currentGroup, { role: 'hash' });
}

function keyDisplayName(session: WorkspaceSession): string {
    const identity = session.selectedAuthor();
    if (identity === undefined) return '-';
    const label = session.keystore?.list().find((key) => key.keyId === identity.keyId)?.label;
    if (label !== undefined) return label;
    return formatDisplayString(session, identity.keyId, { role: 'hash' });
}

export async function promptSecret(rl: Interface, query: string): Promise<string> {
    const promptIn = promptInputStream();

    return new Promise((resolve, reject) => {
        let password = '';
        const wasRaw = promptIn.isRaw;
        const keypressListeners = promptIn.listeners('keypress') as Array<(str: string, key: unknown) => void>;

        output.write(query);
        rl.pause();
        promptIn.removeAllListeners('keypress');
        promptIn.setRawMode(true);
        promptIn.resume();
        promptIn.setEncoding('utf8');

        const cleanup = () => {
            promptIn.removeListener('data', onData);
            if (promptIn.isTTY) promptIn.setRawMode(wasRaw);
            promptIn.pause();
            for (const listener of keypressListeners) promptIn.on('keypress', listener);
            rl.pause();
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

        promptIn.on('data', onData);
    });
}
