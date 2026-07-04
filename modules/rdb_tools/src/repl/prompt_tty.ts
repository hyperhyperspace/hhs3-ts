import { openSync } from "node:fs";
import { stdin as input, stdout as output } from "node:process";
import { createInterface, type Interface } from "node:readline/promises";
import tty from "node:tty";

import type { WorkspaceSession } from "../session/session.js";

let ttyIn: tty.ReadStream | undefined;

export function canPromptForKeys(session: WorkspaceSession): boolean {
    return input.isTTY || session.promptForKeys;
}

export function promptInputStream(): NodeJS.ReadStream {
    if (input.isTTY) return input;
    if (ttyIn === undefined) {
        const path = process.platform === 'win32' ? 'CONIN$' : '/dev/tty';
        const fd = openSync(path, 'r');
        ttyIn = new tty.ReadStream(fd);
    }
    return ttyIn;
}

export function createPromptInterface(session: WorkspaceSession): Interface | undefined {
    if (!canPromptForKeys(session)) return undefined;
    return createInterface({ input: promptInputStream(), output });
}

export function closePromptTty(): void {
    if (input.isTTY) return;
    if (ttyIn === undefined) return;
    ttyIn.removeAllListeners();
    ttyIn.destroy();
    ttyIn = undefined;
}
