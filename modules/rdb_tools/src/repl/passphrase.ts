import { stdin as input, stdout as output } from "node:process";
import { createInterface, type Interface } from "node:readline/promises";

import { WorkspaceSession } from "../session/session.js";
import { promptSecret } from "./prompt.js";

export class KeyUnlockDeclinedError extends Error {
    constructor(readonly label: string) {
        super('unlock declined');
        this.name = 'KeyUnlockDeclinedError';
    }
}

export async function fulfillKeyPassphrase(
    session: WorkspaceSession,
    needs: { kind: 'create' | 'unlock' | 'author'; label: string },
    rl: Interface,
): Promise<string> {
    if (session.keystore === undefined) throw new Error('No keystore configured');
    const displayName = keyDisplayLabel(session, needs.label);
    if (needs.kind === 'create') {
        const passphrase = await promptNewPassphrase(rl, displayName);
        const identity = await session.createKey(needs.label, passphrase);
        return `created ${needs.label} ${identity.keyId}`;
    }
    const passphrase = await promptSecret(rl, `passphrase (${displayName}): `);
    const identity = await session.unlockKey(needs.label, passphrase);
    if (needs.kind === 'author') {
        session.selectAuthor(needs.label);
        return `author ${displayName} (${identity.keyId})`;
    }
    return `unlocked ${identity.keyId}`;
}

export async function fulfillPassphraseNeed(
    session: WorkspaceSession,
    needs: { kind: 'create' | 'unlock' | 'author'; label: string },
    rl?: Interface,
): Promise<string> {
    if (!input.isTTY) {
        throw new Error('passphrase required; use the REPL or pass it inline with -c');
    }
    const owned = rl === undefined;
    const activeRl = rl ?? createInterface({ input, output });
    try {
        return await fulfillKeyPassphrase(session, needs, activeRl);
    } finally {
        if (owned) activeRl.close();
    }
}

export async function confirmStatementUnlock(rl: Interface, displayName: string): Promise<void> {
    const answer = (await rl.question(
        `Key ${displayName} needed to sign statement, unlock? [Y/n] `,
    )).trim().toLowerCase();
    if (answer === 'n' || answer === 'no') {
        throw new KeyUnlockDeclinedError(displayName);
    }
}

export function keyDisplayLabel(session: WorkspaceSession, labelOrPrefix: string): string {
    try {
        return session.keystore!.resolveRecord(labelOrPrefix).label;
    } catch {
        return labelOrPrefix;
    }
}

async function promptNewPassphrase(rl: Interface, label: string): Promise<string> {
    while (true) {
        const passphrase = await promptSecret(rl, `passphrase (${label}): `);
        const repeat = await promptSecret(rl, `repeat (${label}): `);
        if (passphrase === repeat) return passphrase;
        output.write('passphrases do not match\n');
    }
}
