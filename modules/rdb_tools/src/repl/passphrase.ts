import { stdout as output } from "node:process";
import type { Interface } from "node:readline/promises";
import type { PassphraseRequest } from "@hyper-hyper-space/hhs3_rdb_repl";

import { WorkspaceSession } from "../session/session.js";
import { canPromptForKeys, closePromptTty, createPromptInterface } from "./prompt_tty.js";
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
    if (!canPromptForKeys(session)) {
        throw new Error('passphrase required; use the REPL or pass it inline with -c');
    }
    const owned = rl === undefined;
    const activeRl = rl ?? createPromptInterface(session);
    if (activeRl === undefined) {
        throw new Error('passphrase required; use the REPL or pass it inline with -c');
    }
    try {
        return await fulfillKeyPassphrase(session, needs, activeRl);
    } finally {
        if (owned) {
            activeRl.close();
            closePromptTty();
        }
    }
}

export async function requestPassphrase(
    session: WorkspaceSession,
    need: PassphraseRequest,
    rl?: Interface,
): Promise<string | undefined> {
    if (!canPromptForKeys(session) && rl === undefined) return undefined;
    const owned = rl === undefined;
    const activeRl = rl ?? createPromptInterface(session);
    if (activeRl === undefined) return undefined;
    try {
        const displayName = keyDisplayLabel(session, need.label);
        if (need.kind === 'statement-unlock') {
            await confirmStatementUnlock(activeRl, displayName);
        }
        return need.kind === 'create'
            ? promptNewPassphrase(activeRl, displayName)
            : promptSecret(activeRl, `passphrase (${displayName}): `);
    } finally {
        if (owned) {
            activeRl.close();
            closePromptTty();
        }
    }
}

export async function confirmStatementUnlock(rl: Interface, displayName: string): Promise<void> {
    if (!await confirmUnlockForSign(rl, displayName)) {
        throw new KeyUnlockDeclinedError(displayName);
    }
}

export async function confirmRefUpdateUnlock(
    rl: Interface,
    observerGroup: string,
    authorLabel: string,
): Promise<void> {
    const answer = (await rl.question(
        `Update ref on ${observerGroup} needs ${authorLabel}. Unlock? [Y/n] `,
    )).trim().toLowerCase();
    if (answer === 'n' || answer === 'no') {
        throw new KeyUnlockDeclinedError(authorLabel);
    }
}

export async function confirmSignRetry(rl: Interface, authorLabel: string, op: string): Promise<boolean> {
    const answer = (await rl.question(
        `${op} needs ${authorLabel}. Sign and retry? [Y/n] `,
    )).trim().toLowerCase();
    return answer !== 'n' && answer !== 'no';
}

export async function confirmUnlockForSign(rl: Interface, displayName: string): Promise<boolean> {
    const answer = (await rl.question(
        `Key ${displayName} needed to sign statement, unlock? [Y/n] `,
    )).trim().toLowerCase();
    return answer !== 'n' && answer !== 'no';
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
