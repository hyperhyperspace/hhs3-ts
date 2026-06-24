import type { Interface } from "node:readline/promises";
import { stdout as output } from "node:process";

import { WorkspaceSession } from "../session/session.js";
import { promptSecret } from "./prompt.js";

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

function keyDisplayLabel(session: WorkspaceSession, labelOrPrefix: string): string {
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
