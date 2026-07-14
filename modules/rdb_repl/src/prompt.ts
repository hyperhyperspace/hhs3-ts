import { formatDisplayString } from "./format/display.js";
import type { ReplSession } from "./session.js";

export function promptForSession(session: ReplSession, continuation = false): string {
    if (continuation) return '... ';
    return `rdb:${groupDisplayName(session)}:${keyDisplayName(session)}> `;
}

function groupDisplayName(session: ReplSession): string {
    if (session.currentGroup === undefined) return '-';
    return session.workspace.roots.get(session.currentGroup)?.name
        ?? formatDisplayString(session, session.currentGroup, { role: 'hash' });
}

function keyDisplayName(session: ReplSession): string {
    const identity = session.selectedAuthor();
    if (identity === undefined) return '-';
    const label = session.keyVault?.list().find((key) => key.keyId === identity.keyId)?.label;
    return label ?? formatDisplayString(session, identity.keyId, { role: 'hash' });
}
