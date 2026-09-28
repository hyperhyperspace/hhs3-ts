import { formatDisplayString } from "./format/display.js";
import type { ReplSession } from "./session.js";

export function promptForSession(session: ReplSession, continuation = false): string {
    if (continuation) return '... ';
    return `rdb:${groupDisplayName(session)}:${keyDisplayName(session)}> `;
}

// `db.group`, `db` or `-`.
function groupDisplayName(session: ReplSession): string {
    const name = (id: string) => session.workspace.roots.get(id)?.name ?? formatDisplayString(session, id, { role: 'hash' });
    const db = session.currentDatabase === undefined ? undefined : name(session.currentDatabase);
    if (session.currentGroup === undefined) return db ?? '-';
    return db === undefined ? name(session.currentGroup) : `${db}.${name(session.currentGroup)}`;
}

function keyDisplayName(session: ReplSession): string {
    const identity = session.selectedAuthor();
    if (identity === undefined) return '-';
    const label = session.keyVault?.list().find((key) => key.keyId === identity.keyId)?.label;
    return label ?? formatDisplayString(session, identity.keyId, { role: 'hash' });
}
