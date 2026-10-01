// One line per file mount, shared by `\project files` and `rhost status`.

import type { FilesMountStatus } from "@hyper-hyper-space/hhs3_rdb_projection";

// "media at media: 42 files, 3 missing, 1 local-only, 2 changes waiting, read-only for $alice"
// `as` is the label of the mount's writer, when the host knows it.
export function formatFilesMount(mount: FilesMountStatus, as?: string): string {
    if (mount.state === 'pending') {
        return `${mount.name} at ${mount.path}: pending, no FILES ${mount.name} deployed yet${mount.lastError !== undefined ? ` (${mount.lastError})` : ''}`;
    }
    const parts = [`${mount.files ?? 0} files`];
    if ((mount.missing ?? 0) > 0) parts.push(`${mount.missing} missing`);
    if ((mount.localOnly ?? 0) > 0) parts.push(`${mount.localOnly} local-only`);
    if ((mount.waiting ?? 0) > 0) parts.push(`${mount.waiting} ${mount.waiting === 1 ? 'change' : 'changes'} waiting`);
    if (mount.writable === true) parts.push(as === undefined ? 'writable' : `writable by $${as}`);
    else parts.push(as === undefined ? 'read-only' : `read-only for $${as}`);
    if (mount.lastError !== undefined) parts.push(`error: ${mount.lastError}`);
    return `${mount.name} at ${mount.path}: ${parts.join(', ')}`;
}
