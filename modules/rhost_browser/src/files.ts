// File mounts live in the origin private file system, under
// rhost/<app>/<host>/<mount path>. A picked folder is a different
// HandleDirectory constructor; the platform does not ask for one.

import { HandleDirectory } from "@hyper-hyper-space/hhs3_rdb_files_web";
import { browserFilePath } from "@hyper-hyper-space/hhs3_rhost_client";
import type { FilesMountConfig } from "@hyper-hyper-space/hhs3_rhost";

export function opfsDirectory(app: string, host: string, mount: FilesMountConfig): Promise<HandleDirectory> {
    return HandleDirectory.opfs(browserFilePath(app, host, mount.path));
}

export async function removeOpfsHost(app: string, host: string): Promise<void> {
    const storage = globalThis.navigator?.storage;
    if (storage === undefined) return;
    const root = await storage.getDirectory();
    try {
        const rhost = await root.getDirectoryHandle('rhost');
        const appDir = await rhost.getDirectoryHandle(app);
        await appDir.removeEntry(host, { recursive: true });
    } catch (err) {
        if ((err as { name?: string }).name === 'NotFoundError') return;
        throw err;
    }
}
