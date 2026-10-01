// The host's local files for C-SQL's PUT FILE and GET ... TO, on Node.
// Paths resolve against `cwd`. A write goes to a temporary file next to the
// target, then renames over it, creating parent folders.

import { createReadStream } from "node:fs";
import { mkdir, open, rename, rm, stat } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { dirname, resolve } from "node:path";

import type { LocalFileAccess } from "@hyper-hyper-space/hhs3_rdb_lang";

export function nodeLocalFiles(cwd: string = process.cwd()): LocalFileAccess {
    return {
        async open(path) {
            const abs = resolve(cwd, path);
            const s = await stat(abs).catch(() => undefined);
            if (s === undefined || !s.isFile()) throw new Error(`'${path}' is not a file`);
            return { size: s.size, read: () => createReadStream(abs) as AsyncIterable<Uint8Array> };
        },
        async write(path, chunks) {
            const abs = resolve(cwd, path);
            await mkdir(dirname(abs), { recursive: true });
            const tmp = `${abs}.${randomBytes(6).toString('hex')}.tmp`;
            try {
                const handle = await open(tmp, 'w');
                try {
                    for await (const chunk of chunks) await handle.write(chunk);
                    await handle.sync();
                } finally {
                    await handle.close();
                }
                await rename(tmp, abs);
            } catch (err) {
                await rm(tmp, { force: true });
                throw err;
            }
        },
    };
}
