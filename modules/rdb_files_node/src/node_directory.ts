// NodeDirectory: a FileDirectory on the local filesystem, rooted at a folder.
//
//   write   streams into `.hhs/tmp/` under the root, fsyncs, then renames over
//           the target (atomic on one filesystem), creating parent folders;
//   remove  and rename prune the folders they leave empty (never the root,
//           nor a folder passed to preserve);
//   list    walks regular files only (symlinks and specials are skipped);
//   watch   a recursive fs.watch hint, with the changed path when the OS
//           reports one; where recursive watching fails, there is no hint and
//           the mount's periodic scan still sees every change.

import { createReadStream, mkdirSync, watch as fsWatch, type FSWatcher } from "node:fs";
import { mkdir, open, readdir, rename, rm, rmdir, stat } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { basename, dirname, join, relative, resolve, sep } from "node:path";

import type { Chunks, DirEntry, FileDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";

export const TMP_DIR = '.hhs/tmp';

function errorCode(err: unknown): string | undefined {
    return (err as { code?: string } | undefined)?.code;
}

export class NodeDirectory implements FileDirectory {
    readonly root: string;
    private readonly kept = new Set<string>();

    constructor(root: string) {
        this.root = resolve(root);
    }

    // A NodeDirectory whose root folder exists.
    static async open(root: string): Promise<NodeDirectory> {
        const dir = new NodeDirectory(root);
        await mkdir(dir.root, { recursive: true });
        return dir;
    }

    private abs(path: string): string {
        if (path.length === 0 || path.startsWith('/') || path.includes('\\') || path.includes('\0')
            || path.split('/').some((s) => s === '' || s === '.' || s === '..')) {
            throw new Error(`bad path '${path}'`);
        }
        return join(this.root, ...path.split('/'));
    }

    private rel(abs: string): string {
        return relative(this.root, abs).split(sep).join('/');
    }

    async list(prefix = ''): Promise<DirEntry[]> {
        const out: DirEntry[] = [];
        const walk = async (folder: string): Promise<void> => {
            let entries;
            try {
                entries = await readdir(folder, { withFileTypes: true });
            } catch (err) {
                if (errorCode(err) === 'ENOENT' || errorCode(err) === 'ENOTDIR') return;
                throw err;
            }
            for (const entry of entries) {
                const full = join(folder, entry.name);
                if (entry.isDirectory()) {
                    await walk(full);
                } else if (entry.isFile()) {
                    const s = await stat(full).catch(() => undefined);
                    if (s !== undefined) out.push({ path: this.rel(full), size: s.size, mtimeMs: s.mtimeMs });
                }
            }
        };
        await walk(prefix === '' ? this.root : this.abs(prefix));
        return out.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    }

    async stat(path: string): Promise<DirEntry | undefined> {
        try {
            const s = await stat(this.abs(path));
            return s.isFile() ? { path, size: s.size, mtimeMs: s.mtimeMs } : undefined;
        } catch (err) {
            if (errorCode(err) === 'ENOENT' || errorCode(err) === 'ENOTDIR') return undefined;
            throw err;
        }
    }

    async *read(path: string): AsyncIterable<Uint8Array> {
        for await (const chunk of createReadStream(this.abs(path))) yield chunk as Uint8Array;
    }

    async write(path: string, chunks: Chunks): Promise<DirEntry> {
        const target = this.abs(path);
        const tmpDir = join(this.root, ...TMP_DIR.split('/'));
        await mkdir(tmpDir, { recursive: true });
        const tmp = join(tmpDir, `${randomBytes(8).toString('hex')}.tmp`);
        try {
            const handle = await open(tmp, 'w');
            try {
                for await (const chunk of chunks) await handle.write(chunk);
                await handle.sync();
            } finally {
                await handle.close();
            }
            await mkdir(dirname(target), { recursive: true });
            await rename(tmp, target);
        } catch (err) {
            await rm(tmp, { force: true });
            throw err;
        }
        const s = await stat(target);
        return { path, size: s.size, mtimeMs: s.mtimeMs };
    }

    async remove(path: string): Promise<void> {
        const target = this.abs(path);
        await rm(target, { force: true });
        await this.prune(dirname(target));
    }

    async rename(from: string, to: string): Promise<void> {
        const source = this.abs(from);
        const target = this.abs(to);
        if (source === target) return;
        await mkdir(dirname(target), { recursive: true });
        await rename(source, target);
        await this.prune(dirname(source));
    }

    async ensureDir(path: string): Promise<void> {
        await mkdir(this.abs(path), { recursive: true });
    }

    preserve(paths: string[]): void {
        for (const path of paths) this.kept.add(this.abs(path));
    }

    watch(onChange: (path?: string) => void): () => void {
        // macOS reports changes to the root folder itself under the root's own
        // name, which can't be told apart from a top-level entry of that name.
        const self = basename(this.root);
        let watcher: FSWatcher;
        try {
            mkdirSync(this.root, { recursive: true });
            watcher = fsWatch(this.root, { recursive: true }, (_event, filename) => {
                const path = filename === null || filename === undefined ? undefined : String(filename).split(sep).join('/');
                onChange(path === self ? undefined : path);
            });
        } catch {
            return () => undefined;
        }
        watcher.on('error', () => onChange());
        return () => watcher.close();
    }

    // Removes empty folders from `folder` up to (not including) the root,
    // stopping at a preserved one.
    private async prune(folder: string): Promise<void> {
        let current = folder;
        while (current !== this.root && current.startsWith(this.root + sep)) {
            if (this.kept.has(current)) return;
            try {
                await rmdir(current);
            } catch {
                return;
            }
            current = dirname(current);
        }
    }
}
