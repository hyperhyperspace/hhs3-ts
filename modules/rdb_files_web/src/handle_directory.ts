// HandleDirectory: a FileDirectory over a FileSystemDirectoryHandle.
//
// The handle is the origin private file system (HandleDirectory.opfs), or a
// folder the user picked with showDirectoryPicker() and granted readwrite
// access to (the app asks for the permission; it needs a user gesture).
//
//   write   createWritable() stages into a swap file and commits on close,
//           so a reader sees the old bytes or the new ones; a failed write is
//           aborted. Safari writes this way from 26 on.
//   rename  FileSystemHandle.move() where the browser has it, otherwise a
//           copy then a remove.
//   remove  and rename prune the folders they leave empty (never the root,
//           nor a folder passed to preserve).
//   watch   FileSystemObserver where it exists (Chromium); elsewhere there is
//           no hint and the mount's periodic scan sees every change.

import type { Chunks, DirEntry, FileDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";

type ObserverRecord = { relativePathComponents?: string[] };
type ObserverCtor = new (callback: (records: ObserverRecord[]) => void) => {
    observe(handle: FileSystemHandle, options?: { recursive?: boolean }): Promise<void>;
    disconnect(): void;
};

type Movable = FileSystemFileHandle & { move?: (parent: FileSystemDirectoryHandle, name: string) => Promise<void> };

function isNotFound(err: unknown): boolean {
    const name = (err as { name?: string } | undefined)?.name;
    return name === 'NotFoundError' || name === 'TypeMismatchError';
}

function partsOf(path: string): string[] {
    const parts = path.split('/');
    if (path.length === 0 || path.startsWith('/') || path.includes('\\') || parts.some((s) => s === '' || s === '.' || s === '..')) {
        throw new Error(`bad path '${path}'`);
    }
    return parts;
}

export class HandleDirectory implements FileDirectory {
    private readonly kept = new Set<string>();

    constructor(readonly root: FileSystemDirectoryHandle) {}

    // A folder in the origin private file system, created if missing.
    static async opfs(path: string[] = []): Promise<HandleDirectory> {
        let dir = await navigator.storage.getDirectory();
        for (const name of path) dir = await dir.getDirectoryHandle(name, { create: true });
        return new HandleDirectory(dir);
    }

    private async folder(parts: string[], create: boolean): Promise<FileSystemDirectoryHandle | undefined> {
        let dir = this.root;
        for (const name of parts) {
            try {
                dir = await dir.getDirectoryHandle(name, { create });
            } catch (err) {
                if (isNotFound(err)) return undefined;
                throw err;
            }
        }
        return dir;
    }

    private async file(path: string): Promise<{ parent: FileSystemDirectoryHandle; handle: FileSystemFileHandle; parents: string[] } | undefined> {
        const parts = partsOf(path);
        const parents = parts.slice(0, -1);
        const parent = await this.folder(parents, false);
        if (parent === undefined) return undefined;
        try {
            return { parent, handle: await parent.getFileHandle(parts[parts.length - 1]), parents };
        } catch (err) {
            if (isNotFound(err)) return undefined;
            throw err;
        }
    }

    async list(prefix = ''): Promise<DirEntry[]> {
        const out: DirEntry[] = [];
        const walk = async (dir: FileSystemDirectoryHandle, base: string): Promise<void> => {
            for await (const [name, handle] of dir.entries()) {
                const path = base === '' ? name : `${base}/${name}`;
                if (handle.kind === 'directory') {
                    await walk(handle as FileSystemDirectoryHandle, path);
                } else {
                    const file = await (handle as FileSystemFileHandle).getFile();
                    out.push({ path, size: file.size, mtimeMs: file.lastModified });
                }
            }
        };
        const start = prefix === '' ? this.root : await this.folder(partsOf(prefix), false);
        if (start !== undefined) await walk(start, prefix);
        return out.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    }

    async stat(path: string): Promise<DirEntry | undefined> {
        const found = await this.file(path);
        if (found === undefined) return undefined;
        const file = await found.handle.getFile();
        return { path, size: file.size, mtimeMs: file.lastModified };
    }

    async *read(path: string): AsyncIterable<Uint8Array> {
        const found = await this.file(path);
        if (found === undefined) throw new Error(`no file '${path}'`);
        const reader = (await found.handle.getFile()).stream().getReader();
        try {
            for (;;) {
                const { done, value } = await reader.read();
                if (done) return;
                yield value;
            }
        } finally {
            reader.releaseLock();
        }
    }

    async write(path: string, chunks: Chunks): Promise<DirEntry> {
        const parts = partsOf(path);
        const parent = (await this.folder(parts.slice(0, -1), true))!;
        const handle = await parent.getFileHandle(parts[parts.length - 1], { create: true });
        const writable = await handle.createWritable();
        try {
            for await (const chunk of chunks) await writable.write(chunk as BufferSource);
            await writable.close();
        } catch (err) {
            await writable.abort().catch(() => undefined);
            throw err;
        }
        const file = await handle.getFile();
        return { path, size: file.size, mtimeMs: file.lastModified };
    }

    async remove(path: string): Promise<void> {
        const found = await this.file(path);
        if (found === undefined) return;
        await found.parent.removeEntry(partsOf(path).pop()!);
        await this.prune(found.parents);
    }

    async rename(from: string, to: string): Promise<void> {
        if (from === to) return;
        const found = await this.file(from);
        if (found === undefined) throw new Error(`no file '${from}'`);
        const target = partsOf(to);
        const move = (found.handle as Movable).move;
        if (typeof move === 'function') {
            const parent = (await this.folder(target.slice(0, -1), true))!;
            await move.call(found.handle, parent, target[target.length - 1]);
            await this.prune(found.parents);
            return;
        }
        await this.write(to, this.read(from));
        await this.remove(from);
    }

    async ensureDir(path: string): Promise<void> {
        if (await this.folder(partsOf(path), true) === undefined) throw new Error(`a file is in the way of '${path}'`);
    }

    preserve(paths: string[]): void {
        for (const path of paths) this.kept.add(partsOf(path).join('/'));
    }

    watch(onChange: (path?: string) => void): () => void {
        const Observer = (globalThis as { FileSystemObserver?: ObserverCtor }).FileSystemObserver;
        if (Observer === undefined) return () => undefined;
        const observer = new Observer((records) => {
            for (const record of records) onChange(record.relativePathComponents?.join('/'));
        });
        observer.observe(this.root, { recursive: true }).catch(() => undefined);
        return () => observer.disconnect();
    }

    // Removes empty folders along `parents`, deepest first (never the root),
    // stopping at a preserved one.
    private async prune(parents: string[]): Promise<void> {
        for (let depth = parents.length; depth > 0; depth--) {
            if (this.kept.has(parents.slice(0, depth).join('/'))) return;
            const parent = await this.folder(parents.slice(0, depth - 1), false);
            const dir = await this.folder(parents.slice(0, depth), false);
            if (parent === undefined || dir === undefined) return;
            for await (const _ of dir.keys()) return;
            await parent.removeEntry(parents[depth - 1]);
        }
    }
}
