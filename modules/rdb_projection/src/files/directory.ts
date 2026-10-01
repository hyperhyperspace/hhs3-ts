// A mounted folder, as a file mount sees it.
//
// Paths are relative and '/'-separated. `write` replaces a file atomically (a
// reader sees the old bytes or the new ones, never a mix) and creates missing
// parent folders. `remove` and `rename` may drop the folders they leave
// empty, but never one passed to `preserve`. `watch` is only a hint that
// something may have changed, with the path when it is known: a mount's scans
// are authoritative.
//
// MemoryDirectory is an in-memory FileDirectory for tests. Its mtimes come
// from a counter, so every write is a visible change. It holds no folders;
// it records the ones ensured and preserved.

export type DirEntry = {
    path: string;
    size: number;
    mtimeMs: number;
};

export type Chunks = AsyncIterable<Uint8Array> | Iterable<Uint8Array>;

export interface FileDirectory {
    // Every file under the folder `prefix` ('' for all), recursively.
    list(prefix?: string): Promise<DirEntry[]>;
    stat(path: string): Promise<DirEntry | undefined>;
    read(path: string): AsyncIterable<Uint8Array>;
    write(path: string, chunks: Chunks): Promise<DirEntry>;
    // No error when the file is already gone.
    remove(path: string): Promise<void>;
    rename(from: string, to: string): Promise<void>;
    // Creates the folder and its missing parents; no error when it exists.
    ensureDir(path: string): Promise<void>;
    // Folders that stay when a remove or rename leaves them empty.
    preserve(paths: string[]): void;
    watch?(onChange: (path?: string) => void): () => void;
}

export function under(path: string, prefix: string): boolean {
    return prefix === '' || path.startsWith(`${prefix}/`);
}

export async function collectBytes(chunks: Chunks): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    let length = 0;
    for await (const chunk of chunks) {
        parts.push(chunk);
        length += chunk.length;
    }
    const out = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
        out.set(part, offset);
        offset += part.length;
    }
    return out;
}

type MemoryFile = { bytes: Uint8Array; mtimeMs: number };

function checkPath(path: string): void {
    if (path.length === 0 || path.startsWith('/') || path.split('/').some((s) => s === '' || s === '.' || s === '..')) {
        throw new Error(`bad path '${path}'`);
    }
}

export class MemoryDirectory implements FileDirectory {
    private readonly files = new Map<string, MemoryFile>();
    private readonly ensured = new Set<string>();
    private readonly kept = new Set<string>();
    private readonly listeners = new Set<(path?: string) => void>();
    private clock = 0;

    async list(prefix = ''): Promise<DirEntry[]> {
        return [...this.files.entries()]
            .filter(([path]) => under(path, prefix))
            .map(([path, file]) => ({ path, size: file.bytes.length, mtimeMs: file.mtimeMs }))
            .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    }

    async stat(path: string): Promise<DirEntry | undefined> {
        const file = this.files.get(path);
        return file === undefined ? undefined : { path, size: file.bytes.length, mtimeMs: file.mtimeMs };
    }

    async *read(path: string): AsyncIterable<Uint8Array> {
        const file = this.files.get(path);
        if (file === undefined) throw new Error(`no file '${path}'`);
        yield file.bytes.slice();
    }

    async write(path: string, chunks: Chunks): Promise<DirEntry> {
        const bytes = await collectBytes(chunks);
        this.checkWritable(path);
        const file = { bytes, mtimeMs: ++this.clock };
        this.files.set(path, file);
        this.notify(path);
        return { path, size: bytes.length, mtimeMs: file.mtimeMs };
    }

    async remove(path: string): Promise<void> {
        if (this.files.delete(path)) this.notify(path);
    }

    async rename(from: string, to: string): Promise<void> {
        const file = this.files.get(from);
        if (file === undefined) throw new Error(`no file '${from}'`);
        if (from === to) return;
        this.checkWritable(to);
        this.files.delete(from);
        this.files.set(to, file);
        this.notify(from);
        this.notify(to);
    }

    async ensureDir(path: string): Promise<void> {
        checkPath(path);
        for (const other of this.files.keys()) {
            if (other === path || path.startsWith(`${other}/`)) throw new Error(`'${other}' is a file`);
        }
        this.ensured.add(path);
    }

    preserve(paths: string[]): void {
        for (const path of paths) {
            checkPath(path);
            this.kept.add(path);
        }
    }

    watch(onChange: (path?: string) => void): () => void {
        this.listeners.add(onChange);
        return () => { this.listeners.delete(onChange); };
    }

    // Test helpers.

    async writeText(path: string, text: string): Promise<DirEntry> {
        return this.write(path, [new TextEncoder().encode(text)]);
    }

    async readText(path: string): Promise<string | undefined> {
        const file = this.files.get(path);
        return file === undefined ? undefined : new TextDecoder().decode(file.bytes);
    }

    paths(prefix = ''): string[] {
        return [...this.files.keys()].filter((p) => under(p, prefix)).sort();
    }

    ensuredDirs(): string[] {
        return [...this.ensured].sort();
    }

    preservedDirs(): string[] {
        return [...this.kept].sort();
    }

    // A file cannot sit where a folder is, or under a file.
    private checkWritable(path: string): void {
        checkPath(path);
        for (const other of this.files.keys()) {
            if (other.startsWith(`${path}/`)) throw new Error(`'${path}' is a folder`);
            if (path.startsWith(`${other}/`)) throw new Error(`'${other}' is a file`);
        }
    }

    private notify(path: string): void {
        for (const listener of this.listeners) listener(path);
    }
}
