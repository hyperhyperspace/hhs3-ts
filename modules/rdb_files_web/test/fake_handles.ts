// An in-memory stand-in for the File System Access handles HandleDirectory
// uses: directories, files, and writables that commit on close (a swap file,
// like the browsers'). `move` is optional, as it is across browsers.

let clock = 0;

function notFound(name: string): DOMException {
    return new DOMException(`'${name}' not found`, 'NotFoundError');
}

export class FakeFileHandle {
    readonly kind = 'file';
    bytes = new Uint8Array(0);
    mtime = ++clock;
    constructor(public name: string, private readonly owner: FakeDirectoryHandle, movable: boolean) {
        if (!movable) (this as { move?: unknown }).move = undefined;
    }

    async getFile(): Promise<File> {
        return new File([this.bytes], this.name, { lastModified: this.mtime });
    }

    async createWritable() {
        const parts: Uint8Array[] = [];
        return {
            write: async (chunk: Uint8Array) => { parts.push(new Uint8Array(chunk)); },
            close: async () => {
                const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
                let offset = 0;
                for (const p of parts) { out.set(p, offset); offset += p.length; }
                this.bytes = out;
                this.mtime = ++clock;
            },
            abort: async () => { parts.length = 0; },
        };
    }

    async move(parent: FakeDirectoryHandle, name: string): Promise<void> {
        this.owner.children.delete(this.name);
        this.name = name;
        parent.children.set(name, this);
    }
}

export class FakeDirectoryHandle {
    readonly kind = 'directory';
    readonly children = new Map<string, FakeDirectoryHandle | FakeFileHandle>();
    constructor(readonly name: string, private readonly movable = true) {}

    async getDirectoryHandle(name: string, opts: { create?: boolean } = {}): Promise<FakeDirectoryHandle> {
        const child = this.children.get(name);
        if (child !== undefined) {
            if (child.kind !== 'directory') throw new DOMException(`'${name}' is a file`, 'TypeMismatchError');
            return child;
        }
        if (opts.create !== true) throw notFound(name);
        const dir = new FakeDirectoryHandle(name, this.movable);
        this.children.set(name, dir);
        return dir;
    }

    async getFileHandle(name: string, opts: { create?: boolean } = {}): Promise<FakeFileHandle> {
        const child = this.children.get(name);
        if (child !== undefined) {
            if (child.kind !== 'file') throw new DOMException(`'${name}' is a folder`, 'TypeMismatchError');
            return child;
        }
        if (opts.create !== true) throw notFound(name);
        const file = new FakeFileHandle(name, this, this.movable);
        this.children.set(name, file);
        return file;
    }

    async removeEntry(name: string): Promise<void> {
        const child = this.children.get(name);
        if (child === undefined) throw notFound(name);
        if (child.kind === 'directory' && child.children.size > 0) throw new DOMException(`'${name}' is not empty`, 'InvalidModificationError');
        this.children.delete(name);
    }

    async *entries(): AsyncIterable<[string, FakeDirectoryHandle | FakeFileHandle]> {
        for (const entry of [...this.children.entries()]) yield entry;
    }

    async *keys(): AsyncIterable<string> {
        for (const name of [...this.children.keys()]) yield name;
    }
}
