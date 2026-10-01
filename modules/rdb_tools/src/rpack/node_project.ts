// A catalog repository on the file system. Files are written aside and
// renamed into place, and a folder is built aside whole, so an interrupted
// write leaves either the old content or the new.

import { promises as fs } from "node:fs";
import { basename, dirname, join } from "node:path";

import type { RpackProject } from "@hyper-hyper-space/hhs3_rpack";

function isMissing(err: unknown): boolean {
    return (err as NodeJS.ErrnoException).code === 'ENOENT';
}

export class NodeProject implements RpackProject {
    constructor(readonly dir: string) {}

    private path(relative: string): string {
        return join(this.dir, ...relative.split('/'));
    }

    async read(path: string): Promise<string | undefined> {
        try {
            return await fs.readFile(this.path(path), 'utf8');
        } catch (err) {
            if (isMissing(err)) return undefined;
            throw err;
        }
    }

    async list(dir: string): Promise<string[]> {
        try {
            return (await fs.readdir(this.path(dir))).filter((name) => !name.startsWith('.')).sort();
        } catch (err) {
            if (isMissing(err)) return [];
            throw err;
        }
    }

    async write(path: string, text: string): Promise<void> {
        const full = this.path(path);
        await fs.mkdir(dirname(full), { recursive: true });
        const aside = join(dirname(full), `.${basename(full)}.${process.pid}.tmp`);
        await fs.writeFile(aside, text);
        await fs.rename(aside, full);
    }

    async writeFolder(path: string, files: { [name: string]: string }): Promise<void> {
        const full = this.path(path);
        await fs.mkdir(dirname(full), { recursive: true });
        const aside = join(dirname(full), `.${basename(full)}.${process.pid}.tmp`);
        await fs.rm(aside, { recursive: true, force: true });
        await fs.mkdir(aside);
        for (const [name, text] of Object.entries(files)) await fs.writeFile(join(aside, name), text);
        await fs.rm(full, { recursive: true, force: true });
        await fs.rename(aside, full);
    }

    async remove(path: string): Promise<void> {
        await fs.rm(this.path(path), { recursive: true, force: true });
    }
}
