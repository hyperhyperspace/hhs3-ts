// Where a command runs: the catalog repository is the nearest folder, from
// the start folder up, that holds rpack.json; the version folder is the
// work/<folder>/ the start folder is in, if any.

import { promises as fs } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";

import { CONFIG_FILE, WORK_DIR } from "@hyper-hyper-space/hhs3_rpack";

export type Located = { root: string; folder?: string };

export async function locate(start: string): Promise<Located | undefined> {
    const from = resolve(start);
    let dir = from;
    while (!await exists(join(dir, CONFIG_FILE))) {
        const parent = dirname(dir);
        if (parent === dir) return undefined;
        dir = parent;
    }
    const [top, folder] = relative(dir, from).split(sep);
    return top === WORK_DIR && folder !== undefined && folder !== '' ? { root: dir, folder } : { root: dir };
}

async function exists(path: string): Promise<boolean> {
    try {
        await fs.access(path);
        return true;
    } catch {
        return false;
    }
}
