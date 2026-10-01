import { promises as fs } from "node:fs";
import { dirname } from "node:path";

// `hosts/<name>/run/rhost.lock` holds the pid of the process serving the host.
// A lock whose pid is gone is stale and taken over.

const held = new Set<string>();

function isAlive(pid: number): boolean {
    try {
        process.kill(pid, 0);
        return true;
    } catch (e) {
        return (e as NodeJS.ErrnoException).code === 'EPERM';
    }
}

async function readPid(path: string): Promise<number | undefined> {
    let text: string;
    try {
        text = await fs.readFile(path, 'utf8');
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
        throw e;
    }
    const pid = Number(text.trim());
    return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
}

// The pid holding the lock, if that process is alive.
export async function lockHolder(path: string): Promise<number | undefined> {
    const pid = await readPid(path);
    if (pid === undefined) return undefined;
    if (pid === process.pid) return held.has(path) ? pid : undefined;
    return isAlive(pid) ? pid : undefined;
}

export async function acquireLock(path: string, what: string): Promise<() => Promise<void>> {
    await fs.mkdir(dirname(path), { recursive: true });
    for (let attempt = 0; attempt < 2; attempt++) {
        try {
            const handle = await fs.open(path, 'wx');
            try {
                await handle.writeFile(`${process.pid}\n`);
            } finally {
                await handle.close();
            }
            held.add(path);
            let released = false;
            return async () => {
                if (released) return;
                released = true;
                held.delete(path);
                if (await readPid(path) === process.pid) await fs.rm(path, { force: true });
            };
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code !== 'EEXIST') throw e;
            const holder = await lockHolder(path);
            if (holder !== undefined) throw new Error(`${what} is already running (pid ${holder})`);
            await fs.rm(path, { force: true });
        }
    }
    throw new Error(`could not take the lock ${path}`);
}
