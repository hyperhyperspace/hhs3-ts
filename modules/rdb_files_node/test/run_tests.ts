import { rm } from "node:fs/promises";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { randomBytes } from "node:crypto";

import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { MemoryDirectory, collectBytes, type FileDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";

import { NodeDirectory, TMP_DIR } from "../src/index.js";

const ROOT = join(process.cwd(), 'test-tmp');

async function withDir(fn: (dir: NodeDirectory) => Promise<void>): Promise<void> {
    const dir = await NodeDirectory.open(join(ROOT, randomBytes(6).toString('hex')));
    try {
        await fn(dir);
    } finally {
        await rm(dir.root, { recursive: true, force: true });
    }
}

const text = (s: string) => new TextEncoder().encode(s);

async function readText(dir: FileDirectory, path: string): Promise<string> {
    return new TextDecoder().decode(await collectBytes(dir.read(path)));
}

async function expectThrows(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

async function poll(fn: () => boolean, why: string, timeoutMs = 3000): Promise<void> {
    const start = Date.now();
    while (!fn()) {
        if (Date.now() - start > timeoutMs) throw new Error(`poll timed out: ${why}`);
        await new Promise((r) => setTimeout(r, 20));
    }
}

// The same operations on any FileDirectory, for parity with MemoryDirectory.
async function script(dir: FileDirectory): Promise<string> {
    await dir.write('common/a.txt', [text('alpha')]);
    await dir.write('common/docs/b.md', [text('be'), text('ta')]);
    await dir.write('keys/1/c.bin', [new Uint8Array([1, 2, 3])]);
    await dir.rename('common/docs/b.md', 'common/b.md');
    await dir.remove('keys/1/c.bin');
    await dir.remove('keys/1/never-there');
    await dir.write('common/a.txt', [text('alpha, again')]);
    const listed = (await dir.list('')).filter((e) => !e.path.startsWith('.hhs/')).map((e) => `${e.path}:${e.size}`);
    return `${listed.join(',')}|${(await dir.list('common')).length}|${await readText(dir, 'common/b.md')}`;
}

const tests = [
    {
        name: '[NFILES01] write, read, stat and list nested files, by prefix',
        invoke: () => withDir(async (dir) => {
            await dir.write('common/docs/a.txt', [text('hello '), text('world')]);
            await dir.write('keys/4/b.txt', [text('b')]);
            assertEquals(await readText(dir, 'common/docs/a.txt'), 'hello world', 'the bytes read back');
            assertEquals((await dir.stat('common/docs/a.txt'))?.size, 11, 'stat has the size');
            assertTrue(await dir.stat('common/docs') === undefined, 'a folder is not a file');
            assertTrue(await dir.stat('nope.txt') === undefined, 'a missing file has no stat');
            const all = (await dir.list()).map((e) => e.path).filter((p) => !p.startsWith('.hhs/'));
            assertEquals(all.join(','), 'common/docs/a.txt,keys/4/b.txt', 'list walks every folder, with / separators');
            assertEquals((await dir.list('keys')).map((e) => e.path).join(','), 'keys/4/b.txt', 'list takes a folder prefix');
            assertEquals((await dir.list('missing')).length, 0, 'a missing prefix lists nothing');
        }),
    },
    {
        name: '[NFILES02] writes are atomic: staged in .hhs/tmp, and a failed write keeps the old file',
        invoke: () => withDir(async (dir) => {
            await dir.write('common/a.txt', [text('old')]);
            assertEquals(readdirSync(join(dir.root, ...TMP_DIR.split('/'))).length, 0, 'no staging file is left');
            async function* failing(): AsyncIterable<Uint8Array> {
                yield text('partial');
                throw new Error('source failed');
            }
            await expectThrows(() => dir.write('common/a.txt', failing()), 'a failing source fails the write');
            assertEquals(await readText(dir, 'common/a.txt'), 'old', 'the old bytes are intact');
            assertEquals(readdirSync(join(dir.root, ...TMP_DIR.split('/'))).length, 0, 'and the staging file is gone');
        }),
    },
    {
        name: '[NFILES03] remove and rename prune empty folders, except preserved ones; bad paths are refused',
        invoke: () => withDir(async (dir) => {
            await dir.write('common/deep/er/a.txt', [text('a')]);
            await dir.rename('common/deep/er/a.txt', 'common/top/a.txt');
            assertTrue(!existsSync(join(dir.root, 'common', 'deep')), 'rename prunes the folders it empties');
            await dir.remove('common/top/a.txt');
            assertTrue(!existsSync(join(dir.root, 'common')), 'remove prunes up to the root');
            assertTrue(existsSync(dir.root), 'but never the root');

            dir.preserve(['keys/4']);
            await dir.write('keys/4/sub/a.txt', [text('a')]);
            await dir.write('keys/7/b.txt', [text('b')]);
            await dir.remove('keys/7/b.txt');
            assertTrue(!existsSync(join(dir.root, 'keys', '7')), 'another key folder is pruned');
            await dir.rename('keys/4/sub/a.txt', 'common/a.txt');
            assertTrue(!existsSync(join(dir.root, 'keys', '4', 'sub')), 'a folder inside a preserved one is pruned');
            assertTrue(existsSync(join(dir.root, 'keys', '4')), 'the preserved folder stays empty');
            assertTrue(existsSync(join(dir.root, 'keys')), 'and so does its parent');
            for (const bad of ['', '/abs', 'a/../b', 'a//b', './a', 'a\\b']) {
                await expectThrows(() => dir.write(bad, [text('x')]), `'${bad}' is refused`);
            }
        }),
    },
    {
        name: '[NFILES04] watch reports changes below the root',
        invoke: () => withDir(async (dir) => {
            const seen: (string | undefined)[] = [];
            const unwatch = dir.watch((path) => seen.push(path));
            try {
                await new Promise((r) => setTimeout(r, 100));
                await dir.write('common/watched.txt', [text('w')]);
                await poll(() => seen.includes('common/watched.txt'), 'the written file is reported');
                assertTrue(seen.every((p) => p === undefined || p === 'common' || p.startsWith('common/') || p === '.hhs' || p.startsWith('.hhs/')),
                    `the reported paths are relative to the root: ${JSON.stringify(seen)}`);
            } finally {
                unwatch();
            }
        }),
    },
    {
        name: '[NFILES05] NodeDirectory and MemoryDirectory agree on the same operations',
        invoke: () => withDir(async (dir) => {
            assertEquals(await script(dir), await script(new MemoryDirectory()), 'same files, sizes and contents');
        }),
    },
    {
        name: '[NFILES06] ensureDir makes a folder and its parents, again without error; a file in the way fails',
        invoke: () => withDir(async (dir) => {
            await dir.ensureDir('keys/4');
            await dir.ensureDir('keys/4');
            assertTrue(existsSync(join(dir.root, 'keys', '4')), 'the folder and its parent exist');
            assertEquals((await dir.list()).length, 0, 'and list no files');
            await dir.write('common', [text('x')]);
            await expectThrows(() => dir.ensureDir('common'), 'a file at the path fails');
            await expectThrows(() => dir.ensureDir('common/sub'), 'a file at a parent fails');
            await expectThrows(() => dir.ensureDir('a/../b'), 'a bad path is refused');
        }),
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for Hyper Hyper Space v3 rdb_files_node module\n');
    console.log('[RDB_FILES_NODE] NodeDirectory');
    for (const test of tests) {
        if (filters.every((f) => test.name.includes(f))) testing.exitIfFailed(await testing.run(test.name, test.invoke));
        else await testing.skip(test.name);
    }
    await rm(ROOT, { recursive: true, force: true });
}

main();
