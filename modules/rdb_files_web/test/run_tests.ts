import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { MemoryDirectory, collectBytes, type FileDirectory } from "@hyper-hyper-space/hhs3_rdb_projection";

import { HandleDirectory } from "../src/index.js";
import { FakeDirectoryHandle } from "./fake_handles.js";

const text = (s: string) => new TextEncoder().encode(s);

function handleDir(movable = true): { dir: HandleDirectory; root: FakeDirectoryHandle } {
    const root = new FakeDirectoryHandle('root', movable);
    return { dir: new HandleDirectory(root as unknown as FileSystemDirectoryHandle), root };
}

async function readText(dir: FileDirectory, path: string): Promise<string> {
    return new TextDecoder().decode(await collectBytes(dir.read(path)));
}

async function expectThrows(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
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
    const listed = (await dir.list('')).map((e) => `${e.path}:${e.size}`);
    return `${listed.join(',')}|${(await dir.list('common')).length}|${await readText(dir, 'common/b.md')}`;
}

const tests = [
    {
        name: '[WFILES01] HandleDirectory and MemoryDirectory agree, with and without FileSystemHandle.move',
        invoke: async () => {
            const expected = await script(new MemoryDirectory());
            assertEquals(await script(handleDir(true).dir), expected, 'with move()');
            assertEquals(await script(handleDir(false).dir), expected, 'with copy and remove');
        },
    },
    {
        name: '[WFILES02] stat, nested lists and prefixes; a write bumps the mtime',
        invoke: async () => {
            const { dir } = handleDir();
            const first = await dir.write('common/docs/a.txt', [text('hello')]);
            assertEquals((await dir.stat('common/docs/a.txt'))?.size, 5, 'stat has the size');
            assertTrue(await dir.stat('common/docs') === undefined, 'a folder is not a file');
            assertTrue(await dir.stat('common/none.txt') === undefined, 'a missing file has no stat');
            assertEquals((await dir.list('common')).map((e) => e.path).join(','), 'common/docs/a.txt', 'list takes a prefix');
            assertEquals((await dir.list('missing')).length, 0, 'a missing prefix lists nothing');
            const second = await dir.write('common/docs/a.txt', [text('hello!')]);
            assertTrue(second.mtimeMs > first.mtimeMs, 'the mtime moves on each write');
        },
    },
    {
        name: '[WFILES03] a failed write is aborted and keeps the old bytes; empty folders are pruned, except preserved ones',
        invoke: async () => {
            const { dir, root } = handleDir();
            await dir.write('common/a.txt', [text('old')]);
            async function* failing(): AsyncIterable<Uint8Array> {
                yield text('partial');
                throw new Error('source failed');
            }
            await expectThrows(() => dir.write('common/a.txt', failing()), 'a failing source fails the write');
            assertEquals(await readText(dir, 'common/a.txt'), 'old', 'the old bytes are intact');

            await dir.write('deep/er/b.txt', [text('b')]);
            await dir.remove('deep/er/b.txt');
            assertTrue(!root.children.has('deep'), 'remove prunes the folders it empties');
            for (const bad of ['', '/abs', 'a/../b', 'a//b']) await expectThrows(() => dir.write(bad, [text('x')]), `'${bad}' is refused`);

            dir.preserve(['keys/4']);
            await dir.write('keys/4/sub/a.txt', [text('a')]);
            await dir.write('keys/7/b.txt', [text('b')]);
            await dir.remove('keys/7/b.txt');
            const keys = root.children.get('keys') as FakeDirectoryHandle;
            assertTrue(!keys.children.has('7'), 'another key folder is pruned');
            await dir.remove('keys/4/sub/a.txt');
            const mine = keys.children.get('4') as FakeDirectoryHandle | undefined;
            assertTrue(mine !== undefined && mine.children.size === 0, 'the preserved folder stays, and a folder inside it is pruned');
            assertTrue(root.children.get('keys') === keys, 'and so does its parent');
        },
    },
    {
        name: '[WFILES04] watch uses FileSystemObserver where it exists, and is a no-op elsewhere',
        invoke: async () => {
            const { dir } = handleDir();
            const stop = dir.watch(() => undefined);
            stop();

            const seen: (string | undefined)[] = [];
            let observed: unknown;
            let disconnected = false;
            (globalThis as Record<string, unknown>)['FileSystemObserver'] = class {
                constructor(private readonly cb: (records: { relativePathComponents?: string[] }[]) => void) {}
                async observe(handle: unknown) { observed = handle; this.cb([{ relativePathComponents: ['common', 'x.txt'] }]); }
                disconnect() { disconnected = true; }
            };
            try {
                const unwatch = dir.watch((path) => seen.push(path));
                await new Promise((r) => setTimeout(r, 0));
                assertTrue(observed === dir.root, 'the root is observed');
                assertEquals(seen.join(','), 'common/x.txt', 'changes arrive as relative paths');
                unwatch();
                assertTrue(disconnected, 'unwatch disconnects');
            } finally {
                delete (globalThis as Record<string, unknown>)['FileSystemObserver'];
            }
        },
    },
    {
        name: '[WFILES05] ensureDir makes a folder and its parents, again without error; a file in the way fails',
        invoke: async () => {
            const { dir, root } = handleDir();
            await dir.ensureDir('keys/4');
            await dir.ensureDir('keys/4');
            const keys = root.children.get('keys') as FakeDirectoryHandle | undefined;
            assertTrue(keys?.children.get('4')?.kind === 'directory', 'the folder and its parent exist');
            assertEquals((await dir.list()).length, 0, 'and list no files');
            await dir.write('common', [text('x')]);
            await expectThrows(() => dir.ensureDir('common'), 'a file at the path fails');
            await expectThrows(() => dir.ensureDir('common/sub'), 'a file at a parent fails');
            await expectThrows(() => dir.ensureDir('a/../b'), 'a bad path is refused');
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for Hyper Hyper Space v3 rdb_files_web module\n');
    console.log('[RDB_FILES_WEB] HandleDirectory');
    for (const test of tests) {
        if (filters.every((f) => test.name.includes(f))) testing.exitIfFailed(await testing.run(test.name, test.invoke));
        else await testing.skip(test.name);
    }
}

main();
