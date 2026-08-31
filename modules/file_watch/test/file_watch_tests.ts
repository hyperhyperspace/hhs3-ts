import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { watchFile } from "../src/file_watch.js";

function tmpFilePath(label: string): string {
    return path.join(os.tmpdir(), `hhs3-fw-${label}-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`);
}

function cleanup(p: string): void {
    try { fs.unlinkSync(p); } catch (_e) { /* ignore */ }
}

const settle = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function testDetectsAppend(): Promise<void> {
    const file = tmpFilePath("append");
    fs.writeFileSync(file, "seed\n");

    let calls = 0;
    const handle = watchFile(file, () => { calls++; });
    try {
        await settle(100);   // let fs.watch attach
        fs.appendFileSync(file, "more\n");
        await settle(500);   // let the event deliver
        assertTrue(calls >= 1, `watcher should have detected the append (got ${calls})`);
    } finally {
        handle.close();
        cleanup(file);
    }
}

async function testRearmsOnRecreate(): Promise<void> {
    const file = tmpFilePath("recreate");
    fs.writeFileSync(file, "seed\n");

    let calls = 0;
    const handle = watchFile(file, () => { calls++; });
    try {
        await settle(150);

        // Delete and recreate: the direct file watcher must be rearmed (via the
        // directory watcher) onto the new inode so subsequent writes are still
        // detected.
        fs.unlinkSync(file);
        await settle(150);
        fs.writeFileSync(file, "reborn\n");
        await settle(300);   // allow the dir watcher to rearm onto the new inode

        const afterRecreate = calls;
        // Append repeatedly until a post-recreate write is observed. This
        // tolerates fs.watch / FSEvents delivery latency (a single append can
        // race ahead of the rearm) while still proving ongoing writes wake us.
        const deadline = Date.now() + 3000;
        while (calls <= afterRecreate && Date.now() < deadline) {
            fs.appendFileSync(file, "again\n");
            await settle(150);
        }

        assertTrue(calls > afterRecreate,
            `watcher should have detected a write after the file was recreated (before ${afterRecreate}, after ${calls})`);
    } finally {
        handle.close();
        cleanup(file);
    }
}

async function testCloseIsIdempotent(): Promise<void> {
    const file = tmpFilePath("close");
    fs.writeFileSync(file, "seed\n");

    const handle = watchFile(file, () => { /* ignore */ });
    handle.close();
    handle.close();   // must not throw
    cleanup(file);
}

async function testMissingFileArmsLater(): Promise<void> {
    const file = tmpFilePath("missing");
    // Do NOT create the file up front: only the directory watcher can arm.

    let calls = 0;
    const handle = watchFile(file, () => { calls++; });
    try {
        await settle(100);
        fs.writeFileSync(file, "created\n");
        await settle(500);
        assertTrue(calls >= 1, `watcher should have detected the file first appearing (got ${calls})`);
    } finally {
        handle.close();
        cleanup(file);
    }
}

export const fileWatchSuite = {
    title: "\n[FILE_WATCH] watchFile Tests\n",
    tests: [
        { name: "[FILE_WATCH_00] detects an in-place append", invoke: testDetectsAppend },
        { name: "[FILE_WATCH_01] rearms after delete/recreate", invoke: testRearmsOnRecreate },
        { name: "[FILE_WATCH_02] close is idempotent", invoke: testCloseIsIdempotent },
        { name: "[FILE_WATCH_03] arms when file appears later", invoke: testMissingFileArmsLater },
    ],
};
