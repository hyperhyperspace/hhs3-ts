// A file mount: one FILES member (its RBlobStore and RFileMap) kept in sync
// with a folder, `common/` for the shared section and `keys/<rdb_keys id>/`
// for each owner's. The first pass makes `common/`, `keys/` and the writer's
// `keys/<id>/`, and preserves that last one so it stays when it empties.
//
// Each pass:
//   1. applies the store delta (upload chains, lane bytes) and the map delta
//      (live elements) to the mount state;
//   2. computes every live element's disk name (names.ts);
//   3. materializes complete files with readFile; an incomplete one is
//      pending and absent on disk;
//   4. deletes the files of removed elements, only when unchanged on disk.
//      Local changes in the own sections (`common/` and `keys/<myId>/`) are
//      never reverted: they wait there until the key can write. In other
//      keys' folders, deleted or edited files are restored (an edit is moved
//      aside to `~local` first);
//   5. ingests, when there is a writer and canWrite holds at the group's
//      frontier: new or changed files in `common/` and `keys/<myId>/` (by size
//      and mtime, then hash) are uploaded on the least loaded lane and added,
//      removing the element they replace; a deleted file is removed;
//   6. counts the changes waiting, and records every other file as
//      local-only: never synced, only reported.
// A pass that appended ops runs again (bounded), so it settles in one call.
//
// Triggers: DAG growth on both objects, the folder's watch hint, a periodic
// scan, nudge(), and growth of the bound group when it changes whether the
// writer can write. Passes never overlap; a trigger during one schedules one
// more.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64, version, type RObject, type Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    LANES, elementIdOf, filePathReason, hashFileSource,
    type FileElement, type FileSource, type RBlobStore, type RFileMap,
} from "@hyper-hyper-space/hhs3_rdb";
import { DEFAULT_KEY_DOMAIN, type KeyIndex } from "@hyper-hyper-space/hhs3_rdb_adapter";

import type { DirEntry, FileDirectory } from "./directory.js";
import { COMMON_DIR, KEYS_DIR, assignDiskPaths, caseFold, localName, sectionOf } from "./names.js";
import { RESERVED_DIR, emptyState, loadState, saveState, serializeState, type MountState } from "./state.js";

export type FilesMountSpec = {
    name: string;
    path: string;
};

export type LocalOnlyReason =
    | 'outside'       // not under common/ or keys/<id>/
    | 'foreign'       // in another key's folder
    | 'rejected'      // the name breaks the path rules
    | 'failed';       // ingesting it failed (see lastError)

export type LocalOnlyFile = { path: string; reason: LocalOnlyReason };

export type FilesMountStatus = {
    name: string;
    path: string;
    state: 'mounted' | 'pending';
    files?: number;       // live elements
    missing?: number;     // live elements not on disk (incomplete, or not yet written)
    localOnly?: number;
    waiting?: number;     // local changes in the own sections not uploaded yet
    writable?: boolean;
    lastError?: string;
};

export type FileMountOptions = {
    spec: FilesMountSpec;
    dir: FileDirectory;
    store: RBlobStore;
    map: RFileMap;
    keys: KeyIndex;
    writer?: OwnIdentity;
    // The bound group: with a writer, its growth re-checks access.
    group?: RObject;
    debounceMs?: number;
    // Periodic scan (ms); 0 disables. Default 5000.
    scanIntervalMs?: number;
    onError?: (err: unknown) => void;
};

const MAX_ROUNDS = 4;

function versionKey(v: Iterable<B64Hash>): string {
    return [...v].sort().join(',');
}

function unchanged(entry: DirEntry | undefined, record: { size: number; mtimeMs: number }): boolean {
    return entry !== undefined && entry.size === record.size && entry.mtimeMs === record.mtimeMs;
}

function messageOf(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
}

export class FileMount {
    private stopped = false;
    private running = false;
    private rerun = false;
    private timer: ReturnType<typeof setTimeout> | undefined;
    private accessTimer: ReturnType<typeof setTimeout> | undefined;
    private interval: ReturnType<typeof setInterval> | undefined;
    private unwatch: (() => void) | undefined;
    private readonly onObject = (): void => this.schedule();
    private readonly onGroup = (): void => this.scheduleAccessCheck();
    private current: Promise<void> = Promise.resolve();
    // canWriteNow as the latest pass saw it.
    private passWritable: boolean | undefined;

    private savedText: string | undefined;
    private lastStatus: Omit<FilesMountStatus, 'name' | 'path' | 'state'> = {};
    private localOnly: LocalOnlyFile[] = [];
    private lastErrorMessage: string | undefined;
    private failed = new Map<string, string>();
    private scaffolded = false;

    private constructor(private readonly options: FileMountOptions) {}

    // Runs the first pass, then arms the triggers.
    static async open(options: FileMountOptions): Promise<FileMount> {
        const mount = new FileMount(options);
        await mount.pass();
        await mount.arm();
        return mount;
    }

    get spec(): FilesMountSpec {
        return this.options.spec;
    }

    status(): FilesMountStatus {
        const out: FilesMountStatus = { name: this.options.spec.name, path: this.options.spec.path, state: 'mounted', ...this.lastStatus };
        if (this.lastErrorMessage !== undefined) out.lastError = this.lastErrorMessage;
        return out;
    }

    localOnlyFiles(): LocalOnlyFile[] {
        return [...this.localOnly];
    }

    // An explicit pass (after any running one).
    async pass(): Promise<void> {
        if (this.stopped) throw new Error('file mount is stopped');
        const run = this.current.then(() => this.runPass());
        this.current = run.catch(() => undefined);
        return run;
    }

    nudge(): void {
        this.schedule();
    }

    async stop(): Promise<void> {
        if (this.stopped) return;
        this.stopped = true;
        if (this.timer !== undefined) clearTimeout(this.timer);
        if (this.accessTimer !== undefined) clearTimeout(this.accessTimer);
        if (this.interval !== undefined) clearInterval(this.interval);
        this.unwatch?.();
        this.options.store.unsubscribe(this.onObject);
        this.options.map.unsubscribe(this.onObject);
        this.options.group?.unsubscribe(this.onGroup);
        await this.current;
    }

    private async arm(): Promise<void> {
        await this.options.store.subscribe(this.onObject);
        await this.options.map.subscribe(this.onObject);
        if (this.options.writer !== undefined) await this.options.group?.subscribe(this.onGroup);
        this.unwatch = this.options.dir.watch?.((path) => {
            if (path === undefined || !(path === RESERVED_DIR || path.startsWith(`${RESERVED_DIR}/`))) this.schedule();
        });
        const every = this.options.scanIntervalMs ?? 5000;
        if (every > 0) {
            this.interval = setInterval(() => this.schedule(), every);
            (this.interval as unknown as { unref?: () => void }).unref?.();
        }
    }

    private schedule(): void {
        if (this.stopped || this.timer !== undefined) return;
        this.timer = setTimeout(() => { this.timer = undefined; void this.runScheduled(); }, this.options.debounceMs ?? 50);
        (this.timer as unknown as { unref?: () => void }).unref?.();
    }

    // Bound group growth: a pass only when it flips whether the writer can write.
    private scheduleAccessCheck(): void {
        if (this.stopped || this.accessTimer !== undefined) return;
        this.accessTimer = setTimeout(() => { this.accessTimer = undefined; void this.checkAccess(); }, this.options.debounceMs ?? 50);
        (this.accessTimer as unknown as { unref?: () => void }).unref?.();
    }

    private async checkAccess(): Promise<void> {
        const { map, writer } = this.options;
        if (this.stopped || writer === undefined) return;
        try {
            if (await map.canWriteNow(writer.keyId) !== this.passWritable) this.schedule();
        } catch (err) {
            this.options.onError?.(err);
        }
    }

    private async runScheduled(): Promise<void> {
        if (this.stopped) return;
        if (this.running) { this.rerun = true; return; }
        this.running = true;
        try {
            do {
                this.rerun = false;
                try {
                    await this.pass();
                } catch (err) {
                    this.options.onError?.(err);
                }
            } while (this.rerun && !this.stopped);
        } finally {
            this.running = false;
        }
    }

    private async runPass(): Promise<void> {
        try {
            await this.passOnce();
            this.lastErrorMessage = this.failed.size > 0 ? [...this.failed.values()][0] : undefined;
        } catch (err) {
            this.lastErrorMessage = messageOf(err);
            throw err;
        }
    }

    private async passOnce(): Promise<void> {
        const { dir, store, map, writer } = this.options;
        const loaded = await loadState(dir, store.getId(), map.getId());
        const state = loaded ?? emptyState(store.getId(), map.getId());
        const myId = writer === undefined ? undefined : await this.keys().registerKey(DEFAULT_KEY_DOMAIN, writer.keyId, serializePublicKeyToBase64(writer.publicKey));
        const writable = writer !== undefined && await map.canWriteNow(writer.keyId);
        this.passWritable = writable;
        this.failed.clear();
        if (!this.scaffolded) await this.scaffold(myId);

        for (let round = 0; round < MAX_ROUNDS; round++) {
            await this.applyDeltas(state);
            const names = await this.diskNames(state);
            await this.materialize(state, names, myId);
            if (!writable || myId === undefined) break;
            if (!await this.ingest(state, myId)) break;
        }

        const names = await this.diskNames(state);
        await this.summarize(state, names, writable, myId);
        const text = serializeState(state);
        if (text !== this.savedText || loaded === undefined) {
            await saveState(dir, text);
            this.savedText = text;
        }
    }

    private keys(): KeyIndex {
        return this.options.keys;
    }

    // --- 0. folders ---

    // The drop folders, made on the first pass whether or not the key can
    // write yet: common/, keys/, and the writer's keys/<myId>/, which stays
    // even when it empties. A folder that can't be made is retried next pass.
    private async scaffold(myId: number | undefined): Promise<void> {
        const { dir } = this.options;
        const folders = [COMMON_DIR, KEYS_DIR];
        if (myId !== undefined) {
            const mine = `${KEYS_DIR}/${myId}`;
            dir.preserve([mine]);
            folders.push(mine);
        }
        let made = true;
        for (const folder of folders) {
            try {
                await dir.ensureDir(folder);
            } catch (err) {
                this.failed.set(folder, `${folder}: ${messageOf(err)}`);
                made = false;
            }
        }
        this.scaffolded = made;
    }

    // --- 1. deltas ---

    private async applyDeltas(state: MountState): Promise<void> {
        const { store, map } = this.options;

        const storeFrontier = await (await store.getScopedDag()).getFrontier();
        if (versionKey(storeFrontier) !== versionKey(state.storeAt)) {
            const start = state.storeAt.length === 0 ? version(store.getId()) : version(...state.storeAt);
            const delta = await store.computeDelta(start, storeFrontier);
            for (const h of delta.changes.headers) {
                if (state.chains[h.header] !== undefined) continue;
                state.chains[h.header] = { fileHash: h.fileHash, size: h.size, lane: h.lane, author: h.author, received: 0 };
                if (h.size === 0) state.chains[h.header].tail = h.header;
            }
            for (const c of delta.changes.chunks) {
                const chain = state.chains[c.header];
                if (chain === undefined) continue;
                if (c.index + 1 > chain.received) {
                    chain.received = c.index + 1;
                    chain.last = c.hash;
                }
                if (c.complete) chain.tail = c.hash;
                state.lanes[chain.lane] = (state.lanes[chain.lane] ?? 0) + c.length;
            }
            state.storeAt = [...storeFrontier].sort();
        }

        const mapFrontier = await (await map.getScopedDag()).getFrontier();
        if (versionKey(mapFrontier) !== versionKey(state.mapAt)) {
            const start = state.mapAt.length === 0 ? version(map.getId()) : version(...state.mapAt);
            const delta = await map.computeDelta(start, mapFrontier);
            for (const f of delta.changes.removed) delete state.elements[f.id];
            for (const f of delta.changes.added) {
                const element: FileElement = { section: f.section, path: f.path, fileHash: f.fileHash };
                if (f.owner !== undefined) element.owner = f.owner;
                state.elements[f.id] = element;
            }
            state.mapAt = [...mapFrontier].sort();
        }
    }

    // --- 2. names ---

    private async ownerId(owner: KeyId): Promise<number | undefined> {
        const known = await this.keys().idForKeyHash(DEFAULT_KEY_DOMAIN, owner);
        if (known !== undefined) return known;
        const key = await this.options.map.authorKey(owner) ?? await this.options.map.authorKeyNow(owner);
        if (key === undefined) return undefined;
        return this.keys().registerKey(DEFAULT_KEY_DOMAIN, owner, serializePublicKeyToBase64(key));
    }

    private async diskNames(state: MountState): Promise<Map<B64Hash, string>> {
        const items: { id: B64Hash; base: string }[] = [];
        for (const [id, element] of Object.entries(state.elements)) {
            if (element.section === 'common') {
                items.push({ id, base: `${COMMON_DIR}/${element.path}` });
                continue;
            }
            const owner = element.owner === undefined ? undefined : await this.ownerId(element.owner);
            if (owner !== undefined) items.push({ id, base: `${KEYS_DIR}/${owner}/${element.path}` });
        }
        return assignDiskPaths(items);
    }

    // --- 3, 4. materialize, delete, restore ---

    private tailOf(state: MountState, fileHash: B64Hash): B64Hash | undefined {
        let best: B64Hash | undefined;
        for (const chain of Object.values(state.chains)) {
            if (chain.fileHash === fileHash && chain.tail !== undefined && (best === undefined || chain.tail < best)) best = chain.tail;
        }
        return best;
    }

    // common/ and the writer's keys/<myId>/: local changes there are the
    // writer's to upload, so they wait for access instead of being reverted.
    private ownSection(path: string, myId: number | undefined): boolean {
        const section = sectionOf(path);
        return section.kind === 'common' || (section.kind === 'key' && section.keyId === myId);
    }

    private async scan(): Promise<Map<string, DirEntry>> {
        const entries = await this.options.dir.list('');
        return new Map(entries.filter((e) => !e.path.startsWith(`${RESERVED_DIR}/`)).map((e) => [e.path, e]));
    }

    private source(entry: DirEntry): FileSource {
        return { size: entry.size, read: () => this.options.dir.read(entry.path) };
    }

    private async writeElement(state: MountState, path: string, id: B64Hash, tail: B64Hash): Promise<void> {
        const element = state.elements[id];
        try {
            const written = await this.options.dir.write(path, this.options.store.readFile(tail));
            state.disk[path] = { fileHash: element.fileHash, size: written.size, mtimeMs: written.mtimeMs, element: id };
        } catch (err) {
            this.failed.set(path, `${path}: ${messageOf(err)}`);
        }
    }

    // Moves a local file out of a synced name's way, to `~local`.
    private async moveAside(disk: Map<string, DirEntry>, path: string): Promise<void> {
        const folded = new Set([...disk.keys()].map(caseFold));
        const target = localName(path, (f) => folded.has(f));
        await this.options.dir.rename(path, target);
        const entry = disk.get(path)!;
        disk.delete(path);
        disk.set(target, { ...entry, path: target });
    }

    private async materialize(state: MountState, names: Map<B64Hash, string>, myId: number | undefined): Promise<void> {
        const { dir } = this.options;
        const disk = await this.scan();
        const own = (path: string) => this.ownSection(path, myId);

        // records whose element is gone, or whose element moved to another name
        for (const [path, record] of Object.entries(state.disk)) {
            const entry = disk.get(path);
            const target = record.element !== undefined && state.elements[record.element] !== undefined ? names.get(record.element) : undefined;
            if (target === path) continue;
            if (target !== undefined) {
                if (entry === undefined) { delete state.disk[path]; continue; }
                if (unchanged(entry, record) && !disk.has(target)) {
                    await dir.rename(path, target);
                    delete state.disk[path];
                    state.disk[target] = { ...record, mtimeMs: (await dir.stat(target))?.mtimeMs ?? record.mtimeMs };
                    disk.delete(path);
                    disk.set(target, { ...entry, path: target, mtimeMs: state.disk[target].mtimeMs });
                } else if (!own(path)) {
                    delete state.disk[path];
                }
                continue;
            }
            delete state.disk[path];
            if (entry !== undefined && unchanged(entry, record)) {
                await dir.remove(path);
                disk.delete(path);
            }
        }

        const byFold = new Map([...disk.values()].map((e) => [caseFold(e.path), e]));
        for (const [id, path] of [...names.entries()].sort(([, a], [, b]) => (a < b ? -1 : a > b ? 1 : 0))) {
            const element = state.elements[id];
            const record = state.disk[path];
            const entry = disk.get(path) ?? byFold.get(caseFold(path));
            const tail = this.tailOf(state, element.fileHash);

            if (record !== undefined && record.element === id) {
                if (entry !== undefined && entry.path === path && unchanged(entry, record)) continue;
                if (own(path)) continue;
                if (entry !== undefined) await this.moveAside(disk, entry.path);
                delete state.disk[path];
                if (tail !== undefined) await this.writeElement(state, path, id, tail);
                continue;
            }

            if (entry !== undefined) {
                if (await hashFileSource(this.source(entry)) === element.fileHash) {
                    if (entry.path !== path) await dir.rename(entry.path, path);
                    const stat = (await dir.stat(path))!;
                    state.disk[path] = { fileHash: element.fileHash, size: stat.size, mtimeMs: stat.mtimeMs, element: id };
                    continue;
                }
                if (own(entry.path)) continue;
                await this.moveAside(disk, entry.path);
            }
            if (tail !== undefined) await this.writeElement(state, path, id, tail);
        }
    }

    // --- 5. ingest ---

    private pickLane(state: MountState, inFlight: number[]): number {
        let best = 0;
        for (let lane = 1; lane < LANES; lane++) {
            if ((state.lanes[lane] ?? 0) + inFlight[lane] < (state.lanes[best] ?? 0) + inFlight[best]) best = lane;
        }
        return best;
    }

    // Whether it appended ops.
    private async ingest(state: MountState, myId: number): Promise<boolean> {
        const { store, map, writer } = this.options;
        const disk = await this.scan();
        const inFlight = new Array(LANES).fill(0);
        const uploaded = new Set<B64Hash>();
        let appended = false;

        for (const entry of disk.values()) {
            const section = sectionOf(entry.path);
            if (section.kind === 'other' || (section.kind === 'key' && section.keyId !== myId)) continue;
            const record = state.disk[entry.path];
            if (record !== undefined && unchanged(entry, record)) continue;
            const old = record?.element !== undefined ? state.elements[record.element] : undefined;
            const path = old?.path ?? section.path;
            if (filePathReason(path) !== undefined) continue;

            try {
                const fileHash = await hashFileSource(this.source(entry));
                if (record !== undefined && fileHash === record.fileHash) {
                    state.disk[entry.path] = { ...record, size: entry.size, mtimeMs: entry.mtimeMs };
                    continue;
                }
                const element: FileElement = section.kind === 'common'
                    ? { section: 'common', path, fileHash }
                    : { section: 'key', owner: writer!.keyId, path, fileHash };
                const id = elementIdOf(element);
                if (state.elements[id] === undefined) {
                    if (this.tailOf(state, fileHash) === undefined && !uploaded.has(fileHash)) {
                        const lane = this.pickLane(state, inFlight);
                        const resume = this.resumable(state, fileHash, writer!.keyId);
                        inFlight[resume?.lane ?? lane] += entry.size;
                        await store.putFile(this.source(entry), writer!, resume === undefined ? { lane } : { lane: resume.lane, resume: resume.from });
                        uploaded.add(fileHash);
                    }
                    await map.add(element, writer!);
                    appended = true;
                }
                if (old !== undefined && record!.element !== id) {
                    await map.remove(old, writer!);
                    appended = true;
                }
                state.disk[entry.path] = { fileHash, size: entry.size, mtimeMs: entry.mtimeMs, element: id };
            } catch (err) {
                this.failed.set(entry.path, `${entry.path}: ${messageOf(err)}`);
            }
        }

        for (const [path, record] of Object.entries(state.disk)) {
            if (disk.has(path) || !this.ownSection(path, myId)) continue;
            const element = record.element === undefined ? undefined : state.elements[record.element];
            try {
                if (element !== undefined) {
                    await map.remove(element, writer!);
                    appended = true;
                }
                delete state.disk[path];
            } catch (err) {
                this.failed.set(path, `${path}: ${messageOf(err)}`);
            }
        }
        return appended;
    }

    // An incomplete chain of `fileHash` by `author`, to continue.
    private resumable(state: MountState, fileHash: B64Hash, author: KeyId): { lane: number; from: { header: B64Hash; tail?: B64Hash } } | undefined {
        for (const [header, chain] of Object.entries(state.chains).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
            if (chain.fileHash !== fileHash || chain.author !== author || chain.tail !== undefined) continue;
            return { lane: chain.lane, from: chain.last === undefined ? { header } : { header, tail: chain.last } };
        }
        return undefined;
    }

    // --- 6. status ---

    private async summarize(state: MountState, names: Map<B64Hash, string>, writable: boolean, myId: number | undefined): Promise<void> {
        const disk = await this.scan();
        const localOnly: LocalOnlyFile[] = [];
        // Edits, deletes and new files in the own sections, not uploaded yet.
        let waiting = 0;
        for (const entry of disk.values()) {
            const record = state.disk[entry.path];
            if (record !== undefined) {
                if (this.ownSection(entry.path, myId) && !unchanged(entry, record)) waiting++;
                continue;
            }
            const section = sectionOf(entry.path);
            let reason: LocalOnlyReason;
            if (this.failed.has(entry.path)) reason = 'failed';
            else if (section.kind === 'other') reason = 'outside';
            else if (section.kind === 'key' && section.keyId !== myId) reason = 'foreign';
            else if (filePathReason(section.path) !== undefined) reason = 'rejected';
            else if (writable) reason = 'failed';
            else { waiting++; continue; }
            localOnly.push({ path: entry.path, reason });
        }
        for (const path of Object.keys(state.disk)) {
            if (!disk.has(path) && this.ownSection(path, myId)) waiting++;
        }
        this.localOnly = localOnly.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));

        let missing = 0;
        for (const [id, path] of names) {
            const record = state.disk[path];
            if (record === undefined || record.element !== id) missing++;
        }
        missing += Object.keys(state.elements).length - names.size;
        this.lastStatus = {
            files: Object.keys(state.elements).length,
            missing,
            localOnly: localOnly.length,
            waiting,
            writable,
        };
    }
}
