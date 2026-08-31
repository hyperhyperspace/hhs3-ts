import { PRNG } from "../../../rdb/test/delta_parity/prng.js";
import type { CapturedBatch, CapturedChange, SyncMapping } from "../../src/types.js";

const POSTS = 8;
const COMMENTS = 12;
const LEDGER = 4;

export type Occupancy = {
    posts: Set<number>;
    comments: Set<number>;
    ledger: Set<number>;
};

function emptyOccupancy(): Occupancy {
    return { posts: new Set(), comments: new Set(), ledger: new Set() };
}

function pickOccupied(prng: PRNG, set: Set<number>): number | undefined {
    if (set.size === 0) return undefined;
    const arr = [...set];
    return arr[prng.nextInt(0, arr.length - 1)];
}

function pickEmpty(prng: PRNG, used: Set<number>, max: number): number | undefined {
    const empty: number[] = [];
    for (let i = 1; i <= max; i++) if (!used.has(i)) empty.push(i);
    if (empty.length === 0) return undefined;
    return empty[prng.nextInt(0, empty.length - 1)];
}

function occupy(live: Occupancy, used: Occupancy, table: keyof Occupancy, localId: number): void {
    live[table].add(localId);
    used[table].add(localId);
}

export type GeneratedBatch = {
    batch: CapturedBatch;
    occupancy: Occupancy;
    replayLookup: SyncMapping | undefined;
};

// Seeded CapturedBatch in target vocabulary. Local ids are never recycled after
// delete (the projection-local PK / sync mapping outlives the app row).
export function generateCapturedBatch(
    seed: number, n: number, opts: { crashReplay?: boolean } = {},
): GeneratedBatch {
    const prng = new PRNG(seed);
    const live = emptyOccupancy();
    const used = emptyOccupancy();
    const changes: CapturedChange[] = [];
    let nextId = 1;
    let replayLookup: SyncMapping | undefined;

    if (opts.crashReplay) {
        const localId = pickEmpty(prng, used.posts, POSTS) ?? 1;
        replayLookup = {
            table: 'posts', localId, rowId: 'REPLAY_ROWID', uuid: 'reserved-u', status: 'active',
        };
        changes.push({
            id: nextId++, kind: 'insert', table: 'posts', localId,
            values: { title: 'replay' },
        });
        occupy(live, used, 'posts', localId);
    }

    let guard = 0;
    while (changes.length < n && guard++ < n * 20) {
        const roll = prng.nextInt(0, 99);
        if (roll < 18) {
            const localId = pickEmpty(prng, used.posts, POSTS);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'insert', table: 'posts', localId, values: { title: `p${localId}` } });
            occupy(live, used, 'posts', localId);
        } else if (roll < 32) {
            const localId = pickEmpty(prng, used.comments, COMMENTS);
            if (localId === undefined) continue;
            const values: { [c: string]: string | number } = { body: `c${localId}` };
            const post = pickOccupied(prng, live.posts);
            if (post !== undefined) values.post_id = post;
            const parent = pickOccupied(prng, live.comments);
            if (parent !== undefined && prng.next() < 0.4) values.parent_id = parent;
            changes.push({ id: nextId++, kind: 'insert', table: 'comments', localId, values });
            occupy(live, used, 'comments', localId);
        } else if (roll < 40) {
            const localId = pickEmpty(prng, used.ledger, LEDGER);
            if (localId === undefined) continue;
            changes.push({
                id: nextId++, kind: 'insert', table: 'ledger', localId,
                values: { ref: `R-${localId}`, amount: `${prng.nextInt(1, 9)}.00` },
            });
            occupy(live, used, 'ledger', localId);
        } else if (roll < 50) {
            const localId = pickOccupied(prng, live.posts);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'update', table: 'posts', localId, values: { title: `u${prng.nextInt(0, 9)}` } });
        } else if (roll < 58) {
            const localId = pickOccupied(prng, live.posts);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'update', table: 'posts', localId, values: { title: `A${prng.nextInt(0, 9)}` } });
            if (changes.length < n) {
                changes.push({ id: nextId++, kind: 'update', table: 'posts', localId, values: { note: `B${prng.nextInt(0, 9)}` } });
            }
        } else if (roll < 64) {
            const localId = pickOccupied(prng, live.posts);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'update', table: 'posts', localId, values: { title: `X${prng.nextInt(0, 9)}` } });
            if (changes.length < n) {
                changes.push({ id: nextId++, kind: 'update', table: 'posts', localId, values: { title: `Y${prng.nextInt(0, 9)}` } });
            }
        } else if (roll < 70) {
            const localId = pickOccupied(prng, live.comments);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'update', table: 'comments', localId, values: { body: `u${prng.nextInt(0, 9)}` } });
        } else if (roll < 76) {
            const localId = pickOccupied(prng, live.posts);
            if (localId === undefined) continue;
            if (replayLookup !== undefined && localId === replayLookup.localId) continue;
            changes.push({ id: nextId++, kind: 'delete', table: 'posts', localId });
            live.posts.delete(localId);
        } else if (roll < 82) {
            const localId = pickOccupied(prng, live.comments);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'delete', table: 'comments', localId });
            live.comments.delete(localId);
        } else if (roll < 88) {
            const localId = pickEmpty(prng, used.comments, COMMENTS);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'insert', table: 'comments', localId, values: { body: 'tmp' } });
            occupy(live, used, 'comments', localId);
            if (changes.length < n) {
                changes.push({ id: nextId++, kind: 'delete', table: 'comments', localId });
                live.comments.delete(localId);
            }
        } else if (roll < 94) {
            const parent = pickEmpty(prng, used.comments, COMMENTS);
            if (parent === undefined) continue;
            used.comments.add(parent);
            const child = pickEmpty(prng, used.comments, COMMENTS);
            used.comments.delete(parent);
            if (child === undefined) continue;
            changes.push({ id: nextId++, kind: 'insert', table: 'comments', localId: parent, values: { body: 'root' } });
            occupy(live, used, 'comments', parent);
            if (changes.length < n) {
                changes.push({
                    id: nextId++, kind: 'insert', table: 'comments', localId: child,
                    values: { body: 'child', parent_id: parent },
                });
                occupy(live, used, 'comments', child);
            }
        } else if (roll < 97) {
            const localId = pickEmpty(prng, used.comments, COMMENTS);
            if (localId === undefined) continue;
            changes.push({
                id: nextId++, kind: 'insert', table: 'comments', localId,
                values: { body: 'orphan', post_id: 9999 },
            });
            used.comments.add(localId);
        } else {
            const localId = pickOccupied(prng, live.ledger);
            if (localId === undefined) continue;
            changes.push({ id: nextId++, kind: 'update', table: 'ledger', localId, values: { ref: 'nope' } });
        }
    }

    return { batch: { changes: changes.slice(0, n) }, occupancy: live, replayLookup };
}
