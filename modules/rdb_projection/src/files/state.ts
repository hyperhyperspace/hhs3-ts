// A mount's state, kept in `.hhs/state.json` at the mount root and written
// atomically at the end of each pass.
//
//   storeAt, mapAt  the versions the deltas were applied up to
//   chains          upload chains by header: how many chunks arrived, the last
//                   one, and the tail once complete
//   lanes           bytes recorded per lane, for picking upload lanes
//   elements        the live file map elements at mapAt
//   disk            what the mount wrote or ingested, by disk path: the file
//                   hash, size and mtime, and the element it is
//
// It grows with the number of files, not chunks. A missing or unreadable
// state is rebuilt from full deltas, adopting disk files that match by hash.

import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { FileElement } from "@hyper-hyper-space/hhs3_rdb";
import { LANES } from "@hyper-hyper-space/hhs3_rdb";

import { collectBytes, type FileDirectory } from "./directory.js";

export const RESERVED_DIR = '.hhs';
export const STATE_PATH = `${RESERVED_DIR}/state.json`;

export type ChainRecord = {
    fileHash: B64Hash;
    size: number;
    lane: number;
    author: KeyId;
    received: number;
    last?: B64Hash;
    tail?: B64Hash;
};

export type DiskRecord = {
    fileHash: B64Hash;
    size: number;
    mtimeMs: number;
    element?: B64Hash;
};

export type MountState = {
    v: 1;
    store: B64Hash;
    map: B64Hash;
    storeAt: B64Hash[];
    mapAt: B64Hash[];
    chains: { [header: string]: ChainRecord };
    lanes: number[];
    elements: { [id: string]: FileElement };
    disk: { [path: string]: DiskRecord };
};

export function emptyState(store: B64Hash, map: B64Hash): MountState {
    return { v: 1, store, map, storeAt: [], mapAt: [], chains: {}, lanes: new Array(LANES).fill(0), elements: {}, disk: {} };
}

// The saved state for this store and map, or undefined when there is none or
// it belongs to other objects.
export async function loadState(dir: FileDirectory, store: B64Hash, map: B64Hash): Promise<MountState | undefined> {
    if (await dir.stat(STATE_PATH) === undefined) return undefined;
    try {
        const parsed = JSON.parse(new TextDecoder().decode(await collectBytes(dir.read(STATE_PATH)))) as MountState;
        if (parsed.v !== 1 || parsed.store !== store || parsed.map !== map) return undefined;
        if (!Array.isArray(parsed.storeAt) || !Array.isArray(parsed.mapAt) || !Array.isArray(parsed.lanes)) return undefined;
        if (typeof parsed.chains !== 'object' || typeof parsed.elements !== 'object' || typeof parsed.disk !== 'object') return undefined;
        return parsed;
    } catch {
        return undefined;
    }
}

export function serializeState(state: MountState): string {
    return JSON.stringify(state);
}

export async function saveState(dir: FileDirectory, text: string): Promise<void> {
    await dir.write(STATE_PATH, [new TextEncoder().encode(text)]);
}
