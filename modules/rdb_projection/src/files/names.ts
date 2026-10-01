// Disk names for a mount's files.
//
// An element lives at `common/<path>`, or at `keys/<id>/<path>` for its
// owner's rdb_keys id. When several live elements would share a name (more
// than one hash at a path, or paths that differ only in case), or a file name
// is also a folder another file needs, each of them gets `~<8 hex>` from its
// element id before its extension. A local file moved out of a synced name's
// way gets `~local` (then `~local2`, ...).

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { base64 } from "@hyper-hyper-space/hhs3_crypto";

export const COMMON_DIR = 'common';
export const KEYS_DIR = 'keys';

export function caseFold(path: string): string {
    return path.normalize('NFC').toLowerCase();
}

export function elementTag(id: B64Hash): string {
    const bytes = new Uint8Array(base64.toArrayBuffer(id)).subarray(0, 4);
    return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
}

// `path` with `~tag` before the extension of its last segment.
export function withSuffix(path: string, tag: string): string {
    const slash = path.lastIndexOf('/');
    const name = path.slice(slash + 1);
    const dot = name.lastIndexOf('.');
    const stem = dot > 0 ? name.slice(0, dot) : name;
    const ext = dot > 0 ? name.slice(dot) : '';
    return `${path.slice(0, slash + 1)}${stem}~${tag}${ext}`;
}

// The disk path of each element, from its unsuffixed `base`.
export function assignDiskPaths(items: { id: B64Hash; base: string }[]): Map<B64Hash, string> {
    const groups = new Map<string, { id: B64Hash; base: string }[]>();
    for (const item of items) {
        const key = caseFold(item.base);
        groups.set(key, [...(groups.get(key) ?? []), item]);
    }
    const out = new Map<B64Hash, string>();
    for (const group of groups.values()) {
        for (const item of group) out.set(item.id, group.length > 1 ? withSuffix(item.base, elementTag(item.id)) : item.base);
    }

    const folders = new Set<string>();
    for (const path of out.values()) {
        const parts = caseFold(path).split('/');
        for (let i = 1; i < parts.length; i++) folders.add(parts.slice(0, i).join('/'));
    }
    for (const [id, path] of out) {
        if (folders.has(caseFold(path))) out.set(id, withSuffix(path, elementTag(id)));
    }
    return out;
}

// The first `~local` name for `path` that `taken` doesn't hold (case-folded).
export function localName(path: string, taken: (folded: string) => boolean): string {
    for (let i = 1; ; i++) {
        const candidate = withSuffix(path, i === 1 ? 'local' : `local${i}`);
        if (!taken(caseFold(candidate))) return candidate;
    }
}

// The section a disk path is in, and the element path inside it.
export type SectionPath =
    | { kind: 'common'; path: string }
    | { kind: 'key'; keyId: number; path: string }
    | { kind: 'other' };

export function sectionOf(diskPath: string): SectionPath {
    if (diskPath.startsWith(`${COMMON_DIR}/`)) return { kind: 'common', path: diskPath.slice(COMMON_DIR.length + 1) };
    const match = /^keys\/([1-9][0-9]*)\/(.+)$/.exec(diskPath);
    if (match !== null) return { kind: 'key', keyId: Number(match[1]), path: match[2] };
    return { kind: 'other' };
}
