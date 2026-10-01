// Executing PUT, GET and LIST on a FILES member.
//
//   PUT   checks canWrite before uploading anything; skips the upload when
//         the store has the file, resumes this author's incomplete chain of
//         it, and otherwise uploads on the lane the file hash picks; then
//         adds the element unless present, and removes every other element
//         at the same section, owner and path.
//   GET   one present element at the path (HASH picks among several), whose
//         bytes are complete here; written through the host, or returned as
//         UTF-8 text or base64, up to INLINE_LIMIT.
//   LIST  present elements, with their size and completeness here.

import { base64, type B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    LANES, elementIdOf, encodeBase64, hashFileSource,
    type FileElement, type ListedFile, type RBlobStore, type RFileMap,
} from "@hyper-hyper-space/hhs3_rdb";

import type { BoundGetFile, BoundListFiles, BoundPutFile, OwnerRef } from "../bind/files.js";
import type { FileListRow, GetFileLangResult, ListFilesLangResult, PutFileLangResult } from "./result.js";

export const INLINE_LIMIT = 1024 * 1024;

function laneFor(fileHash: B64Hash): number {
    return new Uint8Array(base64.toArrayBuffer(fileHash))[0]! % LANES;
}

function elementOf(file: FileElement): FileElement {
    const element: FileElement = { section: file.section, path: file.path, fileHash: file.fileHash };
    if (file.owner !== undefined) element.owner = file.owner;
    return element;
}

function ownerMatches(file: ListedFile, owner: OwnerRef | undefined): boolean {
    if (owner === undefined) return true;
    if (file.owner === undefined) return false;
    return 'keyId' in owner ? file.owner === owner.keyId : file.owner.startsWith(owner.prefix);
}

function sectionLabel(section: 'common' | 'key', owner: OwnerRef | undefined): string {
    if (section === 'common') return 'common';
    if (owner === undefined) return 'a key section';
    return 'keyId' in owner ? `key ${owner.keyId.slice(0, 12)}` : `key #${owner.prefix}`;
}

function byListOrder(a: FileListRow, b: FileListRow): number {
    const ka = [a.section, a.owner, a.path, a.fileHash];
    const kb = [b.section, b.owner, b.path, b.fileHash];
    for (let i = 0; i < ka.length; i++) if (ka[i] !== kb[i]) return ka[i]! < kb[i]! ? -1 : 1;
    return 0;
}

async function incomplete(store: RBlobStore, fileHash: B64Hash): Promise<string> {
    const chains = await store.findChains(fileHash);
    if (chains.length === 0) return 'none of its bytes have arrived';
    const best = chains.reduce((a, b) => (b.received > a.received ? b : a));
    return `${best.received} of ${best.chunks} chunks have arrived`;
}

async function readAll(chunks: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    let length = 0;
    for await (const part of chunks) {
        parts.push(part);
        length += part.length;
    }
    const out = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
        out.set(part, offset);
        offset += part.length;
    }
    return out;
}

export async function executePutFile(bound: BoundPutFile): Promise<PutFileLangResult> {
    const { store, map, name } = bound.files;
    const author = bound.author;
    if (!await map.canWriteNow(author.keyId)) {
        throw new Error(`PUT: key ${author.keyId.slice(0, 12)} can't write to FILES ${name} (its key or ALLOW WRITE IF doesn't admit it)`);
    }
    const fileHash = await hashFileSource(bound.source);
    const element: FileElement = bound.section === 'common'
        ? { section: 'common', path: bound.path, fileHash }
        : { section: 'key', owner: author.keyId, path: bound.path, fileHash };
    const entries: B64Hash[] = [];

    let uploaded = false;
    if (await store.findFile(fileHash) === undefined) {
        const mine = (await store.findChains(fileHash)).find((c) => c.author === author.keyId && !c.complete);
        const stored = mine === undefined
            ? await store.putFile(bound.source, author, { lane: laneFor(fileHash) })
            : await store.putFile(bound.source, author, {
                lane: mine.lane,
                resume: mine.last === mine.header ? { header: mine.header } : { header: mine.header, tail: mine.last },
            });
        entries.push(stored.tail);
        uploaded = true;
    }

    const id = elementIdOf(element);
    const here = (await map.list()).filter((f) => f.section === element.section && f.owner === element.owner && f.path === element.path);
    if (!here.some((f) => f.id === id)) entries.push(await map.add(element, author));
    for (const other of here) {
        if (other.id !== id) entries.push(await map.remove(elementOf(other), author));
    }

    const result: PutFileLangResult = {
        kind: 'put-file', files: name, section: bound.section, path: bound.path, fileHash, size: bound.source.size, uploaded, entries,
    };
    if (element.owner !== undefined) result.owner = element.owner;
    return result;
}

async function oneFileAt(map: RFileMap, bound: BoundGetFile): Promise<ListedFile> {
    let matches = (await map.list()).filter((f) => f.section === bound.section && f.path === bound.path && ownerMatches(f, bound.owner));
    const where = sectionLabel(bound.section, bound.owner);
    const owners = new Set(matches.map((f) => f.owner));
    if (owners.size > 1) {
        throw new Error(`GET: #${(bound.owner as { prefix: string }).prefix} matches several keys with '${bound.path}'; give more of the key id`);
    }
    if (bound.hash !== undefined) matches = matches.filter((f) => f.fileHash.startsWith(bound.hash!));
    if (matches.length === 0) {
        throw new Error(`GET: FILES ${bound.files.name} has no file '${bound.path}' in ${where}${bound.hash !== undefined ? ` with hash '${bound.hash}'` : ''}`);
    }
    if (matches.length > 1) {
        const hashes = matches.map((f) => f.fileHash.slice(0, 12)).sort().join(', ');
        throw new Error(`GET: FILES ${bound.files.name} has ${matches.length} files at '${bound.path}' in ${where} (${hashes}); add HASH 'prefix'`);
    }
    return matches[0]!;
}

export async function executeGetFile(bound: BoundGetFile): Promise<GetFileLangResult> {
    const { store, map, name } = bound.files;
    const file = await oneFileAt(map, bound);
    const found = await store.findFile(file.fileHash);
    if (found === undefined) throw new Error(`GET: '${bound.path}' is not complete on this replica: ${await incomplete(store, file.fileHash)}`);

    const result: GetFileLangResult = { kind: 'get-file', files: name, section: file.section, path: file.path, fileHash: file.fileHash, size: found.size };
    if (file.owner !== undefined) result.owner = file.owner;
    if (bound.to !== undefined) {
        await bound.to.local.write(bound.to.path, store.readFile(found.tail));
        result.written = bound.to.path;
        return result;
    }
    if (found.size > INLINE_LIMIT) {
        throw new Error(`GET: '${bound.path}' is ${found.size} bytes, and inline output is capped at ${INLINE_LIMIT}; use TO 'file'`);
    }
    const bytes = await readAll(store.readFile(found.tail));
    if (bound.asB64) {
        result.b64 = encodeBase64(bytes);
        return result;
    }
    try {
        result.text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    } catch {
        throw new Error(`GET: '${bound.path}' is not UTF-8 text; use AS B64 or TO 'file'`);
    }
    return result;
}

export async function executeListFiles(bound: BoundListFiles): Promise<ListFilesLangResult> {
    const { store, map, name } = bound.files;
    const prefix = bound.prefix?.replace(/\/+$/, '');
    const rows: FileListRow[] = [];
    for (const file of await map.list()) {
        if (bound.section !== undefined && file.section !== bound.section) continue;
        if (bound.section === 'key' && !ownerMatches(file, bound.owner)) continue;
        if (prefix !== undefined && prefix.length > 0 && file.path !== prefix && !file.path.startsWith(`${prefix}/`)) continue;
        const found = await store.findFile(file.fileHash);
        const size = found?.size ?? (await store.findChains(file.fileHash))[0]?.size ?? null;
        rows.push({ section: file.section, owner: file.owner ?? '', path: file.path, size, fileHash: file.fileHash, complete: found !== undefined });
    }
    return { kind: 'list-files', files: name, columns: ['section', 'owner', 'path', 'size', 'fileHash', 'complete'], rows: rows.sort(byListOrder) };
}
