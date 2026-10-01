// Content hashes for RBlobStore files, and the base64 carriers they travel in.
//
// Normative (they are part of every file header and chunk op), so they are
// domain-separated and locked by golden vectors:
//
//   H_chunk(b)  = H("hhs3-blob-chunk-v1" || b)
//   link_j      = H("hhs3-blob-link-v1" || H_chunk(chunk_j) || link_{j+1})
//   END         = H("hhs3-blob-end-v1")                 terminates the chain
//   first       = link_0                                  END for an empty file
//   fileHash    = H("hhs3-blob-file-v1" || u64be(size) || first)
//
// H is SHA-256 in v1. A chunk op carries its own link (`next` is link_{j+1}),
// so a chunk can be checked on arrival against its predecessor alone, and the
// file hash commits to every byte through `first`.

import { base64, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

export const CONTENT_HASH_BYTES = 32;

const CHUNK_DOMAIN = stringToUint8Array('hhs3-blob-chunk-v1');
const LINK_DOMAIN = stringToUint8Array('hhs3-blob-link-v1');
const END_DOMAIN = stringToUint8Array('hhs3-blob-end-v1');
const FILE_DOMAIN = stringToUint8Array('hhs3-blob-file-v1');

function concat(...parts: Uint8Array[]): Uint8Array {
    let length = 0;
    for (const p of parts) length += p.length;
    const out = new Uint8Array(length);
    let offset = 0;
    for (const p of parts) {
        out.set(p, offset);
        offset += p.length;
    }
    return out;
}

export function chunkHash(bytes: Uint8Array): Uint8Array {
    return sha256.hash(concat(CHUNK_DOMAIN, bytes));
}

export function linkHash(chunk: Uint8Array, next: Uint8Array): Uint8Array {
    return sha256.hash(concat(LINK_DOMAIN, chunk, next));
}

export const END_LINK: B64Hash = encodeBase64(sha256.hash(END_DOMAIN));

export function fileHashOf(size: number, first: B64Hash): B64Hash {
    const firstBytes = decodeCanonicalBase64(first, CONTENT_HASH_BYTES);
    if (firstBytes === undefined) throw new Error("fileHashOf: 'first' is not a canonical 32-byte base64 hash");
    const sizeBytes = new Uint8Array(8);
    new DataView(sizeBytes.buffer).setBigUint64(0, BigInt(size), false);
    return encodeBase64(sha256.hash(concat(FILE_DOMAIN, sizeBytes, firstBytes)));
}

// The link of a chunk given the link after it (END for the last chunk).
export function chunkLink(bytes: Uint8Array, next: B64Hash): B64Hash {
    const nextBytes = decodeCanonicalBase64(next, CONTENT_HASH_BYTES);
    if (nextBytes === undefined) throw new Error("chunkLink: 'next' is not a canonical 32-byte base64 hash");
    return encodeBase64(linkHash(chunkHash(bytes), nextBytes));
}

// --------------------------------------------------------------------------
// base64 (RFC 4648 standard alphabet, padded, canonical)
// --------------------------------------------------------------------------

const B64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
const B64_BODY = /^[A-Za-z0-9+/]*$/;

export function base64Length(bytes: number): number {
    return Math.ceil(bytes / 3) * 4;
}

// The byte length a padded base64 string decodes to (no validation).
export function base64DecodedLength(s: string): number {
    const pad = s.endsWith('==') ? 2 : s.endsWith('=') ? 1 : 0;
    return (s.length / 4) * 3 - pad;
}

export function encodeBase64(bytes: Uint8Array): string {
    const step = 0x8000;
    let binary = '';
    for (let i = 0; i < bytes.length; i += step) {
        binary += String.fromCharCode(...bytes.subarray(i, i + step));
    }
    return btoa(binary);
}

// The bytes of `s` if it is the canonical base64 encoding of exactly
// `expectedBytes` bytes (when given), else undefined. Equivalent to
// isCanonicalBase64 + base64ByteLen from rschema/canonical.ts, in one pass
// that stays cheap for chunk-sized strings.
export function decodeCanonicalBase64(s: string, expectedBytes?: number): Uint8Array | undefined {
    if (typeof s !== 'string' || s.length % 4 !== 0) return undefined;
    if (expectedBytes !== undefined && s.length !== base64Length(expectedBytes)) return undefined;

    let pad = 0;
    if (s.endsWith('==')) pad = 2;
    else if (s.endsWith('=')) pad = 1;
    const body = s.slice(0, s.length - pad);
    if (!B64_BODY.test(body)) return undefined;

    // canonical: the bits a padded final group does not use must be zero
    if (pad > 0) {
        const last = B64_ALPHABET.indexOf(body[body.length - 1]);
        const unused = pad === 2 ? 0x0f : 0x03;
        if ((last & unused) !== 0) return undefined;
    }

    const bytes = new Uint8Array(base64.toArrayBuffer(s));
    if (expectedBytes !== undefined && bytes.length !== expectedBytes) return undefined;
    return bytes;
}

export function isContentHash(s: unknown): s is B64Hash {
    return typeof s === 'string' && decodeCanonicalBase64(s, CONTENT_HASH_BYTES) !== undefined;
}
