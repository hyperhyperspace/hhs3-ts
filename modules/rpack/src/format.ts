// The release file: one catalog release with its whole past, as JSON.
//
//   { format: 1,
//     manifest: { name, catalog, version, release, parents, note? },
//     objects: [ { id, create, entries: [ { hash, prevs, payload } ] } ] }
//
// Objects come in dependency order (the schemas sorted by id, then the
// catalog); each carries its create payload and the entries of the release's
// past in canonical topological order. Payloads are verbatim, signatures
// included (they are payload fields). Entry metadata is not part of an entry's
// hash and these objects don't use it, so it isn't exported. Recorded hashes
// are never trusted: install recomputes them.
//
// Files are written pretty-printed with sorted keys, so the same release
// always serializes to the same bytes; parsing accepts any JSON layout.

import { B64Hash, base64 } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

export const RELEASE_FILE_FORMAT = 1;

export type ReleaseManifest = {
    name: string;
    catalog: B64Hash;
    version: string;
    release: B64Hash;
    parents: B64Hash[];
    note?: string;
};

export type ReleaseEntry = {
    hash: B64Hash;
    prevs: B64Hash[];
    payload: json.Literal;
};

export type ReleaseObject = {
    id: B64Hash;
    create: json.LiteralMap;
    entries: ReleaseEntry[];
};

export type ReleaseFile = {
    format: number;
    manifest: ReleaseManifest;
    objects: ReleaseObject[];
};

export class ReleaseFileError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ReleaseFileError';
    }
}

const manifestFormat: json.Format = {
    name: json.Type.String,
    catalog: json.Type.String,
    version: json.Type.String,
    release: json.Type.String,
    parents: [json.Type.Array, json.Type.String],
    note: [json.Type.Option, json.Type.String],
};

const entryFormat: json.Format = {
    hash: json.Type.String,
    prevs: [json.Type.Array, json.Type.String],
    payload: json.Type.Something,
};

const objectShapeFormat: json.Format = {
    id: json.Type.String,
    create: json.Type.Something,
    entries: [json.Type.Array, json.Type.Something],
};

function isMap(value: unknown): value is json.LiteralMap {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

// Checks a parsed value against the release file format, naming the first
// problem found.
export function checkReleaseFile(value: unknown): ReleaseFile {
    if (!isMap(value)) throw new ReleaseFileError('a release file must be a JSON object');
    if (!json.isLiteral(value)) throw new ReleaseFileError('a release file must not contain null values');

    const format = value['format'];
    if (format !== RELEASE_FILE_FORMAT) {
        throw new ReleaseFileError(`unsupported release file format ${JSON.stringify(format)} (expected ${RELEASE_FILE_FORMAT})`);
    }
    for (const key of Object.keys(value)) {
        if (key !== 'format' && key !== 'manifest' && key !== 'objects') throw new ReleaseFileError(`unexpected top-level key '${key}'`);
    }

    const manifest = value['manifest'];
    if (manifest === undefined || !json.checkFormat(manifestFormat, manifest)) {
        throw new ReleaseFileError('the manifest is malformed (expected name, catalog, version, release, parents and an optional note)');
    }

    const objects = value['objects'];
    if (!Array.isArray(objects)) throw new ReleaseFileError('objects must be an array');
    objects.forEach((object, i) => {
        if (!json.checkFormat(objectShapeFormat, object)) {
            throw new ReleaseFileError(`object ${i} is malformed (expected id, create and entries)`);
        }
        const typed = object as json.LiteralMap;
        if (!isMap(typed['create'])) throw new ReleaseFileError(`object ${i} (${String(typed['id'])}): create must be an object`);
        (typed['entries'] as json.Literal[]).forEach((entry, j) => {
            if (!json.checkFormat(entryFormat, entry)) {
                throw new ReleaseFileError(`object ${i} (${String(typed['id'])}) entry ${j} is malformed (expected hash, prevs and payload)`);
            }
        });
    });

    return value as unknown as ReleaseFile;
}

export function parseReleaseFile(text: string): ReleaseFile {
    let value: unknown;
    try {
        value = JSON.parse(text);
    } catch (e) {
        throw new ReleaseFileError(`not valid JSON: ${e instanceof Error ? e.message : String(e)}`);
    }
    return checkReleaseFile(value);
}

// Pretty-printed JSON with sorted object keys (2-space indent, trailing
// newline). Keys are ordered by our own sort, not by insertion, so the output
// is the same whatever order the object was built in.
export function serializeReleaseFile(file: ReleaseFile): string {
    return serializeSorted(file as unknown as json.Literal, '') + '\n';
}

function serializeSorted(value: json.Literal, indent: string): string {
    if (typeof value === 'string') return JSON.stringify(value);
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) throw new Error(`cannot serialize ${value}`);
        return Object.is(value, -0) ? '0' : JSON.stringify(value);
    }
    const inner = indent + '  ';
    if (Array.isArray(value)) {
        if (value.length === 0) return '[]';
        return '[\n' + value.map((item) => inner + serializeSorted(item, inner)).join(',\n') + '\n' + indent + ']';
    }
    const keys = Object.keys(value).sort();
    if (keys.length === 0) return '{}';
    return '{\n' + keys.map((key) => inner + JSON.stringify(key) + ': ' + serializeSorted(value[key], inner)).join(',\n') + '\n' + indent + '}';
}

// The file-name tag of a release: the first 8 hex digits of its hash (base64
// hashes carry '/' and '+', which aren't safe in file names).
export function releaseTag(hash: B64Hash): string {
    const raw = new Uint8Array(base64.toArrayBuffer(hash));
    return [...raw.slice(0, 4)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export function releaseFileName(name: string, version: string, hash: B64Hash): string {
    return `${name}-${version}-${releaseTag(hash)}.rpack`;
}

// A deterministic topological order: repeatedly emit the smallest-hash entry
// whose prevs are all emitted. Prevs outside `entries` (the create, or
// entries already present) count as emitted.
export function canonicalEntryOrder(entries: ReleaseEntry[]): ReleaseEntry[] {
    const byHash = new Map<B64Hash, ReleaseEntry>();
    for (const entry of entries) byHash.set(entry.hash, entry);

    const emitted = new Set<B64Hash>();
    const out: ReleaseEntry[] = [];
    const pending = new Set(byHash.keys());
    while (pending.size > 0) {
        let next: B64Hash | undefined;
        for (const hash of pending) {
            const ready = byHash.get(hash)!.prevs.every((p) => emitted.has(p) || !byHash.has(p));
            if (ready && (next === undefined || hash < next)) next = hash;
        }
        if (next === undefined) throw new Error('canonicalEntryOrder: the entries have a cycle');
        pending.delete(next);
        emitted.add(next);
        out.push(byHash.get(next)!);
    }
    return out;
}
