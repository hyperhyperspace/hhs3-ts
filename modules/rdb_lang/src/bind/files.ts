// Binding PUT, GET and LIST on a FILES member.
//
//   PUT   the author is BY or the session's author, never NOBODY (every op is
//         signed); the source is a local file (through the host), a string's
//         UTF-8 bytes, or canonical base64; AT defaults to the local file's
//         last path segment, and must pass the file map's path rules.
//   GET   IN KEY picks an owner's section ($name, #prefix, or the current
//         author); TO needs the host's local files.
//   LIST  an optional path prefix, and an optional section filter.

import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { decodeCanonicalBase64, filePathReason, type FileSource } from "@hyper-hyper-space/hhs3_rdb";

import { SpannedError } from "../diagnostics.js";
import type { AuthorExpr, GetFileStatement, KeyOwnerExpr, ListFilesStatement, NameRef, PutFileStatement } from "../syntax/ast.js";
import type { LangBindContext, LocalFileAccess, ResolvedFilesRef } from "./context.js";

// A key section's owner: a key id, or a key id prefix matched against the
// owners in the map.
export type OwnerRef = { keyId: KeyId } | { prefix: string };

export type BoundPutFile = {
    kind: 'put-file';
    ast: PutFileStatement;
    files: ResolvedFilesRef;
    author: OwnIdentity;
    section: 'common' | 'key';
    path: string;
    source: FileSource;
};

export type BoundGetFile = {
    kind: 'get-file';
    ast: GetFileStatement;
    files: ResolvedFilesRef;
    section: 'common' | 'key';
    owner?: OwnerRef;
    path: string;
    hash?: string;
    asB64: boolean;
    to?: { path: string; local: LocalFileAccess };
};

export type BoundListFiles = {
    kind: 'list-files';
    ast: ListFilesStatement;
    files: ResolvedFilesRef;
    section?: 'common' | 'key';
    owner?: OwnerRef;
    prefix?: string;
};

const NO_LOCAL_FILES = 'This host cannot access local files';

async function resolveFilesRef(ref: NameRef, context: LangBindContext): Promise<ResolvedFilesRef> {
    if (context.resolveFiles === undefined) throw new SpannedError('This host has no FILES support', ref.span, 'BIND_UNKNOWN_NAME');
    return context.resolveFiles(ref);
}

async function signingAuthor(expr: AuthorExpr | undefined, context: LangBindContext, span: PutFileStatement['span']): Promise<OwnIdentity> {
    if (expr?.kind === 'nobody') throw new SpannedError('PUT signs every op: BY NOBODY is not allowed', expr.span);
    const author = expr === undefined
        ? await context.currentAuthor()
        : await context.resolveAuthor(expr.kind === 'variable' ? { kind: 'variable', name: expr.name } : { kind: 'hash', prefix: expr.prefix });
    if (author === undefined) throw new SpannedError('PUT needs an author: add BY $name, or select one', span);
    return author;
}

async function ownerOf(expr: KeyOwnerExpr, context: LangBindContext): Promise<OwnerRef> {
    if (expr.kind === 'hash') return { prefix: expr.prefix };
    if (expr.kind === 'current') {
        const author = await context.currentAuthor();
        if (author === undefined) throw new SpannedError('IN KEY needs an owner: add $name or #prefix, or select an author', expr.span);
        return { keyId: author.keyId };
    }
    const value = await context.resolveVariable(expr.name);
    const keyId = typeof value === 'object' && value !== null && !Array.isArray(value) && 'keyId' in value ? value.keyId : undefined;
    if (typeof keyId !== 'string') throw new SpannedError(`$${expr.name} is not a key`, expr.span);
    return { keyId };
}

function pathReason(path: string, span: PutFileStatement['span'], what: string): void {
    const reason = filePathReason(path);
    if (reason !== undefined) throw new SpannedError(`${what}: path '${path}' ${reason}`, span);
}

function lastSegment(path: string): string {
    const parts = path.split(/[/\\]/).filter((p) => p.length > 0);
    return parts[parts.length - 1] ?? '';
}

function bytesSource(bytes: Uint8Array): FileSource {
    return { size: bytes.length, async *read() { yield bytes; } };
}

export async function bindPutFile(ast: PutFileStatement, context: LangBindContext): Promise<BoundPutFile> {
    const files = await resolveFilesRef(ast.files, context);
    const author = await signingAuthor(ast.author, context, ast.span);
    let source: FileSource;
    let path: string;
    switch (ast.source.kind) {
        case 'file': {
            if (context.localFiles === undefined) throw new SpannedError(NO_LOCAL_FILES, ast.span);
            source = await context.localFiles.open(ast.source.path);
            path = ast.at ?? lastSegment(ast.source.path);
            break;
        }
        case 'string':
            source = bytesSource(new TextEncoder().encode(ast.source.text));
            path = ast.at ?? '';
            break;
        case 'b64': {
            const bytes = decodeCanonicalBase64(ast.source.data);
            if (bytes === undefined) throw new SpannedError('PUT B64 takes canonical base64 (padded, standard alphabet)', ast.span);
            source = bytesSource(bytes);
            path = ast.at ?? '';
            break;
        }
    }
    pathReason(path, ast.span, 'PUT');
    return { kind: 'put-file', ast, files, author, section: ast.section, path, source };
}

export async function bindGetFile(ast: GetFileStatement, context: LangBindContext): Promise<BoundGetFile> {
    const files = await resolveFilesRef(ast.files, context);
    pathReason(ast.path, ast.span, 'GET');
    const bound: BoundGetFile = { kind: 'get-file', ast, files, section: ast.section, path: ast.path, asB64: ast.asB64 };
    if (ast.section === 'key') bound.owner = await ownerOf(ast.owner ?? { kind: 'current', span: ast.span }, context);
    if (ast.hash !== undefined) bound.hash = ast.hash;
    if (ast.to !== undefined) {
        if (context.localFiles === undefined) throw new SpannedError(NO_LOCAL_FILES, ast.span);
        bound.to = { path: ast.to, local: context.localFiles };
    }
    return bound;
}

export async function bindListFiles(ast: ListFilesStatement, context: LangBindContext): Promise<BoundListFiles> {
    const files = await resolveFilesRef(ast.files, context);
    const bound: BoundListFiles = { kind: 'list-files', ast, files };
    if (ast.section !== undefined) bound.section = ast.section;
    if (ast.owner !== undefined) bound.owner = await ownerOf(ast.owner, context);
    if (ast.prefix !== undefined) bound.prefix = ast.prefix;
    return bound;
}
