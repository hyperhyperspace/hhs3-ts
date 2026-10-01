import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { CHUNK_BYTES, hashFileSource, type FileSource } from "@hyper-hyper-space/hhs3_rdb";

import type { LocalFileAccess } from "../src/bind/context.js";
import type { GetFileLangResult, ListFilesLangResult, PutFileLangResult } from "../src/exec/result.js";
import { createLangEnv, LangEnv, newIdentity } from "./lang_env.js";

const SETUP = `
CREATE SCHEMA users_schema CREATORS ($dev) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER ALLOW insert IF true,
  TABLE caps (
    label string PUB READONLY,
    grantee string PUB READONLY
  ) ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);
CREATE CATALOG app CREATORS ($dev) VERSION '1.0.0' PARAMS (:admin identity) AS (
  TABLEGROUP users USING SCHEMA users_schema AT LATEST
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  FILES media
    USING IDENTITIES users.identities
    ALLOW WRITE IF EXISTS users.caps WHERE users.caps.label = 'writer' AND users.caps.grantee = $author
) BY $dev;`;

type Actors = { dev: OwnIdentity; admin: OwnIdentity; alice: OwnIdentity };

async function filesEnv(): Promise<{ env: LangEnv; a: Actors }> {
    const a = { dev: await newIdentity(), admin: await newIdentity(), alice: await newIdentity() };
    const env = await createLangEnv({ vars: { ...a, me: a.dev } });
    await env.run(SETUP);
    env.vars['me'] = a.admin;
    await env.run(`
        CREATE DATABASE prod USING CATALOG app WITH PARAMS (:admin = $admin) BY $admin;
        INSERT INTO users.caps (label, grantee) VALUES ('writer', $admin);
        INSERT INTO users.identities (keyId, publicKey, name) VALUES ($alice, publicKey($alice), 'Alice');
    `);
    return { env, a };
}

function memoryLocalFiles(): LocalFileAccess & { files: Map<string, Uint8Array> } {
    const files = new Map<string, Uint8Array>();
    return {
        files,
        async open(path) {
            const bytes = files.get(path);
            if (bytes === undefined) throw new Error(`no local file '${path}'`);
            return { size: bytes.length, async *read() { yield bytes; } };
        },
        async write(path, chunks) {
            const parts: Uint8Array[] = [];
            for await (const chunk of chunks) parts.push(chunk);
            const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
            let offset = 0;
            for (const p of parts) { out.set(p, offset); offset += p.length; }
            files.set(path, out);
        },
    };
}

async function one<T>(env: LangEnv, sql: string): Promise<T> {
    const [result] = await env.run(sql);
    return result as unknown as T;
}

function source(bytes: Uint8Array): FileSource {
    return { size: bytes.length, async *read() { yield bytes; } };
}

export const filesLangTests = {
    title: '[RDB_LANG:FILES] PUT, GET and LIST',
    tests: [
        {
            name: '[FLANG01] PUT STRING and PUT B64, then GET as text and AS B64; LIST by prefix and section',
            invoke: async () => {
                const { env, a } = await filesEnv();
                const put = await one<PutFileLangResult>(env, "PUT STRING 'hello world' INTO media AT 'notes/hello.txt';");
                assertTrue(put.kind === 'put-file' && put.uploaded && put.size === 11 && put.section === 'common', 'PUT STRING uploads and adds');
                assertEquals(put.fileHash, await hashFileSource(source(new TextEncoder().encode('hello world'))), 'the file hash of the UTF-8 bytes');

                const got = await one<GetFileLangResult>(env, "GET 'notes/hello.txt' FROM media;");
                assertEquals(got.text, 'hello world', 'GET returns the text');

                await env.run("PUT B64 'AAEC/w==' INTO prod.media AT 'bin/raw' BY $admin;");
                const raw = await one<GetFileLangResult>(env, "GET 'bin/raw' FROM media AS B64;");
                assertEquals(raw.b64, 'AAEC/w==', 'AS B64 gives the bytes back');
                assertTrue((await env.fail("GET 'bin/raw' FROM media;")).includes('is not UTF-8 text; use AS B64'), 'binary asks for AS B64');

                await env.run("PUT STRING 'mine' INTO media AT 'notes/hello.txt' IN KEY;");
                const mine = await one<GetFileLangResult>(env, "GET 'notes/hello.txt' FROM media IN KEY;");
                assertEquals(`${mine.text} ${mine.owner === a.admin.keyId}`, 'mine true', 'IN KEY writes and reads the author\'s section');
                const byOwner = await one<GetFileLangResult>(env, `GET 'notes/hello.txt' FROM media IN KEY #${a.admin.keyId.slice(0, 10)};`);
                assertEquals(byOwner.text, 'mine', 'IN KEY #prefix names the owner');

                const all = await one<ListFilesLangResult>(env, 'LIST FROM media;');
                assertEquals(all.rows.map((r) => `${r.section}:${r.path}:${r.size}:${r.complete}`).join(','),
                    'common:bin/raw:4:true,common:notes/hello.txt:11:true,key:notes/hello.txt:4:true', 'LIST lists every section');
                const notes = await one<ListFilesLangResult>(env, "LIST 'notes' FROM media IN COMMON;");
                assertEquals(notes.rows.map((r) => r.path).join(','), 'notes/hello.txt', 'a prefix and a section filter');
                const partial = await one<ListFilesLangResult>(env, "LIST 'note' FROM media;");
                assertEquals(partial.rows.length, 0, 'a prefix matches whole segments');
                const keys = await one<ListFilesLangResult>(env, 'LIST FROM media IN KEY;');
                assertEquals(keys.rows.map((r) => r.owner === a.admin.keyId).join(','), 'true', 'IN KEY alone is the current author');
            },
        },
        {
            name: '[FLANG02] PUT skips stored bytes and replaces the file at its path; HASH picks among several',
            invoke: async () => {
                const { env, a } = await filesEnv();
                await env.run("PUT STRING 'v1' INTO media AT 'doc.txt';");
                const copy = await one<PutFileLangResult>(env, "PUT STRING 'v1' INTO media AT 'copy.txt';");
                assertTrue(!copy.uploaded, 'the same bytes are not uploaded again');
                const again = await one<PutFileLangResult>(env, "PUT STRING 'v1' INTO media AT 'copy.txt';");
                assertEquals(again.entries.length, 0, 'putting the same file again appends nothing');

                await env.run("PUT STRING 'v2' INTO media AT 'doc.txt';");
                const listed = await one<ListFilesLangResult>(env, "LIST 'doc.txt' FROM media;");
                assertEquals(listed.rows.length, 1, 'a PUT replaces the file at its path');
                assertEquals((await one<GetFileLangResult>(env, "GET 'doc.txt' FROM media;")).text, 'v2', 'with the new bytes');

                const files = await env.lang.resolveFiles!({ kind: 'name', text: 'media', parts: ['media'], span: { start: 0, end: 0, line: 1, column: 1 } });
                const other = await files.store.putFile(source(new TextEncoder().encode('v3')), a.admin, { lane: 0 });
                await files.map.add({ section: 'common', path: 'doc.txt', fileHash: other.fileHash }, a.admin);
                const several = await env.fail("GET 'doc.txt' FROM media;");
                assertTrue(several.includes("has 2 files at 'doc.txt'") && several.includes("add HASH 'prefix'"), several);
                const picked = await one<GetFileLangResult>(env, `GET 'doc.txt' FROM media HASH '${other.fileHash.slice(0, 8)}';`);
                assertEquals(picked.text, 'v3', 'HASH picks one');

                const missing = await hashFileSource(source(new TextEncoder().encode('never uploaded')));
                await files.map.add({ section: 'common', path: 'ghost.txt', fileHash: missing }, a.admin);
                const ghost = await env.fail("GET 'ghost.txt' FROM media;");
                assertTrue(ghost.includes('is not complete on this replica: none of its bytes have arrived'), ghost);
                const ghostRow = (await one<ListFilesLangResult>(env, "LIST 'ghost.txt' FROM media;")).rows[0]!;
                assertEquals(`${ghostRow.size} ${ghostRow.complete}`, 'null false', 'LIST shows it incomplete');
            },
        },
        {
            name: '[FLANG03] PUT needs a signing author that ALLOW WRITE IF admits, a valid path and a known FILES',
            invoke: async () => {
                const { env } = await filesEnv();
                assertTrue((await env.fail("PUT STRING 'x' INTO media AT 'a.txt' BY NOBODY;")).includes('BY NOBODY is not allowed'), 'NOBODY');
                assertTrue((await env.fail("PUT STRING 'x' INTO media AT 'a.txt' BY $alice;")).includes("can't write to FILES media"), 'a key the predicate refuses');
                assertTrue((await env.fail("PUT STRING 'x' INTO media AT 'bad:name.txt';")).includes("path 'bad:name.txt' contains ':'"), 'the path rules');
                assertTrue((await env.fail("PUT STRING 'x' INTO nothing AT 'a.txt';")).includes("Unknown FILES 'nothing'"), 'an unknown FILES');
                assertTrue((await env.fail("PUT B64 'not base64!' INTO media AT 'a.bin';")).includes('canonical base64'), 'bad base64');
                assertTrue((await env.fail("GET 'nope.txt' FROM media;")).includes("has no file 'nope.txt' in common"), 'a missing file');
                delete env.vars['me'];
                assertTrue((await env.fail("PUT STRING 'x' INTO media AT 'a.txt';")).includes('PUT needs an author'), 'no author at all');
            },
        },
        {
            name: '[FLANG04] PUT FILE and GET ... TO go through the host, and are refused without one',
            invoke: async () => {
                const { env } = await filesEnv();
                assertTrue((await env.fail("PUT FILE 'local/pic.png' INTO media;")).includes('This host cannot access local files'), 'no host files');
                assertTrue((await env.fail("PUT STRING 'x' INTO media AT 'x.txt'; GET 'x.txt' FROM media TO 'out.txt';"))
                    .includes('This host cannot access local files'), 'no host files for TO');

                const local = memoryLocalFiles();
                env.lang.localFiles = local;
                const bytes = new Uint8Array(300_000).map((_, i) => (i * 7) % 256);
                local.files.set('local/pic.png', bytes);
                const put = await one<PutFileLangResult>(env, "PUT FILE 'local/pic.png' INTO media;");
                assertEquals(`${put.path} ${put.size}`, 'pic.png 300000', "AT defaults to the local file's name");
                const got = await one<GetFileLangResult>(env, "GET 'pic.png' FROM media TO 'restored/pic.png';");
                assertEquals(got.written, 'restored/pic.png', 'GET TO writes the local file');
                const restored = local.files.get('restored/pic.png')!;
                assertTrue(restored.length === bytes.length && restored.every((b, i) => b === bytes[i]), 'with the same bytes');
                assertTrue((await env.fail("PUT FILE 'missing.bin' INTO media;")).includes("no local file 'missing.bin'"), 'a missing local file');
            },
        },
        {
            name: "[FLANG05] PUT resumes the author's interrupted upload of the same bytes",
            invoke: async () => {
                const { env, a } = await filesEnv();
                const text = 'x'.repeat(3 * CHUNK_BYTES + 5);
                const bytes = new TextEncoder().encode(text);
                const files = await env.lang.resolveFiles!({ kind: 'name', text: 'media', parts: ['media'], span: { start: 0, end: 0, line: 1, column: 1 } });
                let reads = 0;
                const interrupted: FileSource = {
                    size: bytes.length,
                    async *read() {
                        const upload = ++reads === 2;
                        for (let i = 0, n = 0; i < bytes.length; i += CHUNK_BYTES, n++) {
                            if (upload && n === 2) throw new Error('interrupted');
                            yield bytes.subarray(i, Math.min(i + CHUNK_BYTES, bytes.length));
                        }
                    },
                };
                try { await files.store.putFile(interrupted, a.admin, { lane: 1 }); } catch { /* interrupted */ }
                const fileHash = await hashFileSource(source(bytes));
                const [chain] = await files.store.findChains(fileHash);
                assertEquals(`${chain!.received}/${chain!.chunks} ${chain!.complete}`, '2/4 false', 'two chunks made it');

                const put = await one<PutFileLangResult>(env, `PUT STRING '${text}' INTO media AT 'big.txt';`);
                assertTrue(put.uploaded, 'the rest is uploaded');
                const chains = await files.store.findChains(fileHash);
                assertEquals(chains.map((c) => `${c.header === chain!.header} ${c.complete}`).join(','), 'true true', 'on the same chain, now complete');
                assertEquals((await one<GetFileLangResult>(env, "GET 'big.txt' FROM media;")).text?.length, text.length, 'and it reads back');
            },
        },
    ],
};
