// Shared fixtures for the producer tests: the editor catalog in source form,
// and a developer keystore.

import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { RSchemaView, RSchema } from "@hyper-hyper-space/hhs3_rdb";

import { KeyDirectory } from "../src/keys.js";
import { draftRelease, type ReleaseDraft } from "../src/draft.js";
import { produceRelease, type ProducedRelease } from "../src/produce.js";
import type { Released, ReleaseInfo } from "../src/released.js";
import { readSource } from "../src/source.js";

export const USER_SCHEMA = `CREATE SCHEMA hhs:user CREATORS ($dev) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER,

  TABLE caps (
    label string PUB READONLY,
    grantee identity PUB READONLY
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
    ALLOW delete IF caps.grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);`;

export const DOC_SCHEMA = `CREATE SCHEMA hhs:doc CREATORS ($dev) AS (
  -- the pages of a document
  TABLE pages (
    title string,
    deleted boolean
  ) ALLOW insert IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
    ALLOW update IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
    ALLOW delete IF false,

  TABLE blocks (
    pageId string READONLY REFERENCES pages,
    content string
  ) ALLOW all IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
);`;

export const EDITOR_CATALOG = `-- :admin becomes the first manager.
CREATE CATALOG editor CREATORS ($dev) PARAMS (:admin identity) AS (
  TABLEGROUP user USING SCHEMA hhs:user
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  TABLEGROUP doc USING SCHEMA hhs:doc
    BIND user => user
    USING IDENTITIES user.identities
);`;

export const EDITOR_SOURCE = `-- The editor app.

${USER_SCHEMA}

${DOC_SCHEMA}

${EDITOR_CATALOG}
`;

export const PASSPHRASE = 'pw';

export type DevKeys = { vault: MemoryKeyVault; keys: KeyDirectory };

export async function devKeys(labels: string[] = ['dev', 'alice']): Promise<DevKeys> {
    const vault = new MemoryKeyVault();
    for (const label of labels) await vault.create(label, PASSPHRASE);
    return { vault, keys: KeyDirectory.fromVault(vault) };
}

// A schema's view, from a scratch runtime that creates it.
export async function schemaView(vault: MemoryKeyVault, sql: string, name: string): Promise<{ view: RSchemaView; close: () => Promise<void> }> {
    const runtime = await RdbRuntime.openMemory({ keyVault: vault });
    await runtime.session.unlockKey('dev', PASSPHRASE);
    runtime.session.selectAuthor('dev');
    await runtime.execute(sql);
    const root = await runtime.workspace.roots.resolveSchema({ kind: 'name', text: name, parts: [name], span: { start: 0, end: 0, line: 1, column: 1 } });
    const view = await (root.schema as RSchema).getView();
    return { view, close: () => runtime.close() };
}

export async function draftFor(
    dev: DevKeys, released: Released, source: string, version: string, parents: ReleaseInfo[], nextText = '',
): Promise<ReleaseDraft> {
    const read = await readSource(source, dev.keys, 'dev');
    return draftRelease({ catalog: 'editor', version, parents, released, source: read, nextText, signer: dev.keys.get('dev')! });
}

// Drafts and produces a release; the draft must have no refusals.
export async function releaseFor(
    dev: DevKeys, released: Released, source: string, version: string, parents: ReleaseInfo[], nextText = '',
): Promise<{ draft: ReleaseDraft; produced: ProducedRelease }> {
    const draft = await draftFor(dev, released, source, version, parents, nextText);
    if (draft.refusals.length > 0) throw new Error(`unexpected refusals: ${draft.refusals.map((r) => r.message).join('; ')}`);
    const produced = await produceRelease(draft, released, await dev.vault.unlock('dev', PASSPHRASE));
    return { draft, produced };
}

// A model as plain JSON, for leak checks.
export function modelText(value: unknown): string {
    return JSON.stringify(value, (_key, v) => (v instanceof Map ? Object.fromEntries(v) : v));
}

export async function expectThrows(fn: () => Promise<unknown>, includes: string, what: string): Promise<string> {
    try {
        await fn();
    } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (!message.includes(includes)) throw new Error(`${what}: expected an error including '${includes}', got: ${message}`);
        return message;
    }
    throw new Error(`${what}: expected an error including '${includes}'`);
}
