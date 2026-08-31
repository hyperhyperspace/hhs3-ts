// The rdb group fixture the projection suite drives into a target. Backend-
// independent: it builds an in-memory RTableGroup with a two-table schema
// exercising precise types (readonly pub string, nullable bounded string,
// scale-2 decimal). Relocated from the per-backend adapter tests so every
// backend runs the same suite against the same group shape.

import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory, TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "./mock_rcontext.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

export async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// `ref` readonly pub string, `memo` nullable bounded string, `amount` a
// scale-2 decimal (canonical string carrier); `tags` a second, minimal table.
export function baseTables(): TableDef[] {
    return [
        {
            name: 'ledger',
            columns: {
                ref: { type: 'string', pub: true, readonly: true },
                memo: { type: 'string', nullable: true, constraints: { maxLength: 8 } },
                amount: { type: 'decimal', constraints: { scale: 2 } },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'tags',
            columns: {
                code: { type: 'string', pub: true },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

// FK-bearing tables for the FK-projection conformance. `posts` is a plain
// parent; `comments` carries a self-referential local FK (`parent` -> comments)
// AND a cross-table local FK (`post` -> posts), both nullable so rows can omit
// them. (Cross-group `_row_hash` projection needs a bound foreign group, out of
// scope for this fixture; it is covered by the core mapper/ingest unit tests.)
export function fkTables(): TableDef[] {
    return [
        {
            name: 'posts',
            columns: {
                title: { type: 'string' },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'comments',
            columns: {
                body: { type: 'string' },
                post: { type: 'string', nullable: true },
                parent: { type: 'string', nullable: true },
            },
            fks: { post: 'posts', parent: 'comments' },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

// A plain-start variant of the FK fixture: `comments.post` is a plain nullable
// string (NO fks), so a `set-fks { post: 'posts' }` migration can flip it INTO
// an FK companion (the reverse direction from `fkTables`, which starts as an
// FK). Drives the FK-ness flip live-view backfill conformance.
export function flipTables(): TableDef[] {
    return [
        {
            name: 'posts',
            columns: { title: { type: 'string' } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'comments',
            columns: {
                body: { type: 'string' },
                post: { type: 'string', nullable: true },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

export type GroupFixture = {
    schema: RSchemaImpl;
    group: RTableGroupImpl;
    admin: OwnIdentity;
};

async function buildGroup(name: string, tables: TableDef[]): Promise<GroupFixture> {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: name + '-prod', seed: name + '-prod', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group, admin };
}

export async function createGroup(): Promise<GroupFixture> {
    return buildGroup('finance', baseTables());
}

// A group whose schema exercises FK projection (self-, cross-table, cross-group).
export async function createFkGroup(): Promise<GroupFixture> {
    return buildGroup('forum', fkTables());
}

// A group whose `comments.post` starts PLAIN, so a later `set-fks` flips it into
// an FK companion (drives the FK-ness flip live-view backfill conformance).
export async function createFlipGroup(): Promise<GroupFixture> {
    return buildGroup('flip', flipTables());
}

export async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

export function sameVersion(a: Version | undefined, b: Version): boolean {
    if (a === undefined) return false;
    if (a.size !== b.size) return false;
    for (const h of b) if (!a.has(h)) return false;
    return true;
}
