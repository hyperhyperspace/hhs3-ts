// The rdb group fixture the projection suite drives into a target. Backend-
// independent: it builds an in-memory RTableGroup with a two-table schema
// exercising precise types (readonly pub string, nullable bounded string,
// scale-2 decimal). Relocated from the per-backend adapter tests so every
// backend runs the same suite against the same group shape.

import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { HashSuite, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    IDENTITIES_TABLE, identityRow, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory, TableDef,
    usersSchemaTables,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "./mock_rcontext.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

export async function makeIdentity(suite: HashSuite = hashSuite): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, suite);
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

// Tables for null writes. `topics` is a plain parent. `notes` has nullable
// columns of each kind a null has to pass through: `memo` with no default,
// `tag` with one, the boolean `done`, and `topic`, a foreign key to `topics`.
export function nullTables(): TableDef[] {
    return [
        {
            name: 'topics',
            columns: { name: { type: 'string' } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'notes',
            columns: {
                title: { type: 'string' },
                memo: { type: 'string', nullable: true },
                tag: { type: 'string', nullable: true, default: 'none' },
                done: { type: 'boolean', nullable: true },
                topic: { type: 'string', nullable: true },
            },
            fks: { topic: 'topics' },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

// A table with a column of each type a write can carry, for the writer suite
// and the native write paths: a required column, nullable columns, a nullable
// column with a default and a required one with a default, readonly columns of
// two types, and a nullable identity column, which projects as a key column.
export function typedTables(): TableDef[] {
    return [
        {
            name: 'things',
            columns: {
                label: { type: 'string' },
                note: { type: 'string', nullable: true },
                kind: { type: 'string', nullable: true, default: 'std' },
                level: { type: 'integer', default: 1 },
                ratio: { type: 'float', nullable: true },
                flag: { type: 'boolean', nullable: true },
                data: { type: 'json', nullable: true },
                big: { type: 'bigint', nullable: true },
                price: { type: 'decimal', nullable: true, constraints: { scale: 2 } },
                blob: { type: 'bytes', nullable: true },
                code: { type: 'string', nullable: true, readonly: true },
                spec: { type: 'json', nullable: true, readonly: true },
                owner: { type: 'identity', nullable: true },
            },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
    ];
}

// A table whose rows can be deleted only while a `delete` grant exists, so a
// delete concurrent with the grant's revoke is voided (the writer suite's
// voided delete).
export function gatedTables(): TableDef[] {
    return [
        {
            name: 'grants',
            columns: { label: { type: 'string', pub: true } },
            restrictions: [{ on: 'all', rule: { p: 'true' } }],
        },
        {
            name: 'items',
            columns: { name: { type: 'string', pub: true } },
            restrictions: [
                { on: 'insert', rule: { p: 'true' } },
                { on: 'update', rule: { p: 'true' } },
                { on: 'delete', rule: { p: 'exists', table: 'grants', where: { label: 'delete' } } },
            ],
        },
    ];
}

export type GroupFixture = {
    schema: RSchemaImpl;
    group: RTableGroupImpl;
    admin: OwnIdentity;
};

// The group is its own identity provider with `admin` registered, so the
// suites can sign rows as `admin`. An anonymous group has no identity provider:
// every op in it is unsigned, and `admin` signs nothing.
async function buildGroup(name: string, tables: TableDef[], opts: { anonymous?: boolean; hashSuite?: HashSuite } = {}): Promise<GroupFixture> {
    const ctx = createMockRContext({ selfValidate: true }, opts.hashSuite ?? hashSuite);
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity(opts.hashSuite ?? hashSuite);
    const schemaInit = await RSchemaImpl.create({
        name,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [...tables, usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create(opts.anonymous === true
        ? { name: name + '-prod', seed: name + '-prod', schemaRef: schema.getId(), schemaVersion: pinned }
        : {
            name: name + '-prod', seed: name + '-prod', schemaRef: schema.getId(), schemaVersion: pinned,
            idProvider: IDENTITIES_TABLE,
            initialRows: { [IDENTITIES_TABLE]: [identityRow('admin', admin)] },
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

// A group for null writes: nullable columns with and without a default, a
// nullable boolean and a nullable foreign key.
export async function createNullsGroup(): Promise<GroupFixture> {
    return buildGroup('notebook', nullTables());
}

// A group with one table of every column type (see typedTables).
// `hashSuite` is the replica's, which the admin's key id is hashed with too.
export async function createTypedGroup(opts: { hashSuite?: HashSuite } = {}): Promise<GroupFixture> {
    return buildGroup('typed', typedTables(), opts);
}

// A group whose items' deletes are gated by a grant (see gatedTables).
export async function createGatedGroup(): Promise<GroupFixture> {
    return buildGroup('gated', gatedTables());
}

// The typed table in a group without an identity provider.
export async function createAnonymousGroup(): Promise<GroupFixture> {
    return buildGroup('anon', typedTables(), { anonymous: true });
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
