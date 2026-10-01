import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { formatValidationFailure, serializePublicKeyToBase64, ValidationRejectedError, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate } from "../src/rschema/payload.js";
import { IDENTITIES_TABLE } from "../src/users/users.js";
import { identitiesTableDef, localIdentityProvider } from "./identity_fixture.js";

// Authorship on groups with and without an identity provider. A group
// without one is anonymous: every op that claims an author is rejected
// (only a deploy has another key source, deployKeys), no rule may read
// $author, and update/delete default to open.

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

const AUTHOR_IS_ROW_AUTHOR: Predicate = { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } };

// No declared restrictions: the defaults apply.
const docsTable: TableDef = { name: 'docs', columns: { body: { type: 'string' } } };
const capsTable: TableDef = {
    name: 'caps',
    columns: { label: { type: 'string', pub: true }, grantee: { type: 'string', pub: true, nullable: true } },
};

async function createSchema(ctx: RContext, suffix: string, tables: TableDef[], admin: OwnIdentity): Promise<RSchemaImpl> {
    return (await ctx.createObject(await RSchemaImpl.create({
        name: 'ident:schema_' + suffix,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    }))) as RSchemaImpl;
}

async function frontierOf(object: RSchemaImpl | RTableGroupImpl): Promise<Version> {
    return (await object.getScopedDag()).getFrontier();
}

async function createGroup(ctx: RContext, name: string, schema: RSchemaImpl, extras?: {
    bindings?: { [name: string]: B64Hash };
    canObserve?: { [binding: string]: Predicate };
    canDeploy?: Predicate;
    deployKeys?: OwnIdentity[];
    idProvider?: string;
    initialRows?: { [table: string]: json.Literal[] };
}): Promise<RTableGroupImpl> {
    const { deployKeys, ...rest } = extras ?? {};
    return (await ctx.createObject(await RTableGroupImpl.create({
        name, seed: name,
        schemaRef: schema.getId(), schemaVersion: await frontierOf(schema),
        ...rest,
        ...(deployKeys !== undefined ? {
            deployKeys: deployKeys.map((k) => ({ keyId: k.keyId, publicKey: serializePublicKeyToBase64(k.publicKey) })),
        } : {}),
    }))) as RTableGroupImpl;
}

// An anonymous group with an open caps table, seeded with one 'open' cap,
// for other groups to bind and observe.
async function createForeignGroup(ctx: RContext, admin: OwnIdentity): Promise<RTableGroupImpl> {
    const schema = await createSchema(ctx, 'foreign', [capsTable], admin);
    return createGroup(ctx, 'ident-foreign', schema, {
        initialRows: { caps: [{ action: 'insert', rowId: deriveRowId('open-cap'), uuid: 'open-cap', values: { label: 'open' } }] },
    });
}

async function failureOf(fn: () => Promise<unknown>): Promise<string | undefined> {
    try {
        await fn();
        return undefined;
    } catch (e) {
        return e instanceof ValidationRejectedError ? formatValidationFailure(e.why) : (e as Error).message;
    }
}

async function expectRejected(fn: () => Promise<unknown>, messageIncludes: string, why: string): Promise<void> {
    const failure = await failureOf(fn);
    assertTrue(failure !== undefined, why);
    assertTrue(failure!.includes(messageIncludes), `${why}: expected '${messageIncludes}', got: ${failure}`);
}

const NO_PROVIDER = 'the group has no identity provider, so its ops must be anonymous';

export const rtableIdentityTests = {
    title: '[IDENT] Authorship with and without an identity provider',
    tests: [
        {
            name: '[IDENT01] without a provider, authored row ops and bundles are rejected; the same ops unsigned are accepted',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const group = await createGroup(ctx, 'ident01', await createSchema(ctx, '01', [docsTable], admin));
                const docs = await group.getTable('docs');

                await expectRejected(() => docs.insert('d-1', { body: 'x' }, alice), NO_PROVIDER, 'an authored insert is rejected');
                await docs.insert('d-2', { body: 'x' });
                const rowId = deriveRowId('d-2');

                await expectRejected(() => docs.update(rowId, { body: 'y' }, alice), NO_PROVIDER, 'an authored update is rejected');
                await expectRejected(() => docs.delete(rowId, alice), NO_PROVIDER, 'an authored delete is rejected');

                const bundleInsert = (rowId: string) =>
                    ({ table: 'docs', op: { action: 'insert', rowId, uuid: 'd-3', values: { body: 'z' } } } as const);
                await expectRejected(() => group.bundle([bundleInsert(deriveRowId('d-3', alice.keyId))], alice), NO_PROVIDER,
                    'an authored bundle is rejected');
                await group.bundle([bundleInsert(deriveRowId('d-3'))]);

                const view = await (await group.getView()).getTableView('docs');
                assertFalse(await view.hasRow(deriveRowId('d-1', alice.keyId)), 'the authored insert never lands');
                assertTrue(await view.hasRow(rowId), 'the unsigned insert lands');
                assertTrue(await view.hasRow(deriveRowId('d-3')), 'the unsigned bundle lands');
                assertEquals(await view.getAuthor(rowId), undefined, 'rows of an anonymous group have no author');
            }
        },
        {
            name: '[IDENT02] without a provider, unsigned update and delete pass the default rules',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const group = await createGroup(ctx, 'ident02', await createSchema(ctx, '02', [docsTable], admin));
                const docs = await group.getTable('docs');
                const rowId = deriveRowId('d-1');

                await docs.insert('d-1', { body: 'v1' });
                await docs.update(rowId, { body: 'v2' });
                assertEquals((await (await (await group.getView()).getTableView('docs')).getRow(rowId))!.values['body'], 'v2',
                    'an unsigned update passes the anonymous default');
                await docs.delete(rowId);
                assertFalse(await (await (await group.getView()).getTableView('docs')).hasRow(rowId),
                    'an unsigned delete passes the anonymous default');
            }
        },
        {
            name: '[IDENT03] without a provider, an authored observation is rejected, gated binding or not',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const foreign = await createForeignGroup(ctx, admin);
                const target = await frontierOf(foreign);

                const ungated = await createGroup(ctx, 'ident03-ungated', await createSchema(ctx, '03a', [docsTable], admin),
                    { bindings: { f: foreign.getId() } });
                await expectRejected(() => ungated.observe('f', target, alice), NO_PROVIDER,
                    'an authored observation of an ungated binding is rejected');
                await ungated.observe('f', target);

                const gated = await createGroup(ctx, 'ident03-gated', await createSchema(ctx, '03b', [docsTable], admin), {
                    bindings: { f: foreign.getId() },
                    canObserve: { f: { p: 'exists', table: 'caps', where: { label: 'open' } } },
                });
                assertFalse(gated.observeNeedsAuthor('f'), "a gate that doesn't read $author needs no author");
                await expectRejected(() => gated.observe('f', target, alice), NO_PROVIDER,
                    'an authored observation of a gated binding is rejected');
                await gated.observe('f', target);
            }
        },
        {
            name: '[IDENT04] without a provider, a deploy verifies through deployKeys only',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const stranger = await makeIdentity();
                const schema = await createSchema(ctx, '04', [docsTable], admin);
                const group = await createGroup(ctx, 'ident04', schema, {
                    canDeploy: { p: 'cmp', cmp: 'eq', left: { lit: '$author' }, right: { lit: admin.keyId } },
                    deployKeys: [admin],
                });
                assertTrue(group.deployNeedsAuthor(), 'a canDeploy over $author needs an author');

                await schema.updateSchema([{ rule: 'add-column', table: 'docs', column: 'x', def: { type: 'string', nullable: true } }], admin);
                const v2 = await frontierOf(schema);

                await expectRejected(() => group.deploy(v2, stranger), 'could not be verified',
                    'a deploy signed by a key outside deployKeys is rejected');
                await expectRejected(() => group.deploy(v2), 'must be authored',
                    "an unsigned deploy can't pass a canDeploy over $author");
                await group.deploy(v2, admin);
                assertTrue((await group.getView()).getSchemaView().getTable('docs')!.columns['x'] !== undefined,
                    'a deploy signed by a listed deploy key lands');
            }
        },
        {
            name: '[IDENT05] an open canDeploy takes unsigned deploys, and still verifies a signed one',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const stranger = await makeIdentity();
                const schema = await createSchema(ctx, '05', [docsTable], admin);
                const group = await createGroup(ctx, 'ident05', schema, { canDeploy: { p: 'true' }, deployKeys: [admin] });
                assertFalse(group.deployNeedsAuthor(), "a canDeploy that doesn't read $author needs no author");

                await schema.updateSchema([{ rule: 'add-column', table: 'docs', column: 'x', def: { type: 'string', nullable: true } }], admin);
                const v2 = await frontierOf(schema);
                await expectRejected(() => group.deploy(v2, stranger), 'could not be verified',
                    'a signed deploy is verified even when the gate is open');
                await group.deploy(v2);

                await schema.updateSchema([{ rule: 'add-column', table: 'docs', column: 'y', def: { type: 'string', nullable: true } }], admin);
                await group.deploy(await frontierOf(schema), admin);
                assertTrue((await group.getView()).getSchemaView().getTable('docs')!.columns['y'] !== undefined,
                    'a deploy signed by a listed deploy key lands too');
            }
        },
        {
            name: '[IDENT06] create rejects a canObserve or a restriction that reads $author without a provider',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const foreign = await createForeignGroup(ctx, admin);
                const authorGate: Predicate = { p: 'exists', table: 'caps', where: { grantee: '$author' } };
                const ownedDocs: TableDef = { ...docsTable, restrictions: [{ on: 'update', rule: AUTHOR_IS_ROW_AUTHOR }] };

                const plain = await createSchema(ctx, '06a', [docsTable], admin);
                await expectRejected(() => createGroup(ctx, 'ident06-gate', plain, {
                    bindings: { f: foreign.getId() }, canObserve: { f: authorGate },
                }), "the canObserve gate on 'f' reads $author, which needs an idProvider", 'a canObserve over $author is rejected');

                const owned = await createSchema(ctx, '06b', [ownedDocs], admin);
                await expectRejected(() => createGroup(ctx, 'ident06-rule', owned),
                    "the update restriction of table 'docs' reads $author, which needs an idProvider",
                    'a restriction over $author is rejected');

                const withProvider = await createSchema(ctx, '06c', [ownedDocs, identitiesTableDef()], admin);
                const group = await createGroup(ctx, 'ident06-ok', withProvider, {
                    bindings: { f: foreign.getId() }, canObserve: { f: authorGate }, idProvider: IDENTITIES_TABLE,
                });
                assertTrue(group.observeNeedsAuthor('f'), 'with a provider, the same rules are admissible');
            }
        },
        {
            name: '[IDENT07] without a provider, a deploy of a version that adds a $author restriction is rejected',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const schema = await createSchema(ctx, '07', [docsTable], admin);
                const group = await createGroup(ctx, 'ident07', schema);

                await schema.updateSchema([{ rule: 'set-restrictions', table: 'docs', restrictions: [{ on: 'delete', rule: AUTHOR_IS_ROW_AUTHOR }] }], admin);
                const v2 = await frontierOf(schema);
                await expectRejected(() => group.deploy(v2),
                    "schema deploy rejected: the delete restriction of table 'docs' reads $author, which needs an idProvider",
                    'a version with a $author restriction is not deployable');

                await schema.updateSchema([
                    { rule: 'set-restrictions', table: 'docs', restrictions: [] },
                    { rule: 'add-column', table: 'docs', column: 'x', def: { type: 'string', nullable: true } },
                ], admin);
                await group.deploy(await frontierOf(schema));
                assertTrue((await group.getView()).getSchemaView().getTable('docs')!.columns['x'] !== undefined,
                    'a later version without it deploys');
            }
        },
        {
            name: '[IDENT08] with a provider, an observation of an ungated binding must verify any author it claims',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const stranger = await makeIdentity();
                const foreign = await createForeignGroup(ctx, admin);
                const target = await frontierOf(foreign);
                const group = await createGroup(ctx, 'ident08',
                    await createSchema(ctx, '08', [docsTable, identitiesTableDef()], admin),
                    { bindings: { f: foreign.getId() }, ...localIdentityProvider([alice]) });

                assertFalse(group.observeNeedsAuthor('f'), 'an ungated binding needs no author');
                await expectRejected(() => group.observe('f', target, stranger), 'observation has invalid authorship',
                    'an unverifiable author is rejected on an ungated observation');
                await group.observe('f', target, alice);
                await group.observe('f', target);
            }
        },
        {
            name: '[IDENT09] with a local provider, a deploy that drops or unmarks the provider table is rejected',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const schema = await createSchema(ctx, '09', [docsTable, identitiesTableDef()], admin);
                const group = await createGroup(ctx, 'ident09', schema, { idProvider: IDENTITIES_TABLE });

                await schema.updateSchema([{ rule: 'drop-table', table: IDENTITIES_TABLE }], admin);
                await expectRejected(async () => group.deploy(await frontierOf(schema)),
                    `schema deploy would drop identity provider table '${IDENTITIES_TABLE}'`, 'dropping the provider table is rejected');

                const { idProvider: _, ...unmarked } = identitiesTableDef();
                await schema.updateSchema([{ rule: 'add-table', def: unmarked }], admin);
                await expectRejected(async () => group.deploy(await frontierOf(schema)),
                    `schema deploy would unmark table '${IDENTITIES_TABLE}' as the identity provider`,
                    're-adding the table without the provider flag is rejected');
            }
        },
    ],
};
