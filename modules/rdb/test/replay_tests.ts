import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { RContext, ScopedDag, ValidationResult, Version } from "@hyper-hyper-space/hhs3_mvt";
import { createRefAdvancePayload, formatValidationFailure, signPayload, version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef } from "../src/rschema/payload.js";
import { createUsersGroup, registerIdentity, USERS_IDENTITIES_PROVIDER } from "../src/users/users.js";

// Replaying a stored signed payload anywhere other than its own insertion
// point, or re-wrapping it at the same point, must fail signature
// verification: op signatures bind `at` and the scope path, and bundles are
// signed as a whole.

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

const docsTable: TableDef = { name: 'docs', columns: { body: { type: 'string' } } };
const notesTable: TableDef = { name: 'notes', columns: { body: { type: 'string' } } };

function ordersTable(): TableDef {
    return {
        name: 'orders',
        columns: { customer: { type: 'string' }, total: { type: 'float' } },
        concurrentDeletes: false,
    };
}

async function frontierOf(obj: { getScopedDag(): Promise<ScopedDag> }): Promise<Version> {
    return (await obj.getScopedDag()).getFrontier();
}

// A stored entry's payload plus the position it was appended at.
async function loadStored(obj: { getScopedDag(): Promise<ScopedDag> }, h: B64Hash): Promise<{ payload: json.LiteralMap; at: Version }> {
    const entry = await (await obj.getScopedDag()).loadEntry(h);
    assertTrue(entry !== undefined, `entry ${h} should be stored`);
    return {
        payload: entry!.payload as json.LiteralMap,
        at: version(...json.fromSet(entry!.header.prevEntryHashes)),
    };
}

function assertSignatureRejected(result: ValidationResult, why: string): void {
    assertFalse(result.valid, why);
    if (!result.valid) {
        const msg = formatValidationFailure(result.why);
        assertTrue(msg.includes('signature'), `${why}: expected a signature failure, got: ${msg}`);
    }
}

async function makeSchema(ctx: RContext, name: string, creator: OwnIdentity, tables: TableDef[]): Promise<RSchemaImpl> {
    const init = await RSchemaImpl.create({
        name,
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables,
    });
    return (await ctx.createObject(init)) as RSchemaImpl;
}

// An app group over `schema`, authenticating authors through the bound Users
// group's identities provider.
async function makeAppGroup(ctx: RContext, seed: string, schema: RSchemaImpl, usersGroupId: B64Hash): Promise<RTableGroupImpl> {
    const init = await RTableGroupImpl.create({
        name: seed,
        seed: seed + '-group',
        schemaRef: schema.getId(),
        schemaVersion: await frontierOf(schema),
        bindings: { users: usersGroupId },
        idProvider: USERS_IDENTITIES_PROVIDER,
    });
    return (await ctx.createObject(init)) as RTableGroupImpl;
}

// A Users group with `alice` registered, and one app group observing it.
async function makeEnv(appSeeds: string[]) {
    const ctx = newCtx();
    const admin = await makeIdentity();
    const alice = await makeIdentity();

    const users = await createUsersGroup(ctx, admin);
    await registerIdentity(users.group, alice);

    const schema = await makeSchema(ctx, 'replay_app', admin, [docsTable, notesTable]);
    const apps: RTableGroupImpl[] = [];
    for (const seed of appSeeds) {
        const app = await makeAppGroup(ctx, seed, schema, users.group.getId());
        await app.observe('users', await frontierOf(users.group));
        apps.push(app);
    }

    return { ctx, admin, alice, schema, apps };
}

// Alice's stored two-insert bundle into `docs`.
async function storedAliceBundle(app: RTableGroupImpl, alice: OwnIdentity) {
    const h = await app.bundle([
        { table: 'docs', op: { action: 'insert', rowId: deriveRowId('b-1', alice.keyId), uuid: 'b-1', values: { body: 'one' } } },
        { table: 'docs', op: { action: 'insert', rowId: deriveRowId('b-2', alice.keyId), uuid: 'b-2', values: { body: 'two' } } },
    ], alice);
    const stored = await loadStored(app, h);
    assertTrue((await app.validatePayload(stored.payload, stored.at)).valid,
        'the stored bundle validates at its own position');
    return stored;
}

export const replayTests = {
    title: '[REPLAY] Signed op replay protection',
    tests: [
        {
            name: '[REPLAY01] a signed row update replayed at a later position is rejected',
            invoke: async () => {
                const { alice, apps: [app] } = await makeEnv(['replay01']);
                const docs = await app.getTable('docs');

                await docs.insert('d-1', { body: 'v0' }, alice);
                const rowId = deriveRowId('d-1', alice.keyId);
                const h1 = await docs.update(rowId, { body: 'v1' }, alice);
                await docs.update(rowId, { body: 'v2' }, alice);

                const stored = await loadStored(app, h1);
                assertTrue((await app.validatePayload(stored.payload, stored.at)).valid,
                    'the stored update validates at its own position');
                assertSignatureRejected(await app.validatePayload(stored.payload, await frontierOf(app)),
                    'the stored update replayed at the current frontier (reverting v2 to v1)');
            },
        },
        {
            name: '[REPLAY02] a schema-update replayed at a later position is rejected',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const schema = await makeSchema(ctx, 'replay02', admin, [ordersTable()]);

                const h1 = await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'orders', value: true }], admin);
                await schema.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'orders', value: false }], admin);

                const stored = await loadStored(schema, h1);
                assertTrue((await schema.validatePayload(stored.payload, stored.at)).valid,
                    'the stored schema-update validates at its own position');
                assertSignatureRejected(await schema.validatePayload(stored.payload, await frontierOf(schema)),
                    'the stored schema-update replayed at the current frontier (reverting the later rule)');
            },
        },
        {
            name: '[REPLAY03] a signed row op copied into another group of the same schema is rejected',
            invoke: async () => {
                const { alice, apps: [g1, g2] } = await makeEnv(['replay03-a', 'replay03-b']);

                const h = await (await g1.getTable('docs')).insert('d-1', { body: 'x' }, alice);

                const stored = await loadStored(g1, h);
                assertTrue((await g1.validatePayload(stored.payload, stored.at)).valid,
                    'the stored insert validates in its own group at its own position');
                assertSignatureRejected(await g2.validatePayload(stored.payload, await frontierOf(g2)),
                    'the stored insert copied into a sibling group');
            },
        },
        {
            name: '[REPLAY04] a schema-update copied into another schema with the same creator is rejected',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const s1 = await makeSchema(ctx, 'replay04_scratch', admin, [ordersTable()]);
                const s2 = await makeSchema(ctx, 'replay04_prod', admin, [ordersTable()]);

                const h = await s1.updateSchema(
                    [{ rule: 'set-concurrent-deletes', table: 'orders', value: true }], admin);

                const stored = await loadStored(s1, h);
                assertTrue((await s1.validatePayload(stored.payload, stored.at)).valid,
                    'the stored schema-update validates in its own schema at its own position');
                assertSignatureRejected(await s2.validatePayload(stored.payload, await frontierOf(s2)),
                    'the stored schema-update copied into another schema');
            },
        },
        {
            name: '[REPLAY05] a signed row op re-enveloped for another table at the same position is rejected',
            invoke: async () => {
                const { alice, apps: [app] } = await makeEnv(['replay05']);

                const h = await (await app.getTable('docs')).insert('d-1', { body: 'x' }, alice);

                const stored = await loadStored(app, h);
                assertTrue((await app.validatePayload(stored.payload, stored.at)).valid,
                    'the stored docs insert validates at its own position');
                assertSignatureRejected(await app.validatePayload({ ...stored.payload, table: 'notes' }, stored.at),
                    'the stored docs insert re-enveloped for notes');
            },
        },
        {
            name: '[REPLAY06] an op extracted from a signed bundle is rejected as a single envelope',
            invoke: async () => {
                const { alice, apps: [app] } = await makeEnv(['replay06']);
                const stored = await storedAliceBundle(app, alice);

                const writes = stored.payload['writes'] as json.LiteralMap[];
                const extracted = { action: 'row', table: writes[0]['table'], op: writes[0]['op'] };
                assertSignatureRejected(await app.validatePayload(extracted, stored.at),
                    'the first bundle op appended alone at the bundle position');
            },
        },
        {
            name: '[REPLAY07] a truncated signed bundle is rejected',
            invoke: async () => {
                const { alice, apps: [app] } = await makeEnv(['replay07']);
                const stored = await storedAliceBundle(app, alice);

                const writes = stored.payload['writes'] as json.LiteralMap[];
                assertSignatureRejected(await app.validatePayload({ ...stored.payload, writes: [writes[0]] }, stored.at),
                    'the bundle with its second write dropped');
            },
        },
        {
            name: '[REPLAY08] a group-level deploy signed with an explicit empty scope still verifies',
            invoke: async () => {
                const { admin, alice, schema, apps: [app] } = await makeEnv(['replay08']);

                await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'docs', value: true }], admin);
                const schemaVersion = await frontierOf(schema);
                const at = await frontierOf(app);

                const explicit = await signPayload(
                    createRefAdvancePayload(schema.getId(), schemaVersion) as unknown as json.LiteralMap, alice, at, []);
                assertTrue((await app.validatePayload(explicit, at)).valid,
                    'a deploy signed with an explicit empty scope validates at its own position');

                const h = await app.deploy(schemaVersion, alice, at);
                const stored = await loadStored(app, h);
                assertTrue(stored.payload['signature'] === explicit['signature'],
                    'the deploy written by the group carries the same signature as the explicit empty scope');
            },
        },
    ],
};
