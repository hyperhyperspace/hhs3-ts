import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { Predicate, TableDef } from "../src/rschema/payload.js";
import {
    usersSchemaTables, identityRow, capRow, revokeCap,
    IDENTITIES_TABLE, CAPS_TABLE, USERS_MANAGER_LABEL, USERS_SCHEMA_NAME,
    USERS_BINDING, USERS_IDENTITIES_PROVIDER,
} from "../src/users/users.js";
import { compareGroupDeltaStrategies } from "./delta_parity/parity.js";

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

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

// The observe gate evaluated in the OBSERVED (Users) group's frame: the
// observer must hold a live manager cap there. `$author` is the observation's
// signing identity, resolved through the bound users provider.
const MUST_BE_MANAGER: Predicate = {
    p: 'exists', table: CAPS_TABLE, where: { label: USERS_MANAGER_LABEL, grantee: '$author' },
};

// A Users group whose genesis carries an identity row for every `identities`
// member and a root manager cap for every `managers` member. Putting identities
// at genesis keeps key lookup resolvable from any observed version (so a former
// principal stays AUTHENTICATABLE while losing AUTHORITY when its cap is
// revoked).
async function makeUsers(ctx: RContext, seed: string, opts: {
    identities: OwnIdentity[];
    managers: OwnIdentity[];
}): Promise<{ group: RTableGroupImpl }> {
    const creator = opts.managers[0] ?? opts.identities[0];
    const schemaInit = await RSchemaImpl.create({
        name: USERS_SCHEMA_NAME,
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: usersSchemaTables(),
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: pinned,
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: opts.identities.map((id, i) => identityRow(seed + '-id-' + i, id)),
            [CAPS_TABLE]: opts.managers.map((m, i) => capRow(seed + '-cap-' + i, m.keyId, USERS_MANAGER_LABEL)),
        },
    });
    return { group: (await ctx.createObject(groupInit)) as RTableGroupImpl };
}

// An app group bound to a Users group, authenticating via `users.identities`.
// `gated` selects the canObserve gate on the `users` binding. The schema has a
// single local table plus an optional cross-group exists-gated table used by
// the delta-parity scenario.
async function makeApp(ctx: RContext, seed: string, usersGroupId: B64Hash, opts?: {
    gated?: boolean;
    creator?: OwnIdentity;
    extraTables?: TableDef[];
}): Promise<{ schema: RSchemaImpl; group: RTableGroupImpl; creator: OwnIdentity }> {
    const creator = opts?.creator ?? await makeIdentity();
    const tables: TableDef[] = [
        { name: 'notes', columns: { body: { type: 'string' } }, restrictions: [{ on: 'all', rule: { p: 'true' } }] },
        ...(opts?.extraTables ?? []),
    ];
    const schemaInit = await RSchemaImpl.create({
        name: `${seed.replace(/[^a-zA-Z0-9_]+/g, '_')}:schema`,
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: pinned,
        bindings: { [USERS_BINDING]: usersGroupId },
        idProvider: USERS_IDENTITIES_PROVIDER,
        ...(opts?.gated ? { canObserve: { [USERS_BINDING]: MUST_BE_MANAGER } } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group, creator };
}

export const rtableObserveGateTests = {
    title: '[OBSGATE] Gated observation (stratified at-use canObserve)',
    tests: [
        {
            name: '[OBSGATE01] validation: a gated observe must be authored AND authorized; ungated is open',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const mallory = await makeIdentity();   // registered, but holds no manager cap
                const b = await makeUsers(ctx, 'og01-b', { identities: [admin, mallory], managers: [admin] });

                const a = await makeApp(ctx, 'og01-a', b.group.getId(), { gated: true });
                const v = await frontier(b.group);

                await expectThrow(() => a.group.observe(USERS_BINDING, v),
                    'a gated observe with no author is rejected');
                await expectThrow(() => a.group.observe(USERS_BINDING, v, mallory),
                    'a gated observe by a non-manager is rejected by canObserve');

                const hAdmin = await a.group.observe(USERS_BINDING, v, admin);
                assertTrue(hAdmin.length > 0, 'a gated observe by a manager is admitted');

                // an ungated binding needs no authority
                const a2 = await makeApp(ctx, 'og01-a2', b.group.getId(), { gated: false });
                const hOpen = await a2.group.observe(USERS_BINDING, v);
                assertTrue(hOpen.length > 0, 'an ungated observe is admitted unauthored');
            },
        },
        {
            name: '[OBSGATE02] benign two concurrent observes by managers both stay live (no over-voiding)',
            invoke: async () => {
                const ctx = newCtx();
                const m1 = await makeIdentity();
                const m2 = await makeIdentity();
                const b = await makeUsers(ctx, 'og02-b', { identities: [m1, m2], managers: [m1, m2] });
                const a = await makeApp(ctx, 'og02-a', b.group.getId(), { gated: true });

                const v = await frontier(b.group);
                const base = await frontier(a.group);
                const h1 = await a.group.observe(USERS_BINDING, v, m1, base);
                const h2 = await a.group.observe(USERS_BINDING, v, m2, base);   // concurrent

                const merged = await frontier(a.group);
                assertFalse(await a.group.isEntryVoided(h1, merged), 'concurrent benign observe h1 stays live');
                assertFalse(await a.group.isEntryVoided(h2, merged), 'concurrent benign observe h2 stays live');
            },
        },
        {
            name: '[OBSGATE03] attack 1: a back-dated former principal observe is voided by a live G-above revoke-import',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const p = await makeIdentity();
                // p is a genesis manager (so its observe validates at v1) and a
                // genesis identity (so it stays authenticatable after revoke)
                const b = await makeUsers(ctx, 'og03-b', { identities: [admin, p], managers: [admin, p] });
                const a = await makeApp(ctx, 'og03-a', b.group.getId(), { gated: true });

                const v1 = await frontier(b.group);          // p is a manager here
                const base = await frontier(a.group);

                const hP = await a.group.observe(USERS_BINDING, v1, p, base);   // valid at insert (p manager @ v1)

                await revokeCap(b.group, admin, p.keyId, USERS_MANAGER_LABEL);   // admin revokes p
                const v2 = await frontier(b.group);          // v2 strictly G-above v1, carries Rk(p)
                const hAdmin = await a.group.observe(USERS_BINDING, v2, admin, base);   // concurrent, live, G-above

                const merged = await frontier(a.group);
                assertTrue(await a.group.isEntryVoided(hP, merged),
                    'the former principal observe widens to the revoke version and voids at-use');
                assertFalse(await a.group.isEntryVoided(hAdmin, merged),
                    'the legitimate forward observe stays live (its author is still a manager)');
            },
        },
        {
            name: '[OBSGATE04] attack 2: a voided back-dated observe does not void a concurrent honest observe',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const pAtt = await makeIdentity();   // attacker (later revoked)
                const p2 = await makeIdentity();     // honest principal (never revoked)
                const b = await makeUsers(ctx, 'og04-b', { identities: [admin, pAtt, p2], managers: [admin, pAtt, p2] });
                const a = await makeApp(ctx, 'og04-a', b.group.getId(), { gated: true });

                const v0 = await frontier(b.group);          // all managers
                const base = await frontier(a.group);

                const hMal = await a.group.observe(USERS_BINDING, v0, pAtt, base);   // attacker observe
                const hU = await a.group.observe(USERS_BINDING, v0, p2, base);       // honest observe (concurrent)

                await revokeCap(b.group, admin, pAtt.keyId, USERS_MANAGER_LABEL);    // revoke ONLY the attacker
                const v1 = await frontier(b.group);          // G-above v0, carries Rk(pAtt) only
                const hFwd = await a.group.observe(USERS_BINDING, v1, admin, base);  // legit forward (concurrent)

                const merged = await frontier(a.group);
                assertTrue(await a.group.isEntryVoided(hMal, merged),
                    'the attacker observe widens to the forward revoke version and is voided');
                assertFalse(await a.group.isEntryVoided(hU, merged),
                    'the honest concurrent observe stays live (its author was never revoked)');
                assertFalse(await a.group.isEntryVoided(hFwd, merged),
                    'the legitimate forward observe stays live');
            },
        },
        {
            name: '[OBSGATE05] residual: a G-concurrent cross-revoke is not enforced by the gate (both observes survive)',
            invoke: async () => {
                const ctx = newCtx();
                const p1 = await makeIdentity();
                const p2 = await makeIdentity();
                const b = await makeUsers(ctx, 'og05-b', { identities: [p1, p2], managers: [p1, p2] });
                const a = await makeApp(ctx, 'og05-a', b.group.getId(), { gated: true });

                const v0 = await frontier(b.group);          // both managers

                // two CONCURRENT revokes in B (both anchored at v0): p1 revokes p2, p2 revokes p1
                await revokeCap(b.group, p1, p2.keyId, USERS_MANAGER_LABEL, v0);
                const vB = await frontier(b.group);          // p2 revoked, p1 still manager
                await revokeCap(b.group, p2, p1.keyId, USERS_MANAGER_LABEL, v0);
                const vC = await frontier(b.group);          // includes p1's revoke too? anchored at v0 -> concurrent

                const base = await frontier(a.group);
                // p1 observes the branch where it is still a manager; p2 likewise
                const h1 = await a.group.observe(USERS_BINDING, vB, p1, base);
                const h2 = await a.group.observe(USERS_BINDING, vC, p2, base);

                const merged = await frontier(a.group);
                assertFalse(await a.group.isEntryVoided(h1, merged),
                    'a G-concurrent revoke does not void the concurrent observe (residual all-survive)');
                assertFalse(await a.group.isEntryVoided(h2, merged),
                    'symmetric: the other concurrent observe also survives');
            },
        },
        {
            name: '[OBSGATE06] delta parity: bounded and full agree over a history with a voided gated observe',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const p = await makeIdentity();
                const b = await makeUsers(ctx, 'og06-b', { identities: [admin, p], managers: [admin, p] });
                const a = await makeApp(ctx, 'og06-a', b.group.getId(), {
                    gated: true,
                    // a cross-group exists-gated table so the observe actually
                    // gates a row's liveness (the bound floor must stay exact)
                    extraTables: [{
                        name: 'items',
                        columns: { name: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: USERS_MANAGER_LABEL } } }],
                    }],
                });

                const genesis = await frontier(a.group);
                const v1 = await frontier(b.group);
                const base = await frontier(a.group);

                const hP = await a.group.observe(USERS_BINDING, v1, p, base);
                await revokeCap(b.group, admin, p.keyId, USERS_MANAGER_LABEL);
                const v2 = await frontier(b.group);
                await a.group.observe(USERS_BINDING, v2, admin, base);

                // a dependent insert (admin caps still exist at v2 -> the exists holds)
                const items = await a.group.getTable('items');
                await items.insert('it-1', { name: 'thing' });

                const end = await frontier(a.group);
                await compareGroupDeltaStrategies(a.group, genesis, end, { seed: 0, start: genesis, end });
                assertTrue(await a.group.isEntryVoided(hP, end),
                    'the back-dated former-principal observe is voided in the parity history too');
            },
        },
    ],
};
