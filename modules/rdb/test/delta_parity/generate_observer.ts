// Parity histories for observer-frame canObserve gates. An observer G binds
// `target => H` and `auth => K`, where K holds a cap per member (granted and
// revoked concurrently) and H is a plain data group.
//
//   - generateObserverGateHistory: the gate on `target` reads `auth.caps`,
//     another binding, through G's live fold of K.
//   - generateObserverLocalGateHistory: both gates read G's own `admins`, whose
//     rules read `auth.caps`, so imports of K and deletes of admins rows form
//     cores that collapse (the loop of the observer-frame design).
//
// Ops land at recent, possibly old, G checkpoints, so side-branch observes and
// back-dated deletes sit next to entries below the fork meet. Parity is over
// G's delta.

import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../../src/rtable_group/group.js";
import { deriveRowId } from "../../src/rtable/hash.js";
import type { Predicate, TableDef } from "../../src/rschema/payload.js";
import { usersSchemaTables, identityRow, IDENTITIES_TABLE, CAPS_TABLE } from "../../src/users/users.js";

import { recordCheckpoint } from "./checkpoints.js";
import { PRNG } from "./prng.js";
import type { GroupHistory } from "./parity.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);
const OPEN: Predicate = { p: 'true' };
const IN_AUTH_CAPS: Predicate = { p: 'exists', table: `auth.${CAPS_TABLE}`, where: { grantee: '$author' } };
const IN_ADMINS: Predicate = { p: 'exists', table: 'admins', where: { key: '$author' } };
const MEMBERS = 4;

function newContext(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function createGroup(ctx: RContext, creator: OwnIdentity, name: string, tables: TableDef[], extra: object): Promise<RTableGroupImpl> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `parity:${name.replace(/-/g, '_')}_schema`,
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables,
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name, seed: name, schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(), ...extra,
    }))) as RTableGroupImpl;
}

type Setup = {
    members: OwnIdentity[];
    K: RTableGroupImpl;
    H: RTableGroupImpl;
    kCheckpoints: Version[];
};

// K: identities (the provider) and caps with open rules, one genesis cap per
// member; H: one open table.
async function setup(ctx: RContext, seed: number, tag: string): Promise<Setup> {
    const members: OwnIdentity[] = [];
    for (let i = 0; i < MEMBERS; i++) members.push(await createIdentity(SIGNING_ED25519, hashSuite));
    const K = await createGroup(ctx, members[0]!, `parity-${tag}-k-${seed}`, [
        usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!,
        { name: CAPS_TABLE, columns: { grantee: { type: 'string', pub: true } }, concurrentDeletes: true, restrictions: [{ on: 'all', rule: OPEN }] },
    ], {
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: members.map((m, i) => identityRow(`${tag}-${seed}-id-${i}`, m)),
            [CAPS_TABLE]: members.map((m, i) => ({ action: 'insert' as const, rowId: deriveRowId(`${tag}-${seed}-cap-${i}-0`), uuid: `${tag}-${seed}-cap-${i}-0`, values: { grantee: m.keyId } })),
        },
    });
    const H = await createGroup(ctx, members[0]!, `parity-${tag}-h-${seed}`, [
        { name: 'items', columns: { n: { type: 'string' } }, restrictions: [{ on: 'all', rule: OPEN }] },
    ], {});
    return { members, K, H, kCheckpoints: [await frontier(K)] };
}

// Recent checkpoints, so a branch is concurrent with entries that still matter.
function pickAt(prng: PRNG, checkpoints: Version[], window = 5): Version {
    const lo = Math.max(0, checkpoints.length - 1 - window);
    return checkpoints[prng.nextInt(lo, checkpoints.length - 1)]!;
}

// A grant or a revoke of a member's cap in K, at a recent K checkpoint.
async function kOp(prng: PRNG, s: Setup, seed: number, tag: string, counter: { n: number }): Promise<void> {
    const caps = await s.K.getTable(CAPS_TABLE);
    const at = pickAt(prng, s.kCheckpoints);
    const member = prng.nextInt(0, MEMBERS - 1);
    try {
        if (prng.next() < 0.5) {
            const rows = await (await (await s.K.getView(at, at)).getTableView(CAPS_TABLE)).findRowIds({ grantee: s.members[member]!.keyId });
            if (rows.length === 0) return;
            await caps.delete(rows[prng.nextInt(0, rows.length - 1)]!, undefined, at);
        } else {
            const uuid = `${tag}-${seed}-cap-${member}-${++counter.n}`;
            await caps.insert(uuid, { grantee: s.members[member]!.keyId }, undefined, at);
        }
    } catch {
        return;
    }
    s.kCheckpoints.push(await frontier(s.K));
}

async function adminRows(G: RTableGroupImpl, who: OwnIdentity, at: Version): Promise<B64Hash[]> {
    return (await (await G.getView(at, at)).getTableView('admins')).findRowIds({ key: who.keyId });
}

// The gate on `target` reads `auth.caps`, through G's live fold of K.
export async function generateObserverGateHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = newContext();
    const s = await setup(ctx, seed, 'obsgate');
    const G = await createGroup(ctx, s.members[0]!, `parity-obsgate-g-${seed}`, [
        { name: 'notes', columns: { n: { type: 'string' } }, restrictions: [{ on: 'all', rule: OPEN }] },
    ], {
        bindings: { target: s.H.getId(), auth: s.K.getId() },
        canObserve: { target: IN_AUTH_CAPS },
        idProvider: 'auth.identities',
    });
    const notes = await G.getTable('notes');
    const items = await s.H.getTable('items');
    const counter = { n: 0 };
    const checkpoints: Version[] = [version(G.getId())];
    try {
        await G.observe('auth', await frontier(s.K));
        await recordCheckpoint(checkpoints, await frontier(G));
    } catch { /* nothing to import yet */ }

    for (let i = 0; i < ops; i++) {
        const roll = prng.nextInt(0, 99);
        if (roll < 30) { await kOp(prng, s, seed, 'obsgate', counter); continue; }
        if (roll < 38) { await items.insert(`it-${seed}-${i}`, { n: 'x' }); continue; }
        const at = pickAt(prng, checkpoints);
        try {
            if (roll < 60) {
                await G.observe('auth', pickAt(prng, s.kCheckpoints, 8), undefined, at);
            } else if (roll < 90) {
                await G.observe('target', await frontier(s.H), s.members[prng.nextInt(0, MEMBERS - 1)], at);
            } else {
                await notes.insert(`n-${seed}-${i}`, { n: 'n' }, undefined, at);
            }
        } catch {
            continue;
        }
        await recordCheckpoint(checkpoints, await frontier(G));
    }
    return { group: G, rawDag: (await ctx.getDag(G.getId()))!, checkpoints, seed };
}

// Both gates read G's own `admins`, whose rules read `auth.caps`: imports of
// revokes and deletes of the importers' rows form cores.
export async function generateObserverLocalGateHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = newContext();
    const s = await setup(ctx, seed, 'obslocal');
    const G = await createGroup(ctx, s.members[0]!, `parity-obslocal-g-${seed}`, [
        { name: 'admins', columns: { key: { type: 'string', pub: true } }, concurrentDeletes: true, restrictions: [{ on: 'all', rule: IN_AUTH_CAPS }] },
    ], {
        bindings: { target: s.H.getId(), auth: s.K.getId() },
        canObserve: { auth: IN_ADMINS, target: IN_ADMINS },
        idProvider: 'auth.identities',
        initialRows: {
            admins: s.members.map((m, i) => ({ action: 'insert' as const, rowId: deriveRowId(`obslocal-${seed}-admin-${i}`), uuid: `obslocal-${seed}-admin-${i}`, values: { key: m.keyId } })),
        },
    });
    const admins = await G.getTable('admins');
    const items = await s.H.getTable('items');
    const counter = { n: 0 };
    const checkpoints: Version[] = [version(G.getId())];
    try {
        await G.observe('auth', await frontier(s.K), s.members[0]);
        await recordCheckpoint(checkpoints, await frontier(G));
    } catch { /* nothing to import yet */ }

    for (let i = 0; i < ops; i++) {
        const roll = prng.nextInt(0, 99);
        if (roll < 30) { await kOp(prng, s, seed, 'obslocal', counter); continue; }
        if (roll < 35) { await items.insert(`it-${seed}-${i}`, { n: 'x' }); continue; }
        const at = pickAt(prng, checkpoints);
        const author = s.members[prng.nextInt(0, MEMBERS - 1)]!;
        try {
            if (roll < 60) {
                await G.observe('auth', pickAt(prng, s.kCheckpoints, 8), author, at);
            } else if (roll < 78) {
                const target = s.members[prng.nextInt(0, MEMBERS - 1)]!;
                const rows = await adminRows(G, target, at);
                if (rows.length === 0) continue;
                await admins.delete(rows[0]!, author, at);
            } else if (roll < 88) {
                const target = s.members[prng.nextInt(0, MEMBERS - 1)]!;
                await admins.insert(`admin-${seed}-${i}`, { key: target.keyId }, author, at);
            } else {
                await G.observe('target', await frontier(s.H), author, at);
            }
        } catch {
            continue;
        }
        await recordCheckpoint(checkpoints, await frontier(G));
    }
    return { group: G, rawDag: (await ctx.getDag(G.getId()))!, checkpoints, seed };
}
