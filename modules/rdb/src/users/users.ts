// Users: a standard, reusable RTableGroup that provides identities (an
// identity provider for signature verification) and capabilities (RCap-like
// delegation, expressed entirely in Rdb terms — see ../../CAPABILITIES.md).
//
// It validates the Users stack end-to-end:
//   - a Users group verifies its OWN ops via its in-group `identities` provider
//     (full B2: an intra-group cap revoke is a barrier reaching concurrent uses);
//   - app groups bind `users -> <Users group>` and select `users.identities`
//     as their provider, verifying via the cross-group barrier boundary: a
//     concurrent foreign revoke voids the use at the merged frontier.
//
// Tables:
//   identities  keyId (pub+readonly), publicKey (pub+readonly), name? — the
//               idProvider. Insert restriction is `true`: OPEN self-certifying
//               registration (v1). Registration grants NO authority (authority
//               is 100% caps) and key lookup bypasses liveness, so an enroll
//               gate would not restrict who can be looked up. A registering
//               user inserts their row ANONYMOUSLY (their key is not yet
//               registered to author with) — self-certifying via the keyId ==
//               hash(publicKey) integrity check. A coherent future variant is
//               enroll-gated registration (out of scope for v1).
//   caps        label (pub+readonly), grantee (pub+readonly). concurrentDeletes so a
//               revoke reaches concurrent uses. Each grant is a write-once
//               witness with a random uuid (re-grant after revoke uses a fresh
//               uuid). Revoke looks up live witnesses via findRowIds and
//               bundles all matching deletes into one atomic group op.
//               Delegation: insert/revoke gated by `exists caps where
//               grantee=$author and label=<manager>`; the root manager cap is
//               fiat at genesis.
//   endpoints   address (pub+readonly), identity (pub+readonly). Mesh listen addresses
//               for the RDb directory mode (see peer_directory.ts). Insert gated
//               by `exists caps where label=<peerCap>`; publish via
//               publishEndpoints (announce on UsersPeerDirectory is a no-op).
//
// Peer discovery (two modes, see README):
//   - Tracker + caps: any PeerDiscovery + createUsersPeerAuthorizer (connect-time)
//   - RDb directory: UsersPeerDirectory.discover reads endpoints + cap filter

import { B64Hash, KeyId, OwnIdentity, random, base64 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { TableDef } from "../rschema/payload.js";
import { RSchemaImpl } from "../rschema/rschema.js";
import { RTableGroupImpl } from "../rtable_group/group.js";
import { deriveRowId } from "../rtable/hash.js";
import type { InsertRowPayload } from "../rtable/payload.js";
import type { RowValues } from "../rtable/interfaces.js";

export const IDENTITIES_TABLE = 'identities';
export const CAPS_TABLE = 'caps';
export const ENDPOINTS_TABLE = 'endpoints';
export const USERS_MANAGER_LABEL = 'manager';
export const USERS_PEER_CAP = 'member';

// Conventional binding name + provider ref for app groups that point at a
// Users group: `bindings: { users: <id> }`, `idProvider: 'users.identities'`.
export const USERS_BINDING = 'users';
export const USERS_IDENTITIES_PROVIDER = USERS_BINDING + '.' + IDENTITIES_TABLE;

function newCapUuid(): string {
    const bytes = random.getBytes(16);
    return 'cap-' + base64.fromArrayBuffer(bytes.slice().buffer);
}

async function capsViewAt(group: RTableGroupImpl, at?: Version) {
    at = at ?? await (await group.getScopedDag()).getFrontier();
    return { at, view: await (await group.getView(at, at)).getTableView(CAPS_TABLE) };
}

// The reusable Users RSchema tables. `managerLabel` is the label of the cap
// that authorizes granting / revoking caps (delegation). `peerCapLabel` gates
// endpoint publish and RDb-directory peer discovery.
export function usersSchemaTables(
    managerLabel: string = USERS_MANAGER_LABEL,
    peerCapLabel: string = USERS_PEER_CAP,
): TableDef[] {
    return [
        {
            name: IDENTITIES_TABLE,
            columns: {
                keyId: { type: 'string', pub: true, readonly: true },
                publicKey: { type: 'string', pub: true, readonly: true },
                name: { type: 'string', nullable: true, pub: true },
            },
            idProvider: { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' },
            // open self-certifying registration (v1)
            restrictions: [{ on: 'insert', rule: { p: 'true' } }],
        },
        {
            name: CAPS_TABLE,
            columns: {
                label: { type: 'string', pub: true, readonly: true },
                grantee: { type: 'string', pub: true, readonly: true },
            },
            concurrentDeletes: true,
            restrictions: [
                // grant: the author must hold a manager cap (delegation)
                { on: 'insert', rule: { p: 'exists', table: CAPS_TABLE, where: { label: managerLabel, grantee: '$author' } } },
                // revoke: a manager, or the cap's grantee
                { on: 'delete', rule: { p: 'or', args: [
                    { p: 'exists', table: CAPS_TABLE, where: { label: managerLabel, grantee: '$author' } },
                    { p: 'cmp', cmp: 'eq', left: { col: 'grantee' }, right: { lit: '$author' } },
                ] } },
            ],
        },
        {
            name: ENDPOINTS_TABLE,
            columns: {
                address: { type: 'string', pub: true, readonly: true },
                identity: { type: 'string', pub: true, readonly: true },
            },
            restrictions: [
                { on: 'insert', rule: { p: 'and', args: [
                    { p: 'cmp', cmp: 'eq', left: { col: 'identity' }, right: { lit: '$author' } },
                    { p: 'exists', table: CAPS_TABLE, where: { label: peerCapLabel, grantee: '$author' } },
                ] } },
            ],
        },
    ];
}

// The genesis identity row for an OwnIdentity (anonymous, self-certifying).
export function identityRow(uuid: string, identity: OwnIdentity, name?: string): InsertRowPayload {
    const values: RowValues = {
        keyId: identity.keyId,
        publicKey: serializePublicKeyToBase64(identity.publicKey),
    };
    if (name !== undefined) values.name = name;
    return { action: 'insert', rowId: deriveRowId(uuid), uuid, values };
}

// A cap row granted to `grantee` carrying `label`.
export function capRow(uuid: string, grantee: KeyId, label: string): InsertRowPayload {
    return { action: 'insert', rowId: deriveRowId(uuid), uuid, values: { label, grantee } };
}

export type UsersGroup = {
    schema: RSchemaImpl;
    group: RTableGroupImpl;
    admin: OwnIdentity;
    managerLabel: string;
};

// Build a Users group: the RSchema plus the RTableGroup with the genesis admin
// identity row + the root manager cap (granted to admin). The admin can then
// grant further caps (including more manager caps) by authoring cap inserts.
export async function createUsersGroup(
    ctx: RContext,
    admin: OwnIdentity,
    opts?: { seed?: string; managerLabel?: string; backendLabel?: string },
): Promise<UsersGroup> {
    const seed = opts?.seed ?? 'users';
    const managerLabel = opts?.managerLabel ?? USERS_MANAGER_LABEL;

    const schemaInit = await RSchemaImpl.create({
        seed: seed + '-schema',
        name: 'users',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: usersSchemaTables(managerLabel),
    });
    const schema = (await ctx.createObject(schemaInit, opts?.backendLabel)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: seed + '-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('admin', admin)],
            [CAPS_TABLE]: [capRow('root-cap', admin.keyId, managerLabel)],
        },
    });
    const group = (await ctx.createObject(groupInit, opts?.backendLabel)) as RTableGroupImpl;

    return { schema, group, admin, managerLabel };
}

// Register an identity (open self-certifying, anonymous insert). One row per
// keyId (uuid derives from the keyId).
export async function registerIdentity(
    group: RTableGroupImpl, identity: OwnIdentity, name?: string, at?: Version,
): Promise<B64Hash> {
    const identities = await group.getTable(IDENTITIES_TABLE);
    const row = identityRow('id-' + identity.keyId, identity, name);
    return identities.insert(row.uuid, row.values, undefined, at);
}

// Live cap witness rowIds for `(grantee, label)` at `at`.
export async function findCapGrants(
    group: RTableGroupImpl, grantee: KeyId, label: string, at?: Version,
): Promise<B64Hash[]> {
    const { view } = await capsViewAt(group, at);
    return view.findRowIds({ label, grantee });
}

// Grant a cap: insert a random-uuid witness for `grantee`, authored by
// `granter` (who must hold a manager cap). No-op if a live grant already
// exists (best-effort; concurrency may still produce duplicates).
export async function grantCap(
    group: RTableGroupImpl, granter: OwnIdentity, grantee: KeyId, label: string, at?: Version,
): Promise<B64Hash | undefined> {
    const { at: resolvedAt, view } = await capsViewAt(group, at);
    if ((await view.findRowIds({ label, grantee })).length > 0) return undefined;

    const caps = await group.getTable(CAPS_TABLE);
    return caps.insert(newCapUuid(), { label, grantee }, granter, resolvedAt);
}

// Revoke all live cap witnesses for `(grantee, label)`, bundled into one
// atomic group op. No-op if none are live. Authored by `revoker` (a manager
// or the cap's grantee).
export async function revokeCap(
    group: RTableGroupImpl, revoker: OwnIdentity, grantee: KeyId, label: string, at?: Version,
): Promise<B64Hash | undefined> {
    const { at: resolvedAt, view } = await capsViewAt(group, at);
    const rowIds = await view.findRowIds({ label, grantee });
    if (rowIds.length === 0) return undefined;

    return group.bundle(
        rowIds.map(rowId => ({
            table: CAPS_TABLE,
            op: { action: 'delete', rowId },
        })),
        revoker,
        resolvedAt,
    );
}
