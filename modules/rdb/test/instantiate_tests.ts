import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

import {
    deriveGroupSeed, deriveGenesisRowUuid, instantiateGroup, groupIdOf, defaultDeployPredicate,
    computeMembership, MissingParamError,
} from "../src/rdb/instantiate.js";
import { deriveRowId } from "../src/rtable/hash.js";
import { catalogGroupHash } from "../src/rcatalog/payload.js";
import type { CatalogGroupDef } from "../src/rcatalog/payload.js";
import type { ReleaseState } from "../src/rcatalog/resolve.js";
import type { InsertRowPayload } from "../src/rtable/payload.js";

function userDef(): CatalogGroupDef {
    return {
        name: 'user', seedSource: 'rdb', schemaRef: 'schema-1', schemaVersion: json.toSet(['schema-1']),
        idProvider: 'identities',
        initialRows: {
            identities: [{ values: { name: 'Admin' }, params: { keyId: { param: 'admin' }, publicKey: { param: 'admin', fn: 'publicKey' } } }],
        },
    };
}

const ADMIN = { admin: { identity: { keyId: 'key-a', publicKey: 'pk-a' } } };

// A release state over `defs`, each group at its pin (enough for membership).
function releaseOf(hash: B64Hash, version: string, defs: CatalogGroupDef[]): ReleaseState {
    const groups = new Map();
    const defMap = new Map();
    for (const def of defs) {
        const h = catalogGroupHash(def);
        defMap.set(h, def);
        groups.set(h, { schema: def.schemaRef, version: [...json.fromSet(def.schemaVersion)].sort() });
    }
    return {
        hash, version, parents: [], ancestors: new Set(), groups, defs: defMap, addedIn: new Map(),
        params: new Map(), added: [...defMap.keys()], changed: [], author: 'dev',
    };
}

export const instantiateTests = {
    title: '[INST] Deterministic instantiation tests',
    tests: [
        {
            name: '[INST01] golden vectors: seeds, row uuids and group ids',
            invoke: async () => {
                const seed = deriveGroupSeed('rdb-1', 'grp-1');
                assertEquals(seed, 'Ux+xa6j4fG5ZPJp06QsXRWNlV2T/ZGHU3BVGMucbvx0=', 'group seed vector');
                assertEquals(deriveGenesisRowUuid(seed, 'identities', 0), 'fZahzrni2slfdNdzScU32olYjOySRru9dt6BA5pOMsw=', 'row uuid vector');

                const one = instantiateGroup(userDef(), { seed, bindingIds: {}, params: ADMIN, creators: [{ keyId: 'key-b', publicKey: 'pk-b' }] });
                assertEquals(groupIdOf(one), 'Nk1cyLSJoFr9OUpDNkgHJrovkQi/hq+Bye9tvW94yo4=', 'group id vector, one creator');

                const two = instantiateGroup(userDef(), {
                    seed, bindingIds: {}, params: ADMIN,
                    creators: [{ keyId: 'key-c', publicKey: 'pk-c' }, { keyId: 'key-b', publicKey: 'pk-b' }],
                });
                assertEquals(groupIdOf(two), 'dWB1IU/zH7ffiGL9zomB6EdIOxLQD5hzSwKhnT8CNkE=', 'group id vector, two creators');

                const none = instantiateGroup(userDef(), { seed, bindingIds: {}, params: ADMIN, creators: [] });
                assertEquals(groupIdOf(none), 'f/8z84GVdU9sTsexd+Uoq+h7COTLqRgqaba7RRSkEW4=', 'group id vector, no creators');
            }
        },
        {
            name: '[INST02] the default deploy rule: one bare term, an or of sorted terms, or nothing',
            invoke: async () => {
                const seed = deriveGroupSeed('rdb-1', 'grp-1');

                const one = instantiateGroup(userDef(), { seed, bindingIds: {}, params: ADMIN, creators: [{ keyId: 'key-b', publicKey: 'pk-b' }] });
                assertEquals(json.toStringNormalized(one.canDeploy!),
                    json.toStringNormalized({ p: 'cmp', cmp: 'eq', left: { lit: '$author' }, right: { lit: 'key-b' } }),
                    'one creator gives the bare term');
                assertEquals(one.deployKeys!.map((k) => k.keyId).join(','), 'key-b', 'the creator key is embedded');

                const two = instantiateGroup(userDef(), {
                    seed, bindingIds: {}, params: ADMIN,
                    creators: [{ keyId: 'key-c', publicKey: 'pk-c' }, { keyId: 'key-b', publicKey: 'pk-b' }],
                });
                assertEquals(json.toStringNormalized(two.canDeploy!), json.toStringNormalized({
                    p: 'or', args: [
                        { p: 'cmp', cmp: 'eq', left: { lit: '$author' }, right: { lit: 'key-b' } },
                        { p: 'cmp', cmp: 'eq', left: { lit: '$author' }, right: { lit: 'key-c' } },
                    ],
                }), 'two creators give an or of terms sorted by key id');
                assertEquals(two.deployKeys!.map((k) => k.keyId).join(','), 'key-b,key-c', 'deploy keys are sorted by key id');

                const none = instantiateGroup(userDef(), { seed, bindingIds: {}, params: ADMIN, creators: [] });
                assertTrue(none.canDeploy === undefined && none.deployKeys === undefined, 'no creators and no rule leave deploys open');
                assertTrue(defaultDeployPredicate([]) === undefined, 'no default predicate without creators');

                const declared = { ...userDef(), canDeploy: { p: 'exists', table: 'caps', where: { grantee: '$author' } } } as CatalogGroupDef;
                const copied = instantiateGroup(declared, { seed, bindingIds: {}, params: ADMIN, creators: [{ keyId: 'key-b', publicKey: 'pk-b' }] });
                assertEquals(json.toStringNormalized(copied.canDeploy!), json.toStringNormalized(declared.canDeploy!), 'a declared rule is copied');
                assertTrue(copied.deployKeys === undefined, 'a declared rule embeds no keys');
            }
        },
        {
            name: '[INST03] rows substitute params deterministically; a missing param is an error',
            invoke: async () => {
                const seed = deriveGroupSeed('rdb-1', 'grp-1');
                const payload = instantiateGroup(userDef(), { seed, bindingIds: {}, params: ADMIN, creators: [] });
                const row = payload.initialRows!['identities'][0] as unknown as InsertRowPayload;
                assertEquals(row.uuid, deriveGenesisRowUuid(seed, 'identities', 0), 'the row uuid is derived');
                assertEquals(row.rowId, deriveRowId(row.uuid), 'the row id is unauthored');
                assertEquals(json.toStringNormalized(row.values), json.toStringNormalized({ name: 'Admin', keyId: 'key-a', publicKey: 'pk-a' }),
                    'identity params give the key id and the public key');
                assertTrue(row.author === undefined, 'genesis rows are unauthored');

                let missing: unknown;
                try { instantiateGroup(userDef(), { seed, bindingIds: {}, params: {}, creators: [] }); } catch (e) { missing = e; }
                assertTrue(missing instanceof MissingParamError, 'a missing param is reported');
            }
        },
        {
            name: '[INST04] membership: bottom-up binding resolution, determinism and the name tie-break',
            invoke: async () => {
                const user = userDef();
                const userHash = catalogGroupHash(user);
                const doc: CatalogGroupDef = {
                    name: 'doc', seedSource: 'rdb', schemaRef: 'schema-2', schemaVersion: json.toSet(['schema-2']),
                    bindings: { user: userHash }, idProvider: 'user.identities',
                };
                const notesA: CatalogGroupDef = { name: 'notes', seedSource: 'rdb', schemaRef: 'schema-3', schemaVersion: json.toSet(['s3-a']) };
                const notesB: CatalogGroupDef = { name: 'notes', seedSource: 'rdb', schemaRef: 'schema-3', schemaVersion: json.toSet(['s3-b']) };

                const releases = [releaseOf('r1', '1.1.0', [doc, user, notesA]), releaseOf('r2', '1.0.1', [user, notesB])];
                const input = { rdbId: 'rdb-1', releases, params: ADMIN, creators: [] };
                const a = computeMembership(input);
                const b = computeMembership({ ...input, releases: [...releases].reverse() });

                assertEquals([...a.byId.keys()].sort().join(','), [...b.byId.keys()].sort().join(','), 'membership is deterministic');
                assertEquals(a.byHash.size, 4, 'four member groups');
                assertTrue(a.order.indexOf(userHash) < a.order.indexOf(catalogGroupHash(doc)), 'a bound group is instantiated first');
                assertEquals(a.byHash.get(catalogGroupHash(doc))!.payload.bindings!['user'], a.byHash.get(userHash)!.id,
                    'bindings resolve to the concrete group id');

                const [small, large] = [catalogGroupHash(notesA), catalogGroupHash(notesB)].sort();
                assertEquals(a.byHash.get(small)!.name, 'notes', 'the smallest hash keeps the name');
                assertTrue(a.byHash.get(large)!.name.startsWith('notes_'), 'the other gets a hash suffix');
                assertTrue(/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(a.byHash.get(large)!.name), 'the suffixed name is an identifier');
                assertEquals(a.names.get('notes'), a.byHash.get(small)!.id, 'names map to group ids');
                assertFalse(a.byHash.get(userHash)!.target.size === 0, 'targets are the union of the deployed versions');
            }
        },
    ],
};
