import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

import {
    deriveGroupSeed, deriveGenesisRowUuid, instantiateGroup, groupIdOf, defaultDeployPredicate,
    computeMembership, MissingParamError, deriveFilesSeed, instantiateFiles,
} from "../src/rdb/instantiate.js";
import { deriveRowId } from "../src/rtable/hash.js";
import { catalogFilesHash, catalogGroupHash } from "../src/rcatalog/payload.js";
import type { CatalogFilesDef, CatalogGroupDef } from "../src/rcatalog/payload.js";
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
function releaseOf(hash: B64Hash, version: string, defs: CatalogGroupDef[], filesDefs: CatalogFilesDef[] = []): ReleaseState {
    const groups = new Map();
    const defMap = new Map();
    for (const def of defs) {
        const h = catalogGroupHash(def);
        defMap.set(h, def);
        groups.set(h, { schema: def.schemaRef, version: [...json.fromSet(def.schemaVersion)].sort() });
    }
    const files = new Map(filesDefs.map((def) => [catalogFilesHash(def), def] as const));
    return {
        hash, version, parents: [], ancestors: new Set(), groups, defs: defMap, addedIn: new Map(),
        files, filesAddedIn: new Map([...files.keys()].map((h) => [h, hash] as const)),
        params: new Map(), added: [...defMap.keys()], changed: [], author: 'dev',
    };
}

function mediaDef(userHash: B64Hash, name = 'media'): CatalogFilesDef {
    return {
        name,
        bindings: { user: userHash },
        idProvider: 'user.identities',
        canWrite: { p: 'exists', table: 'user.caps', where: { grantee: '$author' } },
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

                // notesA comes with 1.1.0 and notesB with 1.0.1: the higher release keeps the name
                const [hashA, hashB] = [catalogGroupHash(notesA), catalogGroupHash(notesB)];
                assertEquals(a.byHash.get(hashA)!.name, 'notes', 'the definition from the higher release keeps the name');
                assertTrue(a.byHash.get(hashB)!.name.startsWith('notes_'), 'the other gets a hash suffix');
                assertTrue(/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(a.byHash.get(hashB)!.name), 'the suffixed name is an identifier');
                assertEquals(a.names.get('notes'), a.byHash.get(hashA)!.id, 'names map to group ids');
                assertFalse(a.byHash.get(userHash)!.target.size === 0, 'targets are the union of the deployed versions');

                // the same version on both sides: the larger hash keeps the name
                const tied = computeMembership({
                    ...input,
                    releases: [releaseOf('r1', '1.1.0', [doc, user, notesA]), releaseOf('r2', '1.1.0', [user, notesB])],
                });
                const [, larger] = [hashA, hashB].sort();
                assertEquals(tied.byHash.get(larger)!.name, 'notes', 'on a version tie the larger hash keeps the name');
                assertEquals(a.files.size, 0, 'no FILES without FILES definitions');
            }
        },
        {
            name: '[INST05] FILES golden vectors: definition hash, seed, store and map ids',
            invoke: async () => {
                const def = mediaDef('grp-1');
                const hash = catalogFilesHash(def);
                assertEquals(hash, 'p5dQBfgU2gMdadEHxDVUCbZJcN14d3bmZ8W/VYcMuZs=', 'catalog FILES hash vector');
                const seed = deriveFilesSeed('rdb-1', hash);
                assertEquals(seed, '+IZTU4BohDz9hE/6a4VPnzYUILirzjqqQ69XqVppWhU=', 'FILES seed vector');

                const files = instantiateFiles(def, { seed, bindingIds: { 'grp-1': 'group-id-1' } });
                assertEquals(files.store.bindings['user'], 'group-id-1', 'the store binds the concrete group id');
                assertEquals(files.map.bindings['user'], 'group-id-1', 'the map binds the concrete group id');
                assertEquals(files.map.blobStore, files.storeId, 'the map carries the store id');
                assertEquals(files.storeId, 'qjMCDo+YScbrOMEfNgPN+Dhksoeor+OUQG5dC75bIok=', 'blob store id vector');
                assertEquals(files.mapId, 'AS8Uyw6eeU8qWJJCArK+PIf1f/DRzLyh0cfCIeq8KjQ=', 'file map id vector');

                let unbound: unknown;
                try { instantiateFiles(def, { seed, bindingIds: {} }); } catch (e) { unbound = e; }
                assertTrue(unbound instanceof Error, 'an uninstantiated binding is an error');
            }
        },
        {
            name: '[INST06] FILES membership: bound to the member group, named together with groups',
            invoke: async () => {
                const user = userDef();
                const userHash = catalogGroupHash(user);
                const media = mediaDef(userHash);
                const input = { rdbId: 'rdb-1', params: ADMIN, creators: [] };

                const one = computeMembership({ ...input, releases: [releaseOf('r1', '1.0.0', [user], [media])] });
                const member = one.files.get(catalogFilesHash(media))!;
                assertEquals(member.name, 'media', 'the FILES member keeps its name');
                assertEquals(member.groupId, one.byHash.get(userHash)!.id, 'bound to the member group');
                assertEquals(member.store.bindings['user'], member.groupId, 'the store binds the member group');
                assertEquals(member.store.seed, deriveFilesSeed('rdb-1', catalogFilesHash(media)), 'seeded from the rdb and definition');
                assertEquals(one.byHash.get(userHash)!.name, 'user', 'the group name is unchanged');

                // a concurrent release brings a group named like the FILES: the higher release keeps the name
                const clash: CatalogGroupDef = { name: 'media', seedSource: 'rdb', schemaRef: 'schema-9', schemaVersion: json.toSet(['schema-9']) };
                const both = computeMembership({
                    ...input,
                    releases: [releaseOf('r1', '1.1.0', [user], [media]), releaseOf('r2', '1.0.1', [user, clash])],
                });
                assertEquals(both.files.get(catalogFilesHash(media))!.name, 'media', 'the FILES from the higher release keeps the name');
                const groupName = both.byHash.get(catalogGroupHash(clash))!.name;
                assertTrue(groupName.startsWith('media_'), 'the group gets a hash suffix');
                assertEquals(both.names.get(groupName), both.byHash.get(catalogGroupHash(clash))!.id, 'names map the suffixed group');
                assertFalse(both.names.has('media'), 'group names never include a FILES name');

                const reversed = computeMembership({
                    ...input,
                    releases: [releaseOf('r2', '1.0.1', [user, clash]), releaseOf('r1', '1.1.0', [user], [media])],
                });
                assertEquals(reversed.files.get(catalogFilesHash(media))!.mapId, both.files.get(catalogFilesHash(media))!.mapId,
                    'FILES membership is deterministic');
            }
        },
    ],
};
