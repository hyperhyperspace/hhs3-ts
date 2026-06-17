// Shared harness for real two-replica RDb integration tests (Replica + Mesh +
// MemDagBackend). StaticDiscovery requires every sync topic to be precomputed.

import {
    createBasicCrypto, HASH_SHA256, SIGNING_ED25519, KEM_X25519_HKDF,
    sha256, createIdentity,
} from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { NetworkAddress, PeerInfo, TopicId, Mesh } from "@hyper-hyper-space/hhs3_mesh";
import {
    Mesh as MeshClass, StaticDiscovery, MemTransportProvider, createAuthenticator,
} from "@hyper-hyper-space/hhs3_mesh";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import { Replica, MemDagBackend } from "@hyper-hyper-space/hhs3_replica";

import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { RDbImpl, rDbFactory } from "../src/rdb/rdb.js";
import type { TableDef } from "../src/rschema/payload.js";

export const crypto = createBasicCrypto();
export const hashSuite = crypto.hash(HASH_SHA256);

/** Minimal RContext for deterministic id precomputation only. */
export const dummyCtx = { getCrypto: () => crypto } as RContext;

export function wait(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function waitUntil(predicate: () => Promise<boolean>, intervalMs = 20, timeoutMs = 10000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await wait(intervalMs);
    }
    throw new Error(`waitUntil timed out after ${timeoutMs}ms`);
}

export function registerRdbTypes(replica: Replica): void {
    replica.registerType(RSchemaImpl.typeId, rSchemaFactory);
    replica.registerType(RTableGroupImpl.typeId, rTableGroupFactory);
    replica.registerType(RDbImpl.typeId, rDbFactory);
}

export type PeerSetup = {
    replica: Replica;
    mesh: Mesh;
    addr: NetworkAddress;
    peerInfo: PeerInfo;
};

export function createPeer(
    testId: string,
    peerName: string,
    provider: MemTransportProvider,
    noiseId: OwnIdentity,
    remotePeer: PeerInfo,
    topics: TopicId[],
    config?: { selfValidate?: boolean },
): PeerSetup {
    const addr: NetworkAddress = `mem://${peerName}-${testId}`;

    const mesh = new MeshClass({
        transports: [provider],
        discovery: new StaticDiscovery([remotePeer], topics),
        authenticator: createAuthenticator({
            localKey: noiseId,
            signingName: SIGNING_ED25519,
            kemPrefs: [KEM_X25519_HKDF],
        }),
        localKeyId: noiseId.keyId,
        listenAddresses: [addr],
    });

    const replica = new Replica({ crypto, hashSuite, config: { selfValidate: config?.selfValidate ?? true } });
    replica.attachBackend('default', new MemDagBackend(hashSuite));
    replica.attachMesh('default', mesh);

    return {
        replica,
        mesh,
        addr,
        peerInfo: { keyId: noiseId.keyId, addresses: [addr] },
    };
}

export async function cleanup(peers: PeerSetup[], provider: MemTransportProvider): Promise<void> {
    for (const p of peers) {
        await p.replica.close();
        p.mesh.close();
    }
    provider.close();
}

export function computePinnedVersion(createOpId: B64Hash): Version {
    return version(createOpId);
}

/** Union of RDb + member DAG ids + optional transitive binding targets. */
export function closureTopicIds(
    rdbId: B64Hash,
    schemaIds: B64Hash[],
    groupIds: B64Hash[],
    bindingGroupIds: B64Hash[] = [],
): TopicId[] {
    const seen = new Set<B64Hash>([rdbId, ...schemaIds, ...groupIds, ...bindingGroupIds]);
    return [...seen] as TopicId[];
}

export async function getRDb(replica: Replica, rdbId: B64Hash): Promise<RDbImpl> {
    const obj = await replica.getObject(rdbId);
    if (obj === undefined) throw new Error(`RDb '${rdbId}' not in replica`);
    return obj as RDbImpl;
}

export async function getGroup(replica: Replica, groupId: B64Hash): Promise<RTableGroupImpl> {
    const obj = await replica.getObject(groupId);
    if (obj === undefined) throw new Error(`Group '${groupId}' not in replica`);
    return obj as RTableGroupImpl;
}

export async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

export async function hasRowOn(
    replica: Replica, groupId: B64Hash, table: string, rowId: B64Hash,
): Promise<boolean> {
    const group = await getGroup(replica, groupId);
    const view = await (await group.getTable(table)).getView();
    return view.hasRow(rowId);
}

export async function waitForRowOn(
    replica: Replica, groupId: B64Hash, table: string, rowId: B64Hash,
): Promise<void> {
    await waitUntil(() => hasRowOn(replica, groupId, table, rowId));
}

export function openTable(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

/** Wire two peers with precomputed topics (Alice/Bob naming). */
export async function createAliceBobPeers(testId: string, topics: TopicId[]): Promise<{
    provider: MemTransportProvider;
    alice: PeerSetup;
    bob: PeerSetup;
}> {
    const provider = new MemTransportProvider();
    const aliceNoise = await createIdentity(SIGNING_ED25519, sha256);
    const bobNoise = await createIdentity(SIGNING_ED25519, sha256);

    const aliceAddr: NetworkAddress = `mem://alice-${testId}`;
    const bobAddr: NetworkAddress = `mem://bob-${testId}`;
    const alicePeer: PeerInfo = { keyId: aliceNoise.keyId, addresses: [aliceAddr] };
    const bobPeer: PeerInfo = { keyId: bobNoise.keyId, addresses: [bobAddr] };

    const alice = createPeer(testId, 'alice', provider, aliceNoise, bobPeer, topics);
    const bob = createPeer(testId, 'bob', provider, bobNoise, alicePeer, topics);

    // createPeer derives addr from testId+name; align with our explicit addresses
    alice.addr = aliceAddr;
    alice.peerInfo = alicePeer;
    bob.addr = bobAddr;
    bob.peerInfo = bobPeer;

    registerRdbTypes(alice.replica);
    registerRdbTypes(bob.replica);

    return { provider, alice, bob };
}
