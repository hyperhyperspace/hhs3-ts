import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { TopicId } from "@hyper-hyper-space/hhs3_mesh";
import { sha256 } from "@hyper-hyper-space/hhs3_crypto";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
import {
    createUsersGroup, registerIdentity, grantCap, revokeCap,
    USERS_PEER_CAP,
} from "../src/users/users.js";
import { createUsersPeerAuthorizer } from "../src/users/peer_authorizer.js";
import {
    publishEndpoints, withdrawEndpoints, resolvePeerDirectory, findEndpoints,
} from "../src/users/endpoints.js";
import { UsersPeerDirectory } from "../src/users/peer_directory.js";

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

async function collectDiscover(
    dir: UsersPeerDirectory, topic: TopicId, schemes?: string[],
): Promise<Array<{ keyId: KeyId; addresses: string[] }>> {
    const out: Array<{ keyId: KeyId; addresses: string[] }> = [];
    for await (const p of dir.discover(topic, schemes)) {
        out.push({ keyId: p.keyId, addresses: [...p.addresses] });
    }
    return out;
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

const testTopic = sha256.hashToB64(new TextEncoder().encode('peer-test-topic')) as TopicId;

export const usersPeerTests = {
    title: '[USERS-PEER] Users peer authorizer + RDb directory',
    tests: [
        {
            name: '[USERS-PEER-A01] authorize(keyId) true when live peer cap',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);

                const auth = createUsersPeerAuthorizer(users.group);
                assertTrue(await auth.authorize(alice.keyId), 'holder should authorize');
            },
        },
        {
            name: '[USERS-PEER-A02] authorize false after revokeCap',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);
                await revokeCap(users.group, admin, alice.keyId, USERS_PEER_CAP);

                const auth = createUsersPeerAuthorizer(users.group);
                assertFalse(await auth.authorize(alice.keyId), 'revoked holder should not authorize');
            },
        },
        {
            name: '[USERS-PEER-A03] authorize false for unknown keyId',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const stranger = await makeIdentity();

                const auth = createUsersPeerAuthorizer(users.group);
                assertFalse(await auth.authorize(stranger.keyId), 'unknown keyId should not authorize');
            },
        },
        {
            name: '[USERS-PEER-D01] publishEndpoints + resolvePeerDirectory returns PeerInfo',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);

                await publishEndpoints(users.group, alice, ['mem://alice:9000']);
                const dir = await resolvePeerDirectory(users.group, USERS_PEER_CAP);
                assertEquals(dir.length, 1);
                assertEquals(dir[0].keyId, alice.keyId);
                assertEquals(dir[0].addresses.length, 1);
                assertEquals(dir[0].addresses[0], 'mem://alice:9000');
            },
        },
        {
            name: '[USERS-PEER-D02] no cap → publish rejects / directory excludes',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);

                await expectThrow(() => publishEndpoints(users.group, alice, ['mem://alice:9000']),
                    'endpoint publish without peer cap should reject');
                const dir = await resolvePeerDirectory(users.group, USERS_PEER_CAP);
                assertEquals(dir.length, 0, 'endpoint without peer cap should not appear in directory');
            },
        },
        {
            name: '[USERS-PEER-D03] revokeCap → directory excludes',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);
                await publishEndpoints(users.group, alice, ['mem://alice:9000']);

                await revokeCap(users.group, admin, alice.keyId, USERS_PEER_CAP);
                const dir = await resolvePeerDirectory(users.group, USERS_PEER_CAP);
                assertEquals(dir.length, 0, 'revoked cap should drop peer from directory');
            },
        },
        {
            name: '[USERS-PEER-D04] replace publish removes old addresses',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);

                await publishEndpoints(users.group, alice, ['mem://old:1']);
                await publishEndpoints(users.group, alice, ['mem://new:2']);

                const dir = await resolvePeerDirectory(users.group, USERS_PEER_CAP);
                assertEquals(dir.length, 1);
                assertEquals(dir[0].addresses.length, 1);
                assertEquals(dir[0].addresses[0], 'mem://new:2');
                assertEquals((await findEndpoints(users.group, alice.keyId)).length, 1);
            },
        },
        {
            name: '[USERS-PEER-D05] UsersPeerDirectory discover topic allowlist + scheme filter',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);
                await publishEndpoints(users.group, alice, ['mem://a:1', 'ws://a:2']);

                const peerDir = new UsersPeerDirectory({
                    group: users.group,
                    topics: [testTopic],
                });

                const none = await collectDiscover(peerDir, 'other-topic' as TopicId);
                assertEquals(none.length, 0, 'wrong topic should yield nothing');

                const memOnly = await collectDiscover(peerDir, testTopic, ['mem']);
                assertEquals(memOnly.length, 1);
                assertEquals(memOnly[0].addresses.length, 1);
                assertEquals(memOnly[0].addresses[0], 'mem://a:1');
            },
        },
        {
            name: '[USERS-PEER-D06] announce / leave are no-ops',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);

                const peerDir = new UsersPeerDirectory({ group: users.group, topics: [testTopic] });
                await peerDir.announce(testTopic, { keyId: alice.keyId, addresses: ['mem://ghost:1'] });
                await peerDir.leave(testTopic, alice.keyId);

                const dir = await resolvePeerDirectory(users.group, USERS_PEER_CAP);
                assertEquals(dir.length, 0, 'announce/leave should not write endpoints');
            },
        },
        {
            name: '[USERS-PEER-D07] peer B discovers peer A via UsersPeerDirectory on shared Users group',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                await registerIdentity(users.group, alice);
                await registerIdentity(users.group, bob);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);
                await grantCap(users.group, admin, bob.keyId, USERS_PEER_CAP);

                await publishEndpoints(users.group, alice, ['mem://alice-peer:9000']);

                const peerDir = new UsersPeerDirectory({
                    group: users.group,
                    topics: [testTopic],
                    excludeSelf: bob.keyId,
                });

                const found = await collectDiscover(peerDir, testTopic);
                assertEquals(found.length, 1);
                assertEquals(found[0].keyId, alice.keyId);
                assertEquals(found[0].addresses[0], 'mem://alice-peer:9000');
            },
        },
        {
            name: '[USERS-PEER-D08] withdrawEndpoints clears directory',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const users = await createUsersGroup(ctx, admin);
                const alice = await makeIdentity();
                await registerIdentity(users.group, alice);
                await grantCap(users.group, admin, alice.keyId, USERS_PEER_CAP);
                await publishEndpoints(users.group, alice, ['mem://a:1']);
                await withdrawEndpoints(users.group, alice);

                const dir = await resolvePeerDirectory(users.group, USERS_PEER_CAP);
                assertEquals(dir.length, 0);
            },
        },
    ],
};
