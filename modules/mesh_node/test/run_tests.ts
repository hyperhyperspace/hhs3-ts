import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { testing } from "@hyper-hyper-space/hhs3_util";
import {
    createBasicCrypto,
    createIdentity,
    HASH_SHA256,
    SIGNING_ED25519,
} from "@hyper-hyper-space/hhs3_crypto";

import { createNodeMesh, type BuiltMesh } from "../src/index.js";

const UNREACHABLE_TRACKER = "ws://127.0.0.1:1";

async function newIdentity() {
    const hashSuite = createBasicCrypto().hash(HASH_SHA256);
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
    const dir = await mkdtemp(join(tmpdir(), "mesh-node-"));
    try {
        return await fn(dir);
    } finally {
        await rm(dir, { recursive: true, force: true });
    }
}

async function teardown(...built: (BuiltMesh | undefined)[]): Promise<void> {
    for (const b of built) {
        b?.mesh.close();
        for (const c of b?.closeables ?? []) await c.close();
    }
}

async function testLocalhostDefault() {
    await withTempDir(async (dir) => {
        const alice = await newIdentity();
        let built: BuiltMesh | undefined;
        try {
            built = await createNodeMesh(
                { scope: "localhost", identity: alice, trackerAddress: UNREACHABLE_TRACKER },
                { folderRoot: dir },
            );
            testing.assertEquals(built.listenAddresses.length, 1, "one listen address");
            const addr = built.listenAddresses[0]!;
            testing.assertTrue(addr.startsWith("ws://127.0.0.1:"), `loopback listen (got ${addr})`);
            testing.assertTrue(!addr.endsWith(":0"), "concrete port, not :0");
            testing.assertTrue(!addr.includes("0.0.0.0"), "never binds 0.0.0.0");
        } finally {
            await teardown(built);
        }
    });
}

async function testListenOverride() {
    await withTempDir(async (dir) => {
        const alice = await newIdentity();
        let built: BuiltMesh | undefined;
        try {
            built = await createNodeMesh(
                {
                    scope: "localhost",
                    identity: alice,
                    trackerAddress: UNREACHABLE_TRACKER,
                    listenAddress: "ws://127.0.0.1",
                },
                { folderRoot: dir },
            );
            const addr = built.listenAddresses[0]!;
            testing.assertTrue(addr.startsWith("ws://127.0.0.1:"), `override host preserved (got ${addr})`);
            testing.assertTrue(!addr.endsWith(":0"), "free port filled in");
        } finally {
            await teardown(built);
        }
    });
}

async function testBindAllRejected() {
    const alice = await newIdentity();
    let threw = false;
    try {
        await createNodeMesh({
            scope: "internet",
            identity: alice,
            trackerAddress: UNREACHABLE_TRACKER,
            listenAddress: "ws://0.0.0.0:8080",
        });
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes("bind-all"),
            `error should mention bind-all (got ${(e as Error).message})`,
        );
    }
    testing.assertTrue(threw, "0.0.0.0 listen override should be rejected");
}

async function testInternetDialOut() {
    await withTempDir(async (dir) => {
        const alice = await newIdentity();
        let built: BuiltMesh | undefined;
        try {
            built = await createNodeMesh(
                { scope: "internet", identity: alice, trackerAddress: UNREACHABLE_TRACKER },
                { folderRoot: dir },
            );
            testing.assertEquals(built.listenAddresses.length, 0, "dial-out: no listen addresses");
            testing.assertTrue(
                built.discoveryNotes.some((n) => n.includes("dial-out only")),
                `warns dial-out only (${built.discoveryNotes.join("; ")})`,
            );
            testing.assertTrue(
                built.discoveryNotes.some((n) => n.includes("unreachable")),
                "tracker was still probed",
            );
        } finally {
            await teardown(built);
        }
    });
}

async function testFolderDiscovery() {
    await withTempDir(async (dir) => {
        const alice = await newIdentity();
        const bob = await newIdentity();
        let builtA: BuiltMesh | undefined;
        let builtB: BuiltMesh | undefined;
        try {
            builtA = await createNodeMesh(
                { scope: "localhost", identity: alice, trackerAddress: UNREACHABLE_TRACKER },
                { folderRoot: dir },
            );
            builtB = await createNodeMesh(
                { scope: "localhost", identity: bob, trackerAddress: UNREACHABLE_TRACKER },
                { folderRoot: dir },
            );

            const topic = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
            await builtA.discovery.announce(topic, { keyId: alice.keyId, addresses: builtA.listenAddresses });
            await builtB.discovery.announce(topic, { keyId: bob.keyId, addresses: builtB.listenAddresses });

            const found: string[] = [];
            for await (const peer of builtA.discovery.discover(topic)) found.push(peer.keyId);
            testing.assertTrue(found.includes(bob.keyId), `folder sees the other peer (got ${found.join(", ")})`);
            testing.assertTrue(!found.includes(alice.keyId), "folder never yields self");
        } finally {
            await teardown(builtA, builtB);
        }
    });
}

const allSuites = [
    {
        title: "[MESH_NODE] Node mesh factory",
        tests: [
            { name: "[MESH_NODE_00] localhost default binds loopback (advertise==listen)", invoke: testLocalhostDefault },
            { name: "[MESH_NODE_01] listen override honored", invoke: testListenOverride },
            { name: "[MESH_NODE_02] bind-all listen override rejected", invoke: testBindAllRejected },
            { name: "[MESH_NODE_03] internet without listen is dial-out only", invoke: testInternetDialOut },
            { name: "[MESH_NODE_04] folder discovery backup sees peers, not self", invoke: testFolderDiscovery },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log("Running tests for HHSv3 mesh_node module" + (filters.length > 0 ? ` (filter: ${filters})` : "") + "\n");

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                testing.exitIfFailed(await testing.run(test.name, test.invoke));
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();
