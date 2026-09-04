import { testing } from "@hyper-hyper-space/hhs3_util";
import {
    createBasicCrypto,
    createIdentity,
    HASH_SHA256,
    SIGNING_ED25519,
} from "@hyper-hyper-space/hhs3_crypto";
import type { BroadcastChannelCtor, BroadcastChannelLike } from "@hyper-hyper-space/hhs3_mesh_bc";

import { createBrowserMesh, type BuiltMesh } from "../src/index.js";

const UNREACHABLE_TRACKER = "ws://127.0.0.1:1";

const buses = new Map<string, Set<FakeBroadcastChannel>>();

class FakeBroadcastChannel implements BroadcastChannelLike {
    readonly name: string;
    onmessage: ((ev: MessageEvent) => void) | null = null;
    private closed = false;

    constructor(name: string) {
        this.name = name;
        let set = buses.get(name);
        if (set === undefined) {
            set = new Set();
            buses.set(name, set);
        }
        set.add(this);
    }

    postMessage(message: unknown): void {
        if (this.closed) return;
        const set = buses.get(this.name);
        if (set === undefined) return;
        const copy = structuredClone(message);
        for (const peer of set) {
            if (peer === this || peer.closed) continue;
            const ev = { data: copy } as MessageEvent;
            queueMicrotask(() => {
                if (!peer.closed) peer.onmessage?.(ev);
            });
        }
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.onmessage = null;
        buses.get(this.name)?.delete(this);
    }
}

const FakeCtor = FakeBroadcastChannel as unknown as BroadcastChannelCtor;

async function newIdentity() {
    const hashSuite = createBasicCrypto().hash(HASH_SHA256);
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function teardown(...built: (BuiltMesh | undefined)[]): Promise<void> {
    for (const b of built) {
        b?.mesh.close();
        for (const c of b?.closeables ?? []) await c.close();
    }
}

async function runScope(scope: "localhost" | "internet") {
    buses.clear();
    const alice = await newIdentity();
    const bob = await newIdentity();
    let builtA: BuiltMesh | undefined;
    let builtB: BuiltMesh | undefined;
    try {
        builtA = await createBrowserMesh(
            { scope, identity: alice, trackerAddress: UNREACHABLE_TRACKER },
            { BroadcastChannelCtor: FakeCtor },
        );
        builtB = await createBrowserMesh(
            { scope, identity: bob, trackerAddress: UNREACHABLE_TRACKER },
            { BroadcastChannelCtor: FakeCtor },
        );

        testing.assertEquals(builtA.listenAddresses.length, 1, "one listen address");
        testing.assertTrue(
            builtA.listenAddresses[0]!.startsWith("bc://"),
            `listen is bc:// only (got ${builtA.listenAddresses.join(", ")})`,
        );
        testing.assertTrue(
            builtA.discoveryNotes.some((n) => n.startsWith("broadcast-channel")),
            `broadcast-channel backup present (${builtA.discoveryNotes.join("; ")})`,
        );

        const topic = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        await builtA.discovery.announce(topic, { keyId: alice.keyId, addresses: builtA.listenAddresses });
        await builtB.discovery.announce(topic, { keyId: bob.keyId, addresses: builtB.listenAddresses });

        const found: string[] = [];
        for await (const peer of builtA.discovery.discover(topic)) found.push(peer.keyId);
        testing.assertTrue(found.includes(bob.keyId), `bc discovery sees the other peer (got ${found.join(", ")})`);
        testing.assertTrue(!found.includes(alice.keyId), "never yields self");
    } finally {
        await teardown(builtA, builtB);
    }
}

async function testLocalhost() {
    await runScope("localhost");
}

async function testInternet() {
    await runScope("internet");
}

const allSuites = [
    {
        title: "[MESH_BROWSER] Browser mesh factory",
        tests: [
            { name: "[MESH_BROWSER_00] localhost: bc listen + discovery", invoke: testLocalhost },
            { name: "[MESH_BROWSER_01] internet: bc listen + discovery (tracker not advertised bc)", invoke: testInternet },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log("Running tests for HHSv3 mesh_browser module" + (filters.length > 0 ? ` (filter: ${filters})` : "") + "\n");

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
