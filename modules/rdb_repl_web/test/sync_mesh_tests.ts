import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { BroadcastChannelCtor, BroadcastChannelLike } from "@hyper-hyper-space/hhs3_mesh_bc";

import { createBrowserSyncMeshFactory } from "../src/sync_mesh.js";

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

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

export async function runBrowserSyncMeshTests(): Promise<void> {
    buses.clear();
    const hashSuite = createBasicCrypto().hash(HASH_SHA256);
    const alice = await createIdentity(SIGNING_ED25519, hashSuite);
    const bob = await createIdentity(SIGNING_ED25519, hashSuite);
    const factory = createBrowserSyncMeshFactory({ BroadcastChannelCtor: FakeCtor });

    const builtA = await factory({ scope: 'localhost', identity: alice });
    const builtB = await factory({ scope: 'localhost', identity: bob });
    try {
        assert(
            builtA.listenAddresses.length === 1 && builtA.listenAddresses[0]!.startsWith('bc://'),
            `listen is bc:// only (got ${builtA.listenAddresses.join(', ')})`,
        );
        assert(
            builtA.announcedAddresses.every((a) => a.startsWith('bc://')),
            'announced addresses are bc://',
        );
        assert(
            builtA.discoveryNotes.some((n) => n.includes('unreachable') || n.startsWith('broadcast-channel')),
            `discovery notes mention backup (${builtA.discoveryNotes.join('; ')})`,
        );

        const topic = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
        await builtA.discovery.announce(topic, {
            keyId: alice.keyId,
            addresses: builtA.announcedAddresses,
        });
        await builtB.discovery.announce(topic, {
            keyId: bob.keyId,
            addresses: builtB.announcedAddresses,
        });

        const found: string[] = [];
        for await (const peer of builtA.discovery.discover(topic)) {
            found.push(peer.keyId);
        }
        assert(found.includes(bob.keyId), `two discoveries see each other (got ${found.join(', ')})`);
    } finally {
        builtA.mesh.close();
        builtB.mesh.close();
        for (const c of [...builtA.closeables, ...builtB.closeables]) await c.close();
    }
}
