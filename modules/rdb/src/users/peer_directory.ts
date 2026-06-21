// Mode 2: read-only PeerDiscovery over synced Users endpoints + cap filter.
// announce / leave are no-ops — use publishEndpoints / withdrawEndpoints.

import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { PeerDiscovery, PeerInfo, TopicId } from "@hyper-hyper-space/hhs3_mesh";

import type { RTableGroupImpl } from "../rtable_group/group.js";
import { resolvePeerDirectory } from "./endpoints.js";
import { USERS_PEER_CAP } from "./users.js";

function fisherYatesShuffle<T>(arr: T[]): T[] {
    for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
}

function filterSchemes(peer: PeerInfo, schemes?: string[]): PeerInfo | undefined {
    if (schemes === undefined || schemes.length === 0) return peer;
    const filtered = peer.addresses.filter(
        addr => schemes.some(s => addr.startsWith(s + '://')),
    );
    if (filtered.length === 0) return undefined;
    return { keyId: peer.keyId, addresses: filtered };
}

export type UsersPeerDirectoryConfig = {
    group: RTableGroupImpl;
    peerCapLabel?: string;
    topics: TopicId[];
    excludeSelf?: KeyId;
};

export class UsersPeerDirectory implements PeerDiscovery {
    private group: RTableGroupImpl;
    private peerCapLabel: string;
    private topicSet: Set<TopicId>;
    private excludeSelf?: KeyId;

    constructor(config: UsersPeerDirectoryConfig) {
        this.group = config.group;
        this.peerCapLabel = config.peerCapLabel ?? USERS_PEER_CAP;
        this.topicSet = new Set(config.topics);
        this.excludeSelf = config.excludeSelf;
    }

    async *discover(
        topic: TopicId, schemes?: string[], targetPeers?: number,
    ): AsyncIterable<PeerInfo> {
        if (!this.topicSet.has(topic)) return;

        const limit = targetPeers ?? 10;
        const seen = new Set<string>();
        let count = 0;

        const peers = fisherYatesShuffle(
            await resolvePeerDirectory(this.group, this.peerCapLabel, {
                excludeKeyId: this.excludeSelf,
            }),
        );

        for (const peer of peers) {
            const filtered = filterSchemes(peer, schemes);
            if (filtered === undefined) continue;

            let yielded = false;
            for (const addr of filtered.addresses) {
                const key = `${filtered.keyId}@${addr}`;
                if (!seen.has(key)) {
                    seen.add(key);
                    if (!yielded) yielded = true;
                }
            }
            if (yielded) {
                yield filtered;
                count++;
                if (count >= limit) return;
            }
        }
    }

    async announce(_topic: TopicId, _self: PeerInfo): Promise<void> {}

    async leave(_topic: TopicId, _self: KeyId): Promise<void> {}
}
