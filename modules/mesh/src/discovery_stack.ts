// Priority-based discovery composition. Layers are grouped by numeric priority
// (lower = higher priority) and processed in order. Within a group, sources run
// in parallel and results are merged. The stack yields deduplicated peers until
// targetPeers is reached or all sources are exhausted.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerInfo, PeerDiscovery, TopicId } from './discovery.js';

export interface DiscoveryLayer {
    source:   PeerDiscovery;
    priority: number;
}

export class DiscoveryStack implements PeerDiscovery {

    private groups: PeerDiscovery[][];
    private allSources: PeerDiscovery[];

    constructor(layers: DiscoveryLayer[]) {
        this.allSources = layers.map(l => l.source);

        const byPriority = new Map<number, PeerDiscovery[]>();
        for (const layer of layers) {
            let group = byPriority.get(layer.priority);
            if (group === undefined) {
                group = [];
                byPriority.set(layer.priority, group);
            }
            group.push(layer.source);
        }

        const sortedKeys = [...byPriority.keys()].sort((a, b) => a - b);
        this.groups = sortedKeys.map(k => byPriority.get(k)!);
    }

    async *discover(topic: TopicId, schemes?: string[], targetPeers?: number): AsyncIterable<PeerInfo> {
        const seen = new Set<string>();
        let count = 0;
        const limit = targetPeers ?? 10;

        for (const group of this.groups) {
            if (count >= limit) break;

            const merged = group.length === 1
                ? group[0].discover(topic, schemes)
                : mergeAsyncIterables(group.map(s => s.discover(topic, schemes)));

            for await (const peer of merged) {
                let yielded = false;
                for (const addr of peer.addresses) {
                    const key = `${peer.keyId}@${addr}`;
                    if (!seen.has(key)) {
                        seen.add(key);
                        if (!yielded) {
                            yielded = true;
                        }
                    }
                }
                if (yielded) {
                    count++;
                    yield peer;
                    if (count >= limit) break;
                }
            }
        }
    }

    async announce(topic: TopicId, self: PeerInfo): Promise<void> {
        await Promise.allSettled(
            this.allSources.map(s => s.announce(topic, self))
        );
    }

    async leave(topic: TopicId, self: KeyId): Promise<void> {
        await Promise.allSettled(
            this.allSources.map(s => s.leave(topic, self))
        );
    }
}

// Merges multiple async iterables into a single stream, yielding values as
// they arrive from any source. All iterables are advanced concurrently.

async function* mergeAsyncIterables<T>(iterables: AsyncIterable<T>[]): AsyncIterable<T> {
    const iterators = iterables.map(it => {
        const iter = it[Symbol.asyncIterator]();
        return { iter, done: false };
    });

    const pending = new Map<number, Promise<{ idx: number; result: IteratorResult<T> }>>();

    function advance(idx: number) {
        const entry = iterators[idx];
        if (entry.done) return;
        pending.set(idx, entry.iter.next().then(result => ({ idx, result })));
    }

    for (let i = 0; i < iterators.length; i++) advance(i);

    while (pending.size > 0) {
        const { idx, result } = await Promise.race(pending.values());
        pending.delete(idx);

        if (result.done) {
            iterators[idx].done = true;
        } else {
            yield result.value;
            advance(idx);
        }
    }
}
