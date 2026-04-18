import type { B64Hash, HashSuite } from '@hyper-hyper-space/hhs3_crypto';
import type { Dag } from '@hyper-hyper-space/hhs3_dag';
import type { Swarm, SwarmPeer } from '@hyper-hyper-space/hhs3_mesh';
import type { RObject } from '@hyper-hyper-space/hhs3_mvt';

import { decode, encode } from './codec.js';
import { createDagProvider } from './provider.js';
import { createDagSynchronizer } from './synchronizer.js';
import type { DagProvider } from './provider.js';
import type { DagSynchronizer } from './synchronizer.js';
import type { SyncMsg } from './protocol.js';

export type SyncTarget = {
    dagId: B64Hash;
    dag: Dag;
    rObject: RObject;
    hashSuite: HashSuite;
};

export interface SyncSession {
    destroy(): void;
}

type PeerHandle = {
    key: string;
    channel: import('@hyper-hyper-space/hhs3_mesh').TopicChannel;
};

const REQUEST_TYPES = new Set([
    'header-request', 'payload-request', 'cancel-request',
]);

export function createSyncSession(target: SyncTarget, swarms: Swarm[]): SyncSession {

    const activePeers = new Map<string, PeerHandle>();

    function getPeers(): PeerHandle[] {
        return [...activePeers.values()];
    }

    function sendTo(peer: PeerHandle, msg: SyncMsg) {
        try {
            if (peer.channel.open) {
                peer.channel.send(encode(msg));
            }
        } catch {
            // channel closed
        }
    }

    const provider: DagProvider = createDagProvider(target.dag);
    const synchronizer: DagSynchronizer = createDagSynchronizer(
        target.dagId,
        target.dag,
        target.rObject,
        target.hashSuite,
        getPeers,
        sendTo,
    );

    function peerKeyOf(sp: SwarmPeer): string {
        return `${sp.keyId}@${sp.endpoint}`;
    }

    function onPeerJoin(sp: SwarmPeer) {
        const key = peerKeyOf(sp);
        if (activePeers.has(key)) return;

        const handle: PeerHandle = { key, channel: sp.channel };
        activePeers.set(key, handle);

        sp.channel.onMessage((data: Uint8Array) => {
            let msg: SyncMsg;
            try {
                msg = decode(data);
            } catch {
                return;
            }

            if (REQUEST_TYPES.has(msg.type) || msg.type === 'new-frontier') {
                provider.handleMessage(msg, sp.channel);
            }
            synchronizer.handleMessage(msg, sp.channel);
        });

        sp.channel.onClose(() => {
            activePeers.delete(key);
            provider.cancelPeer(key);
            synchronizer.removePeer(key);
        });

        synchronizer.addPeer(handle);
    }

    function onPeerLeave(sp: SwarmPeer) {
        const key = peerKeyOf(sp);
        activePeers.delete(key);
        synchronizer.removePeer(key);
    }

    for (const swarm of swarms) {
        for (const existing of swarm.peers()) {
            onPeerJoin(existing);
        }
        swarm.onPeerJoin(onPeerJoin);
        swarm.onPeerLeave(onPeerLeave);
    }

    function destroy() {
        synchronizer.destroy();
        provider.destroy();
        activePeers.clear();
    }

    return { destroy };
}
