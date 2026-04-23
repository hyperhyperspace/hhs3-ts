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

export type SendResult = 'sent' | 'closed' | 'error';

export type PeerIssue =
    | 'send-closed'
    | 'send-error'
    | 'timeout'
    | 'validation-failed'
    | 'decode-failed';

export interface SyncSessionDiagnostics {
    activePeerCount: number;
    pendingHeaderRequests: number;
    pendingPayloadRequests: number;
}

export type SyncTarget = {
    dagId: B64Hash;
    dag: Dag;
    rObject: RObject;
    hashSuite: HashSuite;
};

export interface SyncSession {
    destroy(): void;
    onPeerIssue(cb: (peerKey: string, issue: PeerIssue) => void): void;
    getDiagnostics(): SyncSessionDiagnostics;
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
    const issueCallbacks: Array<(peerKey: string, issue: PeerIssue) => void> = [];

    function reportIssue(peerKey: string, issue: PeerIssue) {
        for (const cb of issueCallbacks) cb(peerKey, issue);
    }

    function getPeers(): PeerHandle[] {
        return [...activePeers.values()];
    }

    function sendTo(peer: PeerHandle, msg: SyncMsg): SendResult {
        if (!peer.channel.open) return 'closed';
        try {
            peer.channel.send(encode(msg));
            return 'sent';
        } catch {
            return 'error';
        }
    }

    function sendToWithReport(peer: PeerHandle, msg: SyncMsg): SendResult {
        const result = sendTo(peer, msg);
        if (result === 'closed') reportIssue(peer.key, 'send-closed');
        if (result === 'error')  reportIssue(peer.key, 'send-error');
        return result;
    }

    const provider: DagProvider = createDagProvider(target.dag);
    const synchronizer: DagSynchronizer = createDagSynchronizer(
        target.dagId,
        target.dag,
        target.rObject,
        target.hashSuite,
        getPeers,
        sendToWithReport,
    );

    synchronizer.onPeerIssue((peerKey, issue) => {
        reportIssue(peerKey, issue);
    });

    function cleanupPeer(key: string) {
        if (!activePeers.has(key)) return;
        activePeers.delete(key);
        provider.cancelPeer(key);
        synchronizer.removePeer(key);
    }

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
                reportIssue(key, 'decode-failed');
                return;
            }

            if (REQUEST_TYPES.has(msg.type) || msg.type === 'new-frontier') {
                provider.handleMessage(msg, sp.channel);
            }
            synchronizer.handleMessage(msg, sp.channel);
        });

        sp.channel.onClose(() => {
            cleanupPeer(key);
        });

        synchronizer.addPeer(handle);
    }

    function onPeerLeave(sp: SwarmPeer) {
        const key = peerKeyOf(sp);
        cleanupPeer(key);
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

    return {
        destroy,
        onPeerIssue: (cb) => { issueCallbacks.push(cb); },
        getDiagnostics: () => ({
            activePeerCount: activePeers.size,
            ...synchronizer.getDiagnostics(),
        }),
    };
}
