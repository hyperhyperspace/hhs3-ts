import type { B64Hash, HashSuite } from '@hyper-hyper-space/hhs3_crypto';
import type { Dag } from '@hyper-hyper-space/hhs3_dag';
import type { Swarm, SwarmPeer } from '@hyper-hyper-space/hhs3_mesh';
import type { RObject, RContext } from '@hyper-hyper-space/hhs3_mvt';
import { extractCreatePayloadType } from '@hyper-hyper-space/hhs3_mvt';

import { decode, encode } from './codec.js';
import { createDagProvider } from './provider.js';
import { createDagSynchronizer } from './synchronizer.js';
import type { DagProvider } from './provider.js';
import type { DagSynchronizer } from './synchronizer.js';
import type { SyncMsg, InitResponse } from './protocol.js';
import { TRACE_SYNC, trace } from './trace.js';

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
    ctx?: RContext;
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
        if (TRACE_SYNC) trace('sync.issue', { peer: peerKey, issue });
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
        target.ctx,
    );

    synchronizer.onPeerIssue((peerKey, issue) => {
        reportIssue(peerKey, issue);
    });

    function cleanupPeer(key: string) {
        if (!activePeers.has(key)) return;
        activePeers.delete(key);
        if (TRACE_SYNC) trace('sync.peer remove', { dag: target.dagId, peer: key });
        provider.cancelPeer(key);
        synchronizer.removePeer(key);
    }

    function peerKeyOf(sp: SwarmPeer): string {
        return `${sp.keyId}@${sp.endpoint}`;
    }

    async function handleInitRequest(sp: SwarmPeer, key: string): Promise<void> {
        const rootEntry = await target.dag.loadEntry(target.dagId);
        if (!activePeers.has(key) || rootEntry === undefined) return;

        const payloadType = extractCreatePayloadType(rootEntry.payload);
        if (payloadType !== target.rObject.getType()) {
            reportIssue(key, 'validation-failed');
            return;
        }
        const resp: InitResponse = {
            type: 'init-response',
            objectId: target.dagId,
            createPayload: rootEntry.payload,
        };
        if (TRACE_SYNC) {
            trace('sync.init send', { objectId: target.dagId, peer: key });
        }
        if (sp.channel.open) sp.channel.send(encode(resp));
    }

    function onPeerJoin(sp: SwarmPeer) {
        const key = peerKeyOf(sp);
        if (activePeers.has(key)) return;

        const handle: PeerHandle = { key, channel: sp.channel };
        activePeers.set(key, handle);
        if (TRACE_SYNC) trace('sync.peer add', { dag: target.dagId, peer: key });

        // Per-peer inbound FIFO: message N fully settles before N+1 starts.
        // Isolates the auto-payload race where payload frames arrived during
        // handleHeaderBatch's loadHeader awaits and were dropped.
        let chain: Promise<void> = Promise.resolve();

        async function processMessage(data: Uint8Array): Promise<void> {
            if (!activePeers.has(key)) return;

            let msg: SyncMsg;
            try {
                msg = decode(data);
            } catch {
                reportIssue(key, 'decode-failed');
                return;
            }

            if (msg.type === 'init-request' && msg.objectId === target.dagId) {
                await handleInitRequest(sp, key);
                return;
            }
            if (REQUEST_TYPES.has(msg.type) || msg.type === 'new-frontier') {
                provider.handleMessage(msg, sp.channel);
            }
            await synchronizer.handleMessage(msg, sp.channel);
        }

        sp.channel.onMessage((data: Uint8Array) => {
            chain = chain.then(() => processMessage(data)).catch(() => { /* isolate */ });
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
