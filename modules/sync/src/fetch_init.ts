import type { B64Hash, HashSuite } from '@hyper-hyper-space/hhs3_crypto';
import { dag } from '@hyper-hyper-space/hhs3_dag';
import type { Swarm, SwarmPeer } from '@hyper-hyper-space/hhs3_mesh';
import type { Payload } from '@hyper-hyper-space/hhs3_mvt';
import { extractCreatePayloadType } from '@hyper-hyper-space/hhs3_mvt';

import { encode, decode } from './codec.js';
import type { InitRequest, SyncMsg } from './protocol.js';
import { TRACE_SYNC, trace } from './trace.js';

const DEFAULT_TIMEOUT_MS = 10_000;
// Discovery plus one RTC connect. The connect budget alone is 20s.
const DEFAULT_PEER_TIMEOUT_MS = 25_000;
const RETRY_INTERVAL_MS = 200;

export function fetchInit(
    objectId: B64Hash,
    swarms: Swarm[],
    hashSuite: HashSuite,
    timeoutMs: number = DEFAULT_TIMEOUT_MS,
    peerTimeoutMs: number = DEFAULT_PEER_TIMEOUT_MS,
): Promise<Payload> {

    return new Promise<Payload>((resolve, reject) => {
        let settled = false;
        let initStarted = false;
        const retryTimers: ReturnType<typeof setInterval>[] = [];
        const started = Date.now();
        let initTimer: ReturnType<typeof setTimeout> | undefined;
        let peerTimer: ReturnType<typeof setTimeout> | undefined;

        // The init clock starts when the first peer is handled. Until then only
        // the peer budget is running, so a slow handshake is not a missed init.
        function startInitBudget() {
            if (initStarted || settled) return;
            initStarted = true;
            if (peerTimer !== undefined) {
                clearTimeout(peerTimer);
                peerTimer = undefined;
            }
            initTimer = setTimeout(() => {
                if (settled) return;
                settled = true;
                cleanup();
                if (TRACE_SYNC) {
                    trace('sync.init timeout', { objectId, ms: Date.now() - started });
                }
                reject(new Error(`fetchInit timed out after ${timeoutMs}ms for object ${objectId}`));
            }, timeoutMs);
        }

        function cleanup() {
            if (initTimer !== undefined) clearTimeout(initTimer);
            if (peerTimer !== undefined) clearTimeout(peerTimer);
            for (const t of retryTimers) clearInterval(t);
        }

        function finish(createPayload: Payload) {
            if (settled) return;
            settled = true;
            cleanup();
            resolve(createPayload);
        }

        function fail(reason: string) {
            if (settled) return;
            settled = true;
            cleanup();
            if (TRACE_SYNC) {
                trace('sync.init fail', { objectId, ms: Date.now() - started, err: reason });
            }
            reject(new Error(reason));
        }

        const seen = new Set<string>();

        function handlePeer(sp: SwarmPeer) {
            if (settled) return;
            const peer = `${sp.keyId}@${sp.endpoint}`;
            if (seen.has(peer)) return;
            seen.add(peer);
            startInitBudget();

            const request: InitRequest = { type: 'init-request', objectId };
            let attempt = 0;

            function sendRequest() {
                if (settled || !sp.channel.open) return;
                attempt++;
                if (TRACE_SYNC) {
                    trace('sync.init send', { objectId, peer, attempt });
                }
                try { sp.channel.send(encode(request)); } catch { /* channel may close */ }
            }

            sendRequest();

            const retryTimer = setInterval(sendRequest, RETRY_INTERVAL_MS);
            retryTimers.push(retryTimer);

            sp.channel.onMessage((data: Uint8Array) => {
                if (settled) return;

                let msg: SyncMsg;
                try {
                    msg = decode(data);
                } catch {
                    return;
                }

                if (msg.type !== 'init-response' || msg.objectId !== objectId) return;

                const entry = dag.createEntry(msg.createPayload, {}, dag.position(), hashSuite);
                if (entry.hash !== objectId) {
                    fail(`Creation payload hash mismatch: expected ${objectId}, got ${entry.hash}`);
                    return;
                }

                const payloadType = extractCreatePayloadType(msg.createPayload);
                if (payloadType === undefined) {
                    fail('Creation payload missing type');
                    return;
                }

                if (TRACE_SYNC) {
                    trace('sync.init recv', { objectId, peer, ms: Date.now() - started });
                }
                finish(msg.createPayload);
            });
        }

        const initial: SwarmPeer[] = [];
        for (const swarm of swarms) {
            swarm.onPeerJoin(handlePeer);
            for (const existing of swarm.peers()) initial.push(existing);
        }
        if (initial.length === 0 && !initStarted) {
            peerTimer = setTimeout(() => {
                if (settled) return;
                settled = true;
                cleanup();
                if (TRACE_SYNC) {
                    trace('sync.init no-peer', { objectId, ms: Date.now() - started });
                }
                reject(new Error(`no peer for ${objectId}`));
            }, peerTimeoutMs);
        }
        for (const existing of initial) handlePeer(existing);
    });
}
