import type { B64Hash } from '@hyper-hyper-space/hhs3_crypto';
import type { Dag, Header } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';
import type { TopicChannel } from '@hyper-hyper-space/hhs3_mesh';

import { encode } from './codec.js';
import type {
    HeaderRequest,
    HeaderResponseMeta,
    HeaderBatch,
    PayloadRequest,
    PayloadResponseMeta,
    PayloadMsg,
    SyncMsg,
} from './protocol.js';
import { TRACE_SYNC, trace } from './trace.js';

const HEADER_BATCH_SIZE    = 128;
const PAYLOAD_BURST_SIZE   = 1024;
const TICK_INTERVAL_MS     = 100;

type QueuedResponse =
    | { kind: 'headers'; request: HeaderRequest; headers: Array<{ hash: B64Hash; header: Header }>; complete: boolean }
    | { kind: 'payloads'; requestId: string; hashes: B64Hash[] };

export interface DagProvider {
    handleMessage(msg: SyncMsg, channel: TopicChannel): void;
    cancelRequest(requestId: string, peerKey: string): void;
    cancelPeer(peerKey: string): void;
    destroy(): void;
}

// What a peer's answer under way holds in `activeResp`.
type Slot = { requestId: string; cancel: () => void };

// `mayServe` says whether a peer may be sent data. It is asked before every
// batch of an answer, the first included, and a no drops everything queued for
// that peer. Without it every peer may.
export function createDagProvider(dag: Dag, mayServe?: (peerKey: string) => true | Promise<boolean>): DagProvider {

    const peerQueues  = new Map<string, QueuedResponse[]>();
    const activeResp  = new Map<string, Slot>();

    async function served(pk: string): Promise<boolean> {
        if (mayServe === undefined) return true;
        try {
            return (await mayServe(pk)) === true;
        } catch {
            return false;
        }
    }

    // A stream that ended gives up the peer's slot, if it still holds it, and
    // lets the next queued answer start.
    function release(pk: string, slot: Slot, channel: TopicChannel) {
        if (activeResp.get(pk) === slot) activeResp.delete(pk);
        if (!activeResp.has(pk)) startNext(pk, channel);
    }
    let tickTimer: ReturnType<typeof setInterval> | undefined;

    function peerKey(ch: TopicChannel): string {
        return `${ch.peerId}@${ch.endpoint}`;
    }

    function ensureTick() {
        if (tickTimer === undefined) {
            tickTimer = setInterval(tick, TICK_INTERVAL_MS);
        }
    }

    function stopTickIfIdle() {
        if (peerQueues.size === 0 && activeResp.size === 0 && tickTimer !== undefined) {
            clearInterval(tickTimer);
            tickTimer = undefined;
        }
    }

    // --- header request handling ---

    async function handleHeaderRequest(req: HeaderRequest, channel: TopicChannel) {
        const pk = peerKey(channel);

        const limits = new Set(req.limits);
        const visited = new Set<B64Hash>();
        const collected: Array<{ hash: B64Hash; header: Header }> = [];

        const queue: B64Hash[] = [...req.start];
        let complete = true;

        while (queue.length > 0 && collected.length < req.maxHeaders) {
            const h = queue.shift()!;
            if (visited.has(h) || limits.has(h)) continue;
            visited.add(h);

            const header = await dag.loadHeader(h);
            if (header === undefined) continue;

            collected.push({ hash: h, header });

            if (collected.length >= req.maxHeaders) {
                complete = false;
                break;
            }

            for (const prev of json.fromSet(header.prevEntryHashes)) {
                if (!visited.has(prev) && !limits.has(prev)) {
                    queue.push(prev);
                }
            }
        }

        const resp: QueuedResponse = {
            kind: 'headers',
            request: req,
            headers: collected,
            complete,
        };

        enqueue(pk, resp, channel);
    }

    // --- payload request handling ---

    function handlePayloadRequest(req: PayloadRequest, channel: TopicChannel) {
        const pk = peerKey(channel);
        const resp: QueuedResponse = {
            kind: 'payloads',
            requestId: req.requestId,
            hashes: [...req.hashes],
        };
        enqueue(pk, resp, channel);
    }

    // --- queue management ---

    function enqueue(pk: string, resp: QueuedResponse, channel: TopicChannel) {
        let q = peerQueues.get(pk);
        if (q === undefined) {
            q = [];
            peerQueues.set(pk, q);
        }
        q.push(resp);

        if (!activeResp.has(pk)) {
            startNext(pk, channel);
        }
        ensureTick();
    }

    function startNext(pk: string, channel: TopicChannel) {
        const q = peerQueues.get(pk);
        if (q === undefined || q.length === 0) {
            peerQueues.delete(pk);
            activeResp.delete(pk);
            stopTickIfIdle();
            return;
        }

        const resp = q.shift()!;
        if (q.length === 0) peerQueues.delete(pk);

        const start = () => {
            if (resp.kind === 'headers') {
                startHeaderStream(pk, resp, channel);
            } else {
                startPayloadStream(pk, resp, channel);
            }
        };
        if (mayServe === undefined) {
            start();
            return;
        }

        // An answer may have waited in the queue, or follow a header answer
        // with no request of its own (auto-payloads): ask before its first
        // batch. The slot is held meanwhile, so a request that arrives now
        // queues behind this one.
        let cancelled = false;
        const slot: Slot = {
            requestId: resp.kind === 'headers' ? resp.request.requestId : resp.requestId,
            cancel: () => { cancelled = true; },
        };
        activeResp.set(pk, slot);
        void served(pk).then((ok) => {
            if (cancelled) {
                release(pk, slot, channel);
            } else if (!ok) {
                cancelPeer(pk);
            } else {
                start();
            }
        });
    }

    // --- header streaming ---

    function startHeaderStream(
        pk: string,
        resp: QueuedResponse & { kind: 'headers' },
        channel: TopicChannel,
    ) {
        const { request, headers, complete } = resp;
        let autoPayloadHashes: B64Hash[] | undefined;

        if (TRACE_SYNC) {
            trace('sync.provide headers', {
                dag: request.dagId,
                peer: pk,
                n: headers.length,
            });
        }

        if (request.autoPayload && complete) {
            autoPayloadHashes = headers.map(h => h.hash).reverse();
        }

        const meta: HeaderResponseMeta = {
            type: 'header-response-meta',
            requestId: request.requestId,
            headerCount: headers.length,
            complete,
            payloadCount: autoPayloadHashes?.length ?? 0,
        };
        trySend(channel, meta);

        let offset = 0;
        let seq = 0;
        let cancelled = false;

        function sendNextBatch(): boolean {
            if (cancelled || !channel.open) return true;

            if (offset < headers.length) {
                const batch = headers.slice(offset, offset + HEADER_BATCH_SIZE);
                offset += batch.length;
                const msg: HeaderBatch = {
                    type: 'header-batch',
                    requestId: request.requestId,
                    sequence: seq++,
                    headers: batch,
                };
                trySend(channel, msg);
                if (offset < headers.length) return false;
                // last batch sent: fall through and enqueue auto-payloads
            }

            if (autoPayloadHashes !== undefined && autoPayloadHashes.length > 0) {
                const payloadResp: QueuedResponse = {
                    kind: 'payloads',
                    requestId: request.requestId,
                    hashes: autoPayloadHashes,
                };
                autoPayloadHashes = undefined;

                const q = peerQueues.get(pk) ?? [];
                q.unshift(payloadResp);
                peerQueues.set(pk, q);
            }

            return true;
        }

        const slot: Slot = {
            requestId: request.requestId,
            cancel: () => { cancelled = true; },
        };
        activeResp.set(pk, slot);

        // The first batch goes out now: startNext has just asked.
        if (sendNextBatch()) {
            release(pk, slot, channel);
            return;
        }

        // The remaining batches go out one per tick, each after asking again.
        // A tick does nothing while the previous one is still asking.
        let asking = false;
        const interval = setInterval(async () => {
            if (asking) return;
            if (mayServe !== undefined && !cancelled && channel.open) {
                asking = true;
                const ok = await served(pk);
                asking = false;
                if (!ok && !cancelled) {
                    clearInterval(interval);
                    cancelPeer(pk);
                    return;
                }
            }
            // sendNextBatch answers true, and sends nothing, once cancelled or closed.
            if (sendNextBatch()) {
                clearInterval(interval);
                release(pk, slot, channel);
            }
        }, TICK_INTERVAL_MS);
    }

    // --- payload streaming ---

    function startPayloadStream(
        pk: string,
        resp: QueuedResponse & { kind: 'payloads' },
        channel: TopicChannel,
    ) {
        const { requestId, hashes } = resp;

        if (TRACE_SYNC) {
            trace('sync.provide payloads', { peer: pk, n: hashes.length });
        }

        const meta: PayloadResponseMeta = {
            type: 'payload-response-meta',
            requestId,
            payloadCount: hashes.length,
        };
        trySend(channel, meta);

        let idx = 0;
        let seq = 0;
        let cancelled = false;

        async function sendBurst(): Promise<boolean> {
            let sent = 0;
            while (sent < PAYLOAD_BURST_SIZE && idx < hashes.length) {
                if (cancelled || !channel.open) return true;

                const hash = hashes[idx++];
                const entry = await dag.loadEntry(hash);
                if (entry === undefined) return true;

                const msg: PayloadMsg = {
                    type: 'payload-msg',
                    requestId,
                    sequence: seq++,
                    hash,
                    payload: entry.payload,
                };
                trySend(channel, msg);
                sent++;
            }
            return idx >= hashes.length;
        }

        const slot: Slot = {
            requestId,
            cancel: () => { cancelled = true; },
        };
        activeResp.set(pk, slot);

        // The first burst goes out now: startNext has just asked.
        void sendBurst().then(done => {
            if (done || cancelled) {
                release(pk, slot, channel);
                return;
            }
            // The remaining bursts go out one per tick, each after asking
            // again. A tick does nothing while the previous one is still
            // asking or sending.
            let busy = false;
            const interval = setInterval(async () => {
                if (busy) return;
                busy = true;
                try {
                    if (mayServe !== undefined && !cancelled && channel.open && !(await served(pk))) {
                        if (!cancelled) {
                            clearInterval(interval);
                            cancelPeer(pk);
                            return;
                        }
                    }
                    const finished = await sendBurst();
                    if (finished || cancelled) {
                        clearInterval(interval);
                        release(pk, slot, channel);
                    }
                } finally {
                    busy = false;
                }
            }, TICK_INTERVAL_MS);
        });
    }

    // --- util ---

    function trySend(channel: TopicChannel, msg: SyncMsg) {
        try {
            if (channel.open) {
                channel.send(encode(msg));
            }
        } catch {
            // channel closed, will be cleaned up by the caller
        }
    }

    function tick() {
        // The per-stream intervals handle their own pacing.
        // The global tick is only used for idle detection.
        if (activeResp.size === 0 && peerQueues.size === 0) {
            stopTickIfIdle();
        }
    }

    // --- public API ---

    function handleMessage(msg: SyncMsg, channel: TopicChannel) {
        switch (msg.type) {
            case 'header-request':
                handleHeaderRequest(msg, channel);
                break;
            case 'payload-request':
                handlePayloadRequest(msg, channel);
                break;
            case 'cancel-request': {
                const pk = peerKey(channel);
                cancelRequest(msg.requestId, pk);
                break;
            }
            default:
                break;
        }
    }

    function cancelRequest(requestId: string, pk: string) {
        const active = activeResp.get(pk);
        if (active?.requestId === requestId) {
            active.cancel();
        }
        const q = peerQueues.get(pk);
        if (q !== undefined) {
            const idx = q.findIndex(r => {
                if (r.kind === 'headers') return r.request.requestId === requestId;
                return r.requestId === requestId;
            });
            if (idx !== -1) q.splice(idx, 1);
            if (q.length === 0) peerQueues.delete(pk);
        }
    }

    function destroy() {
        if (tickTimer !== undefined) {
            clearInterval(tickTimer);
            tickTimer = undefined;
        }
        for (const active of activeResp.values()) {
            active.cancel();
        }
        activeResp.clear();
        peerQueues.clear();
    }

    function cancelPeer(pk: string) {
        const active = activeResp.get(pk);
        if (active !== undefined) {
            active.cancel();
            activeResp.delete(pk);
        }
        peerQueues.delete(pk);
        stopTickIfIdle();
    }

    return { handleMessage, cancelRequest, cancelPeer, destroy };
}
