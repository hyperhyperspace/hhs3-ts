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

export function createDagProvider(dag: Dag): DagProvider {

    const peerQueues  = new Map<string, QueuedResponse[]>();
    const activeResp  = new Map<string, { requestId: string; cancel: () => void }>();
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

        if (resp.kind === 'headers') {
            startHeaderStream(pk, resp, channel);
        } else {
            startPayloadStream(pk, resp, channel);
        }
    }

    // --- header streaming ---

    function startHeaderStream(
        pk: string,
        resp: QueuedResponse & { kind: 'headers' },
        channel: TopicChannel,
    ) {
        const { request, headers, complete } = resp;
        let autoPayloadHashes: B64Hash[] | undefined;

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
                return false;
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

        activeResp.set(pk, {
            requestId: request.requestId,
            cancel: () => { cancelled = true; },
        });

        // try to send the first batch immediately
        const done = sendNextBatch();
        if (done) {
            activeResp.delete(pk);
            startNext(pk, channel);
            return;
        }

        // register tick handler for remaining batches
        const tickHandler = () => {
            if (cancelled || !channel.open) {
                activeResp.delete(pk);
                startNext(pk, channel);
                return;
            }
            const finished = sendNextBatch();
            if (finished) {
                activeResp.delete(pk);
                startNext(pk, channel);
            }
        };

        activeResp.set(pk, {
            requestId: request.requestId,
            cancel: () => {
                cancelled = true;
            },
        });

        // Replace tick with per-peer drainer
        const interval = setInterval(() => {
            tickHandler();
            if (cancelled || !activeResp.has(pk) || activeResp.get(pk)?.requestId !== request.requestId) {
                clearInterval(interval);
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

        activeResp.set(pk, {
            requestId,
            cancel: () => { cancelled = true; },
        });

        // Fire the first burst immediately; only use interval for subsequent bursts
        sendBurst().then(done => {
            if (done || cancelled) {
                activeResp.delete(pk);
                startNext(pk, channel);
                return;
            }
            const interval = setInterval(async () => {
                const finished = await sendBurst();
                if (finished || cancelled) {
                    clearInterval(interval);
                    activeResp.delete(pk);
                    startNext(pk, channel);
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
