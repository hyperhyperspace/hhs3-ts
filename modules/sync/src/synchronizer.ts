import type { B64Hash, HashSuite } from '@hyper-hyper-space/hhs3_crypto';
import { random } from '@hyper-hyper-space/hhs3_crypto';
import type { Dag, Header } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';
import type { TopicChannel } from '@hyper-hyper-space/hhs3_mesh';
import type { RObject, Version } from '@hyper-hyper-space/hhs3_mvt';

import { stringToUint8Array } from '@hyper-hyper-space/hhs3_crypto';
import type {
    HeaderRequest,
    HeaderResponseMeta,
    HeaderBatch,
    PayloadRequest,
    PayloadResponseMeta,
    PayloadMsg,
    NewFrontierMsg,
    SyncMsg,
} from './protocol.js';

const MAX_HEADERS_PER_REQUEST = 1024;
const MAX_PAYLOAD_REQUESTS_PER_PEER = 2;
const PAYLOAD_CHUNK_SIZE = 64;
const REQUEST_TIMEOUT_MS = 30_000;

type PeerHandle = {
    key: string;
    channel: TopicChannel;
};

type HeaderRequestState = {
    requestId: string;
    peer: PeerHandle;
    startHashes: B64Hash[];
    receivedHashes: Set<B64Hash>;
    expectedHeaderCount: number | undefined;
    receivedHeaderCount: number;
    nextSequence: number;
    complete: boolean;
    autoPayload: boolean;
    expectedPayloadCount: number | undefined;
    timeout: ReturnType<typeof setTimeout>;
};

type PayloadRequestState = {
    requestId: string;
    peer: PeerHandle;
    requestedHashes: Set<B64Hash>;
    expectedPayloadCount: number | undefined;
    receivedPayloadCount: number;
    nextSequence: number;
    timeout: ReturnType<typeof setTimeout>;
};

export interface DagSynchronizer {
    handleMessage(msg: SyncMsg, channel: TopicChannel): void;
    addPeer(peer: PeerHandle): void;
    removePeer(peerKey: string): void;
    broadcastFrontier(): Promise<void>;
    destroy(): void;
}

export function createDagSynchronizer(
    dagId: B64Hash,
    dag: Dag,
    rObject: RObject,
    hashSuite: HashSuite,
    getPeers: () => PeerHandle[],
    sendTo: (peer: PeerHandle, msg: SyncMsg) => void,
): DagSynchronizer {

    // --- accumulative state ---

    const peerFrontiers         = new Map<string, Set<B64Hash>>();
    const peerDiscoveredFrontier = new Map<string, Set<B64Hash>>();

    const discoveredHeaders  = new Map<B64Hash, Header>();
    const readyToApply       = new Map<B64Hash, Header>();
    const requestedPayloads  = new Set<B64Hash>();
    const receivedPayloads   = new Map<B64Hash, json.Literal>();
    const appliedEntries     = new Set<B64Hash>();
    const hashSourcePeer     = new Map<B64Hash, string>();

    const pendingHeaderRequests  = new Map<string, HeaderRequestState>();
    const pendingPayloadRequests = new Map<string, PayloadRequestState>();

    const suspectPeers = new Set<string>();

    let autoPayloadRequestId: string | undefined;

    let destroyed = false;

    const onGrowth = () => {
        if (destroyed) return;
        broadcastFrontier();
        attemptWork();
    };
    dag.addListener(onGrowth);

    let workInProgress = false;
    let workNeeded = false;

    // --- request ID generation ---

    function newRequestId(): string {
        const bytes = random.getBytes(8);
        return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
    }

    // --- gossip ---

    async function broadcastFrontier() {
        const frontier = await dag.getFrontier();
        const msg: NewFrontierMsg = {
            type: 'new-frontier',
            dagId,
            frontier: [...frontier],
        };
        for (const peer of getPeers()) {
            sendTo(peer, msg);
        }
    }

    async function sendFrontierTo(peer: PeerHandle) {
        const frontier = await dag.getFrontier();
        const msg: NewFrontierMsg = {
            type: 'new-frontier',
            dagId,
            frontier: [...frontier],
        };
        sendTo(peer, msg);
    }

    async function handleNewFrontier(msg: NewFrontierMsg, peerKey: string) {
        peerFrontiers.set(peerKey, new Set(msg.frontier));

        for (const h of msg.frontier) {
            if (discoveredHeaders.has(h) || readyToApply.has(h) || appliedEntries.has(h)) {
                addToPeerDiscoveredFrontier(peerKey, h);
            }
        }

        await attemptWork();

        // Push-back: if the peer's frontier is entirely known to us (i.e. the
        // peer appears to be behind), send our own frontier back so it can
        // discover it is behind and start fetching. Skip when the frontiers are
        // identical to avoid a ping-pong loop.
        const localFrontier = await dag.getFrontier();
        const remoteFrontier = new Set(msg.frontier);

        const sameSize = localFrontier.size === remoteFrontier.size;
        let allRemoteKnown = true;
        let allMatch = sameSize;

        for (const h of remoteFrontier) {
            if (!localFrontier.has(h)) {
                const local = await dag.loadEntry(h);
                if (local === undefined) {
                    allRemoteKnown = false;
                    break;
                }
                allMatch = false;
            }
        }

        if (allRemoteKnown && !allMatch) {
            const peer = getPeers().find(p => p.key === peerKey);
            if (peer !== undefined) {
                sendFrontierTo(peer);
            }
        }
    }

    function addToPeerDiscoveredFrontier(peerKey: string, hash: B64Hash) {
        let df = peerDiscoveredFrontier.get(peerKey);
        if (df === undefined) {
            df = new Set();
            peerDiscoveredFrontier.set(peerKey, df);
        }
        df.add(hash);
    }

    // --- core: attemptWork() ---

    async function attemptWork() {
        if (destroyed) return;

        if (workInProgress) {
            workNeeded = true;
            return;
        }

        workInProgress = true;
        workNeeded = true;

        while (workNeeded) {
            workNeeded = false;
            try {
                await attemptWorkOnce();
            } catch (_e) {
                // prevent a single error from killing the work loop
            }
        }

        workInProgress = false;
    }

    async function attemptWorkOnce() {
        // Step 1-2: issue header requests for unresolved prevs and unknown frontier hashes
        await dispatchHeaderRequests();

        // Step 3: dispatch payload requests if there are discovered headers awaiting payloads
        if (discoveredHeaders.size > 0) {
            dispatchPayloads();
        }

        // Step 4-5: run validation loop, re-dispatch if progress was made
        if (readyToApply.size > 0) {
            const progress = await runValidationLoop();
            if (progress) {
                if (discoveredHeaders.size > 0) {
                    dispatchPayloads();
                }
                if (readyToApply.size > 0) {
                    const more = await runValidationLoop();
                    if (more) {
                        workNeeded = true;
                    }
                }
            }
        }
    }

    // --- header dispatch ---

    async function dispatchHeaderRequests() {
        const localFrontier = await dag.getFrontier();

        // Collect unresolved prevs: hashes referenced by discoveredHeaders but not in
        // discoveredHeaders, readyToApply, appliedEntries, or local DAG
        const unresolvedPrevs = new Set<B64Hash>();
        for (const [_hash, header] of discoveredHeaders) {
            for (const prev of json.fromSet(header.prevEntryHashes)) {
                if (!discoveredHeaders.has(prev) && !readyToApply.has(prev)
                    && !appliedEntries.has(prev) && !requestedPayloads.has(prev)) {
                    const local = await dag.loadEntry(prev);
                    if (local === undefined) {
                        unresolvedPrevs.add(prev);
                    }
                }
            }
        }

        // Also check peer frontiers for unknown hashes
        const unknownFrontierHashes = new Set<B64Hash>();
        for (const [_peerKey, frontier] of peerFrontiers) {
            for (const h of frontier) {
                if (!localFrontier.has(h) && !discoveredHeaders.has(h)
                    && !readyToApply.has(h) && !appliedEntries.has(h)) {
                    const local = await dag.loadEntry(h);
                    if (local === undefined) {
                        unknownFrontierHashes.add(h);
                    }
                }
            }
        }

        // Merge: everything we need to fetch headers for
        const needed = new Set([...unresolvedPrevs, ...unknownFrontierHashes]);
        if (needed.size === 0) return;

        // Filter out hashes that are already being fetched by an active header request
        const activelyFetching = new Set<B64Hash>();
        for (const req of pendingHeaderRequests.values()) {
            for (const h of req.startHashes) {
                activelyFetching.add(h);
            }
        }

        // Group needed hashes by which peer can serve them
        const peersWithSlots = getPeers().filter(p => !suspectPeers.has(p.key));

        for (const peer of peersWithSlots) {
            const peerFrontier = peerFrontiers.get(peer.key);
            if (peerFrontier === undefined) continue;

            // Already has a header request in flight to this peer? Skip.
            let hasActiveRequest = false;
            for (const req of pendingHeaderRequests.values()) {
                if (req.peer.key === peer.key) {
                    hasActiveRequest = true;
                    break;
                }
            }
            if (hasActiveRequest) continue;

            // Collect hashes from this peer's frontier that we need
            const startHashes: B64Hash[] = [];
            for (const h of needed) {
                if (!activelyFetching.has(h)) {
                    startHashes.push(h);
                }
            }

            // Also add unknown frontier hashes that this peer specifically announced
            for (const h of peerFrontier) {
                if (unknownFrontierHashes.has(h) && !activelyFetching.has(h)
                    && !startHashes.includes(h)) {
                    startHashes.push(h);
                }
            }

            if (startHashes.length === 0) continue;

            const useAutoPayload = autoPayloadRequestId === undefined;
            const requestId = newRequestId();

            const req: HeaderRequest = {
                type: 'header-request',
                requestId,
                dagId,
                start: startHashes,
                limits: [...localFrontier],
                maxHeaders: MAX_HEADERS_PER_REQUEST,
                autoPayload: useAutoPayload,
            };

            const timeout = setTimeout(() => {
                handleHeaderTimeout(requestId);
            }, REQUEST_TIMEOUT_MS);

            pendingHeaderRequests.set(requestId, {
                requestId,
                peer,
                startHashes,
                receivedHashes: new Set(),
                expectedHeaderCount: undefined,
                receivedHeaderCount: 0,
                nextSequence: 0,
                complete: false,
                autoPayload: useAutoPayload,
                expectedPayloadCount: undefined,
                timeout,
            });

            if (useAutoPayload) {
                autoPayloadRequestId = requestId;
            }

            for (const h of startHashes) {
                activelyFetching.add(h);
            }

            sendTo(peer, req);
        }
    }

    // --- header response handling ---

    function handleHeaderResponseMeta(msg: HeaderResponseMeta) {
        const state = pendingHeaderRequests.get(msg.requestId);
        if (state === undefined) return;

        if (msg.headerCount > MAX_HEADERS_PER_REQUEST) {
            failHeaderRequest(msg.requestId, 'headerCount exceeds maxHeaders');
            return;
        }

        if (msg.payloadCount > 0 && (!state.autoPayload || !msg.complete)) {
            failHeaderRequest(msg.requestId, 'payloadCount > 0 but autoPayload not set or not complete');
            return;
        }

        state.expectedHeaderCount = msg.headerCount;
        state.complete = msg.complete;
        state.expectedPayloadCount = msg.payloadCount > 0 ? msg.payloadCount : undefined;

        resetHeaderTimeout(state);

        if (msg.headerCount === 0 && msg.complete) {
            onHeaderRequestComplete(msg.requestId);
        }
    }

    async function handleHeaderBatch(msg: HeaderBatch) {
        const state = pendingHeaderRequests.get(msg.requestId);
        if (state === undefined) return;

        if (msg.sequence !== state.nextSequence) {
            failHeaderRequest(msg.requestId, `expected sequence ${state.nextSequence}, got ${msg.sequence}`);
            return;
        }
        state.nextSequence++;

        if (state.expectedHeaderCount !== undefined &&
            state.receivedHeaderCount + msg.headers.length > state.expectedHeaderCount) {
            failHeaderRequest(msg.requestId, 'received more headers than announced');
            return;
        }

        for (const item of msg.headers) {
            const computed = hashSuite.hashToB64(
                stringToUint8Array(json.toStringNormalized(item.header))
            );
            if (computed !== item.hash) {
                failHeaderRequest(msg.requestId, `hash mismatch for header: expected ${item.hash}, computed ${computed}`);
                return;
            }

            state.receivedHashes.add(item.hash);

            // Skip duplicates (another peer may have sent the same header)
            if (!discoveredHeaders.has(item.hash) && !readyToApply.has(item.hash)
                && !appliedEntries.has(item.hash)) {
                discoveredHeaders.set(item.hash, item.header);
            }
        }

        state.receivedHeaderCount += msg.headers.length;
        resetHeaderTimeout(state);

        if (state.expectedHeaderCount !== undefined &&
            state.receivedHeaderCount >= state.expectedHeaderCount) {
            onHeaderRequestComplete(msg.requestId);
        }

        await attemptWork();
    }

    async function onHeaderRequestComplete(requestId: string) {
        const state = pendingHeaderRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingHeaderRequests.delete(requestId);

        // Update peerDiscoveredFrontier for the serving peer
        for (const h of state.startHashes) {
            if (discoveredHeaders.has(h) || readyToApply.has(h) || appliedEntries.has(h)) {
                addToPeerDiscoveredFrontier(state.peer.key, h);
            }
        }

        // Handle auto-payload transition: use receivedHashes (all hashes from this
        // request's batches) rather than discoveredHeaders.keys(), because some entries
        // may have already been applied by a concurrent auto-payload stream.
        if (state.autoPayload && state.expectedPayloadCount !== undefined
            && state.expectedPayloadCount > 0) {
            const autoHashes = state.receivedHashes;

            const timeout = setTimeout(() => {
                handlePayloadTimeout(requestId);
            }, REQUEST_TIMEOUT_MS);

            pendingPayloadRequests.set(requestId, {
                requestId,
                peer: state.peer,
                requestedHashes: autoHashes,
                expectedPayloadCount: state.expectedPayloadCount,
                receivedPayloadCount: 0,
                nextSequence: 0,
                timeout,
            });

            for (const h of autoHashes) {
                requestedPayloads.add(h);
            }
        }

        if (autoPayloadRequestId === requestId) {
            autoPayloadRequestId = undefined;
        }

        await attemptWork();
    }

    function handleHeaderTimeout(requestId: string) {
        const state = pendingHeaderRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingHeaderRequests.delete(requestId);
        suspectPeers.add(state.peer.key);

        if (autoPayloadRequestId === requestId) {
            autoPayloadRequestId = undefined;
        }

        attemptWork();
    }

    function failHeaderRequest(requestId: string, _reason: string) {
        const state = pendingHeaderRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingHeaderRequests.delete(requestId);
        suspectPeers.add(state.peer.key);

        if (autoPayloadRequestId === requestId) {
            autoPayloadRequestId = undefined;
        }

        attemptWork();
    }

    // --- payload discovery and dispatch ---

    function findPayloadsForPeer(peerKey: string, maxCount: number): B64Hash[] {
        const frontier = peerDiscoveredFrontier.get(peerKey);
        if (frontier === undefined) return [];

        const eligible: B64Hash[] = [];
        const visited = new Set<B64Hash>();
        const queue: B64Hash[] = [...frontier];

        while (queue.length > 0) {
            const h = queue.pop()!;
            if (visited.has(h)) continue;
            visited.add(h);

            const header = discoveredHeaders.get(h);
            if (header === undefined) continue;

            for (const prev of json.fromSet(header.prevEntryHashes)) {
                if (discoveredHeaders.has(prev) && !visited.has(prev)) {
                    queue.push(prev);
                }
            }

            if (requestedPayloads.has(h)) continue;

            let ready = true;
            for (const prev of json.fromSet(header.prevEntryHashes)) {
                if (!appliedEntries.has(prev) && !receivedPayloads.has(prev)
                    && !requestedPayloads.has(prev)) {
                    const inDag = discoveredHeaders.has(prev) || readyToApply.has(prev);
                    if (inDag) {
                        ready = false;
                        break;
                    }
                    // If not in any discovered/ready map, it must be in the local DAG
                    // (otherwise it would be an unresolved prev and we'd still be fetching headers)
                }
            }
            if (ready) eligible.push(h);
        }

        return topoSort(eligible, discoveredHeaders).slice(0, maxCount);
    }

    function topoSort(hashes: B64Hash[], headers: Map<B64Hash, Header>): B64Hash[] {
        const set = new Set(hashes);
        const order: B64Hash[] = [];
        const visited = new Set<B64Hash>();

        function visit(h: B64Hash) {
            if (visited.has(h)) return;
            if (!set.has(h)) return;
            visited.add(h);

            const header = headers.get(h);
            if (header !== undefined) {
                for (const prev of json.fromSet(header.prevEntryHashes)) {
                    visit(prev);
                }
            }
            order.push(h);
        }

        for (const h of hashes) {
            visit(h);
        }

        return order;
    }

    function dispatchPayloads() {
        const peers = getPeers().filter(p => !suspectPeers.has(p.key));

        const peerInFlight = new Map<string, number>();
        for (const ps of pendingPayloadRequests.values()) {
            const count = peerInFlight.get(ps.peer.key) ?? 0;
            peerInFlight.set(ps.peer.key, count + 1);
        }

        for (const peer of peers) {
            const inflight = peerInFlight.get(peer.key) ?? 0;
            if (inflight >= MAX_PAYLOAD_REQUESTS_PER_PEER) continue;

            const slotsAvailable = MAX_PAYLOAD_REQUESTS_PER_PEER - inflight;
            for (let i = 0; i < slotsAvailable; i++) {
                const chunk = findPayloadsForPeer(peer.key, PAYLOAD_CHUNK_SIZE);
                if (chunk.length === 0) break;

                sendPayloadRequest(peer, chunk);
            }
        }
    }

    function sendPayloadRequest(peer: PeerHandle, hashes: B64Hash[]) {
        const requestId = newRequestId();

        const req: PayloadRequest = {
            type: 'payload-request',
            requestId,
            dagId,
            hashes,
        };

        const timeout = setTimeout(() => {
            handlePayloadTimeout(requestId);
        }, REQUEST_TIMEOUT_MS);

        pendingPayloadRequests.set(requestId, {
            requestId,
            peer,
            requestedHashes: new Set(hashes),
            expectedPayloadCount: undefined,
            receivedPayloadCount: 0,
            nextSequence: 0,
            timeout,
        });

        for (const h of hashes) {
            requestedPayloads.add(h);
        }

        sendTo(peer, req);
    }

    // --- payload response handling ---

    function handlePayloadResponseMeta(msg: PayloadResponseMeta) {
        const state = pendingPayloadRequests.get(msg.requestId);
        if (state === undefined) return;

        if (msg.payloadCount !== state.requestedHashes.size) {
            failPayloadRequest(msg.requestId, 'payloadCount mismatch');
            return;
        }

        state.expectedPayloadCount = msg.payloadCount;
        resetPayloadTimeout(state);
    }

    async function handlePayloadMsg(msg: PayloadMsg) {
        const state = pendingPayloadRequests.get(msg.requestId);
        if (state === undefined) return;

        if (msg.sequence !== state.nextSequence) {
            failPayloadRequest(msg.requestId, `expected sequence ${state.nextSequence}, got ${msg.sequence}`);
            return;
        }
        state.nextSequence++;

        if (!state.requestedHashes.has(msg.hash)) {
            failPayloadRequest(msg.requestId, `received unrequested hash: ${msg.hash}`);
            return;
        }

        if (receivedPayloads.has(msg.hash) || appliedEntries.has(msg.hash)) {
            // Already have this payload (e.g. from a concurrent auto-payload stream).
            // Skip gracefully rather than failing the whole request.
            if (state.expectedPayloadCount !== undefined &&
                state.receivedPayloadCount >= state.expectedPayloadCount) {
                clearTimeout(state.timeout);
                pendingPayloadRequests.delete(msg.requestId);
            }
            return;
        }

        if (state.expectedPayloadCount !== undefined &&
            state.receivedPayloadCount + 1 > state.expectedPayloadCount) {
            failPayloadRequest(msg.requestId, 'received more payloads than announced');
            return;
        }

        state.receivedPayloadCount++;
        hashSourcePeer.set(msg.hash, state.peer.key);
        resetPayloadTimeout(state);

        // Verify payload hash against header
        const header = discoveredHeaders.get(msg.hash) ?? readyToApply.get(msg.hash);
        if (header !== undefined) {
            const computedPayloadHash = hashSuite.hashToB64(
                stringToUint8Array(json.toStringNormalized(msg.payload))
            );
            if (computedPayloadHash === header.payloadHash) {
                // Hash verified: move from discoveredHeaders to readyToApply
                discoveredHeaders.delete(msg.hash);
                readyToApply.set(msg.hash, header);
                receivedPayloads.set(msg.hash, msg.payload);
                requestedPayloads.delete(msg.hash);
            } else {
                suspectPeers.add(state.peer.key);
                requestedPayloads.delete(msg.hash);
            }
        } else {
            receivedPayloads.set(msg.hash, msg.payload);
            requestedPayloads.delete(msg.hash);
        }

        if (state.expectedPayloadCount !== undefined &&
            state.receivedPayloadCount >= state.expectedPayloadCount) {
            clearTimeout(state.timeout);
            pendingPayloadRequests.delete(msg.requestId);
        }

        await attemptWork();
    }

    function handlePayloadTimeout(requestId: string) {
        const state = pendingPayloadRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingPayloadRequests.delete(requestId);
        suspectPeers.add(state.peer.key);

        for (const h of state.requestedHashes) {
            if (!receivedPayloads.has(h) && !appliedEntries.has(h)) {
                requestedPayloads.delete(h);
            }
        }

        attemptWork();
    }

    function failPayloadRequest(requestId: string, _reason: string) {
        const state = pendingPayloadRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingPayloadRequests.delete(requestId);
        suspectPeers.add(state.peer.key);

        for (const h of state.requestedHashes) {
            if (!receivedPayloads.has(h) && !appliedEntries.has(h)) {
                requestedPayloads.delete(h);
            }
        }

        attemptWork();
    }

    // --- validation / import loop ---

    async function runValidationLoop(): Promise<boolean> {
        const order = topoSort([...readyToApply.keys()], readyToApply);

        let anyProgress = false;
        let progress = true;

        while (progress) {
            progress = false;

            for (const hash of order) {
                if (appliedEntries.has(hash)) continue;

                const payload = receivedPayloads.get(hash);
                if (payload === undefined) continue;

                const header = readyToApply.get(hash);
                if (header === undefined) continue;

                const prevHashes = [...json.fromSet(header.prevEntryHashes)];
                let allPredecessorsReady = true;
                for (const prev of prevHashes) {
                    if (!appliedEntries.has(prev)) {
                        const localEntry = await dag.loadEntry(prev);
                        if (localEntry === undefined) {
                            allPredecessorsReady = false;
                            break;
                        }
                    }
                }
                if (!allPredecessorsReady) continue;

                const version: Version = new Set(prevHashes);

                const valid = await rObject.validatePayload(payload, version);
                if (!valid) {
                    const source = hashSourcePeer.get(hash);
                    if (source !== undefined) suspectPeers.add(source);
                    receivedPayloads.delete(hash);
                    readyToApply.delete(hash);
                    discardDependents(hash);
                    continue;
                }

                const resultHash = await rObject.applyPayload(payload, version);
                if (resultHash !== hash) {
                    const source = hashSourcePeer.get(hash);
                    if (source !== undefined) suspectPeers.add(source);
                    receivedPayloads.delete(hash);
                    readyToApply.delete(hash);
                    discardDependents(hash);
                    continue;
                }

                appliedEntries.add(hash);
                receivedPayloads.delete(hash);
                readyToApply.delete(hash);
                progress = true;
                anyProgress = true;
            }
        }

        return anyProgress;
    }

    function discardDependents(badHash: B64Hash) {
        const discarded = new Set<B64Hash>();
        discarded.add(badHash);

        const allHeaders = new Map([...discoveredHeaders, ...readyToApply]);
        const order = topoSort([...allHeaders.keys()], allHeaders);

        for (const hash of order) {
            if (discarded.has(hash)) continue;
            const header = allHeaders.get(hash);
            if (header === undefined) continue;

            for (const prev of json.fromSet(header.prevEntryHashes)) {
                if (discarded.has(prev)) {
                    discarded.add(hash);
                    receivedPayloads.delete(hash);
                    discoveredHeaders.delete(hash);
                    readyToApply.delete(hash);
                    requestedPayloads.delete(hash);
                    break;
                }
            }
        }
    }

    // --- helpers ---

    function resetHeaderTimeout(state: HeaderRequestState) {
        clearTimeout(state.timeout);
        state.timeout = setTimeout(() => {
            handleHeaderTimeout(state.requestId);
        }, REQUEST_TIMEOUT_MS);
    }

    function resetPayloadTimeout(state: PayloadRequestState) {
        clearTimeout(state.timeout);
        state.timeout = setTimeout(() => {
            handlePayloadTimeout(state.requestId);
        }, REQUEST_TIMEOUT_MS);
    }

    // --- message handling ---

    function handleMessage(msg: SyncMsg, channel: TopicChannel) {
        if (destroyed) return;

        const pk = `${channel.peerId}@${channel.endpoint}`;

        switch (msg.type) {
            case 'new-frontier':
                if (msg.dagId === dagId) handleNewFrontier(msg, pk);
                break;
            case 'header-response-meta':
                handleHeaderResponseMeta(msg);
                break;
            case 'header-batch':
                handleHeaderBatch(msg);
                break;
            case 'payload-response-meta':
                handlePayloadResponseMeta(msg);
                break;
            case 'payload-msg':
                handlePayloadMsg(msg);
                break;
            default:
                break;
        }
    }

    function addPeer(_peer: PeerHandle) {
        broadcastFrontier();
        attemptWork();
    }

    function removePeer(peerKey: string) {
        peerFrontiers.delete(peerKey);
        peerDiscoveredFrontier.delete(peerKey);

        for (const [rid, state] of pendingHeaderRequests) {
            if (state.peer.key === peerKey) {
                clearTimeout(state.timeout);
                pendingHeaderRequests.delete(rid);
                if (autoPayloadRequestId === rid) {
                    autoPayloadRequestId = undefined;
                }
            }
        }

        for (const [rid, state] of pendingPayloadRequests) {
            if (state.peer.key === peerKey) {
                clearTimeout(state.timeout);
                pendingPayloadRequests.delete(rid);
                for (const h of state.requestedHashes) {
                    if (!receivedPayloads.has(h) && !appliedEntries.has(h)) {
                        requestedPayloads.delete(h);
                    }
                }
            }
        }

        attemptWork();
    }

    function destroy() {
        destroyed = true;

        dag.removeListener(onGrowth);

        for (const state of pendingHeaderRequests.values()) {
            clearTimeout(state.timeout);
        }
        pendingHeaderRequests.clear();

        for (const state of pendingPayloadRequests.values()) {
            clearTimeout(state.timeout);
        }
        pendingPayloadRequests.clear();
    }

    return { handleMessage, addPeer, removePeer, broadcastFrontier, destroy };
}
