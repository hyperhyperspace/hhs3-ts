import type { B64Hash, HashSuite } from '@hyper-hyper-space/hhs3_crypto';
import { random } from '@hyper-hyper-space/hhs3_crypto';
import type { Dag, Header } from '@hyper-hyper-space/hhs3_dag';
import { json } from '@hyper-hyper-space/hhs3_json';
import type { TopicChannel } from '@hyper-hyper-space/hhs3_mesh';
import { formatValidationFailure, type RObject, type Version, type RContext } from '@hyper-hyper-space/hhs3_mvt';

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
import type { SendResult } from './session.js';
import type { IssueReport, IssueReporter } from '@hyper-hyper-space/hhs3_util';
import { TRACE_SYNC, trace } from './trace.js';

const MAX_HEADERS_PER_REQUEST = 1024;
const MAX_PAYLOAD_REQUESTS_PER_PEER = 2;
const PAYLOAD_CHUNK_SIZE = 64;
const REQUEST_TIMEOUT_MS = 30_000;

// Upper bound on the per-synchronizer set of type-rejected entry hashes. When
// full, the oldest hash is evicted (it may be re-fetched later; that is the
// bound). Exported so tests can reason about eviction.
export const REJECTED_ENTRIES_CAP = 4096;

// Split a sync peer key (`${keyId}@${endpoint}`) into its parts for reporting.
function splitPeerKey(pk: string): { keyId?: string; endpoint?: string } {
    const at = pk.indexOf('@');
    if (at === -1) return { keyId: pk };
    return { keyId: pk.slice(0, at), endpoint: pk.slice(at + 1) };
}

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
    // The peer's frontier Set (by reference) at the time this request was sent.
    // Used to stamp negative-cache misses; see headerMisses.
    peerFrontierAtSend: Set<B64Hash>;
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
    handleMessage(msg: SyncMsg, channel: TopicChannel): void | Promise<void>;
    addPeer(peer: PeerHandle): void;
    removePeer(peerKey: string): void;
    broadcastFrontier(): Promise<void>;
    destroy(): void;
    getDiagnostics(): { pendingHeaderRequests: number; pendingPayloadRequests: number };
}

export function createDagSynchronizer(
    dagId: B64Hash,
    dag: Dag,
    rObject: RObject,
    hashSuite: HashSuite,
    getPeers: () => PeerHandle[],
    sendTo: (peer: PeerHandle, msg: SyncMsg) => SendResult,
    ctx?: RContext,
    opts?: { report?: IssueReporter; rejectedCap?: number },
): DagSynchronizer {

    const emitReport = opts?.report;
    const rejectedCap = opts?.rejectedCap ?? REJECTED_ENTRIES_CAP;

    // --- accumulative state ---

    // The value Set for each peer is always REPLACED wholesale (never mutated in
    // place); see handleNewFrontier. The headerMisses negative cache relies on
    // this: reference equality against the current value is a "frontier unchanged"
    // test. Do not `.add`/`.delete` on a stored Set.
    const peerFrontiers         = new Map<string, Set<B64Hash>>();
    const peerDiscoveredFrontier = new Map<string, Set<B64Hash>>();

    // Negative cache: hash -> (peerKey -> the peer's frontier Set ref when it
    // answered "complete" without providing the hash). A miss is honored only
    // while the recorded ref === the peer's current peerFrontiers value; once the
    // peer gossips a new frontier (a fresh Set), the ref differs and the miss is
    // ignored, so the peer is retried. This makes the cache independent of the
    // order in which an empty reply and a new-frontier are processed. Depends on
    // peerFrontiers Sets being replaced wholesale, never mutated in place.
    // Entries are evicted per-hash the moment the header is learned (see
    // forgetHeaderMiss), and per-peer on disconnect (see removePeer), so the
    // cache stays bounded to hashes we've asked about but do not yet have.
    const headerMisses = new Map<B64Hash, Map<string, Set<B64Hash>>>();

    const discoveredHeaders  = new Map<B64Hash, Header>();
    const readyToApply       = new Map<B64Hash, Header>();
    const requestedPayloads  = new Set<B64Hash>();
    const receivedPayloads   = new Map<B64Hash, json.Literal>();
    const appliedEntries     = new Set<B64Hash>();

    // Provenance: the channel-verified peer that delivered the (hash-verified)
    // payload now sitting in receivedPayloads/readyToApply. Only ever written
    // after a payload-hash match on the delivering channel, so a type reject
    // blames the peer that actually sent the bytes, not a request target or a
    // peer who merely gossiped the same hash.
    const payloadSourcePeer  = new Map<B64Hash, string>();

    // Payloads that arrived before their header (unverified: we cannot check
    // the payload hash without header.payloadHash). Scoped to the delivering
    // request; promoted by tryPromote when the header lands, dropped when the
    // request ends.
    const unpairedPayloads   = new Map<B64Hash, { payload: json.Literal; source: string; requestId: string }>();

    // Bounded, insertion-ordered set of type-rejected entry hashes (see
    // addRejected / REJECTED_ENTRIES_CAP).
    const rejectedEntries    = new Map<B64Hash, true>();

    const pendingHeaderRequests  = new Map<string, HeaderRequestState>();
    const pendingPayloadRequests = new Map<string, PayloadRequestState>();

    const lastSentFrontier = new Map<string, { frontier: Set<B64Hash>, timestamp: number }>();
    const PUSHBACK_INTERVAL_MS = 15_000;

    // Structured issue reporting. Fills the sync-layer defaults; callers pass
    // the peer key (`${keyId}@${endpoint}`) plus any known specifics.
    function reportIssue(peerKey: string | undefined, partial: Omit<IssueReport, 'source' | 'dagId' | 'keyId' | 'endpoint'>) {
        if (emitReport === undefined) return;
        emitReport({
            source: 'sync',
            dagId,
            ...(peerKey !== undefined ? splitPeerKey(peerKey) : {}),
            ...partial,
        });
    }

    function addRejected(hash: B64Hash) {
        if (rejectedEntries.has(hash)) return;
        if (rejectedEntries.size >= rejectedCap) {
            const oldest = rejectedEntries.keys().next().value;
            if (oldest !== undefined) rejectedEntries.delete(oldest);
        }
        rejectedEntries.set(hash, true);
    }

    // Move a buffered unpaired payload into readyToApply once its header is
    // known. No-op for rejected hashes. A payload-hash mismatch here blames the
    // channel that delivered the payload.
    function tryPromote(hash: B64Hash) {
        const buffered = unpairedPayloads.get(hash);
        if (buffered === undefined) return;
        if (rejectedEntries.has(hash)) {
            unpairedPayloads.delete(hash);
            return;
        }
        const header = discoveredHeaders.get(hash);
        if (header === undefined) return;
        unpairedPayloads.delete(hash);
        const computedPayloadHash = hashSuite.hashToB64(
            stringToUint8Array(json.toStringNormalized(buffered.payload))
        );
        if (computedPayloadHash === header.payloadHash) {
            discoveredHeaders.delete(hash);
            readyToApply.set(hash, header);
            receivedPayloads.set(hash, buffered.payload);
            requestedPayloads.delete(hash);
            payloadSourcePeer.set(hash, buffered.source);
        } else {
            reportIssue(buffered.source, { kind: 'hash-mismatch', severity: 'high', opHash: hash });
            requestedPayloads.delete(hash);
        }
    }

    // Drop unpaired payloads buffered for a payload request that is ending.
    function dropUnpairedForRequest(state: PayloadRequestState) {
        for (const [hash, buffered] of unpairedPayloads) {
            if (buffered.requestId === state.requestId) {
                unpairedPayloads.delete(hash);
            }
        }
    }

    // After a discard/reject, a pending payload request whose hashes no longer
    // map to any live header is dead: drop it locally so we do not wait out the
    // 30s timeout. (Payload requests only ever target already-installed
    // headers, so "no live header" means every hash was applied or discarded.)
    function pruneEmptyPayloadRequests() {
        for (const [rid, state] of pendingPayloadRequests) {
            let anyLive = false;
            for (const h of state.requestedHashes) {
                if (discoveredHeaders.has(h) || readyToApply.has(h)) {
                    anyLive = true;
                    break;
                }
            }
            if (anyLive) continue;
            clearTimeout(state.timeout);
            pendingPayloadRequests.delete(rid);
            dropUnpairedForRequest(state);
            for (const h of state.requestedHashes) {
                if (!receivedPayloads.has(h) && !appliedEntries.has(h)) {
                    requestedPayloads.delete(h);
                }
            }
        }
    }

    let autoPayloadRequestId: string | undefined;

    function traceDrop(
        kind: 'header-meta' | 'header-batch' | 'payload-meta' | 'payload-msg',
        requestId: string,
        extra?: Record<string, unknown>,
    ) {
        if (!TRACE_SYNC) return;
        trace('sync.drop', {
            dag: dagId,
            kind,
            requestId,
            why: pendingHeaderRequests.has(requestId) ? 'pending-headers' : 'unknown',
            ...extra,
        });
    }

    let destroyed = false;

    const onGrowth = () => {
        if (destroyed) return;
        broadcastFrontier();
        attemptWork();
    };
    dag.addListener(onGrowth);

    let workInProgress = false;
    let workNeeded = false;

    const refSubs = new Map<B64Hash, { obj: RObject; cb: (version: Version) => void }>();
    const waitingMissing = new Set<B64Hash>();
    let newObjectCb: ((obj: RObject) => void) | undefined;

    async function watchRef(obj: RObject): Promise<void> {
        const id = obj.getId();
        if (refSubs.has(id)) return;
        const cb = () => { if (!destroyed) attemptWork(); };
        obj.subscribe(cb);
        refSubs.set(id, { obj, cb });
        // Subscribe-then-read: wait for ScopedDagSubscription.attach (async
        // getScopedDag + addListener) before the caller checks loadEntry.
        await obj.getScopedDag();
        await Promise.resolve();
    }

    function ensureNewObjectListener() {
        if (newObjectCb !== undefined || ctx === undefined || ctx.subscribeNewObject === undefined) return;
        const cb = (obj: RObject) => {
            if (destroyed) return;
            const id = obj.getId();
            if (!waitingMissing.has(id)) return;
            waitingMissing.delete(id);
            void watchRef(obj).then(() => attemptWork());
            if (waitingMissing.size === 0) detachNewObjectListener();
        };
        // Call as a method so `this` binds (Replica.subscribeNewObject reads
        // this.newObjectCallbacks); only record the handle once subscribe succeeds.
        ctx.subscribeNewObject(cb);
        newObjectCb = cb;
    }

    function detachNewObjectListener() {
        if (newObjectCb === undefined) return;
        ctx?.unsubscribeNewObject?.(newObjectCb);
        newObjectCb = undefined;
    }

    function dropRefWatches() {
        for (const { obj, cb } of refSubs.values()) {
            obj.unsubscribe(cb);
        }
        refSubs.clear();
        waitingMissing.clear();
        detachNewObjectListener();
    }

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
        if (TRACE_SYNC) {
            const peers = getPeers();
            trace('sync.frontier broadcast', {
                dag: dagId,
                frontier: [...frontier],
                peers: peers.length,
            });
        }
        const now = Date.now();
        for (const peer of getPeers()) {
            sendTo(peer, msg);
            if (peerFrontiers.has(peer.key)) {
                lastSentFrontier.set(peer.key, { frontier: new Set(frontier), timestamp: now });
            }
        }
    }

    async function sendFrontierTo(peer: PeerHandle) {
        const frontier = await dag.getFrontier();
        const msg: NewFrontierMsg = {
            type: 'new-frontier',
            dagId,
            frontier: [...frontier],
        };
        if (TRACE_SYNC) {
            trace('sync.frontier send', {
                dag: dagId,
                peer: peer.key,
                frontier: [...frontier],
            });
        }
        sendTo(peer, msg);
        lastSentFrontier.set(peer.key, { frontier: new Set(frontier), timestamp: Date.now() });
    }

    async function handleNewFrontier(msg: NewFrontierMsg, peerKey: string) {
        if (TRACE_SYNC) {
            trace('sync.frontier recv', {
                dag: msg.dagId,
                peer: peerKey,
                frontier: [...msg.frontier],
            });
        }

        peerFrontiers.set(peerKey, new Set(msg.frontier));

        for (const h of msg.frontier) {
            if (discoveredHeaders.has(h) || readyToApply.has(h) || appliedEntries.has(h)) {
                addToPeerDiscoveredFrontier(peerKey, h);
            }
        }

        await attemptWork();

        // Push-back: send our frontier so the peer can discover it is behind
        // (or in a fork). Suppress if frontiers are identical, or if we already
        // sent the same frontier to this peer recently (within PUSHBACK_INTERVAL_MS).
        const localFrontier = await dag.getFrontier();
        const remoteFrontier = new Set(msg.frontier);

        if (frontiersEqual(localFrontier, remoteFrontier)) {
            if (TRACE_SYNC) {
                trace('sync.frontier skip', { dag: dagId, peer: peerKey, why: 'identical' });
            }
            return;
        }

        const prev = lastSentFrontier.get(peerKey);
        if (prev !== undefined
            && frontiersEqual(prev.frontier, localFrontier)
            && Date.now() - prev.timestamp < PUSHBACK_INTERVAL_MS) {
            if (TRACE_SYNC) {
                trace('sync.frontier skip', { dag: dagId, peer: peerKey, why: 'rate-limited' });
            }
            return;
        }

        const peer = getPeers().find(p => p.key === peerKey);
        if (peer !== undefined) {
            if (TRACE_SYNC) {
                trace('sync.frontier push-back', {
                    dag: dagId,
                    peer: peerKey,
                    local: [...localFrontier],
                    remote: [...remoteFrontier],
                });
            }
            sendFrontierTo(peer);
        }
    }

    function frontiersEqual(a: Set<B64Hash>, b: Set<B64Hash>): boolean {
        if (a.size !== b.size) return false;
        for (const h of a) {
            if (!b.has(h)) return false;
        }
        return true;
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

        if (TRACE_SYNC) {
            trace('sync.work', {
                dag: dagId,
                pendingHeaders: pendingHeaderRequests.size,
                pendingPayloads: pendingPayloadRequests.size,
                ready: readyToApply.size,
                discovered: discoveredHeaders.size,
            });
        }
    }

    // --- header dispatch ---

    // A (hash, peer) pair is a cached miss only while the peer's frontier is
    // unchanged since it answered empty. Reference equality works because
    // peerFrontiers Sets are replaced wholesale on every gossip (see declaration).
    function isCachedMiss(hash: B64Hash, peerKey: string): boolean {
        const recorded = headerMisses.get(hash)?.get(peerKey);
        return recorded !== undefined && recorded === peerFrontiers.get(peerKey);
    }

    function recordHeaderMiss(hash: B64Hash, peerKey: string, frontier: Set<B64Hash>) {
        let byPeer = headerMisses.get(hash);
        if (byPeer === undefined) {
            byPeer = new Map();
            headerMisses.set(hash, byPeer);
        }
        byPeer.set(peerKey, frontier);
    }

    // Once a header is learned it is never requested again, so its miss row is
    // dead weight; drop it to bound the negative cache to not-yet-known hashes.
    function forgetHeaderMiss(hash: B64Hash) {
        headerMisses.delete(hash);
    }

    async function dispatchHeaderRequests() {
        const localFrontier = await dag.getFrontier();

        // Collect unresolved prevs of every header we already know. Snapshot
        // both maps: a live walk of discoveredHeaders skips a later key if a
        // concurrent payload moves it to readyToApply during loadEntry, and
        // that header's prevs would never be requested.
        const unresolvedPrevs = new Set<B64Hash>();
        const knownHeaders = new Map<B64Hash, Header>([
            ...readyToApply,
            ...discoveredHeaders,
        ]);
        for (const [_hash, header] of knownHeaders) {
            for (const prev of json.fromSet(header.prevEntryHashes)) {
                if (!discoveredHeaders.has(prev) && !readyToApply.has(prev)
                    && !appliedEntries.has(prev) && !requestedPayloads.has(prev)
                    && !rejectedEntries.has(prev)) {
                    const local = await dag.loadEntry(prev);
                    if (local === undefined
                        && !discoveredHeaders.has(prev) && !readyToApply.has(prev)
                        && !requestedPayloads.has(prev)
                        && !appliedEntries.has(prev)
                        && !rejectedEntries.has(prev)) {
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
                    && !readyToApply.has(h) && !appliedEntries.has(h)
                    && !rejectedEntries.has(h)) {
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

        const peersWithSlots = getPeers();

        for (const peer of peersWithSlots) {
            const peerFrontier = peerFrontiers.get(peer.key);
            if (peerFrontier === undefined) {
                if (TRACE_SYNC) {
                    trace('sync.header-req skip', { dag: dagId, peer: peer.key });
                }
                continue;
            }

            // Already has a header request in flight to this peer? Skip.
            let hasActiveRequest = false;
            for (const req of pendingHeaderRequests.values()) {
                if (req.peer.key === peer.key) {
                    hasActiveRequest = true;
                    break;
                }
            }
            if (hasActiveRequest) continue;

            // Ancestors (unresolved prevs) are not in any peer's frontier, so we
            // broadcast them to any peer with a slot. Skip pairs the peer already
            // answered empty for (negative cache), unless it has since re-gossiped.
            const startHashes: B64Hash[] = [];
            for (const h of unresolvedPrevs) {
                if (!activelyFetching.has(h) && !isCachedMiss(h, peer.key)) {
                    startHashes.push(h);
                }
            }

            // Unknown frontier hashes go ONLY to the peer that advertised them.
            for (const h of peerFrontier) {
                if (unknownFrontierHashes.has(h) && !activelyFetching.has(h)
                    && !isCachedMiss(h, peer.key) && !startHashes.includes(h)) {
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
                peerFrontierAtSend: peerFrontier,
            });

            if (useAutoPayload) {
                autoPayloadRequestId = requestId;
            }

            for (const h of startHashes) {
                activelyFetching.add(h);
            }

            const result = sendTo(peer, req);
            if (result !== 'sent') {
                clearTimeout(timeout);
                pendingHeaderRequests.delete(requestId);
                if (autoPayloadRequestId === requestId) {
                    autoPayloadRequestId = undefined;
                }
                if (result === 'closed') {
                    removePeer(peer.key);
                }
            } else if (TRACE_SYNC) {
                trace('sync.header-req', {
                    dag: dagId,
                    peer: peer.key,
                    n: startHashes.length,
                    autoPayload: useAutoPayload,
                });
            }
        }
    }

    // --- header response handling ---

    // A response frame arrived on channel `pk` but the request `requestId`
    // belongs to a different peer: protocol abuse on `pk`. Blame `pk`, drop the
    // frame, and never mutate the other peer's request.
    function isCollision(state: { peer: PeerHandle }, pk: string, kind: string, requestId: string): boolean {
        if (state.peer.key === pk) return false;
        reportIssue(pk, { kind: 'protocol', severity: 'moderate', message: `requestId collision on ${kind}` });
        if (TRACE_SYNC) {
            trace('sync.drop', { dag: dagId, kind, requestId, why: 'requestId-collision', peer: pk });
        }
        return true;
    }

    function handleHeaderResponseMeta(msg: HeaderResponseMeta, pk: string) {
        const state = pendingHeaderRequests.get(msg.requestId);
        if (state === undefined) {
            traceDrop('header-meta', msg.requestId);
            return;
        }
        if (isCollision(state, pk, 'header-response-meta', msg.requestId)) return;

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

        if (TRACE_SYNC) {
            trace('sync.header-meta', {
                dag: dagId,
                peer: state.peer.key,
                headers: msg.headerCount,
                payloads: msg.payloadCount,
                complete: msg.complete,
            });
        }

        resetHeaderTimeout(state);

        if (msg.headerCount === 0 && msg.complete) {
            return onHeaderRequestComplete(msg.requestId);
        }
        return;
    }

    async function handleHeaderBatch(msg: HeaderBatch, pk: string) {
        const state = pendingHeaderRequests.get(msg.requestId);
        if (state === undefined) {
            traceDrop('header-batch', msg.requestId);
            return;
        }
        if (isCollision(state, pk, 'header-batch', msg.requestId)) return;

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

            // Already applied or type-rejected: never re-queue.
            if (appliedEntries.has(item.hash)) {
                addToPeerDiscoveredFrontier(state.peer.key, item.hash);
                forgetHeaderMiss(item.hash);
                continue;
            }
            if (rejectedEntries.has(item.hash)) {
                unpairedPayloads.delete(item.hash);
                forgetHeaderMiss(item.hash);
                continue;
            }

            // A one-hop dependent of a rejected entry: do not admit it, and do
            // not add it to the rejected set (only the offending hash is stored).
            let prevRejected = false;
            for (const prev of json.fromSet(item.header.prevEntryHashes)) {
                if (rejectedEntries.has(prev)) { prevRejected = true; break; }
            }
            if (prevRejected) {
                unpairedPayloads.delete(item.hash);
                forgetHeaderMiss(item.hash);
                continue;
            }

            // If we already have this entry locally (e.g. common ancestor
            // returned in a divergent-frontier response), do not queue it for
            // payload fetch/validation again.
            const localHeader = await dag.loadHeader(item.hash);
            if (localHeader !== undefined) {
                addToPeerDiscoveredFrontier(state.peer.key, item.hash);
                forgetHeaderMiss(item.hash);
                continue;
            }

            // Skip duplicates (another peer may have sent the same header)
            if (!discoveredHeaders.has(item.hash) && !readyToApply.has(item.hash)
                && !appliedEntries.has(item.hash)) {
                discoveredHeaders.set(item.hash, item.header);
                forgetHeaderMiss(item.hash);
                // Pair with a payload that arrived ahead of this header.
                tryPromote(item.hash);
            }
        }

        state.receivedHeaderCount += msg.headers.length;
        resetHeaderTimeout(state);

        if (state.expectedHeaderCount !== undefined &&
            state.receivedHeaderCount >= state.expectedHeaderCount) {
            await onHeaderRequestComplete(msg.requestId);
        }

        await attemptWork();
    }

    async function onHeaderRequestComplete(requestId: string) {
        const state = pendingHeaderRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingHeaderRequests.delete(requestId);

        // Update peerDiscoveredFrontier for the serving peer, and figure out which
        // start hashes this peer did NOT provide so we can negatively cache them.
        let learnedAny = false;
        for (const h of state.startHashes) {
            if (discoveredHeaders.has(h) || readyToApply.has(h) || appliedEntries.has(h)) {
                addToPeerDiscoveredFrontier(state.peer.key, h);
                learnedAny = true;
            } else {
                // The peer answered without giving us this hash. Remember the miss
                // against the frontier it had when we asked, so we don't spin
                // re-asking the same peer until it gossips a newer frontier.
                recordHeaderMiss(h, state.peer.key, state.peerFrontierAtSend);
            }
        }

        // Handle auto-payload transition: use receivedHashes (all hashes from this
        // request's batches) rather than discoveredHeaders.keys(), because some entries
        // may have already been applied by a concurrent auto-payload stream.
        let autoPayloadSetup = false;
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
            autoPayloadSetup = true;
        }

        if (autoPayloadRequestId === requestId) {
            autoPayloadRequestId = undefined;
        }

        // Only re-run the work loop if this completion actually made progress.
        // An empty "complete" reply that taught us nothing would otherwise spin.
        if (learnedAny || autoPayloadSetup) {
            await attemptWork();
        }
    }

    function handleHeaderTimeout(requestId: string) {
        const state = pendingHeaderRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingHeaderRequests.delete(requestId);
        reportIssue(state.peer.key, { kind: 'timeout', severity: 'moderate', message: 'header request timed out' });

        if (autoPayloadRequestId === requestId) {
            autoPayloadRequestId = undefined;
        }

        attemptWork();
    }

    function failHeaderRequest(requestId: string, reason: string) {
        const state = pendingHeaderRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingHeaderRequests.delete(requestId);
        if (TRACE_SYNC) {
            trace('sync.fail', {
                dag: dagId,
                peer: state.peer.key,
                issue: 'protocol',
                reason,
            });
        }
        reportIssue(state.peer.key, { kind: 'protocol', severity: 'high', message: reason });

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
        const peers = getPeers();

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

        const result = sendTo(peer, req);
        if (result !== 'sent') {
            clearTimeout(timeout);
            pendingPayloadRequests.delete(requestId);
            for (const h of hashes) {
                requestedPayloads.delete(h);
            }
            if (result === 'closed') {
                removePeer(peer.key);
            }
        } else if (TRACE_SYNC) {
            trace('sync.payload-req', { dag: dagId, peer: peer.key, n: hashes.length });
        }
    }

    // --- payload response handling ---

    function handlePayloadResponseMeta(msg: PayloadResponseMeta, pk: string) {
        const state = pendingPayloadRequests.get(msg.requestId);
        if (state === undefined) {
            traceDrop('payload-meta', msg.requestId);
            return;
        }
        if (isCollision(state, pk, 'payload-response-meta', msg.requestId)) return;

        if (msg.payloadCount !== state.requestedHashes.size) {
            failPayloadRequest(msg.requestId, 'payloadCount mismatch');
            return;
        }

        state.expectedPayloadCount = msg.payloadCount;
        resetPayloadTimeout(state);
    }

    // Complete a payload request once every announced frame has been counted,
    // regardless of whether each frame was stored, skipped, or rejected.
    function finalizePayloadRequestIfDone(state: PayloadRequestState) {
        if (state.expectedPayloadCount === undefined
            || state.receivedPayloadCount < state.expectedPayloadCount) {
            return;
        }
        clearTimeout(state.timeout);
        pendingPayloadRequests.delete(state.requestId);
        dropUnpairedForRequest(state);
        if (TRACE_SYNC) {
            trace('sync.payload done', {
                dag: dagId,
                peer: state.peer.key,
                n: state.receivedPayloadCount,
            });
        }
    }

    async function handlePayloadMsg(msg: PayloadMsg, pk: string) {
        const state = pendingPayloadRequests.get(msg.requestId);
        if (state === undefined) {
            traceDrop('payload-msg', msg.requestId, { seq: msg.sequence });
            return;
        }
        if (isCollision(state, pk, 'payload-msg', msg.requestId)) return;

        if (msg.sequence !== state.nextSequence) {
            failPayloadRequest(msg.requestId, `expected sequence ${state.nextSequence}, got ${msg.sequence}`);
            return;
        }
        state.nextSequence++;

        if (!state.requestedHashes.has(msg.hash)) {
            failPayloadRequest(msg.requestId, `received unrequested hash: ${msg.hash}`);
            return;
        }

        // Every in-order frame for a live request counts, including duplicate,
        // already-applied, rejected, and no-header frames. This is what lets a
        // stream complete after some of its entries were type-rejected instead
        // of hanging until the 30s timeout.
        state.receivedPayloadCount++;
        resetPayloadTimeout(state);

        if (state.expectedPayloadCount !== undefined &&
            state.receivedPayloadCount > state.expectedPayloadCount) {
            failPayloadRequest(msg.requestId, 'received more payloads than announced');
            return;
        }

        // Nothing to store: already have it, or it was type-rejected. Frame is
        // already counted above.
        if (receivedPayloads.has(msg.hash) || appliedEntries.has(msg.hash)
            || rejectedEntries.has(msg.hash)) {
            requestedPayloads.delete(msg.hash);
            unpairedPayloads.delete(msg.hash);
            finalizePayloadRequestIfDone(state);
            await attemptWork();
            return;
        }

        const header = discoveredHeaders.get(msg.hash) ?? readyToApply.get(msg.hash);
        if (header !== undefined) {
            const computedPayloadHash = hashSuite.hashToB64(
                stringToUint8Array(json.toStringNormalized(msg.payload))
            );
            if (computedPayloadHash === header.payloadHash) {
                // Hash verified: move from discoveredHeaders to readyToApply and
                // record the delivering channel as the provenance for this hash.
                discoveredHeaders.delete(msg.hash);
                readyToApply.set(msg.hash, header);
                receivedPayloads.set(msg.hash, msg.payload);
                requestedPayloads.delete(msg.hash);
                payloadSourcePeer.set(msg.hash, pk);
            } else {
                // Hash mismatch is misbehavior of this channel. Do not store, do
                // not poison rejectedEntries (we never had trustworthy bytes).
                reportIssue(pk, { kind: 'hash-mismatch', severity: 'high', opHash: msg.hash });
                requestedPayloads.delete(msg.hash);
            }
        } else {
            // Payload ahead of its header: buffer as unpaired (unverified),
            // scoped to this request. tryPromote verifies it when the header
            // lands; dropUnpairedForRequest drops it if the request ends first.
            unpairedPayloads.set(msg.hash, { payload: msg.payload, source: pk, requestId: msg.requestId });
        }

        finalizePayloadRequestIfDone(state);

        await attemptWork();
    }

    function handlePayloadTimeout(requestId: string) {
        const state = pendingPayloadRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingPayloadRequests.delete(requestId);
        reportIssue(state.peer.key, { kind: 'timeout', severity: 'moderate', message: 'payload request timed out' });

        dropUnpairedForRequest(state);
        for (const h of state.requestedHashes) {
            if (!receivedPayloads.has(h) && !appliedEntries.has(h)) {
                requestedPayloads.delete(h);
            }
        }

        attemptWork();
    }

    function failPayloadRequest(requestId: string, reason: string) {
        const state = pendingPayloadRequests.get(requestId);
        if (state === undefined) return;

        clearTimeout(state.timeout);
        pendingPayloadRequests.delete(requestId);
        if (TRACE_SYNC) {
            trace('sync.fail', {
                dag: dagId,
                peer: state.peer.key,
                issue: 'protocol',
                reason,
            });
        }
        reportIssue(state.peer.key, { kind: 'protocol', severity: 'high', message: reason });

        dropUnpairedForRequest(state);
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
                let missingPrev: B64Hash | undefined;
                for (const prev of prevHashes) {
                    if (!appliedEntries.has(prev)) {
                        const localEntry = await dag.loadEntry(prev);
                        if (localEntry === undefined) {
                            allPredecessorsReady = false;
                            missingPrev = prev;
                            break;
                        }
                    }
                }
                if (!allPredecessorsReady) {
                    if (TRACE_SYNC) {
                        trace('sync.defer', { dag: dagId, hash, missingHash: missingPrev });
                    }
                    continue;
                }

                const version: Version = new Set(prevHashes);

                const foreignDeps = rObject.extractForeignDeps(payload, version);
                if (foreignDeps !== undefined && ctx !== undefined) {
                    let allDepsAvailable = true;
                    let missingObject: B64Hash | undefined;
                    let missingHash: B64Hash | undefined;
                    for (const dep of foreignDeps) {
                        const obj = await ctx.getObject(dep.objectId);
                        if (obj === undefined) {
                            waitingMissing.add(dep.objectId);
                            ensureNewObjectListener();
                            allDepsAvailable = false;
                            missingObject = dep.objectId;
                            break;
                        }
                        waitingMissing.delete(dep.objectId);
                        await watchRef(obj);
                        const scoped = await obj.getScopedDag();
                        for (const requiredHash of dep.requiredHashes) {
                            if (await scoped.loadEntry(requiredHash) === undefined) {
                                allDepsAvailable = false;
                                missingHash = requiredHash;
                                break;
                            }
                        }
                        if (!allDepsAvailable) break;
                    }
                    if (!allDepsAvailable) {
                        if (TRACE_SYNC) {
                            trace('sync.defer', { dag: dagId, hash, missingObject, missingHash });
                        }
                        continue;
                    }
                }

                const result = await rObject.validatePayload(payload, version);
                if (!result.valid) {
                    const source = payloadSourcePeer.get(hash);
                    reportIssue(source, {
                        kind: 'validation-failed',
                        severity: 'moderate',
                        opHash: hash,
                        message: formatValidationFailure(result.why),
                    });
                    if (TRACE_SYNC) {
                        trace('sync.apply', {
                            dag: dagId,
                            hash,
                            result: 'reject',
                            why: 'invalid',
                            reason: formatValidationFailure(result.why),
                        });
                    }
                    rejectEntry(hash);
                    continue;
                }

                const resultHash = await rObject.applyPayload(payload, version);
                if (resultHash !== hash) {
                    const source = payloadSourcePeer.get(hash);
                    reportIssue(source, { kind: 'hash-mismatch', severity: 'high', opHash: hash });
                    if (TRACE_SYNC) {
                        trace('sync.apply', { dag: dagId, hash, result: 'reject', why: 'hash' });
                    }
                    rejectEntry(hash);
                    continue;
                }

                appliedEntries.add(hash);
                receivedPayloads.delete(hash);
                readyToApply.delete(hash);
                payloadSourcePeer.delete(hash);
                if (TRACE_SYNC) {
                    trace('sync.apply', { dag: dagId, hash, result: 'ok' });
                }
                progress = true;
                anyProgress = true;
            }
        }

        return anyProgress;
    }

    // Type-level reject of a hash-verified, payload-verified entry: remember the
    // offending hash in the bounded set, drop it and its live dependents from
    // the receive pipeline, and free any now-dead payload requests. Only the
    // offending hash is remembered; dependents are dropped, not stored.
    function rejectEntry(hash: B64Hash) {
        addRejected(hash);
        receivedPayloads.delete(hash);
        readyToApply.delete(hash);
        discoveredHeaders.delete(hash);
        requestedPayloads.delete(hash);
        payloadSourcePeer.delete(hash);
        unpairedPayloads.delete(hash);
        discardDependents(hash);
        pruneEmptyPayloadRequests();
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
                    payloadSourcePeer.delete(hash);
                    unpairedPayloads.delete(hash);
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

    function handleMessage(msg: SyncMsg, channel: TopicChannel): void | Promise<void> {
        if (destroyed) return;

        const pk = `${channel.peerId}@${channel.endpoint}`;

        switch (msg.type) {
            case 'new-frontier':
                if (msg.dagId === dagId) return handleNewFrontier(msg, pk);
                return;
            case 'header-response-meta':
                return handleHeaderResponseMeta(msg, pk);
            case 'header-batch':
                return handleHeaderBatch(msg, pk);
            case 'payload-response-meta':
                return handlePayloadResponseMeta(msg, pk);
            case 'payload-msg':
                return handlePayloadMsg(msg, pk);
            default:
                return;
        }
    }

    function addPeer(_peer: PeerHandle) {
        broadcastFrontier();
        attemptWork();
    }

    function removePeer(peerKey: string) {
        peerFrontiers.delete(peerKey);
        peerDiscoveredFrontier.delete(peerKey);
        lastSentFrontier.delete(peerKey);

        // Drop this peer from the negative cache, discarding now-empty entries.
        for (const [hash, byPeer] of headerMisses) {
            byPeer.delete(peerKey);
            if (byPeer.size === 0) headerMisses.delete(hash);
        }

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
                dropUnpairedForRequest(state);
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
        dropRefWatches();

        for (const state of pendingHeaderRequests.values()) {
            clearTimeout(state.timeout);
        }
        pendingHeaderRequests.clear();

        for (const state of pendingPayloadRequests.values()) {
            clearTimeout(state.timeout);
        }
        pendingPayloadRequests.clear();
    }

    function getDiagnostics() {
        return {
            pendingHeaderRequests: pendingHeaderRequests.size,
            pendingPayloadRequests: pendingPayloadRequests.size,
        };
    }

    return { handleMessage, addPeer, removePeer, broadcastFrontier, destroy, getDiagnostics };
}
