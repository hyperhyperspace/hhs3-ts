import type { B64Hash, HashSuite } from '@hyper-hyper-space/hhs3_crypto';
import { dag } from '@hyper-hyper-space/hhs3_dag';
import type { Swarm, SwarmPeer } from '@hyper-hyper-space/hhs3_mesh';
import type { RObjectInit } from '@hyper-hyper-space/hhs3_mvt';

import { encode, decode } from './codec.js';
import type { InitRequest, SyncMsg } from './protocol.js';

const DEFAULT_TIMEOUT_MS = 10_000;
const RETRY_INTERVAL_MS = 200;

export function fetchInit(
    objectId: B64Hash,
    swarms: Swarm[],
    hashSuite: HashSuite,
    timeoutMs: number = DEFAULT_TIMEOUT_MS,
): Promise<RObjectInit> {

    return new Promise<RObjectInit>((resolve, reject) => {
        let settled = false;
        const retryTimers: ReturnType<typeof setInterval>[] = [];

        const timer = setTimeout(() => {
            if (settled) return;
            settled = true;
            cleanup();
            reject(new Error(`fetchInit timed out after ${timeoutMs}ms for object ${objectId}`));
        }, timeoutMs);

        function cleanup() {
            clearTimeout(timer);
            for (const t of retryTimers) clearInterval(t);
        }

        function finish(init: RObjectInit) {
            if (settled) return;
            settled = true;
            cleanup();
            resolve(init);
        }

        function fail(reason: string) {
            if (settled) return;
            settled = true;
            cleanup();
            reject(new Error(reason));
        }

        function handlePeer(sp: SwarmPeer) {
            if (settled) return;

            const request: InitRequest = { type: 'init-request', objectId };

            function sendRequest() {
                if (settled || !sp.channel.open) return;
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

                const entry = dag.createEntry(msg.init.payload, {}, dag.position(), hashSuite);
                if (entry.hash !== objectId) {
                    fail(`Creation payload hash mismatch: expected ${objectId}, got ${entry.hash}`);
                    return;
                }

                finish({ type: msg.init.type, payload: msg.init.payload });
            });
        }

        for (const swarm of swarms) {
            for (const existing of swarm.peers()) {
                handlePeer(existing);
            }
            swarm.onPeerJoin(handlePeer);
        }
    });
}
