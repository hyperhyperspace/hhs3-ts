// Preflight reachability check for a tracker (or any transport endpoint).
// Two attempts, each long enough for a real handshake. Factories skip a tracker
// discovery layer when both fail, and keep the reason, rather than hanging mesh
// setup on a dead tracker. A socket that opens after its attempt has already
// timed out is closed.

import type { NetworkAddress, Transport, TransportProvider } from './transport.js';

export const TRACKER_PROBE_TIMEOUT_MS = 5_000;

const ATTEMPTS = 2;

export type TrackerProbeResult =
    | { ok: true }
    | { ok: false; reason: string };

export async function probeTracker(
    provider: TransportProvider,
    address: NetworkAddress,
    timeoutMs = TRACKER_PROBE_TIMEOUT_MS,
): Promise<TrackerProbeResult> {
    const reasons: string[] = [];
    for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
        const result = await probeOnce(provider, address, timeoutMs);
        if (result.ok) return result;
        reasons.push(result.reason);
    }
    return { ok: false, reason: combinedReason(reasons) };
}

async function probeOnce(
    provider: TransportProvider,
    address: NetworkAddress,
    timeoutMs: number,
): Promise<TrackerProbeResult> {
    let transport: Transport | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    const connected = provider.connect(address).then((opened) => {
        if (timedOut) {
            try { opened.close(); } catch { /* ignore */ }
            throw new Error('timed out');
        }
        transport = opened;
        return opened;
    });
    // A timeout wins the race while connect is still pending. Observe the late
    // rejection from the close-on-timeout path above.
    const observed = connected.then(() => undefined, () => undefined);
    try {
        await Promise.race([
            connected,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => {
                    timedOut = true;
                    reject(new Error('timed out'));
                }, timeoutMs);
            }),
        ]);
        return { ok: true };
    } catch (err) {
        return { ok: false, reason: reasonOf(err) };
    } finally {
        if (timer !== undefined) clearTimeout(timer);
        if (transport !== undefined) {
            try { transport.close(); } catch { /* ignore */ }
        }
        void observed;
    }
}

function combinedReason(reasons: string[]): string {
    const detail = reasons.every((reason) => reason === reasons[0]) ? reasons[0]! : reasons.join('; ');
    return `${detail} after ${reasons.length} attempts`;
}

function reasonOf(err: unknown): string {
    if (err instanceof Error && err.message.length > 0) return err.message;
    return 'connect failed';
}
