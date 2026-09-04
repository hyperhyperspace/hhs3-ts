// Preflight reachability check for a tracker (or any transport endpoint):
// attempt a connection with a short timeout, then close. Factories use this to
// skip registering a tracker discovery layer when the endpoint is down, rather
// than hanging mesh setup on a dead tracker.

import type { NetworkAddress, Transport, TransportProvider } from './transport.js';

export const TRACKER_PROBE_TIMEOUT_MS = 1500;

export async function probeTracker(
    provider: TransportProvider,
    address: NetworkAddress,
    timeoutMs = TRACKER_PROBE_TIMEOUT_MS,
): Promise<boolean> {
    let transport: Transport | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    try {
        const connected = provider.connect(address).then((t) => {
            if (timedOut) {
                try { t.close(); } catch { /* ignore */ }
                throw new Error('probe timed out');
            }
            transport = t;
            return t;
        });
        await Promise.race([
            connected,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => {
                    timedOut = true;
                    reject(new Error('timeout'));
                }, timeoutMs);
            }),
        ]);
        return true;
    } catch {
        return false;
    } finally {
        if (timer !== undefined) clearTimeout(timer);
        if (transport !== undefined) {
            try { transport.close(); } catch { /* ignore */ }
        }
    }
}
