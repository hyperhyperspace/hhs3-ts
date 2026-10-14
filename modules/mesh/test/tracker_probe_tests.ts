import { testing } from '@hyper-hyper-space/hhs3_util';
import { probeTracker } from '../src/tracker_probe.js';
import type { Transport, TransportProvider } from '../src/transport.js';

type Closable = Transport & { closed: boolean };
type Step = 'open' | 'fail' | 'hang';

function fakeTransport(): Closable {
    const transport: Closable = {
        open: true,
        closed: false,
        send() {},
        close() { transport.closed = true; },
        onMessage() {},
        onClose() {},
    };
    return transport;
}

function scripted(steps: Step[], hangMs: number): {
    provider: TransportProvider;
    calls: () => number;
    opened: Closable[];
} {
    let calls = 0;
    const opened: Closable[] = [];
    const provider: TransportProvider = {
        scheme: 'ws',
        async listen() { throw new Error('no listen'); },
        close() {},
        connect() {
            const step = steps[calls++];
            if (step === undefined) return Promise.reject(new Error(`unexpected attempt ${calls}`));
            if (step === 'fail') return Promise.reject(new Error('refused'));
            if (step === 'open') {
                const transport = fakeTransport();
                opened.push(transport);
                return Promise.resolve(transport);
            }
            return new Promise((resolve) => {
                setTimeout(() => {
                    const transport = fakeTransport();
                    opened.push(transport);
                    resolve(transport);
                }, hangMs);
            });
        },
    };
    return { provider, calls: () => calls, opened };
}

async function testOpenSucceeds(): Promise<void> {
    const script = scripted(['open'], 0);
    const result = await probeTracker(script.provider, 'ws://tracker', 20);
    testing.assertTrue(result.ok, 'an open socket is reachable');
    testing.assertEquals(script.calls(), 1, 'success does not retry');
    testing.assertTrue(script.opened[0]?.closed === true, 'probe closes the socket it opened');
}

async function testRetryThenOpen(): Promise<void> {
    const script = scripted(['fail', 'open'], 0);
    const result = await probeTracker(script.provider, 'ws://tracker', 20);
    testing.assertTrue(result.ok, 'the second attempt can succeed');
    testing.assertEquals(script.calls(), 2, 'failed once, then opened');
    testing.assertTrue(script.opened[0]?.closed === true, 'the successful socket is closed');
}

async function testHangReportsTimeoutAndClosesLate(): Promise<void> {
    const timeoutMs = 30;
    const script = scripted(['hang', 'hang'], timeoutMs + 40);
    const result = await probeTracker(script.provider, 'ws://tracker', timeoutMs);
    testing.assertTrue(!result.ok, 'a hang is not reachable');
    if (!result.ok) {
        testing.assertTrue(result.reason.includes('timed out'), `reason names the timeout (${result.reason})`);
        testing.assertTrue(result.reason.includes('after 2 attempts'), `reason says it was tried twice (${result.reason})`);
    }
    const deadline = Date.now() + 1_000;
    while (script.opened.length < 2 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    testing.assertEquals(script.opened.length, 2, 'both late sockets opened');
    testing.assertTrue(script.opened.every((transport) => transport.closed), 'late sockets are closed');
}

export const trackerProbeTests = {
    title: '[TRACKER_PROBE] Tracker reachability probe',
    tests: [
        { name: '[TRACKER_PROBE_00] an open socket succeeds without a retry', invoke: testOpenSucceeds },
        { name: '[TRACKER_PROBE_01] a failed attempt is retried', invoke: testRetryThenOpen },
        { name: '[TRACKER_PROBE_02] a hang times out and a late socket is closed', invoke: testHangReportsTimeoutAndClosesLate },
    ],
};
