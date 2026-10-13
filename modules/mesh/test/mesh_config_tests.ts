import { testing } from '@hyper-hyper-space/hhs3_util';
import { parseMeshConfig, planDiscovery, planWebrtc, planWs, requireMeshScope } from '../src/mesh_config.js';

function throws(fn: () => void, needle: string): void {
    let message = '';
    try {
        fn();
    } catch (err) {
        message = err instanceof Error ? err.message : String(err);
    }
    testing.assertTrue(message.includes(needle), `expected '${needle}' in '${message}'`);
}

function testParse(): void {
    const parsed = parseMeshConfig({
        scope: 'internet',
        transports: {
            ws: { listen: 'ws://127.0.0.1:9' },
            webrtc: { signal: 'wss://signal.example:4612' },
            broadcastChannel: false,
        },
        discovery: [
            { type: 'tracker', url: 'wss://tracker.example:4610', key: 'abc', priority: 0 },
            { type: 'folder' },
        ],
    }, 'file', 'mesh');
    testing.assertEquals(parsed.scope, 'internet', 'scope');
    testing.assertEquals(parsed.transports?.ws && parsed.transports.ws.listen, 'ws://127.0.0.1:9', 'ws listen');
    testing.assertEquals(parsed.discovery?.length, 2, 'two discovery sources');

    throws(() => parseMeshConfig({ scope: 'lan' }), "must be 'localhost' or 'internet'");
    throws(() => parseMeshConfig({ scope: 'internet', extra: 1 }), 'is not a known field');
    throws(() => parseMeshConfig({ transports: { ws: { listen: 'ws://0.0.0.0:1' } } }), 'bind-all');
    throws(() => parseMeshConfig({ transports: { ws: { listen: 'wss://example' } } }), 'ws://');
    throws(() => parseMeshConfig({ transports: { webrtc: { signal: 'ws://example' } } }), 'wss://');
    throws(() => parseMeshConfig({ discovery: [{ type: 'folder', priority: -1 }] }), 'non-negative');
    throws(() => requireMeshScope({}), 'scope is missing');
}

function testPlans(): void {
    const internet = requireMeshScope({ scope: 'internet' });
    const webrtc = planWebrtc(internet, 'wss://signal.mypeer.net:4612');
    testing.assertTrue(webrtc.install && webrtc.listen && webrtc.signal === 'wss://signal.mypeer.net:4612', 'internet webrtc listens');
    const dialOut = planWebrtc(requireMeshScope({ scope: 'internet', transports: { webrtc: { listen: false } } }), 'wss://signal.mypeer.net:4612');
    testing.assertTrue(dialOut.install && !dialOut.listen, 'listen false still installs');
    const removed = planWebrtc(requireMeshScope({ scope: 'internet', transports: { webrtc: false } }), 'wss://x');
    testing.assertTrue(!removed.install, 'false removes webrtc');
    const ws = planWs(requireMeshScope({ scope: 'localhost' }), 'node');
    testing.assertTrue(ws.dial && ws.listen === 'loopback', 'localhost ws listens on node');
    const browserWs = planWs(requireMeshScope({ scope: 'localhost' }), 'browser');
    testing.assertTrue(browserWs.dial && browserWs.listen === undefined, 'browser does not bind ws');
    const discovery = planDiscovery(requireMeshScope({ scope: 'internet' }), 'node', false);
    testing.assertEquals(discovery.map(s => s.type).join(','), 'tracker,folder', 'node default discovery');
    throws(() => planDiscovery(requireMeshScope({ scope: 'internet', discovery: [{ type: 'folder' }] }), 'browser', true), 'only available on Node');
}

export const meshConfigTests = {
    title: '[MESH_CONFIG] Mesh config document',
    tests: [
        { name: '[MESH_CONFIG_00] parse rejects unknown fields and bind-all', invoke: async () => { testParse(); } },
        { name: '[MESH_CONFIG_01] omitted sections take scope defaults', invoke: async () => { testPlans(); } },
    ],
};
