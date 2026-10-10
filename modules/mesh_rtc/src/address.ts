// rtc:// addresses name a signaling server and a stable endpoint id.
// The mesh dials this string. The provider rings wss:// at the same host,
// port, and path. The endpoint id does not change when a delegation is renewed.

export interface ParsedRtcAddress {
    readonly host: string;
    readonly port?: number;
    readonly mount: string;
    readonly endpointId: string;
    readonly signalingOrigin: string;
    readonly signalingUrl: string;
    readonly address: string;
}

function hostPort(host: string, port: number | undefined): string {
    const h = host.includes(':') ? `[${host}]` : host;
    return port === undefined ? h : `${h}:${port}`;
}

function formatOrigin(host: string, port: number | undefined, mount: string): string {
    const base = `wss://${hostPort(host, port)}`;
    return mount.length === 0 ? base : `${base}/${mount}`;
}

export function parseRtcAddress(address: string): ParsedRtcAddress {
    if (!address.startsWith('rtc://')) {
        throw new Error(`not an rtc address: ${address}`);
    }
    let url: URL;
    try {
        url = new URL(address);
    } catch {
        throw new Error(`not an rtc address: ${address}`);
    }
    if (url.username !== '' || url.password !== '' || url.search !== '' || url.hash !== '') {
        throw new Error(`rtc address must be host, optional port, path, and endpoint id: ${address}`);
    }
    const parts = url.pathname.split('/').filter(s => s.length > 0).map(decodeURIComponent);
    if (parts.length === 0) throw new Error(`rtc address missing endpoint id: ${address}`);
    const endpointId = parts[parts.length - 1]!;
    const mount = parts.slice(0, -1).join('/');
    const port = url.port === '' ? undefined : Number(url.port);
    if (port !== undefined && (!Number.isInteger(port) || port <= 0)) {
        throw new Error(`invalid rtc port in ${address}`);
    }
    const host = url.hostname;
    if (host.length === 0) throw new Error(`rtc address missing host: ${address}`);
    const signalingOrigin = formatOrigin(host, port, mount);
    const signalingUrl = `${signalingOrigin}/${encodeURIComponent(endpointId)}`;
    const formatted = `rtc://${hostPort(host, port)}${mount.length === 0 ? '' : `/${mount}`}/${encodeURIComponent(endpointId)}`;
    return {
        host,
        port,
        mount,
        endpointId,
        signalingOrigin,
        signalingUrl,
        address: formatted,
    };
}

/** `signalBase` is the public wss URL of the server, without an endpoint id. */
export function rtcAddressFor(signalBase: string, endpointId: string): string {
    let url: URL;
    try {
        url = new URL(signalBase);
    } catch {
        throw new Error(`invalid signaling URL: ${signalBase}`);
    }
    if (url.protocol !== 'wss:' && url.protocol !== 'https:') {
        throw new Error(`signaling URL must be wss: ${signalBase}`);
    }
    const mount = url.pathname.split('/').filter(s => s.length > 0).map(decodeURIComponent).join('/');
    const port = url.port === '' ? undefined : Number(url.port);
    const host = url.hostname;
    return `rtc://${hostPort(host, port)}${mount.length === 0 ? '' : `/${mount}`}/${encodeURIComponent(endpointId)}`;
}
