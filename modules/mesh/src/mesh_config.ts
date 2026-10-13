// The mesh document shared by app.json, host.json, the REPL, and the mesh
// factories. Scope may be omitted on a partial document; the factory receives
// a config whose scope is set. This parser fills no defaults and does not
// know which platform will run the mesh.

import type { MeshScope } from './scope.js';

export type IceServer = {
    urls: string | string[];
    username?: string;
    credential?: string;
};

export type WsTransportConfig = { listen?: string };

export type WebrtcTransportConfig = {
    listen?: boolean;
    /** Public wss URL of a signaling server, without an endpoint id. */
    signal?: string;
    iceServers?: IceServer[];
};

export type BroadcastChannelTransportConfig = { base?: string };

export type MeshTransports = {
    ws?: WsTransportConfig | false;
    webrtc?: WebrtcTransportConfig | false;
    broadcastChannel?: BroadcastChannelTransportConfig | false;
};

export type TrackerDiscovery = {
    type: 'tracker';
    url?: string;
    key?: string;
    priority?: number;
};

export type FolderDiscoveryConfig = {
    type: 'folder';
    root?: string;
    priority?: number;
};

export type BroadcastChannelDiscoveryConfig = {
    type: 'broadcast-channel';
    priority?: number;
};

export type DiscoverySource = TrackerDiscovery | FolderDiscoveryConfig | BroadcastChannelDiscoveryConfig;

export type MeshConfig = {
    scope?: MeshScope;
    transports?: MeshTransports;
    discovery?: DiscoverySource[];
};

export type ScopedMeshConfig = MeshConfig & { scope: MeshScope };

const TRANSPORT_KEYS = ['ws', 'webrtc', 'broadcastChannel'] as const;
const DISCOVERY_TYPES = ['tracker', 'folder', 'broadcast-channel'] as const;

export function parseMeshConfig(value: unknown, source = 'mesh', field = 'mesh'): MeshConfig {
    const c = new Checker(source);
    return readMesh(c, value, field);
}

export function requireMeshScope(config: MeshConfig, source = 'mesh'): ScopedMeshConfig {
    if (config.scope !== 'localhost' && config.scope !== 'internet') {
        throw new Error(`${source}: scope is missing`);
    }
    return config as ScopedMeshConfig;
}

class Checker {
    constructor(private readonly source: string) {}

    fail(field: string, problem: string): never {
        const where = field.length === 0 ? this.source : `${this.source}: ${field}`;
        throw new Error(`${where} ${problem}`);
    }
}

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readMesh(c: Checker, value: unknown, field: string): MeshConfig {
    const o = object(c, value, field, ['scope', 'transports', 'discovery']);
    const out: MeshConfig = {};
    if (o['scope'] !== undefined) out.scope = readScope(c, o['scope'], child(field, 'scope'));
    if (o['transports'] !== undefined) out.transports = readTransports(c, o['transports'], child(field, 'transports'));
    if (o['discovery'] !== undefined) out.discovery = readDiscovery(c, o['discovery'], child(field, 'discovery'));
    return out;
}

function readScope(c: Checker, value: unknown, field: string): MeshScope {
    if (value !== 'localhost' && value !== 'internet') {
        c.fail(field, `must be 'localhost' or 'internet', got ${JSON.stringify(value)}`);
    }
    return value;
}

function readTransports(c: Checker, value: unknown, field: string): MeshTransports {
    const o = object(c, value, field, [...TRANSPORT_KEYS]);
    const out: MeshTransports = {};
    if (o['ws'] !== undefined) out.ws = readWs(c, o['ws'], child(field, 'ws'));
    if (o['webrtc'] !== undefined) out.webrtc = readWebrtc(c, o['webrtc'], child(field, 'webrtc'));
    if (o['broadcastChannel'] !== undefined) {
        out.broadcastChannel = readBroadcastChannel(c, o['broadcastChannel'], child(field, 'broadcastChannel'));
    }
    return out;
}

function readWs(c: Checker, value: unknown, field: string): WsTransportConfig | false {
    if (value === false) return false;
    const o = object(c, value, field, ['listen']);
    if (o['listen'] === undefined) return {};
    const listen = stringField(c, o['listen'], child(field, 'listen'));
    checkWsListen(c, listen, child(field, 'listen'));
    return { listen };
}

function readWebrtc(c: Checker, value: unknown, field: string): WebrtcTransportConfig | false {
    if (value === false) return false;
    const o = object(c, value, field, ['listen', 'signal', 'iceServers']);
    const out: WebrtcTransportConfig = {};
    if (o['listen'] !== undefined) {
        if (typeof o['listen'] !== 'boolean') c.fail(child(field, 'listen'), 'must be true or false');
        out.listen = o['listen'];
    }
    if (o['signal'] !== undefined) {
        const signal = stringField(c, o['signal'], child(field, 'signal'));
        checkSignal(c, signal, child(field, 'signal'));
        out.signal = signal;
    }
    if (o['iceServers'] !== undefined) out.iceServers = readIceServers(c, o['iceServers'], child(field, 'iceServers'));
    return out;
}

function readBroadcastChannel(c: Checker, value: unknown, field: string): BroadcastChannelTransportConfig | false {
    if (value === false) return false;
    const o = object(c, value, field, ['base']);
    if (o['base'] === undefined) return {};
    const base = stringField(c, o['base'], child(field, 'base'));
    if (base.trim() === '') c.fail(child(field, 'base'), 'must be a non-empty string');
    return { base };
}

function readIceServers(c: Checker, value: unknown, field: string): IceServer[] {
    if (!Array.isArray(value)) c.fail(field, 'must be an array');
    return value.map((entry, i) => readIceServer(c, entry, `${field}[${i}]`));
}

function readIceServer(c: Checker, value: unknown, field: string): IceServer {
    const o = object(c, value, field, ['urls', 'username', 'credential']);
    if (o['urls'] === undefined) c.fail(child(field, 'urls'), 'is missing');
    const urls = o['urls'];
    let parsed: string | string[];
    if (typeof urls === 'string') {
        if (urls.length === 0) c.fail(child(field, 'urls'), 'must be a non-empty string');
        parsed = urls;
    } else if (Array.isArray(urls) && urls.every(u => typeof u === 'string' && u.length > 0)) {
        parsed = urls as string[];
    } else {
        c.fail(child(field, 'urls'), 'must be a string or an array of strings');
    }
    const out: IceServer = { urls: parsed };
    if (o['username'] !== undefined) out.username = stringField(c, o['username'], child(field, 'username'));
    if (o['credential'] !== undefined) out.credential = stringField(c, o['credential'], child(field, 'credential'));
    return out;
}

function readDiscovery(c: Checker, value: unknown, field: string): DiscoverySource[] {
    if (!Array.isArray(value)) c.fail(field, 'must be an array');
    return value.map((entry, i) => readDiscoverySource(c, entry, `${field}[${i}]`));
}

function readDiscoverySource(c: Checker, value: unknown, field: string): DiscoverySource {
    if (!isObject(value)) c.fail(field, 'must be an object');
    const type = value['type'];
    if (type === 'tracker') return readTracker(c, value, field);
    if (type === 'folder') return readFolder(c, value, field);
    if (type === 'broadcast-channel') return readBcDiscovery(c, value, field);
    c.fail(child(field, 'type'), `must be ${DISCOVERY_TYPES.map(t => `'${t}'`).join(', ')}, got ${JSON.stringify(type)}`);
}

function readTracker(c: Checker, value: Record<string, unknown>, field: string): TrackerDiscovery {
    object(c, value, field, ['type', 'url', 'key', 'priority']);
    const out: TrackerDiscovery = { type: 'tracker' };
    if (value['url'] !== undefined) {
        const url = stringField(c, value['url'], child(field, 'url'));
        checkTrackerUrl(c, url, child(field, 'url'));
        out.url = url;
    }
    if (value['key'] !== undefined) out.key = stringField(c, value['key'], child(field, 'key'));
    if (value['priority'] !== undefined) out.priority = readPriority(c, value['priority'], child(field, 'priority'));
    return out;
}

function readFolder(c: Checker, value: Record<string, unknown>, field: string): FolderDiscoveryConfig {
    object(c, value, field, ['type', 'root', 'priority']);
    const out: FolderDiscoveryConfig = { type: 'folder' };
    if (value['root'] !== undefined) {
        const root = stringField(c, value['root'], child(field, 'root'));
        if (root.trim() === '') c.fail(child(field, 'root'), 'must be a non-empty string');
        out.root = root;
    }
    if (value['priority'] !== undefined) out.priority = readPriority(c, value['priority'], child(field, 'priority'));
    return out;
}

function readBcDiscovery(c: Checker, value: Record<string, unknown>, field: string): BroadcastChannelDiscoveryConfig {
    object(c, value, field, ['type', 'priority']);
    const out: BroadcastChannelDiscoveryConfig = { type: 'broadcast-channel' };
    if (value['priority'] !== undefined) out.priority = readPriority(c, value['priority'], child(field, 'priority'));
    return out;
}

function readPriority(c: Checker, value: unknown, field: string): number {
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
        c.fail(field, `must be a non-negative integer, got ${JSON.stringify(value)}`);
    }
    return value;
}

function checkWsListen(c: Checker, listen: string, field: string): void {
    const url = parseUrl(c, listen, field);
    if (url.protocol !== 'ws:') c.fail(field, `must be a ws:// URL, got '${listen}'`);
    rejectUrlExtras(c, url, field, listen);
    if (isBindAll(url.hostname)) c.fail(field, `must be a dialable address, not bind-all (${listen})`);
    checkPort(c, url, field, listen);
}

function checkSignal(c: Checker, signal: string, field: string): void {
    const url = parseUrl(c, signal, field);
    if (url.protocol !== 'wss:') c.fail(field, `must be a wss:// URL with no endpoint id, got '${signal}'`);
    rejectUrlExtras(c, url, field, signal);
    if (url.hostname.length === 0) c.fail(field, `must be a wss:// URL, got '${signal}'`);
    checkPort(c, url, field, signal);
}

function checkTrackerUrl(c: Checker, tracker: string, field: string): void {
    const url = parseUrl(c, tracker, field);
    if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
        c.fail(field, `must be a ws:// or wss:// URL, got '${tracker}'`);
    }
    rejectUrlExtras(c, url, field, tracker);
    if (url.hostname.length === 0) c.fail(field, `must be a ws:// or wss:// URL, got '${tracker}'`);
    checkPort(c, url, field, tracker);
}

function parseUrl(c: Checker, text: string, field: string): URL {
    try {
        return new URL(text);
    } catch {
        c.fail(field, `must be a URL, got '${text}'`);
    }
}

function rejectUrlExtras(c: Checker, url: URL, field: string, text: string): void {
    if (url.username !== '' || url.password !== '' || url.search !== '' || url.hash !== '') {
        c.fail(field, `must not include user info, a query, or a fragment, got '${text}'`);
    }
}

function checkPort(c: Checker, url: URL, field: string, text: string): void {
    if (url.port === '') return;
    const port = Number(url.port);
    if (!Number.isInteger(port) || port <= 0) c.fail(field, `has an invalid port in '${text}'`);
}

function isBindAll(hostname: string): boolean {
    return hostname === '0.0.0.0' || hostname === '::' || hostname === '[::]';
}

function object(c: Checker, value: unknown, field: string, allowed: string[]): Record<string, unknown> {
    if (!isObject(value)) c.fail(field, 'must be an object');
    for (const key of Object.keys(value)) {
        if (!allowed.includes(key)) c.fail(child(field, key), 'is not a known field');
    }
    return value;
}

function stringField(c: Checker, value: unknown, field: string): string {
    if (typeof value !== 'string' || value.length === 0) c.fail(field, 'must be a non-empty string');
    return value;
}

function child(field: string, name: string): string {
    return field.length === 0 ? name : `${field}.${name}`;
}

export type WebrtcPlan =
    | { install: false }
    | { install: true; listen: false; iceServers?: IceServer[] }
    | { install: true; listen: true; signal: string; iceServers?: IceServer[] };

/** What an omitted or partial `transports.webrtc` means for a scope. */
export function planWebrtc(config: ScopedMeshConfig, defaultSignal: string): WebrtcPlan {
    const section = config.transports?.webrtc;
    if (section === false) return { install: false };
    const iceServers = section?.iceServers;
    const withIce = iceServers !== undefined ? { iceServers } : {};
    if (section === undefined) {
        if (config.scope === 'internet') return { install: true, listen: true, signal: defaultSignal };
        return { install: true, listen: false, ...withIce };
    }
    const listen = section.listen ?? (section.signal !== undefined || config.scope === 'internet');
    if (!listen) return { install: true, listen: false, ...withIce };
    return { install: true, listen: true, signal: section.signal ?? defaultSignal, ...withIce };
}

export type WsPlan = { dial: boolean; listen?: string | 'loopback' };

/** What an omitted or partial `transports.ws` means. Only Node can listen. */
export function planWs(config: ScopedMeshConfig, platform: 'node' | 'browser'): WsPlan {
    const section = config.transports?.ws;
    if (section === false) return { dial: false };
    if (section?.listen !== undefined) {
        if (platform === 'browser') throw new Error('the browser cannot listen on ws');
        return { dial: true, listen: section.listen };
    }
    if (platform === 'node' && config.scope === 'localhost') return { dial: true, listen: 'loopback' };
    return { dial: true };
}

export type BroadcastChannelPlan = { install: false } | { install: true; base?: string };

/** Browser installs BroadcastChannel unless it is `false`. Node rejects an explicit config. */
export function planBroadcastChannel(config: ScopedMeshConfig, platform: 'node' | 'browser'): BroadcastChannelPlan {
    const section = config.transports?.broadcastChannel;
    if (section !== undefined && section !== false) {
        if (platform === 'node') throw new Error('broadcast-channel transport is only available in the browser');
        return section.base !== undefined ? { install: true, base: section.base } : { install: true };
    }
    if (platform === 'browser' && section !== false) return { install: true };
    return { install: false };
}

/** Omitted `discovery` is the platform default. An explicit list replaces it. */
export function planDiscovery(
    config: ScopedMeshConfig,
    platform: 'node' | 'browser',
    broadcastChannel: boolean,
): DiscoverySource[] {
    const sources = config.discovery ?? defaultDiscovery(platform, broadcastChannel);
    for (const source of sources) {
        if (source.type === 'folder' && platform === 'browser') {
            throw new Error('folder discovery is only available on Node');
        }
        if (source.type === 'broadcast-channel' && platform === 'node') {
            throw new Error('broadcast-channel discovery is only available in the browser');
        }
        if (source.type === 'broadcast-channel' && !broadcastChannel) {
            throw new Error('broadcast-channel discovery requires the broadcastChannel transport');
        }
    }
    return sources;
}

function defaultDiscovery(platform: 'node' | 'browser', broadcastChannel: boolean): DiscoverySource[] {
    const tracker: DiscoverySource = { type: 'tracker' };
    if (platform === 'node') return [tracker, { type: 'folder' }];
    if (!broadcastChannel) return [tracker];
    return [tracker, { type: 'broadcast-channel' }];
}
