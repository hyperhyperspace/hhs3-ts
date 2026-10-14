// Names a browser uses for one app on an origin. There is no host folder, so
// the app name keeps two apps on the same origin from sharing databases.
// The lock and the client channel are the same string. The mesh's
// BroadcastChannel and the projection's ping channel are different names.

function segment(label: string, value: string): string {
    if (value.length === 0 || value.includes('/') || value.includes('\\')) {
        throw new Error(`${label} name '${value}' must be a non-empty string without slashes`);
    }
    return value;
}

// The Web Lock and the client BroadcastChannel for one host.
export function browserLockName(app: string, host: string): string {
    return `rhost-${segment('app', app)}-${segment('host', host)}`;
}

export function browserClientChannel(app: string, host: string): string {
    return browserLockName(app, host);
}

export function browserKeysDatabase(app: string): string {
    return `rhost-${segment('app', app)}-keys`;
}

export function browserHostsDatabase(app: string): string {
    return `rhost-${segment('app', app)}-hosts`;
}

export function browserReplicaDatabase(app: string, host: string): string {
    return `rhost-${segment('app', app)}-${segment('host', host)}-store`;
}

export function browserProjectionDatabase(app: string, host: string): string {
    return `rhost-${segment('app', app)}-${segment('host', host)}-data`;
}

// Origin private file system path of one file mount: rhost/<app>/<host>/<mount path>.
export function browserFilePath(app: string, host: string, mountPath: string): string[] {
    const parts = mountPath.split('/');
    if (mountPath.length === 0 || parts.some((part) => part.length === 0 || part === '.' || part === '..')) {
        throw new Error(`mount path '${mountPath}' must be a relative path`);
    }
    return ['rhost', segment('app', app), segment('host', host), ...parts];
}

// The host-records database. Both the platform and a client open this store.
export const BROWSER_HOSTS_STORE = 'hosts';
export const BROWSER_HOSTS_VERSION = 1;

// A message on the client channel. `stop` is the serving tab telling standbys
// not to take over. Everything else is one client connection.
export type BrowserChannelMessage =
    | { stop: true }
    | { client: string; line: string }
    | { client: string; close: true };

export function isBrowserChannelMessage(value: unknown): value is BrowserChannelMessage {
    if (typeof value !== 'object' || value === null) return false;
    const message = value as { stop?: unknown; client?: unknown; line?: unknown; close?: unknown };
    if (message.stop === true && message.client === undefined) return true;
    if (typeof message.client !== 'string' || message.client.length === 0) return false;
    if (message.close === true) return true;
    return typeof message.line === 'string';
}
