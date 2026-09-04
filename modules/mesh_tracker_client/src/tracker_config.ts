// Default tracker endpoints per mesh environment and a small resolver that maps
// a MeshScope (plus optional overrides) to a concrete tracker address and key.
// No RDb / process-env knowledge lives here: callers pass explicit overrides.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { MeshScope } from '@hyper-hyper-space/hhs3_mesh';

export const DEFAULT_LOCAL_TRACKER = 'ws://127.0.0.1:4610';
export const DEFAULT_INTERNET_TRACKER = 'wss://mypeer.net:4610';
export const DEFAULT_INTERNET_TRACKER_KEY = 'VjrVDsjsuEOzhsREAA0jl9Wj/TnQu8IF+XMLLbnXBeQ=';

export type ResolvedTracker = {
    address: string;
    keyId?: KeyId;
};

export type TrackerOverrides = {
    tracker?: string;
    trackerKey?: string;
};

export function resolveTrackerConfig(
    scope: MeshScope,
    flags: TrackerOverrides = {},
    env: TrackerOverrides = {},
): ResolvedTracker {
    const address = flags.tracker ?? env.tracker ?? (scope === 'internet' ? DEFAULT_INTERNET_TRACKER : DEFAULT_LOCAL_TRACKER);
    const keyId = (flags.trackerKey ?? env.trackerKey ?? (address === DEFAULT_INTERNET_TRACKER ? DEFAULT_INTERNET_TRACKER_KEY : undefined)) as KeyId | undefined;
    return keyId === undefined ? { address } : { address, keyId };
}
