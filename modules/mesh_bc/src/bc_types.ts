// Minimal BroadcastChannel surface so tests can inject an in-process bus
// without depending on the full DOM BroadcastChannel type.

export interface BroadcastChannelLike {
    onmessage: ((ev: MessageEvent) => void) | null;
    postMessage(message: unknown): void;
    close(): void;
}

export type BroadcastChannelCtor = new (name: string) => BroadcastChannelLike;

export function defaultBroadcastChannelCtor(): BroadcastChannelCtor {
    const ctor = (globalThis as unknown as { BroadcastChannel?: BroadcastChannelCtor }).BroadcastChannel;
    if (ctor === undefined) {
        throw new Error('no BroadcastChannel implementation available');
    }
    return ctor;
}

export const DEFAULT_BC_BASE = 'hhs3-mesh-bc';
