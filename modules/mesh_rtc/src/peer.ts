// Minimal peer-connection surface shared by the browser stack, werift, and tests.

export interface RtcIceServer {
    urls: string | string[];
    username?: string;
    credential?: string;
}

export interface RtcDescription {
    type: 'offer' | 'answer';
    sdp?: string;
}

export interface RtcCandidateInit {
    candidate?: string;
    sdpMid?: string | null;
    sdpMLineIndex?: number | null;
}

export interface RtcDataChannelLike {
    readyState: string;
    binaryType: string;
    send(data: Uint8Array): void;
    close(): void;
    onopen: (() => void) | null;
    onclose: (() => void) | null;
    onerror: (() => void) | null;
    onmessage: ((data: Uint8Array) => void) | null;
}

export interface RtcPeerLike {
    connectionState: string;
    createDataChannel(label: string): RtcDataChannelLike;
    createOffer(): Promise<RtcDescription>;
    createAnswer(): Promise<RtcDescription>;
    setLocalDescription(desc: RtcDescription): Promise<void>;
    setRemoteDescription(desc: RtcDescription): Promise<void>;
    addIceCandidate(candidate: RtcCandidateInit): Promise<void>;
    close(): void;
    onicecandidate: ((candidate: RtcCandidateInit | null) => void) | null;
    ondatachannel: ((channel: RtcDataChannelLike) => void) | null;
    onconnectionstatechange: (() => void) | null;
}

export interface SignalSocket {
    send(text: string): void;
    close(): void;
    onMessage(callback: (text: string) => void): void;
    onClose(callback: () => void): void;
}

export const DEFAULT_ICE_SERVERS: RtcIceServer[] = [
    { urls: 'stun:stun.l.google.com:19302' },
];

export const DATA_CHANNEL_LABEL = 'hhs3';
