// JSON text frames on the signaling WebSocket. SDP and ICE candidates are
// opaque. Topics never appear here.

import type { DelegationWire } from './codec.js';

export interface HelloMsg { type: 'hello'; nonce: string }
export interface RegisterMsg { type: 'register'; delegation: DelegationWire; possessionSig: string }
export interface RegisteredMsg { type: 'registered' }
export interface RenewMsg { type: 'renew'; nonce: string }
export interface DialMsg { type: 'dial'; session: string; from?: string }
export interface WarrantMsg { type: 'warrant'; session: string; delegation: DelegationWire }
export interface ByeMsg { type: 'bye'; session: string }
export interface RejectMsg { type: 'reject'; session?: string; reason: string }

export interface OfferData { kind: 'offer'; sdp: string }
export interface AnswerData { kind: 'answer'; sdp: string }
export interface CandidateData {
    kind: 'candidate';
    candidate: string;
    sdpMid?: string | null;
    sdpMLineIndex?: number | null;
}
export type SignalData = OfferData | AnswerData | CandidateData;
export interface SignalMsg { type: 'signal'; session: string; data: SignalData }

export type SignalMessage =
    | HelloMsg
    | RegisterMsg
    | RegisteredMsg
    | RenewMsg
    | DialMsg
    | WarrantMsg
    | SignalMsg
    | ByeMsg
    | RejectMsg;

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function isWire(value: unknown): value is DelegationWire {
    if (!isRecord(value)) return false;
    return typeof value.publicKey === 'string'
        && typeof value.origin === 'string'
        && typeof value.endpointId === 'string'
        && typeof value.expiry === 'number'
        && typeof value.signature === 'string';
}

function isSignalData(value: unknown): value is SignalData {
    if (!isRecord(value) || typeof value.kind !== 'string') return false;
    if (value.kind === 'offer' || value.kind === 'answer') return typeof value.sdp === 'string';
    if (value.kind === 'candidate') return typeof value.candidate === 'string';
    return false;
}

export function parseSignalMessage(text: string): SignalMessage {
    let value: unknown;
    try {
        value = JSON.parse(text);
    } catch {
        throw new Error('malformed signaling message');
    }
    if (!isRecord(value) || typeof value.type !== 'string') {
        throw new Error('malformed signaling message');
    }
    switch (value.type) {
        case 'hello':
        case 'renew':
            if (typeof value.nonce !== 'string') break;
            return value.type === 'hello'
                ? { type: 'hello', nonce: value.nonce }
                : { type: 'renew', nonce: value.nonce };
        case 'register':
            if (!isWire(value.delegation) || typeof value.possessionSig !== 'string') break;
            return { type: 'register', delegation: value.delegation, possessionSig: value.possessionSig };
        case 'registered':
            return { type: 'registered' };
        case 'dial':
            if (typeof value.session !== 'string') break;
            if (value.from !== undefined && typeof value.from !== 'string') break;
            return { type: 'dial', session: value.session, from: value.from as string | undefined };
        case 'warrant':
            if (typeof value.session !== 'string' || !isWire(value.delegation)) break;
            return { type: 'warrant', session: value.session, delegation: value.delegation };
        case 'signal':
            if (typeof value.session !== 'string' || !isSignalData(value.data)) break;
            return { type: 'signal', session: value.session, data: value.data };
        case 'bye':
            if (typeof value.session !== 'string') break;
            return { type: 'bye', session: value.session };
        case 'reject':
            if (typeof value.reason !== 'string') break;
            if (value.session !== undefined && typeof value.session !== 'string') break;
            return { type: 'reject', session: value.session as string | undefined, reason: value.reason };
        default:
            break;
    }
    throw new Error('malformed signaling message');
}

export function encodeSignalMessage(msg: SignalMessage): string {
    return JSON.stringify(msg);
}
