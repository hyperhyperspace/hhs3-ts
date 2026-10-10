import type {
    RtcDataChannelLike,
    RtcDescription,
    RtcPeerLike,
} from '@hyper-hyper-space/hhs3_mesh_rtc';

const offers = new Map<string, FakePeer>();
let seq = 0;

export function resetFakePeers(): void {
    offers.clear();
}

export class FakePeer implements RtcPeerLike {
    connectionState = 'new';
    onicecandidate: RtcPeerLike['onicecandidate'] = null;
    ondatachannel: RtcPeerLike['ondatachannel'] = null;
    onconnectionstatechange: RtcPeerLike['onconnectionstatechange'] = null;

    channel?: FakeChannel;
    private local?: RtcDescription;
    private remote?: RtcDescription;
    private linked = false;

    createDataChannel(_label: string): RtcDataChannelLike {
        this.channel = new FakeChannel();
        return this.channel;
    }

    async createOffer(): Promise<RtcDescription> {
        return { type: 'offer', sdp: `offer-${++seq}` };
    }

    async createAnswer(): Promise<RtcDescription> {
        return { type: 'answer', sdp: `answer-for-${this.remote?.sdp ?? 'none'}` };
    }

    async setLocalDescription(desc: RtcDescription): Promise<void> {
        this.local = desc;
        if (desc.type === 'offer' && desc.sdp !== undefined) offers.set(desc.sdp, this);
        if (desc.type === 'answer') this.tryLink();
        queueMicrotask(() => this.onicecandidate?.({ candidate: `cand-${desc.type}`, sdpMid: '0', sdpMLineIndex: 0 }));
    }

    async setRemoteDescription(desc: RtcDescription): Promise<void> {
        this.remote = desc;
        this.tryLink();
    }

    async addIceCandidate(): Promise<void> {}

    close(): void {
        this.connectionState = 'closed';
        this.channel?.close();
        this.onconnectionstatechange?.();
    }

    private tryLink(): void {
        if (this.linked) return;
        if (this.local?.type !== 'answer' || this.remote?.type !== 'offer' || this.remote.sdp === undefined) return;
        const offerer = offers.get(this.remote.sdp);
        if (offerer === undefined || offerer.channel === undefined) return;
        this.linked = true;
        offerer.linked = true;
        const incoming = new FakeChannel();
        offerer.channel.link(incoming);
        incoming.link(offerer.channel);
        const offererChannel = offerer.channel;
        queueMicrotask(() => {
            offererChannel.fireOpen();
            incoming.fireOpen();
            this.ondatachannel?.(incoming);
        });
    }
}

class FakeChannel implements RtcDataChannelLike {
    readyState = 'connecting';
    binaryType = 'arraybuffer';
    onopen: (() => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;
    onmessage: ((data: Uint8Array) => void) | null = null;
    private other?: FakeChannel;

    link(other: FakeChannel): void { this.other = other; }

    send(data: Uint8Array): void {
        const copy = new Uint8Array(data);
        queueMicrotask(() => this.other?.onmessage?.(copy));
    }

    close(): void {
        if (this.readyState === 'closed') return;
        this.readyState = 'closed';
        this.onclose?.();
    }

    fireOpen(): void {
        this.readyState = 'open';
        this.onopen?.();
    }
}

export function hungPeer(): RtcPeerLike {
    const channel: RtcDataChannelLike = {
        readyState: 'connecting',
        binaryType: 'arraybuffer',
        send() {},
        close() { this.readyState = 'closed'; this.onclose?.(); },
        onopen: null,
        onclose: null,
        onerror: null,
        onmessage: null,
    };
    return {
        connectionState: 'connecting',
        createDataChannel: () => channel,
        async createOffer() { return { type: 'offer', sdp: 'hung-offer' }; },
        async createAnswer() { return { type: 'answer', sdp: 'hung-answer' }; },
        async setLocalDescription() {},
        async setRemoteDescription() {},
        async addIceCandidate() {},
        close() {},
        onicecandidate: null,
        ondatachannel: null,
        onconnectionstatechange: null,
    };
}
