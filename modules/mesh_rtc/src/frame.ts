// One mesh message may exceed the SCTP max. Frames stay inside the transport
// so Transport.send / onMessage still see one message each.

const FULL = 0x01;
const PART = 0x02;
const HEADER_PART = 9;

export const DEFAULT_MAX_FRAME = 16 * 1024;

export function encodeFrames(payload: Uint8Array, maxFrame = DEFAULT_MAX_FRAME): Uint8Array[] {
    if (payload.byteLength + 1 <= maxFrame) {
        const out = new Uint8Array(1 + payload.byteLength);
        out[0] = FULL;
        out.set(payload, 1);
        return [out];
    }
    const chunk = maxFrame - HEADER_PART;
    if (chunk <= 0) throw new Error('rtc frame size too small');
    const count = Math.ceil(payload.byteLength / chunk);
    const frames: Uint8Array[] = [];
    for (let i = 0; i < count; i++) {
        const slice = payload.subarray(i * chunk, Math.min(payload.byteLength, (i + 1) * chunk));
        const frame = new Uint8Array(HEADER_PART + slice.byteLength);
        const view = new DataView(frame.buffer);
        frame[0] = PART;
        view.setUint32(1, i);
        view.setUint32(5, count);
        frame.set(slice, HEADER_PART);
        frames.push(frame);
    }
    return frames;
}

export class FrameDecoder {
    private parts: (Uint8Array | undefined)[] | undefined;
    private expected = 0;
    private got = 0;

    push(frame: Uint8Array): Uint8Array | undefined {
        if (frame.byteLength === 0) return undefined;
        const kind = frame[0];
        if (kind === FULL) {
            this.parts = undefined;
            this.got = 0;
            return new Uint8Array(frame.subarray(1));
        }
        if (kind !== PART || frame.byteLength < HEADER_PART) return undefined;
        const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
        const index = view.getUint32(1);
        const count = view.getUint32(5);
        if (count === 0 || index >= count) return undefined;
        if (this.parts === undefined || this.expected !== count || (index === 0 && this.got > 0 && this.parts[0] !== undefined)) {
            this.parts = new Array(count).fill(undefined);
            this.expected = count;
            this.got = 0;
        }
        if (this.parts[index] === undefined) this.got++;
        this.parts[index] = new Uint8Array(frame.subarray(HEADER_PART));
        if (this.got !== count) return undefined;
        const parts = this.parts as Uint8Array[];
        const total = parts.reduce((n, p) => n + p.byteLength, 0);
        const out = new Uint8Array(total);
        let offset = 0;
        for (const part of parts) {
            out.set(part, offset);
            offset += part.byteLength;
        }
        this.parts = undefined;
        this.got = 0;
        return out;
    }
}
