// Noise-like authenticated key exchange over a raw Transport. Uses a 3-message
// (1.5 round-trip) handshake: the initiator sends its signing identity and KEM
// preference list, the responder picks the best common KEM suite and generates
// an ephemeral keypair, the initiator encapsulates to it, and both derive AEAD
// session keys. All post-handshake traffic is encrypted with ChaCha20-Poly1305.

import type {
    PublicKey, KeyId, SigningSuite, KemName, SigningName,
    AeadSuite, KdfSuite,
} from '@hyper-hyper-space/hhs3_crypto';
import {
    serializePublicKey, deserializePublicKey, keyIdFromPublicKey,
    getSigningSuite, getKemSuite,
    sha256 as sha256Suite, chacha20Poly1305, hkdfSha256,
} from '@hyper-hyper-space/hhs3_crypto';
import type { Transport } from './transport.js';
import type { AuthenticatedChannel, PeerAuthenticator } from './authenticator.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

export interface NoiseAuthenticatorConfig {
    localKey: { publicKey: PublicKey; secretKey: Uint8Array };
    signingName: SigningName;
    kemPrefs: KemName[];
}

// ---------------------------------------------------------------------------
// Binary field helpers
// ---------------------------------------------------------------------------

function writeFields(fields: Uint8Array[]): Uint8Array {
    let total = 4;
    for (const f of fields) total += 4 + f.length;
    const buf = new Uint8Array(total);
    const view = new DataView(buf.buffer);
    view.setUint32(0, fields.length, false);
    let off = 4;
    for (const f of fields) {
        view.setUint32(off, f.length, false);
        buf.set(f, off + 4);
        off += 4 + f.length;
    }
    return buf;
}

function readFields(buf: Uint8Array): Uint8Array[] {
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const count = view.getUint32(0, false);
    const fields: Uint8Array[] = [];
    let off = 4;
    for (let i = 0; i < count; i++) {
        const len = view.getUint32(off, false);
        fields.push(buf.slice(off + 4, off + 4 + len));
        off += 4 + len;
    }
    return fields;
}

function toUtf8(s: string): Uint8Array {
    return new TextEncoder().encode(s);
}

function fromUtf8(b: Uint8Array): string {
    return new TextDecoder().decode(b);
}

function concatBytes(...arrays: Uint8Array[]): Uint8Array {
    const total = arrays.reduce((s, a) => s + a.length, 0);
    const r = new Uint8Array(total);
    let off = 0;
    for (const a of arrays) { r.set(a, off); off += a.length; }
    return r;
}

function hashRaw(input: Uint8Array): Uint8Array {
    return sha256Suite.hash(input);
}

// ---------------------------------------------------------------------------
// Message queue: promise-based message reading during handshake
// ---------------------------------------------------------------------------

class MessageQueue {
    private queue: Uint8Array[] = [];
    private waiting: ((msg: Uint8Array) => void) | null = null;
    private errCb: ((err: Error) => void) | null = null;
    private done = false;

    constructor(transport: Transport) {
        transport.onMessage((msg) => {
            if (this.done) return;
            if (this.waiting) {
                const resolve = this.waiting;
                this.waiting = null;
                resolve(msg);
            } else {
                this.queue.push(msg);
            }
        });
        transport.onClose(() => {
            if (this.done) return;
            if (this.errCb) {
                this.errCb(new Error('transport closed during handshake'));
                this.errCb = null;
                this.waiting = null;
            }
        });
    }

    next(): Promise<Uint8Array> {
        if (this.queue.length > 0) {
            return Promise.resolve(this.queue.shift()!);
        }
        return new Promise<Uint8Array>((resolve, reject) => {
            this.waiting = resolve;
            this.errCb = reject;
        });
    }

    finish(): void {
        this.done = true;
        this.waiting = null;
        this.errCb = null;
    }
}

// ---------------------------------------------------------------------------
// Encrypted channel wrapper
// ---------------------------------------------------------------------------

const NONCE_SIZE = 12;

function makeNonce(counter: number): Uint8Array {
    const nonce = new Uint8Array(NONCE_SIZE);
    const view = new DataView(nonce.buffer);
    view.setUint32(4, (counter >>> 0) & 0xFFFFFFFF, true);
    view.setUint32(8, Math.floor(counter / 0x100000000) & 0xFFFFFFFF, true);
    return nonce;
}

class EncryptedChannel implements AuthenticatedChannel {
    readonly remotePeer: PublicKey;
    readonly remoteKeyId: KeyId;

    private transport: Transport;
    private aead: AeadSuite;
    private sendKey: Uint8Array;
    private recvKey: Uint8Array;
    private sendCounter = 0;
    private recvCounter = 0;
    private messageCallbacks: ((msg: Uint8Array) => void)[] = [];
    private closeCallbacks: (() => void)[] = [];
    private _closed = false;

    constructor(
        transport: Transport,
        remotePeer: PublicKey,
        remoteKeyId: KeyId,
        aead: AeadSuite,
        sendKey: Uint8Array,
        recvKey: Uint8Array,
    ) {
        this.transport = transport;
        this.remotePeer = remotePeer;
        this.remoteKeyId = remoteKeyId;
        this.aead = aead;
        this.sendKey = sendKey;
        this.recvKey = recvKey;

        transport.onMessage((ct) => {
            if (this._closed) return;
            try {
                const nonce = makeNonce(this.recvCounter++);
                const pt = this.aead.decrypt(ct, this.recvKey, nonce);
                for (const cb of this.messageCallbacks) cb(pt);
            } catch {
                this.close();
            }
        });

        transport.onClose(() => {
            if (this._closed) return;
            this._closed = true;
            for (const cb of this.closeCallbacks) cb();
        });
    }

    get open(): boolean {
        return !this._closed && this.transport.open;
    }

    send(message: Uint8Array): void {
        if (!this.open) throw new Error('channel closed');
        const nonce = makeNonce(this.sendCounter++);
        const ct = this.aead.encrypt(message, this.sendKey, nonce);
        this.transport.send(ct);
    }

    close(): void {
        if (this._closed) return;
        this._closed = true;
        this.transport.close();
        for (const cb of this.closeCallbacks) cb();
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.messageCallbacks.push(callback);
    }

    onClose(callback: () => void): void {
        this.closeCallbacks.push(callback);
    }
}

// ---------------------------------------------------------------------------
// KEM negotiation
// ---------------------------------------------------------------------------

function negotiate(initiatorPrefs: string[], responderPrefs: string[]): string | undefined {
    for (const pref of initiatorPrefs) {
        if (responderPrefs.includes(pref)) return pref;
    }
    return undefined;
}

// ---------------------------------------------------------------------------
// Handshake protocol labels
// ---------------------------------------------------------------------------

const LABEL_I2R = toUtf8('hhs3-noise-i2r');
const LABEL_R2I = toUtf8('hhs3-noise-r2i');
const LABEL_CONFIRM = toUtf8('hhs3-noise-confirm');

// ---------------------------------------------------------------------------
// Handshake implementation
// ---------------------------------------------------------------------------

async function handshakeAsInitiator(
    transport: Transport,
    queue: MessageQueue,
    localKey: { publicKey: PublicKey; secretKey: Uint8Array },
    signing: SigningSuite,
    kemPrefs: KemName[],
    kdf: KdfSuite,
    aead: AeadSuite,
    expectedRemote?: KeyId,
): Promise<AuthenticatedChannel> {

    // --- Msg1: [serialized_pk, kem_prefs_json, sig1] ---
    const localPkBytes = serializePublicKey(localKey.publicKey);
    const prefsJson = toUtf8(JSON.stringify(kemPrefs));

    const msg1PreSig = writeFields([localPkBytes, prefsJson]);
    const transcript1 = hashRaw(msg1PreSig);
    const sig1 = await signing.sign(transcript1, localKey.secretKey);

    const msg1 = writeFields([localPkBytes, prefsJson, sig1]);
    transport.send(msg1);

    // --- Msg2: [serialized_pk, chosen_kem, eph_kem_pk, sig2] ---
    const msg2 = await queue.next();
    const msg2Fields = readFields(msg2);
    if (msg2Fields.length < 4) throw new Error('invalid msg2');

    const remotePk = deserializePublicKey(msg2Fields[0]);
    const chosenKemName = fromUtf8(msg2Fields[1]);
    const ephKemPk = msg2Fields[2];
    const sig2 = msg2Fields[3];

    if (expectedRemote !== undefined) {
        const remoteKeyId = keyIdFromPublicKey(remotePk, sha256Suite);
        if (remoteKeyId !== expectedRemote) {
            throw new Error('unexpected remote peer identity');
        }
    }

    const remoteSigning = getSigningSuite(remotePk.suite);
    if (!remoteSigning) throw new Error(`unknown signing suite: ${remotePk.suite}`);

    const msg2PreSig = writeFields([msg2Fields[0], msg2Fields[1], msg2Fields[2]]);
    const transcript2 = hashRaw(concatBytes(transcript1, msg2PreSig));
    const sig2Valid = await remoteSigning.verify(transcript2, sig2, remotePk.key);
    if (!sig2Valid) throw new Error('msg2 signature verification failed');

    if (!kemPrefs.includes(chosenKemName as KemName)) {
        throw new Error(`responder chose unsupported KEM: ${chosenKemName}`);
    }
    const kem = getKemSuite(chosenKemName);
    if (!kem) throw new Error(`KEM suite not found: ${chosenKemName}`);

    // --- Msg3: [kem_ciphertext, aead_confirmation] ---
    const { ciphertext, sharedSecret } = await kem.encapsulate(ephKemPk);

    const transcript3 = hashRaw(concatBytes(transcript2, ciphertext));

    const i2rKey = kdf.deriveKey(sharedSecret, transcript3, LABEL_I2R, aead.keySize);
    const r2iKey = kdf.deriveKey(sharedSecret, transcript3, LABEL_R2I, aead.keySize);

    const confirmNonce = new Uint8Array(aead.nonceSize);
    const confirmCt = aead.encrypt(LABEL_CONFIRM, i2rKey, confirmNonce);

    const msg3 = writeFields([ciphertext, confirmCt]);
    transport.send(msg3);

    queue.finish();

    const remoteKeyId = keyIdFromPublicKey(remotePk, sha256Suite);
    return new EncryptedChannel(transport, remotePk, remoteKeyId, aead, i2rKey, r2iKey);
}

async function handshakeAsResponder(
    transport: Transport,
    queue: MessageQueue,
    localKey: { publicKey: PublicKey; secretKey: Uint8Array },
    signing: SigningSuite,
    kemPrefs: KemName[],
    kdf: KdfSuite,
    aead: AeadSuite,
): Promise<AuthenticatedChannel> {

    // --- Msg1: [serialized_pk, kem_prefs_json, sig1] ---
    const msg1 = await queue.next();
    const msg1Fields = readFields(msg1);
    if (msg1Fields.length < 3) throw new Error('invalid msg1');

    const remotePk = deserializePublicKey(msg1Fields[0]);
    const initiatorPrefs: string[] = JSON.parse(fromUtf8(msg1Fields[1]));
    const sig1 = msg1Fields[2];

    const remoteSigning = getSigningSuite(remotePk.suite);
    if (!remoteSigning) throw new Error(`unknown signing suite: ${remotePk.suite}`);

    const msg1PreSig = writeFields([msg1Fields[0], msg1Fields[1]]);
    const transcript1 = hashRaw(msg1PreSig);
    const sig1Valid = await remoteSigning.verify(transcript1, sig1, remotePk.key);
    if (!sig1Valid) throw new Error('msg1 signature verification failed');

    const chosenKemName = negotiate(initiatorPrefs, kemPrefs as string[]);
    if (!chosenKemName) throw new Error('no common KEM suite');

    const kem = getKemSuite(chosenKemName);
    if (!kem) throw new Error(`KEM suite not found: ${chosenKemName}`);

    const ephKp = await kem.generateKeyPair();

    // --- Msg2: [serialized_pk, chosen_kem, eph_kem_pk, sig2] ---
    const localPkBytes = serializePublicKey(localKey.publicKey);
    const chosenKemBytes = toUtf8(chosenKemName);

    const msg2PreSig = writeFields([localPkBytes, chosenKemBytes, ephKp.publicKey]);
    const transcript2 = hashRaw(concatBytes(transcript1, msg2PreSig));
    const sig2 = await signing.sign(transcript2, localKey.secretKey);

    const msg2 = writeFields([localPkBytes, chosenKemBytes, ephKp.publicKey, sig2]);
    transport.send(msg2);

    // --- Msg3: [kem_ciphertext, aead_confirmation] ---
    const msg3 = await queue.next();
    const msg3Fields = readFields(msg3);
    if (msg3Fields.length < 2) throw new Error('invalid msg3');

    const ciphertext = msg3Fields[0];
    const confirmCt = msg3Fields[1];

    const sharedSecret = await kem.decapsulate(ciphertext, ephKp.secretKey);

    const transcript3 = hashRaw(concatBytes(transcript2, ciphertext));

    const i2rKey = kdf.deriveKey(sharedSecret, transcript3, LABEL_I2R, aead.keySize);
    const r2iKey = kdf.deriveKey(sharedSecret, transcript3, LABEL_R2I, aead.keySize);

    const confirmNonce = new Uint8Array(aead.nonceSize);
    try {
        const confirmPt = aead.decrypt(confirmCt, i2rKey, confirmNonce);
        const expected = LABEL_CONFIRM;
        if (confirmPt.length !== expected.length) throw new Error('bad confirm');
        for (let i = 0; i < expected.length; i++) {
            if (confirmPt[i] !== expected[i]) throw new Error('bad confirm');
        }
    } catch {
        throw new Error('handshake confirmation failed');
    }

    queue.finish();

    const remoteKeyId = keyIdFromPublicKey(remotePk, sha256Suite);
    return new EncryptedChannel(transport, remotePk, remoteKeyId, aead, r2iKey, i2rKey);
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createNoiseAuthenticator(config: NoiseAuthenticatorConfig): PeerAuthenticator {
    const { localKey, signingName, kemPrefs } = config;

    const signing = getSigningSuite(signingName);
    if (!signing) throw new Error(`signing suite not found: ${signingName}`);
    if (kemPrefs.length === 0) throw new Error('kemPrefs must not be empty');
    for (const name of kemPrefs) {
        if (!getKemSuite(name)) throw new Error(`KEM suite not found: ${name}`);
    }

    const kdf = hkdfSha256;
    const aead = chacha20Poly1305;

    return {
        async authenticate(
            transport: Transport,
            expectedRemote?: KeyId,
        ): Promise<AuthenticatedChannel> {
            const queue = new MessageQueue(transport);
            if (expectedRemote !== undefined) {
                return handshakeAsInitiator(
                    transport, queue, localKey, signing, kemPrefs, kdf, aead, expectedRemote,
                );
            } else {
                return handshakeAsResponder(
                    transport, queue, localKey, signing, kemPrefs, kdf, aead,
                );
            }
        },
    };
}
