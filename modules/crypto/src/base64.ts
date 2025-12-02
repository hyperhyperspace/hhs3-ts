
export function fromArrayBuffer(digest: ArrayBuffer) {
    let binary = '';
    let bytes = new Uint8Array(digest);
    let len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}

export function toArrayBuffer(b64: string): ArrayBuffer {
    const raw = atob(b64);

    const array = new Uint8Array(raw.length);

    for (let i=0; i<raw.length; i++) {
        array[i] = raw.charCodeAt(i);
    }

    return array.buffer;    
} 