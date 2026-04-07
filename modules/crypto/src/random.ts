export function getInt(min:number, max: number) {
    const array = new Uint32Array(1);
    globalThis.crypto.getRandomValues(array);

    return min + (array[0] % (max - min + 1));
}

export function getBytes(n: number): Uint8Array {
    const bytes = new Uint8Array(n);
    globalThis.crypto.getRandomValues(bytes);
    return bytes;
}