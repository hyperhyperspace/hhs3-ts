export type Hash = string;

export type HashFn = (content: Uint8Array | string) => Promise<Hash>;

export function stringToUint8Array(str: string): Uint8Array {
    return new TextEncoder().encode(str);
}

export function uint8ArrayToString(bytes: Uint8Array): string {
    return new TextDecoder().decode(bytes);
}