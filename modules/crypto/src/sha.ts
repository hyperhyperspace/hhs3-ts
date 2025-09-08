import { fromArrayBuffer } from "./base64";
import { Hash, stringToUint8Array } from "./hash";

export async function sha256(contents: Uint8Array|string): Promise<Hash>  {

    if (typeof contents === 'string') {
        contents = stringToUint8Array(contents);
    }

    return fromArrayBuffer(await crypto.subtle.digest('SHA-256', contents));
}
