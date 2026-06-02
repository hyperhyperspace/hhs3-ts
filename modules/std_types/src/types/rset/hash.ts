import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";

export function hashElement<T extends json.Literal>(element: T): B64Hash {
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(element)));
}
