import { HashFn } from "./hash";
import * as sha from "./sha";

export type BasicCrypto = {
    hash: {
        sha256: HashFn;
    };
};

export function createBasicCrypto(): BasicCrypto {
    return { hash: { sha256: sha.sha256 } };
}
