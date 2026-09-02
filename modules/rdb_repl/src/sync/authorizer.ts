import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { PeerAuthorizer } from "@hyper-hyper-space/hhs3_mesh";

import { allowIsEveryone, type AllowSource } from "./parse.js";

export type ColumnLookup = (source: Extract<AllowSource, { type: 'column' }>) => Promise<Iterable<unknown>>;

export function createAllowAuthorizer(
    sources: AllowSource[],
    lookup: ColumnLookup,
): PeerAuthorizer | undefined {
    if (allowIsEveryone(sources)) return undefined;
    const columns = sources.filter((s): s is Extract<AllowSource, { type: 'column' }> => s.type === 'column');
    return {
        async authorize(keyId: KeyId): Promise<boolean> {
            for (const source of columns) {
                let values: Iterable<unknown>;
                try {
                    values = await lookup(source);
                } catch {
                    continue;
                }
                for (const value of values) {
                    if (value === keyId || String(value) === keyId) return true;
                }
            }
            return false;
        },
    };
}
