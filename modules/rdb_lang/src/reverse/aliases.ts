import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

export type RenderAliasScope = 'key' | 'schema' | 'group' | 'db' | 'version';

export type RenderVersionScope = {
    objectId: B64Hash;
    objectName: string;
};

export interface RenderAliasContext {
    /** Register if needed; return the name to use in output */
    key(keyId: B64Hash, hint?: string): string;
    schema(id: B64Hash, hint?: string): string;
    group(id: B64Hash, hint?: string): string;
    db(id: B64Hash, hint?: string): string;
    /** Lazy: only allocates a name the first time this hash is referenced */
    version(hash: B64Hash, scope: RenderVersionScope): string;
    /** `\alias` lines not yet emitted for the upcoming statement */
    drainDefinitions(): string[];
    /** Returns alias name if this keyId was already registered via key(); does not allocate */
    lookupKeyAlias?(keyId: B64Hash): string | undefined;
    /** Returns alias name if serialized public key matches a registered key with known public key */
    lookupPublicKeyAlias?(serialized: string): string | undefined;
}
