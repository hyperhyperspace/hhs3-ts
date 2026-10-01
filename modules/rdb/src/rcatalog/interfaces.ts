// Public RCatalog interfaces.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";

import type {
    CatalogGroupDef, CatalogFilesDef, CatalogGroupChange, CatalogParamDecl, SchemaCreator,
} from "./payload.js";
import type { DeclareInfo, ReleaseState } from "./resolve.js";

// What a release adds on top of its parents (see RCatalog.release).
export type CatalogReleaseSpec = {
    version: string;
    changes?: { [catalogGroupHash: string]: CatalogGroupChange };
    add?: CatalogGroupDef[];
    files?: CatalogFilesDef[];
    params?: CatalogParamDecl[];
    note?: string;
};

export type PublishedRelease = {
    declare?: B64Hash;
    release: B64Hash;
};

export interface RCatalog extends RObject {
    getName(): string;
    hashAlgorithm(): string | undefined;
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;

    // Appends a signed release at `at` (defaults to the frontier).
    release(spec: CatalogReleaseSpec, author: OwnIdentity, at?: Version): Promise<B64Hash>;

    // Appends a signed declare naming `schemas` at `at`.
    declare(schemas: B64Hash[], author: OwnIdentity, at?: Version): Promise<B64Hash>;

    // Declares the schemas the release references that are not referenced
    // yet, then chains the release on top of the declare.
    publishRelease(spec: CatalogReleaseSpec, author: OwnIdentity, at?: Version): Promise<PublishedRelease>;

    getView(at?: Version, from?: Version): Promise<RCatalogView>;
}

// A view over the catalog at a version. The catalog has no barriers, so `from`
// never changes what a view shows.
export interface RCatalogView extends View {
    getObject(): RCatalog;

    getName(): string;
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;

    getGenesisHash(): B64Hash;

    // Every release at or below this view's version (local topological order).
    getReleaseHashes(): B64Hash[];
    getRelease(hash: B64Hash): ReleaseState | undefined;
    getMaximalReleases(): B64Hash[];
    findReleasesByVersion(version: string): B64Hash[];

    // `a` strictly in `b`'s causal past (both releases at or below the view).
    isReleaseBelow(a: B64Hash, b: B64Hash): boolean;

    getGroupDef(hash: B64Hash): CatalogGroupDef | undefined;
    getFilesDef(hash: B64Hash): CatalogFilesDef | undefined;

    getReferencedSchemas(): B64Hash[];
    getDeclares(): DeclareInfo[];
}
