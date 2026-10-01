// Public RFileMap interfaces.

import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { Delta, RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";

import type { FileElement } from "./payload.js";

export type ListedFile = FileElement & { id: B64Hash };

// Elements whose presence differs between the delta's start and end views.
export type FileMapChanges = {
    added: ListedFile[];
    removed: ListedFile[];
};

export type FileMapDelta = Delta<FileMapChanges>;

export interface RFileMapView extends View {
    getObject(): RFileMap;
    has(element: FileElement): Promise<boolean>;
    list(): Promise<ListedFile[]>;
}

export interface RFileMap extends RObject {
    getName(): string;
    getGroupId(): B64Hash;
    getBlobStoreId(): B64Hash;

    // Signed writes at the frontier. Each first appends a ref-advance when the
    // bound group has moved past the version the map observes.
    add(element: FileElement, author: OwnIdentity): Promise<B64Hash>;
    remove(element: FileElement, author: OwnIdentity): Promise<B64Hash>;
    refAdvance(author: OwnIdentity, to?: Version): Promise<B64Hash>;

    list(at?: Version): Promise<ListedFile[]>;

    // The key `author` has in the bound group's provider, as observed at `at`.
    authorKey(author: KeyId, at?: Version): Promise<PublicKey | undefined>;
    canWrite(author: KeyId, at?: Version): Promise<boolean>;
    // The same at the bound group's frontier: what the next write observes.
    authorKeyNow(author: KeyId): Promise<PublicKey | undefined>;
    canWriteNow(author: KeyId): Promise<boolean>;

    getView(at?: Version, from?: Version): Promise<RFileMapView>;
    computeDelta(start: Version, end: Version): Promise<FileMapDelta>;
}
