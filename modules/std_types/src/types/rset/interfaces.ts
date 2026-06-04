import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

import {
    Delta,
    ForeignDep,
    NestingParent,
    Payload,
    RContext,
    RObject,
    RObjectFactory,
    SyncableObject,
    Version,
    View,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RCap } from "../rcap/interfaces.js";
import type { RAddEvent, RDeleteEvent } from "./events.js";

export interface RSet<T extends json.Literal = json.Literal> extends RObject, SyncableObject, NestingParent {
    add(element: T, at?: Version): Promise<B64Hash>;
    addWithBarrier(element: T, at?: Version): Promise<B64Hash>;
    delete(element: T, at?: Version): Promise<B64Hash>;
    deleteByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash>;
    deleteWithBarrier(element: T, at?: Version): Promise<B64Hash>;
    deleteWithBarrierByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash>;

    addSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    addWithBarrierSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    deleteSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    deleteByHashSigned(elementHash: B64Hash, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    deleteWithBarrierSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    deleteWithBarrierByHashSigned(elementHash: B64Hash, author: OwnIdentity, at?: Version): Promise<B64Hash>;
    refAdvance(refVersion: Version, author: OwnIdentity, at?: Version): Promise<B64Hash>;

    validatePayload(payload: Payload, at: Version): Promise<boolean>;
    applyPayload(payload: Payload, at: Version): Promise<B64Hash>;
    getView(at?: Version, from?: Version): Promise<RSetView<T>>;

    getContext(): RContext;
    configure(config: { meshLabel?: string }): void;
    loadChildObject(innerFactory: RObjectFactory, elementHash: B64Hash): Promise<RObject>;

    seed(): string;
    contentType(): string | undefined;
    acceptRedundantAdd(): boolean;
    acceptRedundantDelete(): boolean;
    acceptUpdateForDeleted(): boolean;
    supportBarrierAdd(): boolean;
    supportBarrierDelete(): boolean;
    isPermissioned(): boolean;
    capabilityRef(): B64Hash | undefined;
    capRequirementForAdd(): string | undefined;
    capRequirementForDelete(): string | undefined;
    refAdvanceCaps(): string[];
    refAdvanceCreators(): boolean;
    selfValidate(): boolean;
    extractForeignDeps(payload: Payload, at: Version): ForeignDep[] | undefined;
    loadRCap(): Promise<RCap | undefined>;

    subscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void;
    unsubscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void;
    setDeltaStrategy(strategy: "full" | "bounded"): void;
    computeDelta(start: Version, end: Version): Promise<Delta>;
}

export interface RSetView<T extends json.Literal = json.Literal> extends View {
    getObject(): RSet<T>;
    has(element: T): Promise<boolean>;
    hasByHash(elementHash: B64Hash): Promise<boolean>;
    checkEntryAuthorization(entryHash: B64Hash): Promise<boolean>;
    getReferences(): Promise<B64Hash[]>;
    resolveRefVersion(refId: B64Hash): Promise<Version>;
    loadRObjectByHash(elementHash: B64Hash): Promise<RObject | undefined>;
}
