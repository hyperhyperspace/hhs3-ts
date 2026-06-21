import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { RObject, ScopedDag, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RSchema, RTable, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import type { HashRef, NameOrHashRef, TableRef, VersionExpr } from "../syntax/ast.js";

export type HashScope =
    | { kind: 'global' }
    | { kind: 'object'; objectId: B64Hash };

export type VersionScope =
    | { kind: 'schema'; id: B64Hash; schema?: RSchema }
    | { kind: 'group'; id: B64Hash; group?: RTableGroup }
    | { kind: 'table'; groupId: B64Hash; tableName: string; table?: RTable }
    | { kind: 'object'; id: B64Hash; object?: RObject };

export type LangValue =
    | json.Literal
    | null
    | OwnIdentity
    | { keyId: KeyId; publicKey?: PublicKey }
    | { kind: 'key-id'; keyId: KeyId };

export type ResolvedSchemaRef = { id: B64Hash; schema?: RSchema };
export type ResolvedGroupRef = { id: B64Hash; group?: RTableGroup };
export type ResolvedTableRef = {
    groupId: B64Hash;
    group: RTableGroup;
    tableName: string;
    table: RTable;
};

export type LoggableObject = RObject & {
    getScopedDag(): Promise<ScopedDag>;
};

export type ResolvedLogTarget =
    | { kind: 'database'; id: B64Hash; object: RDb & LoggableObject }
    | { kind: 'schema'; id: B64Hash; object: RSchema & LoggableObject }
    | { kind: 'group'; id: B64Hash; object: RTableGroup & LoggableObject }
    | { kind: 'table'; id: B64Hash; object: RTable & LoggableObject; groupId: B64Hash; tableName: string };

export interface LangBindContext {
    resolveSchema(ref: NameOrHashRef): Promise<ResolvedSchemaRef>;
    resolveGroup(ref: NameOrHashRef): Promise<ResolvedGroupRef>;
    resolveTable(ref: TableRef): Promise<ResolvedTableRef>;
    resolveDefaultGroup?(): Promise<NameOrHashRef | undefined>;
    resolveHash(ref: NameOrHashRef, scope: HashScope): Promise<B64Hash>;
    resolveRowId?(ref: HashRef, table: ResolvedTableRef, at: Version, from?: Version): Promise<B64Hash>;
    resolveVersion(expr: VersionExpr | undefined, scope: VersionScope): Promise<Version>;
    resolveDefaultView?(scope: VersionScope): Promise<{ at: Version; from?: Version } | undefined>;
    resolveVariable(name: string): Promise<LangValue>;
    resolveLogTarget(ref: NameOrHashRef): Promise<ResolvedLogTarget>;
    currentAuthor(): Promise<OwnIdentity | undefined>;
    createUuid(): string;
    createSeed(kind: 'rdb' | 'schema' | 'group', name?: string): string;
}
