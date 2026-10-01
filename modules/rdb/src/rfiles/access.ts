// Write access for RBlobStore and RFileMap: ordinary state observation of one
// bound RTableGroup.
//
// Both create payloads carry the same three fields, copied from the catalog
// FILES definition:
//
//   bindings     { alias: groupId }, exactly one entry
//   idProvider   'alias.table', an identity provider table of that group
//   canWrite     an 'object'-context predicate ($author, no subject row) whose
//                tables are all qualified with the alias
//
// Every op is signed. An op's observed group version is the usual mvt
// resolution resolveRefVersionAtPosition(dag, groupId, at, at): the ref-advances
// in its causal past, or the group's genesis when there are none. The op is
// admitted when its author's key resolves through the provider at that version
// and canWrite holds there. A ref-advance is admitted at the version it moves
// to, after the usual monotonicity check. Ref-advances are non-barrier:
// admission is validation-only, so a key that later loses access can still
// append after a position from before the revocation (no view-time voiding).
//
// A missing bound group object throws (the sync layer defers), never rejects.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext, ScopedDag, Version, ValidationResult, SigningScope, RefAdvancePayload,
    validationFailure, validationOk, verifyPayloadSignature,
    resolveRefVersionAtPosition, validateRefAdvanceMonotonicity, extractRefVersion,
} from "@hyper-hyper-space/hhs3_mvt";

import {
    Predicate, TableDef, splitTableRef, MAX_NAME_LENGTH, MAX_QUALIFIED_NAME_LENGTH, MAX_HASH_LENGTH,
    MAX_KEY_ID_LENGTH, MAX_SIGNATURE_LENGTH,
} from "../rschema/payload.js";
import { isValidName, validatePredicate, collectExistsAtoms, type ExistsAtom } from "../rschema/validate.js";
import { evaluatePredicate } from "../rtable_group/predicates.js";
import type { RTableGroupImpl } from "../rtable_group/group.js";
import type { RTableView } from "../rtable/interfaces.js";
import { decodeCanonicalBase64, isContentHash } from "./hashes.js";

export const MAX_REF_VERSION_WIDTH = 64;

export type FilesAccess = {
    bindings: { [alias: string]: B64Hash };
    idProvider: string;
    canWrite: Predicate;
};

export const filesAccessFormat: { [key: string]: json.Format } = {
    bindings: [json.Type.BoundedMap,
        [json.Type.BoundedString, MAX_NAME_LENGTH],
        [json.Type.BoundedString, MAX_HASH_LENGTH],
        1],
    idProvider: [json.Type.BoundedString, MAX_QUALIFIED_NAME_LENGTH],
    canWrite: json.Type.Something,   // checked with validatePredicate('object')
};

export const authorFormat: { [key: string]: json.Format } = {
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

export const refVersionFormat: json.Format =
    [json.Type.BoundedMap, [json.Type.BoundedString, MAX_HASH_LENGTH], [json.Type.Constant, ''], MAX_REF_VERSION_WIDTH];

// The signed ref-advance of both types: the canonical mvt payload plus the
// author fields (and `lane` in the store, added by the caller).
export const signedRefAdvanceFormat: { [key: string]: json.Format } = {
    action: [json.Type.Constant, 'ref-advance'],
    refId: [json.Type.BoundedString, MAX_HASH_LENGTH],
    refVersion: refVersionFormat,
    ...authorFormat,
};

// Position-independent semantics of the access fields (after checkFormat).
export function filesAccessReason(access: FilesAccess): string | undefined {
    const aliases = Object.keys(access.bindings);
    if (aliases.length !== 1) return 'bindings must hold exactly one group';
    const alias = aliases[0];
    if (!isValidName(alias)) return `invalid binding alias '${alias}'`;
    if (!isContentHash(access.bindings[alias])) return `binding '${alias}' is not a group id`;

    const [providerGroup, providerTable] = splitTableRef(access.idProvider);
    if (providerGroup !== alias || !isValidName(providerTable)) {
        return `idProvider '${access.idProvider}' must be ${alias}.<table>`;
    }

    if (!validatePredicate(access.canWrite as unknown as json.Literal, 'object')) return 'canWrite is not a valid object predicate';
    for (const atom of collectExistsAtoms(access.canWrite)) {
        const [group, table] = splitTableRef(atom.table);
        if (group !== alias || !isValidName(table)) {
            return `canWrite table '${atom.table}' must be ${alias}.<table>`;
        }
    }
    return undefined;
}

// An EXISTS atom fits a table when the table exists and every where field is
// a PUB column or rowAuthor (what RTableView.findRowIds requires).
export function existsAtomReason(
    atom: ExistsAtom, getTable: (t: string) => TableDef | undefined, schemaName: string,
): string | undefined {
    const [, table] = splitTableRef(atom.table);
    const def = getTable(table);
    if (def === undefined) return `ALLOW WRITE IF reads ${atom.table}: schema ${schemaName} has no table ${table}`;
    for (const field of Object.keys(atom.where ?? {})) {
        if (field === 'rowAuthor') continue;
        const column = def.columns[field];
        if (column === undefined) return `ALLOW WRITE IF reads ${atom.table}.${field}, which ${table} doesn't have`;
        if (!(column.pub ?? false)) return `ALLOW WRITE IF reads ${atom.table}.${field}, which isn't PUB in ${table}`;
    }
    return undefined;
}

// Why a FILES doesn't fit its bound group's schema: USING IDENTITIES must be
// an IDENTITY PROVIDER table, and every ALLOW WRITE IF atom must fit.
// Messages use C-SQL terms.
export function filesSchemaReason(
    files: { idProvider: string; canWrite: Predicate }, schemaName: string, getTable: (t: string) => TableDef | undefined,
): string | undefined {
    const [, table] = splitTableRef(files.idProvider);
    const provider = getTable(table);
    if (provider === undefined) return `USING IDENTITIES ${files.idProvider}: schema ${schemaName} has no table ${table}`;
    if (provider.idProvider === undefined) return `USING IDENTITIES ${files.idProvider}: ${table} isn't an IDENTITY PROVIDER table`;
    for (const atom of collectExistsAtoms(files.canWrite)) {
        const reason = existsAtomReason(atom, getTable, schemaName);
        if (reason !== undefined) return reason;
    }
    return undefined;
}

// Semantic checks shared by every signed op (after checkFormat).
export function authorReason(payload: json.LiteralMap): string | undefined {
    if (!isContentHash(payload['author'])) return 'author is not a key id';
    if (decodeCanonicalBase64(payload['signature'] as string) === undefined) return 'signature is not canonical base64';
    return undefined;
}

export function refVersionReason(refVersion: json.LiteralMap): string | undefined {
    const hashes = Object.keys(refVersion);
    if (hashes.length === 0) return 'refVersion is empty';
    for (const h of hashes) {
        if (!isContentHash(h)) return 'refVersion holds a malformed entry hash';
    }
    return undefined;
}

export function accessOf(payload: FilesAccess): FilesAccess {
    return { bindings: payload.bindings, idProvider: payload.idProvider, canWrite: payload.canWrite };
}

export type Admission = { key: PublicKey };

// The access rules of one RBlobStore or RFileMap.
export class FilesAccessControl {

    readonly alias: string;
    readonly groupId: B64Hash;
    readonly providerTable: string;

    constructor(private readonly ctx: RContext, readonly access: FilesAccess) {
        this.alias = Object.keys(access.bindings)[0];
        this.groupId = access.bindings[this.alias];
        this.providerTable = splitTableRef(access.idProvider)[1];
    }

    async loadGroup(): Promise<RTableGroupImpl> {
        const obj = await this.ctx.getObject(this.groupId);
        if (obj === undefined) throw new Error(`Bound group '${this.alias}' -> '${this.groupId}' is not present in the replica`);
        return obj as RTableGroupImpl;
    }

    observedVersion(dag: ScopedDag, at: Version): Promise<Version> {
        return resolveRefVersionAtPosition(dag, this.groupId, at, at);
    }

    // The key registered for `author` in the provider table at group version `v`.
    async authorKey(author: KeyId, v: Version): Promise<PublicKey | undefined> {
        return (await this.loadGroup()).providerPublicKeyAt(this.providerTable, author, v);
    }

    // canWrite for `author` at group version `v`. The schema may have drifted
    // since the FILES was added: a table any atom no longer fits at `v`
    // (absent, or a where column missing or not PUB) makes all of its atoms
    // false, so the FILES degrades to read-only instead of throwing.
    async canWriteAt(author: KeyId, v: Version): Promise<boolean> {
        const view = await (await this.loadGroup()).getView(v, v);
        const schema = view.getSchemaView();
        const unfit = new Set<string>();
        for (const atom of collectExistsAtoms(this.access.canWrite)) {
            if (existsAtomReason(atom, (t) => schema.getTable(t), schema.getName()) !== undefined) {
                unfit.add(splitTableRef(atom.table)[1]);
            }
        }
        return evaluatePredicate(this.access.canWrite, {
            getTableView: async (table: string): Promise<RTableView> => {
                throw new Error(`canWrite has no local table '${table}'`);
            },
            getForeignTableView: async (group, table) => {
                if (group !== this.alias || unfit.has(table)) return undefined;
                return view.getTableView(table);
            },
            author,
            context: 'object',
        });
    }

    // Signature and canWrite for a signed op at `at`, with the group observed
    // at `v`. Returns the verified key on success.
    async admit(payload: json.LiteralMap, at: Version, v: Version, scope: SigningScope): Promise<ValidationResult & Partial<Admission>> {
        const author = payload['author'] as KeyId;
        const key = await this.authorKey(author, v);
        if (key === undefined) return validationFailure(`author '${author}' has no key in ${this.access.idProvider} at the observed version`);
        const signed = await verifyPayloadSignature(payload, at, async (id) => (id === author ? key : undefined), scope);
        if (!signed) return validationFailure('bad signature');
        if (!await this.canWriteAt(author, v)) return validationFailure(`author '${author}' is not allowed to write at the observed version`);
        return { valid: true, key };
    }

    // A ref-advance of the bound group: monotonic, and its author admitted at
    // the version it moves to. A version whose entries are not local throws.
    async validateRefAdvance(dag: ScopedDag, payload: RefAdvancePayload & json.LiteralMap, at: Version, scope: SigningScope): Promise<ValidationResult> {
        if (payload.refId !== this.groupId) return validationFailure(`ref '${payload.refId}' is not the bound group`);
        const newVersion = extractRefVersion(payload);

        const group = await this.loadGroup();
        const groupDag = await group.getScopedDag();
        for (const h of newVersion) {
            if (await groupDag.loadEntry(h) === undefined) throw new Error(`group entry '${h}' is not present in the replica`);
        }

        if (!await validateRefAdvanceMonotonicity(dag, await group.getCausalDag(), this.groupId, newVersion, at)) {
            return validationFailure('ref-advance is not monotonic');
        }

        const admission = await this.admit(payload, at, newVersion, scope);
        return admission.valid ? validationOk() : admission;
    }
}
