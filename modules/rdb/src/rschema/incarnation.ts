// Structural, drop-aware incarnation identity for tables and columns.
//
// An incarnation id is a content hash of the birth definition plus a "drop
// generation" (the number of tombstone writes in the slot that causally
// precede the birth write). This makes structurally-identical concurrent adds
// converge to ONE incarnation (same id, no LWW pick), keeps structurally-
// different concurrent adds distinct (LWW picks a winner, the loser's writes
// are masked), and makes a drop+re-add of an identical def reset (its drop
// generation is higher, so the id differs). See resolve.ts for how the ids
// drive per-slot resolution.
//
// The drop generation is a COUNT, not the set of drop entry-hashes: two
// replicas that INDEPENDENTLY run the same drop+re-add migration produce
// different drop entries but the same count, so a count converges where a
// hash-of-drops would wrongly diverge. Two histories that share (def, count)
// are deliberately treated as the same incarnation and merge.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";

import type { TableDef, ColumnDef } from "./payload.js";

// An opaque structural incarnation id (a base64 content hash). Embedded
// verbatim in row-op meta tags (see ../rtable_group/scopes.ts).
export type IncarnationId = B64Hash;

function hashCanonical(canonical: json.Literal): IncarnationId {
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(canonical)));
}

// A table incarnation is the hash of the BASE def carried by the winning
// add-table write (NEVER the resolved def: later add-column / set-* writes must
// not perturb it) plus its drop generation.
export function tableIncarnationId(baseDef: TableDef, dropGeneration: number): IncarnationId {
    return hashCanonical({ kind: 'table', def: baseDef as json.Literal, drops: dropGeneration });
}

// A column incarnation is the hash of the winning column def, SEEDED by the
// table incarnation (so a table reset also resets every column) plus the
// column's own drop generation (a drop-column + re-add-column within one table
// incarnation).
export function columnIncarnationId(
    def: ColumnDef, tableIncarnation: IncarnationId, dropGeneration: number,
): IncarnationId {
    return hashCanonical({ kind: 'column', def: def as json.Literal, table: tableIncarnation, drops: dropGeneration });
}
