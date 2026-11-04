// Structured void reasons for op-channel delta flips.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

import { formatPredicate, formatRestrictionFailureReason, formatRowNotLiveFailureReason } from "../rschema/format_predicate.js";
import type { Predicate } from "../rschema/payload.js";
import type { RowOpPayload } from "../rtable/payload.js";

export type OpVoidHorizon = 'start' | 'end';

export type OpVoidDetail =
    | { kind: 'restriction'; table: string; action: RowOpPayload['action']; rowId: B64Hash; rule: Predicate }
    | { kind: 'row-not-live'; table: string; action: 'update' | 'delete'; rowId: B64Hash }
    | { kind: 'fk'; table: string; action: RowOpPayload['action']; rowId: B64Hash; column: string; targetRef: string; targetRowId: B64Hash }
    | { kind: 'observe-gate'; binding: string; rule: Predicate }
    | { kind: 'undecided-cycle' }
    | { kind: 'bundle'; index: number; detail: OpVoidDetail };

// What a printer knows about the void entry beyond its reason. `observe` is
// set for an observe of a bound group (a ref-advance that is not a schema
// deploy), named by its binding when the printer knows it.
export type OpVoidContext = { observe?: { binding?: string } };

// A void entry's reason, for people. A void observe gets two notes: a collapsed
// one was contested by an entry it raced and can be observed again from the
// merge, and any void one still advanced the observer's horizon of the
// binding, since the horizon folds every observe, so what it imported can
// still change the observer's reads (a revoke among it, for one).
export function formatOpVoidDetail(detail: OpVoidDetail, context?: OpVoidContext): string {
    const reason = formatReason(detail);
    if (context?.observe === undefined) return reason;
    const contested = detail.kind === 'undecided-cycle' ? `contested: ${reason}; observe again from the merge` : reason;
    const binding = context.observe.binding;
    return `${contested}; its version still advanced the horizon${binding === undefined ? '' : ` of '${binding}'`}`;
}

function formatReason(detail: OpVoidDetail): string {
    switch (detail.kind) {
        case 'restriction':
            return formatRestrictionFailureReason(detail.table, {
                action: detail.action,
                rowId: detail.rowId,
                values: {},
            } as RowOpPayload, detail.rule);
        case 'row-not-live':
            return formatRowNotLiveFailureReason(detail.table, detail);
        case 'fk':
            return `FK column '${detail.column}' in table '${detail.table}' points to non-live row '${detail.targetRowId}' in '${detail.targetRef}'`;
        case 'observe-gate':
            return `canObserve predicate rejected observation of '${detail.binding}': ${formatPredicate(detail.rule)}`;
        case 'undecided-cycle':
            return 'undecided dependency cycle (denied)';
        case 'bundle':
            return `bundle write ${detail.index}: ${formatReason(detail.detail)}`;
    }
}
