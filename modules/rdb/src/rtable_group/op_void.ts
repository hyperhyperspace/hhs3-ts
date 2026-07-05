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
    | { kind: 'authorization-cycle' }
    | { kind: 'bundle'; index: number; detail: OpVoidDetail };

export function formatOpVoidDetail(detail: OpVoidDetail): string {
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
        case 'authorization-cycle':
            return 'authorization cycle (least-fixpoint deny)';
        case 'bundle':
            return `bundle write ${detail.index}: ${formatOpVoidDetail(detail.detail)}`;
    }
}
