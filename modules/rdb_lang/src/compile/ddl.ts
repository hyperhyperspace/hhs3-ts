import type { FKs, MigrationRule, Restriction } from "@hyper-hyper-space/hhs3_rdb";

import type { MigrationRuleExpr } from "../syntax/ast.js";
import { compileColumn, compileTable } from "./create.js";
import { lowerRestrictionPredicate } from "./query.js";

export function compileMigrationRules(rules: MigrationRuleExpr[]): MigrationRule[] {
    return rules.map((rule) => {
        switch (rule.kind) {
            case 'add-table':
                return { rule: 'add-table', def: compileTable(rule.table) };
            case 'drop-table':
                return { rule: 'drop-table', table: rule.table };
            case 'add-column':
                return { rule: 'add-column', table: rule.table, column: rule.column.name, def: compileColumn(rule.column) };
            case 'drop-column':
                return { rule: 'drop-column', table: rule.table, column: rule.column };
            case 'set-concurrent-deletes':
                return { rule: 'set-concurrent-deletes', table: rule.table, value: rule.value };
            case 'set-fks':
                return { rule: 'set-fks', table: rule.table, fks: rule.fks as FKs };
            case 'set-allow-rules':
                return {
                    rule: 'set-restrictions',
                    table: rule.table,
                    restrictions: rule.allowRules.map((r): Restriction => ({
                        on: r.op,
                        rule: lowerRestrictionPredicate(r.predicate),
                    })),
                };
        }
    });
}
