import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RSchemaView, RTableGroupDelta } from "@hyper-hyper-space/hhs3_rdb";

import { initialRowActions, planIncrementalRowActions } from "../../src/project.js";
import { initialSchemaActions, reprojectedTables, schemaDeltaActions } from "../../src/schema_actions.js";
import {
    groupIndexDecls, planIndexActions, resolveIndexes, validateIndexSpec, withIndexActions,
} from "../../src/index_actions.js";
import { AUTHOR_INDEX_COLUMN, type IndexDecl, type IndexSpec, type ResolvedIndex, type SchemaAction } from "../../src/types.js";
import {
    collectExtendingPairs, generateProjectHistory, resolveFuzzSweepOptions, subsamplePairs,
    mergeTallies, assertPathologicalCoverage,
    type ProjectHistory, type ResolvedFuzzSweepOptions, type Tallies,
} from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";

import { ActionStore } from "./action_store.js";
import { fingerprintRdbProjectedRows, rowsOnlyFingerprint, schemaOnlyFingerprint } from "./fingerprint.js";

function mismatch(kind: string, history: ProjectHistory, startIdx: number, endIdx: number, extra: string): Error {
    return new Error(
        `kind=${kind} seed=${history.seed} pair=${startIdx}..${endIdx}\n${extra}\n`
        + `tallies=${JSON.stringify(history.tallies)}\nlog:\n${history.opLog.join('\n')}`,
    );
}

// An index spec covering every column either view knows (so the delta both
// kills and completes declarations): one single-column index per column, an
// author index and a two-column composite per table, plus indexPub.
function fuzzIndexSpec(groupName: string, views: RSchemaView[]): IndexSpec {
    const columnsByTable = new Map<string, Set<string>>();
    for (const view of views) {
        for (const table of view.getTableNames()) {
            const cols = columnsByTable.get(table) ?? new Set<string>();
            for (const c of Object.keys(view.getTable(table)?.columns ?? {})) cols.add(c);
            columnsByTable.set(table, cols);
        }
    }
    const indexes: IndexDecl[] = [];
    for (const table of [...columnsByTable.keys()].sort()) {
        const cols = [...columnsByTable.get(table)!].sort();
        for (const c of cols) indexes.push({ name: `ix_${table}_${c}`, group: groupName, table, columns: [c] });
        indexes.push({ name: `ix_${table}_author`, group: groupName, table, columns: [AUTHOR_INDEX_COLUMN] });
        if (cols.length >= 2) {
            indexes.push({ name: `ix_${table}_pair`, group: groupName, table, columns: [cols[0]!, cols[1]!] });
        }
    }
    const spec: IndexSpec = { version: 1, indexes, indexPub: true };
    const invalid = validateIndexSpec(spec);
    if (invalid !== undefined) throw new Error(`fuzz index spec is invalid: ${invalid}`);
    return spec;
}

function desiredIndexes(spec: IndexSpec, groupName: string, groupId: string, view: RSchemaView): ResolvedIndex[] {
    return resolveIndexes(groupIndexDecls(spec, groupName, view), groupId, view, {}).resolved;
}

function withPlannedIndexes(
    spec: IndexSpec, groupName: string, groupId: string, view: RSchemaView,
    materialized: ResolvedIndex[], schemaActions: SchemaAction[],
): SchemaAction[] {
    const plan = planIndexActions(desiredIndexes(spec, groupName, groupId, view), materialized, schemaActions);
    return withIndexActions(schemaActions, plan);
}

async function checkPair(history: ProjectHistory, startIdx: number, endIdx: number, start: Version, end: Version): Promise<void> {
    const { group } = history;
    const groupId = group.getId();
    const groupName = group.getName();
    const startGroupView = await group.getView(start, start);
    const endGroupView = await group.getView(end, end);
    const startView = startGroupView.getSchemaView();
    const endView = endGroupView.getSchemaView();
    const delta = (await group.computeDelta(start, end)) as RTableGroupDelta;
    const indexSpec = fuzzIndexSpec(groupName, [startView, endView]);

    const full = new ActionStore();
    full.apply(
        withPlannedIndexes(indexSpec, groupName, groupId, endView, [], initialSchemaActions(endView)),
        await initialRowActions(endGroupView),
    );

    const inc = new ActionStore();
    inc.apply(
        withPlannedIndexes(indexSpec, groupName, groupId, startView, [], initialSchemaActions(startView)),
        await initialRowActions(startGroupView),
    );
    try {
        inc.apply(
            withPlannedIndexes(indexSpec, groupName, groupId, endView, inc.materializedIndexes(),
                schemaDeltaActions(delta.schemaChanges, endView, startView)),
            await planIncrementalRowActions(endGroupView, delta, startView, endView, groupId),
        );
    } catch (e) {
        throw mismatch('project-index-apply', history, startIdx, endIdx, String(e));
    }

    const fullIdx = full.indexFingerprint();
    const incIdx = inc.indexFingerprint();
    const wantIdx = desiredIndexes(indexSpec, groupName, groupId, endView).map((i) => i.fingerprint).sort().join('\n');
    if (fullIdx !== incIdx || fullIdx !== wantIdx) {
        throw mismatch('project-indexes', history, startIdx, endIdx,
            `fullIdx=${fullIdx}\nincIdx=${incIdx}\nwantIdx=${wantIdx}`);
    }

    const fullFp = full.fingerprint();
    const incFp = inc.fingerprint();
    const fullCanon = schemaOnlyFingerprint(fullFp);
    const incCanon = schemaOnlyFingerprint(incFp);
    if (fullCanon !== incCanon) {
        throw mismatch('project-schema', history, startIdx, endIdx,
            `fullSchema=${fullCanon}\nincSchema=${incCanon}`);
    }
    const fullRows = rowsOnlyFingerprint(fullFp);
    const incRows = rowsOnlyFingerprint(incFp);
    if (fullRows !== incRows) {
        throw mismatch('project-rows-full-vs-inc', history, startIdx, endIdx,
            `fullRows=${fullRows}\nincRows=${incRows}`);
    }

    const rdbRows = await fingerprintRdbProjectedRows(endGroupView);
    if (fullRows !== rdbRows) {
        throw mismatch('project-rows-vs-rdb', history, startIdx, endIdx,
            `storeRows=${fullRows}\nrdbRows=${rdbRows}`);
    }

    const empty = (await group.computeDelta(end, end)) as RTableGroupDelta;
    const emptySchema = schemaDeltaActions(empty.schemaChanges, endView, endView);
    const emptyRows = await planIncrementalRowActions(endGroupView, empty, endView, endView, groupId);
    const emptyFlip = reprojectedTables(empty.schemaChanges, endView, endView);
    if (emptySchema.length !== 0 || emptyRows.length !== 0 || emptyFlip.size !== 0) {
        throw mismatch('project-empty-delta', history, startIdx, endIdx,
            `schema=${emptySchema.length} rows=${emptyRows.length} flip=${emptyFlip.size}`);
    }
}

export async function runProjectPlannerSweep(options: ResolvedFuzzSweepOptions): Promise<void> {
    const combined: Tallies = {};
    for (const seed of options.seeds) {
        const history = await generateProjectHistory(seed, options.ops);
        mergeTallies(combined, history.tallies);
        const pairs = subsamplePairs(
            await collectExtendingPairs(history.rawDag, history.checkpoints),
            seed, options.maxPairs,
        );
        for (const [startIdx, endIdx, start, end] of pairs) {
            await checkPair(history, startIdx, endIdx, start, end);
            process.stdout.write('.');
        }
        process.stdout.write(
            `\n  seed=${seed} pairs=${pairs.length} (max ${options.maxPairs}) `
            + `tallies=${JSON.stringify(history.tallies)}\n`,
        );
    }
    assertPathologicalCoverage(combined, `kind=project-generate profile=${options.profile}`);
}

export async function runProjectPlannerFromArgv(): Promise<void> {
    await runProjectPlannerSweep(resolveFuzzSweepOptions(process.argv.slice(2)));
}
