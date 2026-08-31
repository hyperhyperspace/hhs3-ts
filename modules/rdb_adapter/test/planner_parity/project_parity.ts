import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RTableGroupDelta } from "@hyper-hyper-space/hhs3_rdb";

import { initialRowActions, planIncrementalRowActions } from "../../src/project.js";
import { initialSchemaActions, reprojectedTables, schemaDeltaActions } from "../../src/schema_actions.js";
import { collectExtendingPairs } from "../../../rdb/test/delta_parity/checkpoints.js";

import { ActionStore } from "./action_store.js";
import { fingerprintRdbProjectedRows, rowsOnlyFingerprint, schemaOnlyFingerprint } from "./fingerprint.js";
import { generateProjectHistory, type ProjectHistory } from "./project_generate.js";
import { resolveFuzzSweepOptions, subsamplePairs, type ResolvedFuzzSweepOptions } from "./profiles.js";

function mismatch(kind: string, history: ProjectHistory, startIdx: number, endIdx: number, extra: string): Error {
    return new Error(
        `kind=${kind} seed=${history.seed} pair=${startIdx}..${endIdx}\n${extra}\n`
        + `tallies=${JSON.stringify(history.tallies)}\nlog:\n${history.opLog.join('\n')}`,
    );
}

async function checkPair(history: ProjectHistory, startIdx: number, endIdx: number, start: Version, end: Version): Promise<void> {
    const { group } = history;
    const groupId = group.getId();
    const startGroupView = await group.getView(start, start);
    const endGroupView = await group.getView(end, end);
    const startView = startGroupView.getSchemaView();
    const endView = endGroupView.getSchemaView();
    const delta = (await group.computeDelta(start, end)) as RTableGroupDelta;

    const full = new ActionStore();
    full.apply(initialSchemaActions(endView), await initialRowActions(endGroupView));

    const inc = new ActionStore();
    inc.apply(initialSchemaActions(startView), await initialRowActions(startGroupView));
    inc.apply(
        schemaDeltaActions(delta.schemaChanges, endView, startView),
        await planIncrementalRowActions(endGroupView, delta, startView, endView, groupId),
    );

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
    for (const seed of options.seeds) {
        const history = await generateProjectHistory(seed, options.ops);
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
}

export async function runProjectPlannerFromArgv(): Promise<void> {
    await runProjectPlannerSweep(resolveFuzzSweepOptions(process.argv.slice(2)));
}
