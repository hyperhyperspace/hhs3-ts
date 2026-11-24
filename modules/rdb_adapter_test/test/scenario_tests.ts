// Checks of HIP-29's multi-group fixtures: each one stands where its cases
// need it to, as rdb has it today.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { observeVersion, readingsDiffer } from "@hyper-hyper-space/hhs3_rdb_adapter/testing";

import { createCapsItems, createGateGraph, createRevocationScenario, createVoidScenario, frontierOf } from "../src/index.js";

async function verdict(group: { explainEntryVoided(entry: string, from: Set<string>): Promise<unknown> }, entry: string, from: Set<string>): Promise<string> {
    const detail = await group.explainEntryVoided(entry, from) as { kind?: string } | undefined;
    return detail === undefined ? 'live' : (detail.kind ?? 'void');
}

export const scenarioTests = {
    title: '[SCN] HIP-29 fixtures',
    tests: [
        {
            name: '[SCN01] caps and items: an item needs a cap in the bound group',
            invoke: async () => {
                const { perm, items, admin } = await createCapsItems();
                await (await perm.getTable('caps')).insert('c1', { label: 'grant' }, admin);
                await items.observe('perm', await frontierOf(perm));
                await (await items.getTable('items')).insert('i1', { name: 'thing' });
                const rows = await (await (await items.getTable('items')).getView()).query({});
                assertEquals(rows.length, 1, 'the item is accepted once items observes the cap');
            },
        },
        {
            name: "[SCN02] the void scenario: Mallory's observe is void where the removal is concurrent, and rdb accepts Bob's note there, void from the start",
            invoke: async () => {
                for (const form of ['rule', 'fk'] as const) {
                    const s = await createVoidScenario(form);
                    assertEquals(await verdict(s.G, s.malloryObserve, s.voidAt), 'observe-gate', `${form}: the observe is void`);
                    const note = await (await s.G.getTable('notes')).insert(`n-${form}`, s.note('hi'), s.bob, s.voidAt);
                    assertTrue(await verdict(s.G, note, new Set([note])) !== 'live', `${form}: the note is void where it stands`);
                }
            },
        },
        {
            name: "[SCN03] the gate graph: Alice's observe of M is live after G's observe of K at the grant, and void once G observes the revocation",
            invoke: async () => {
                const s = await createGateGraph();
                assertEquals(await verdict(s.G, s.observeOfM, s.at), 'live', 'live at the checkpoint');
                const observeRevoked = await s.G.observe('k', s.kRevoked, undefined, s.at);
                assertTrue(await verdict(s.G, s.observeOfM, new Set([observeRevoked])) !== 'live',
                    'void once G observes the revocation');
            },
        },
        {
            name: "[SCN04] the revocation scenario: the guarded note is live at G's checkpoint; an observe of K's removal voids it when the removal is concurrent with the grant, and not when it comes after",
            invoke: async () => {
                for (const guard of ['insert', 'update', 'delete'] as const) {
                    for (const kind of ['concurrent', 'after'] as const) {
                        const s = await createRevocationScenario(kind, guard);
                        if (guard === 'delete') await s.deleteNote();
                        const at = await frontierOf(s.G);
                        const body = async (from: Set<string>): Promise<string> => {
                            const row = await (await (await s.G.getView(from, from)).getTableView('notes')).getRow(s.noteRow);
                            return row === undefined ? '<none>' : String(row.values.body);
                        };
                        const atC = guard === 'insert' ? "alice's" : guard === 'update' ? "alice's edit" : '<none>';
                        const voided = guard === 'insert' ? '<none>' : "root's";
                        assertEquals(await body(at), atC, `${guard} ${kind}: the note at the checkpoint`);
                        const observe = await s.G.observe('k', await frontierOf(s.K), undefined, at);
                        assertEquals(await body(new Set([observe])), kind === 'concurrent' ? voided : atC,
                            `${guard} ${kind}: after G observes the removal`);
                    }
                }
            },
        },
        {
            name: "[ADPTI23] ingestion's comparison of the two readings of a gated binding: equal where every observe is live, different where one is void, and equal again after a live observe",
            invoke: async () => {
                const v = await createVoidScenario('rule');
                assertEquals(await readingsDiffer(v.G, v.voidAt), true, "the void observe gives the two readings apart");
                const live = await v.G.observe('k', v.kGrant, v.alice, v.voidAt);
                assertEquals(await readingsDiffer(v.G, new Set([live])), false, 'a live observe of the same version joins them');

                const s = await createGateGraph();
                assertEquals(await readingsDiffer(s.G, s.at), false, 'the gate graph at its checkpoint: every observe live');
                const observeRevoked = await s.G.observe('k', s.kRevoked, undefined, s.at);
                assertEquals(await readingsDiffer(s.G, new Set([observeRevoked])), true,
                    "an observe of one binding voids the other's: compared for every gated binding");
            },
        },
        {
            name: "[ADPTI24] an observe's version: the bound group's chain end joined with what the observer already observes, or none when that already covers it",
            invoke: async () => {
                const { perm, admin } = await createCapsItems();
                const dag = await perm.getCausalDag();
                const caps = await perm.getTable('caps');
                const base = await frontierOf(perm);
                const a = await caps.insert('a', { label: 'a' }, admin, base);
                const b = await caps.insert('b', { label: 'b' }, admin, base);   // concurrent with a
                const ab = await caps.insert('ab', { label: 'ab' }, admin, new Set([a, b]));
                const show = (v: Set<string> | undefined): string => v === undefined ? 'covered' : [...v].map((h) => h === a ? 'a' : h === b ? 'b' : h === ab ? 'ab' : '?').sort().join('+');
                assertEquals(show(await observeVersion(dag, new Set([ab]), new Set([a]))), 'covered', 'an observer ahead of the chain end needs none');
                assertEquals(show(await observeVersion(dag, new Set([a]), new Set([a]))), 'covered', 'nor one at it');
                assertEquals(show(await observeVersion(dag, base, new Set([a]))), 'a', 'one behind advances to the chain end');
                assertEquals(show(await observeVersion(dag, new Set([b]), new Set([a]))), 'a+b', 'one beside it joins both');
                assertEquals(show(await observeVersion(dag, new Set([a, b]), new Set([ab]))), 'ab', 'in canonical form');
            },
        },
    ],
};
