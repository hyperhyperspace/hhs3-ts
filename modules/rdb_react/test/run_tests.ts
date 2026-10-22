import "./register_dom.js";

import { createElement, StrictMode, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";

import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { MemoryTarget, QueryEngine, type ProjectedRow, type QuerySnapshot } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { ProjectionProvider, useQuery } from "../src/index.js";

async function notesTable(target: MemoryTarget): Promise<void> {
    await target.apply("g", [{
        kind: "create-table", table: "notes", syncTable: "notes_sync", primaryKey: "id",
        columns: [
            { name: "title", def: { type: "string", nullable: true, pub: true, readonly: false } },
            { name: "folder_id", def: { type: "integer", nullable: true, pub: true, readonly: false } },
        ],
    }, {
        kind: "create-table", table: "folders", syncTable: "folders_sync", primaryKey: "id",
        columns: [{ name: "name", def: { type: "string", nullable: false, pub: true, readonly: false } }],
    }], [], new Set(["c"]));
}

async function messageTables(target: MemoryTarget): Promise<void> {
    await target.apply("g", [{
        kind: "create-table", table: "profiles", syncTable: "profiles_sync", primaryKey: "id",
        columns: [{ name: "name", def: { type: "string", nullable: false, pub: true, readonly: false } }],
    }, {
        kind: "create-table", table: "messages", syncTable: "messages_sync", primaryKey: "id",
        columns: [
            { name: "body", def: { type: "string", nullable: false, pub: true, readonly: false } },
            { name: "author_id", def: { type: "integer", nullable: false, pub: true, readonly: false } },
        ],
    }], [], new Set(["c"]));
}

function mount(node: ReturnType<typeof createElement>): { root: Root; host: HTMLElement } {
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    return { root, host };
}

async function settle(): Promise<void> {
    await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 30));
    });
}

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: "[REACT01] a fresh query with the same values keeps one monitor, and an unrelated write does not render",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await notesTable(target);
            let queries = 0;
            const original = target.executeQuery.bind(target);
            target.executeQuery = async (plan) => { queries++; return original(plan); };
            const engine = new QueryEngine(target);
            let opened = 0;
            const monitor = engine.monitor.bind(engine);
            engine.monitor = ((query: Parameters<QueryEngine["monitor"]>[0]) => {
                opened++;
                return monitor(query);
            }) as QueryEngine["monitor"];
            let renders = 0;
            function List() {
                const snapshot = useQuery(engine.selectFrom("notes").where("folder_id = :id", { id: 1 }));
                renders++;
                return createElement("div", null, snapshot.data.map((row) => String(row.title)).join(","));
            }
            function App() {
                const [tick, setTick] = useState(0);
                return createElement("div", null,
                    createElement("button", { onClick: () => setTick(tick + 1) }, String(tick)),
                    createElement(List),
                );
            }
            const { root, host } = mount(createElement(ProjectionProvider, { source: engine.source }, createElement(App)));
            await act(async () => { root.render(createElement(ProjectionProvider, { source: engine.source }, createElement(App))); });
            await settle();
            const afterReady = opened;
            assertEquals(afterReady, 1, "one monitor");
            const rendered = renders;
            await act(async () => {
                const button = host.querySelector("button");
                button?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
            });
            assertEquals(opened, afterReady, "the same fingerprint reuses the monitor");
            const quiet = queries;
            target.localInsert("folders", { name: "box" });
            await settle();
            assertEquals(queries, quiet, "another table does not query");
            assertEquals(renders, rendered + 1, "the click rendered once and the other table did not");
            await act(async () => { root.unmount(); });
            const parked = queries;
            target.localInsert("notes", { title: "later", folder_id: 1 });
            await settle();
            assertEquals(queries, parked, "unmount releases the monitor");
            engine.close();
        },
    },
    {
        name: "[REACT02] a local write renders, and the matching project-back does not render again",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await notesTable(target);
            let renders = 0;
            function List() {
                const snapshot = useQuery(engine.selectFrom("notes").select("title"));
                renders++;
                return createElement("div", null, snapshot.data.map((row) => String(row.title)).join(","));
            }
            const engine = new QueryEngine(target);
            const { root, host } = mount(createElement(ProjectionProvider, { source: engine.source }, createElement(List)));
            await act(async () => { root.render(createElement(ProjectionProvider, { source: engine.source }, createElement(List))); });
            await settle();
            let id = 0;
            await act(async () => { id = target.localInsert("notes", { title: "hello", folder_id: 1 }); });
            await settle();
            assertEquals(host.textContent, "hello", "the local write is on screen");
            const painted = renders;
            await target.reserveMint([{ table: "notes", localId: id, rowId: "row-hello", uuid: "u1" }]);
            await act(async () => {
                await target.apply("g", [], [{
                    kind: "upsert-row", table: "notes", rowId: "row-hello", values: { title: "hello", folder_id: 1 },
                }], new Set(["d"]));
            });
            await settle();
            assertEquals(renders, painted, "the matching project-back does not render");
            assertEquals(host.textContent, "hello", "the row stays");
            await act(async () => { root.unmount(); });
            engine.close();
        },
    },
    {
        name: "[REACT03] strict mode settles at one subscription, and a query set keeps the unchanged array",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await messageTables(target);
            const author = target.localInsert("profiles", { name: "ada" });
            target.localInsert("messages", { body: "hi", author_id: author });
            let queries = 0;
            const original = target.executeQuery.bind(target);
            target.executeQuery = async (plan) => { queries++; return original(plan); };
            const engine = new QueryEngine(target);
            const seen: { profiles?: ProjectedRow[]; messages?: ProjectedRow[] } = {};
            function Board() {
                const profilesQuery = engine.selectFrom("profiles").select("name");
                const snapshot = useQuery({
                    messages: engine.selectFrom("messages").select("body").where("author_id IN :ids", { ids: profilesQuery.col("id") }),
                    profiles: profilesQuery,
                }) as QuerySnapshot<{ [name: string]: ProjectedRow[] }>;
                seen.profiles = snapshot.data.profiles;
                seen.messages = snapshot.data.messages;
                return createElement("div", null, snapshot.data.messages.map((row) => String(row.body)).join(","));
            }
            const { root, host } = mount(createElement(StrictMode, null,
                createElement(ProjectionProvider, { source: engine.source }, createElement(Board))));
            await act(async () => {
                root.render(createElement(StrictMode, null,
                    createElement(ProjectionProvider, { source: engine.source }, createElement(Board))));
            });
            await settle();
            assertEquals(host.textContent, "hi", "the message is rendered");
            const quiet = queries;
            const messagesBefore = seen.messages;
            target.localUpdate("profiles", author, { name: "ada lovelace" });
            await settle();
            assertEquals(seen.messages, messagesBefore, "a profile edit keeps the messages array");
            assertTrue(queries > quiet, "the profile query ran");
            const profilesBefore = seen.profiles;
            const beforeMessage = queries;
            target.localInsert("messages", { body: "again", author_id: author });
            await settle();
            assertTrue(queries > beforeMessage, "the new message re-queries messages");
            assertEquals(seen.profiles, profilesBefore, "the profiles array is kept");
            assertTrue(host.textContent?.includes("again") === true, "both messages are on screen");
            await act(async () => { root.unmount(); });
            const parked = queries;
            target.localInsert("messages", { body: "gone", author_id: author });
            await settle();
            assertEquals(queries, parked, "strict mode did not leave a second subscription");
            engine.close();
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log("Running tests for Hyper Hyper Space v3 react module"
        + (filters.length > 0 ? " (applying filter: " + filters.toString() + ")" : "") + "\n");
    for (const test of tests) {
        let match = true;
        for (const filter of filters) match = match && test.name.indexOf(filter) >= 0;
        if (match) testing.exitIfFailed(await testing.run(test.name, test.invoke));
        else await testing.skip(test.name);
    }
    console.log();
}

main();
