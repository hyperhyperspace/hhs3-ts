import "./register_dom.js";

import { createElement, StrictMode, Suspense, useEffect, useState } from "react";
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

// Renders a tree under the engine's provider, in Strict Mode when asked.
async function show(engine: QueryEngine, node: ReturnType<typeof createElement>, strict = false): Promise<{ root: Root; host: HTMLElement }> {
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    const tree = createElement(ProjectionProvider, { source: engine.source }, node);
    await act(async () => { root.render(strict ? createElement(StrictMode, null, tree) : tree); });
    return { root, host };
}

// The table of each query the target runs from now on. With a latency, each
// query takes that long, as on an asynchronous store.
function countQueries(target: MemoryTarget, latencyMs = 0): string[] {
    const tables: string[] = [];
    const original = target.executeQuery.bind(target);
    target.executeQuery = async (plan) => {
        tables.push(plan.table);
        if (latencyMs > 0) await new Promise((resolve) => setTimeout(resolve, latencyMs));
        return original(plan);
    };
    return tables;
}

// What the engine holds: its monitors and its query sets.
function held(engine: QueryEngine): string {
    const inside = engine as unknown as { monitors: Map<string, unknown>; sets: Set<unknown> };
    return `${inside.monitors.size} monitors, ${inside.sets.size} sets`;
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
            // Settle the insert, as ingestion would, with its mapping: a projection doesn't apply over a pending change (HIP-29).
            await target.commitIngest({ consumed: (await target.drainChanges()).changes.map((c) => c.id),
                inserts: [{ table: "notes", localId: id, rowId: "row-hello", uuid: "u1", status: "active" }] });
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
    {
        name: "[REACT04] a render React throws away leaves nothing in the engine",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await notesTable(target);
            const queries = countQueries(target);
            const engine = new QueryEngine(target);
            // A child that never finishes loading: React shows the fallback and
            // throws away its render of Screen, which never mounts.
            const never = new Promise<void>(() => {});
            function Pending(): null { throw never; }
            function Screen() {
                useQuery(engine.selectFrom("notes").where("folder_id = :id", { id: 1 }));
                useQuery({ notes: engine.selectFrom("notes"), folders: engine.selectFrom("folders") });
                return createElement("div", null, createElement(Pending));
            }
            const { root, host } = await show(engine, createElement(Suspense, { fallback: "wait" }, createElement(Screen)));
            await settle();
            assertEquals(host.textContent, "wait", "the screen never mounted");
            assertEquals(held(engine), "0 monitors, 0 sets", "its queries left nothing in the engine");
            await act(async () => { root.unmount(); });
            target.localInsert("notes", { title: "later", folder_id: 1 });
            target.localInsert("folders", { name: "box" });
            await settle();
            assertEquals(queries.length, 0, "and no query ever ran for them");
            assertEquals(held(engine), "0 monitors, 0 sets", "nor is anything there once the tree is gone");
            engine.close();
        },
    },
    {
        name: "[REACT05] strict mode runs no query twice, and nothing is left in the engine after unmount",
        invoke: async () => {
            const mountBoard = async (strict: boolean) => {
                const target = new MemoryTarget({ captureChanges: true });
                await messageTables(target);
                const author = target.localInsert("profiles", { name: "ada" });
                target.localInsert("messages", { body: "hi", author_id: author });
                const queries = countQueries(target);
                const engine = new QueryEngine(target);
                function Board() {
                    const profiles = engine.selectFrom("profiles").select("name");
                    const snapshot = useQuery({
                        messages: engine.selectFrom("messages").select("body").where("author_id IN :ids", { ids: profiles.col("id") }),
                        profiles,
                    });
                    return createElement("div", null, snapshot.data.messages!.map((row) => String(row.body)).join(","));
                }
                const { root, host } = await show(engine, createElement(Board), strict);
                await settle();
                const shown = host.textContent;
                const mounted = held(engine);
                await act(async () => { root.unmount(); });
                await settle();
                engine.close();
                return { shown, queries: [...queries].sort().join(","), mounted, left: held(engine) };
            };
            const plain = await mountBoard(false);
            const strict = await mountBoard(true);
            assertEquals(`${plain.shown} ${strict.shown}`, "hi hi", "both show the message");
            assertEquals(strict.queries, plain.queries, "strict mode runs the same queries as a plain mount");
            assertEquals(`${plain.mounted} | ${strict.mounted}`, "2 monitors, 1 sets | 2 monitors, 1 sets", "while mounted: one set and its two monitors");
            assertEquals(`${plain.left} | ${strict.left}`, "0 monitors, 0 sets | 0 monitors, 0 sets", "and nothing after unmount");
        },
    },
    {
        name: "[REACT06] a second component showing a query set already on screen never renders loading",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await messageTables(target);
            target.localInsert("profiles", { name: "ada" });
            const engine = new QueryEngine(target);
            const second: boolean[] = [];
            function Panel(props: { name: string }) {
                const snapshot = useQuery({ profiles: engine.selectFrom("profiles").select("name") });
                if (props.name === "second") second.push(snapshot.loading);
                return createElement("div", null, `${props.name}:${snapshot.loading ? "loading" : snapshot.data.profiles!.length} `);
            }
            let open = () => {};
            function App() {
                const [both, setBoth] = useState(false);
                open = () => setBoth(true);
                return createElement("div", null,
                    createElement(Panel, { name: "first" }),
                    both ? createElement(Panel, { name: "second" }) : null);
            }
            const { root, host } = await show(engine, createElement(App));
            await settle();
            await act(async () => { open(); });
            await settle();
            assertEquals(host.textContent, "first:1 second:1 ", "both panels show the profile");
            assertEquals(JSON.stringify([...new Set(second)]), "[false]", "the second one never rendered loading");
            await act(async () => { root.unmount(); });
            engine.close();
        },
    },
    {
        name: "[REACT07] one component leaving as another arrives with the same query keeps the monitor: no loading, no query",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await notesTable(target);
            target.localInsert("notes", { title: "a", folder_id: 1 });
            const queries = countQueries(target);
            const engine = new QueryEngine(target);
            const query = () => engine.selectFrom("notes").where("folder_id = :id", { id: 1 });
            const arriving: boolean[] = [];
            function Leaving() {
                const snapshot = useQuery(query());
                return createElement("i", null, String(snapshot.data.length));
            }
            function Arriving() {
                const snapshot = useQuery(query());
                arriving.push(snapshot.loading);
                return createElement("b", null, String(snapshot.data.length));
            }
            let swap = () => {};
            function App() {
                const [swapped, setSwapped] = useState(false);
                swap = () => setSwapped(true);
                return swapped ? createElement(Arriving) : createElement(Leaving);
            }
            const { root, host } = await show(engine, createElement(App));
            await settle();
            const before = queries.length;
            await act(async () => { swap(); });
            await settle();
            assertEquals(host.innerHTML, "<b>1</b>", "the arriving component shows the note");
            assertEquals(JSON.stringify([...new Set(arriving)]), "[false]", "and never rendered loading");
            assertEquals(queries.length, before, "no query ran");
            await act(async () => { root.unmount(); });
            await settle();
            assertEquals(held(engine), "0 monitors, 0 sets", "nothing is left after unmount");
            engine.close();
        },
    },
    {
        name: "[REACT08] a component that mounts and leaves before its first result leaves nothing in the engine",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await messageTables(target);
            const author = target.localInsert("profiles", { name: "ada" });
            target.localInsert("messages", { body: "hi", author_id: author });
            // Queries that take a while, as on an asynchronous store.
            const queries = countQueries(target, 10);
            const engine = new QueryEngine(target);
            function Guarded(props: { leave: () => void }) {
                const profiles = engine.selectFrom("profiles").select("name");
                useQuery(engine.selectFrom("messages").select("body").where("author_id IN :ids", { ids: profiles.col("id") }));
                useEffect(() => { props.leave(); }, []);
                return createElement("div", null, "guarded");
            }
            function App() {
                const [page, setPage] = useState("guarded");
                return page === "guarded"
                    ? createElement(Guarded, { leave: () => setPage("login") })
                    : createElement("div", null, page);
            }
            const { root, host } = await show(engine, createElement(App));
            await settle();
            await settle();
            assertEquals(host.textContent, "login", "the page redirected as soon as it mounted");
            assertEquals(held(engine), "0 monitors, 0 sets", "its query and the one it read through are gone");
            const before = queries.length;
            target.localInsert("profiles", { name: "bob" });
            target.localInsert("messages", { body: "again", author_id: author });
            await settle();
            await settle();
            assertEquals(queries.length, before, "later writes to either table run no query");
            await act(async () => { root.unmount(); });
            engine.close();
        },
    },
    {
        name: "[REACT09] a mount renders twice: loading, then the rows",
        invoke: async () => {
            const target = new MemoryTarget({ captureChanges: true });
            await notesTable(target);
            target.localInsert("notes", { title: "a", folder_id: 1 });
            const engine = new QueryEngine(target);
            const renders: string[] = [];
            function List() {
                const snapshot = useQuery(engine.selectFrom("notes"));
                renders.push(snapshot.loading ? "loading" : String(snapshot.data.length));
                return createElement("div", null, String(snapshot.data.length));
            }
            const { root } = await show(engine, createElement(List));
            await settle();
            assertEquals(JSON.stringify(renders), '["loading","1"]', "subscribing after the first render causes no render of its own");
            await act(async () => { root.unmount(); });
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
