// A joined host under an allow list: whom it admits, whom it serves, and how
// long it bootstraps. Bob is the newcomer, Carol another member, Dave a member
// who is removed, and Eve was never added.

import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import type { HostStatus } from "@hyper-hyper-space/hhs3_rhost_client";
import type { ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import { Rhost, type Host, type HostRecord } from "../src/index.js";

import {
    MemPlatform, RelabeledVault, SharedMesh, appConfig, editor, hostSetup, rowsOf, waitFor, withGroup,
} from "./fixtures.js";

const BY_IDENTITY = ['user.identities.keyId'];
const BY_MEMBER_CAP = ["user.caps.grantee where label = 'member'"];

// Long enough for a peer to connect and sync, when the test is that it doesn't.
const SETTLE_MS = 1500;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type World = { mesh: SharedMesh; platforms: Map<string, MemPlatform>; apps: Rhost[] };

// The admin's vault holds every key, and each other platform sees its own key
// as 'me'. The admin's session can then write `$<name>` for any of them.
async function world(names: string[], releases?: ReleaseFile[]): Promise<World> {
    const files = releases ?? [(await editor()).v100];
    const mesh = new SharedMesh();
    const admin = await MemPlatform.withKey(files, mesh);
    const platforms = new Map<string, MemPlatform>([['admin', admin]]);
    for (const name of names) {
        await admin.vault.create(name, 'pw');
        const platform = new MemPlatform(files, mesh.factory);
        platform.vaultView = new RelabeledVault(admin.vault, name, 'me');
        platforms.set(name, platform);
    }
    return { mesh, platforms, apps: [] };
}

function platformOf(w: World, name: string): MemPlatform {
    const platform = w.platforms.get(name);
    if (platform === undefined) throw new Error(`no platform '${name}'`);
    return platform;
}

function keyOf(w: World, name: string): string {
    const platform = platformOf(w, name);
    return (platform.vaultView ?? platform.vault).resolvePublic('me').keyId;
}

// `allow: null` is an app with no allow list: it accepts, serves and asks every peer.
async function open(w: World, name: string, allow: string[] | null): Promise<Rhost> {
    const app = await Rhost.open(appConfig(allow === null ? {} : { allow }), platformOf(w, name));
    w.apps.push(app);
    return app;
}

async function reopen(w: World, app: Rhost, name: string, allow: string[] | null): Promise<Rhost> {
    await app.close();
    w.apps.splice(w.apps.indexOf(app), 1);
    return open(w, name, allow);
}

async function closeAll(w: World): Promise<void> {
    for (const app of [...w.apps].reverse()) {
        try { await app.close(); } catch { /* best-effort */ }
    }
    w.mesh.close();
}

async function asAuthor(host: Host): Promise<void> {
    await host.runtime.session.unlockKey('me', 'pw');
    host.runtime.session.selectAuthor('me');
}

async function addIdentity(admin: Host, label: string, name: string): Promise<void> {
    await admin.runtime.execute(
        `INSERT INTO user.identities (keyId, publicKey, name) VALUES ($${label}, publicKey($${label}), '${name}') BY $me;`);
}

// Grants a capability and returns its row id, which a later DELETE takes.
async function grant(admin: Host, label: string, grantee: string): Promise<string> {
    const run = await admin.runtime.execute(`INSERT INTO user.caps (label, grantee) VALUES ('${label}', $${grantee}) BY $me;`);
    const result = run.results[0]?.result as unknown as { rowId?: string } | undefined;
    if (result?.rowId === undefined) throw new Error(`no row id for the '${label}' capability of ${grantee}`);
    return result.rowId;
}

async function revoke(admin: Host, rowId: string): Promise<void> {
    await admin.runtime.execute(`DELETE FROM user.caps WHERE rowId = '${rowId}' BY $me;`);
}

// Lets the doc group see the capabilities granted so far.
async function bindDoc(admin: Host): Promise<void> {
    await admin.runtime.execute(`UPDATE REF user TO LATEST ON doc BY $me;`);
}

async function writePage(host: Host, title: string): Promise<void> {
    await host.runtime.execute(`INSERT INTO doc.pages (title, deleted) VALUES ('${title}', false) BY $me;`);
}

function column(w: World, name: string, table: string, field: string): string {
    return rowsOf(platformOf(w, name).targets.get('default'), table).map((row) => String(row[field])).sort().join(',');
}

const identities = (w: World, name: string) => column(w, name, 'user_identities', 'name');
const pages = (w: World, name: string) => column(w, name, 'doc_pages', 'title');
const memberCaps = (w: World, name: string) =>
    rowsOf(platformOf(w, name).targets.get('default'), 'user_caps').filter((row) => row['label'] === 'member').length;

function peersOf(host: Host): string[] {
    return [...new Set((host.sync?.peers() ?? []).map((peer) => peer.keyId))];
}

async function bootstrapping(host: Host): Promise<boolean> {
    return (await host.status()).bootstrapping === true;
}

async function storedRecord(w: World, name: string): Promise<HostRecord | undefined> {
    return platformOf(w, name).hosts.read('default');
}

async function rejectsWith(fn: () => Promise<unknown>, fragment: string, what: string): Promise<void> {
    try {
        await fn();
    } catch (e) {
        assertTrue(String(e).includes(fragment), `${what}: expected '${fragment}' in '${String(e)}'`);
        return;
    }
    throw new Error(`${what}: expected an error`);
}

// The admin's host under a list that reads `member` capabilities: created,
// started, with the admin a member and a writer.
async function memberListAdmin(w: World): Promise<{ app: Rhost; host: Host }> {
    const app = await open(w, 'admin', BY_MEMBER_CAP);
    const host = await app.create(undefined, hostSetup());
    await app.start('default');
    await asAuthor(host);
    await grant(host, 'member', 'me');
    await grant(host, 'writer', 'me');
    await bindDoc(host);
    return { app, host };
}

export const bootstrapTests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST14] a joined host under an allow list starts, syncs, and its own write reaches the admin',
        invoke: async () => {
            const w = await world(['bob']);
            try {
                const appA = await open(w, 'admin', BY_IDENTITY);
                const a = await appA.create(undefined, hostSetup());
                await appA.start('default');
                await asAuthor(a);
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'writer', 'bob');
                await grant(a, 'writer', 'me');
                await bindDoc(a);
                await writePage(a, 'from-admin');
                assertFalse(await bootstrapping(a), 'a host that created its database never bootstraps');

                const appB = await open(w, 'bob', BY_IDENTITY);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                assertEquals(b.record.listedAt, undefined, 'a joined host starts with no position in its record');
                await appB.start('default');
                await waitFor(() => identities(w, 'bob') === 'Admin,Bob' && pages(w, 'bob') === 'from-admin',
                    'bob has the list and the page');
                await waitFor(async () => !(await bootstrapping(b)), 'bob is done bootstrapping');
                assertEquals(b.record.listedAt?.group, 'user', 'his record names the group that listed him');
                assertTrue((b.record.listedAt?.frontier.length ?? 0) > 0, 'and a position in it');

                await asAuthor(b);
                await writePage(b, 'from-bob');
                await waitFor(() => pages(w, 'admin') === 'from-admin,from-bob', "bob's page reaches the admin");
                assertEquals((await storedRecord(w, 'admin'))?.listedAt, undefined, "the creator's record names no position");
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST15] with the admin offline, a newcomer syncs from another member, who gets its write',
        invoke: async () => {
            const w = await world(['bob', 'carol']);
            try {
                const appA = await open(w, 'admin', BY_IDENTITY);
                const a = await appA.create(undefined, hostSetup());
                await appA.start('default');
                await asAuthor(a);
                await addIdentity(a, 'carol', 'Carol');
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'writer', 'bob');
                await grant(a, 'writer', 'carol');
                await grant(a, 'writer', 'me');
                await bindDoc(a);
                await writePage(a, 'from-admin');

                const appC = await open(w, 'carol', BY_IDENTITY);
                await appC.join(a.db.getId(), undefined, hostSetup());
                await appC.start('default');
                await waitFor(() => identities(w, 'carol') === 'Admin,Bob,Carol' && pages(w, 'carol') === 'from-admin', 'carol synced');
                await appA.stop('default');

                // The seed rows name only the admin, who is offline.
                const appB = await open(w, 'bob', BY_IDENTITY);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await waitFor(() => identities(w, 'bob') === 'Admin,Bob,Carol' && pages(w, 'bob') === 'from-admin',
                    'bob syncs from carol');
                await asAuthor(b);
                await writePage(b, 'from-bob');
                await waitFor(() => pages(w, 'carol') === 'from-admin,from-bob', "bob's page reaches carol");
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST16] a peer that is not on the list is admitted by a bootstrapping host and receives nothing',
        invoke: async () => {
            const w = await world(['bob', 'dave']);
            try {
                const { host: a } = await memberListAdmin(w);
                await addIdentity(a, 'dave', 'Dave');
                const daveCap = await grant(a, 'member', 'dave');
                await writePage(a, 'before');

                // Dave's host serves and asks everyone.
                const appD = await open(w, 'dave', null);
                await appD.join(a.db.getId(), undefined, hostSetup());
                await appD.start('default');
                await waitFor(() => pages(w, 'dave') === 'before', 'dave synced while a member');

                await revoke(a, daveCap);
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'member', 'bob');
                await writePage(a, 'after');
                // Dave reconnects, so the admin now refuses him.
                await appD.stop('default');
                await appD.start('default');
                await sleep(SETTLE_MS);
                assertEquals(pages(w, 'dave'), 'before', 'the admin gives dave nothing more');

                const appB = await open(w, 'bob', BY_MEMBER_CAP);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await waitFor(() => pages(w, 'bob') === 'after,before', 'bob synced');
                await waitFor(() => peersOf(b).includes(keyOf(w, 'dave')), 'bob admitted dave while bootstrapping');
                await writePage(a, 'later');
                await waitFor(() => pages(w, 'bob') === 'after,before,later', 'bob has the later page');
                await sleep(SETTLE_MS);
                assertEquals(pages(w, 'dave'), 'before', 'dave receives nothing through bob');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST17] a member removed while connected receives nothing written afterwards',
        invoke: async () => {
            const w = await world(['dave']);
            try {
                const { host: a } = await memberListAdmin(w);
                await addIdentity(a, 'dave', 'Dave');
                const daveCap = await grant(a, 'member', 'dave');
                await writePage(a, 'before');

                const appD = await open(w, 'dave', BY_MEMBER_CAP);
                await appD.join(a.db.getId(), undefined, hostSetup());
                await appD.start('default');
                await waitFor(() => pages(w, 'dave') === 'before' && memberCaps(w, 'dave') === 2, 'dave synced while a member');

                await revoke(a, daveCap);
                await writePage(a, 'after');
                await sleep(SETTLE_MS);
                assertTrue(peersOf(a).includes(keyOf(w, 'dave')), 'dave is still connected');
                assertEquals(pages(w, 'dave'), 'before', 'he does not receive the page written after his removal');
                assertEquals(memberCaps(w, 'dave'), 2, 'nor the removal itself');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST18] an established member refuses a peer its list does not name',
        invoke: async () => {
            const w = await world(['bob', 'carol']);
            try {
                const appA = await open(w, 'admin', BY_IDENTITY);
                const a = await appA.create(undefined, hostSetup());
                await appA.start('default');
                await asAuthor(a);
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'writer', 'me');
                await bindDoc(a);
                await writePage(a, 'first');

                const appB = await open(w, 'bob', BY_IDENTITY);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await waitFor(() => identities(w, 'bob') === 'Admin,Bob' && pages(w, 'bob') === 'first', 'bob synced');
                await appB.stop('default');

                await addIdentity(a, 'carol', 'Carol');
                await writePage(a, 'second');
                const appC = await open(w, 'carol', BY_IDENTITY);
                await appC.join(a.db.getId(), undefined, hostSetup());
                await appC.start('default');
                await waitFor(() => identities(w, 'carol') === 'Admin,Bob,Carol' && pages(w, 'carol') === 'first,second', 'carol synced');
                await appA.stop('default');

                // Bob's list is out of date: it names him, and not carol.
                await appB.start('default');
                await sleep(SETTLE_MS);
                assertFalse(await bootstrapping(b), 'bob holds his position, so he does not bootstrap');
                assertEquals(peersOf(b).length, 0, 'bob refuses carol');
                assertEquals(pages(w, 'bob'), 'first', 'and stays where he was');

                await appA.start('default');
                await waitFor(() => identities(w, 'bob') === 'Admin,Bob,Carol' && pages(w, 'bob') === 'first,second',
                    'bob catches up once a peer both know is back');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST19] a misspelled group fails a joined host\'s first start, a misspelled column its second',
        invoke: async () => {
            const w = await world(['bob']);
            try {
                const appA = await open(w, 'admin', BY_IDENTITY);
                const a = await appA.create(undefined, hostSetup());
                await appA.start('default');
                await asAuthor(a);
                await addIdentity(a, 'bob', 'Bob');

                let appB = await open(w, 'bob', ['usr.identities.keyId']);
                await appB.join(a.db.getId(), undefined, hostSetup());
                await rejectsWith(() => appB.start('default'), "Unknown group 'usr'", 'a group the database does not have');

                // The table is not there to check yet: the host starts, and
                // bootstraps since no key is on such a list.
                appB = await reopen(w, appB, 'bob', ['user.identities.nope']);
                await appB.start('default');
                const b = await appB.host('default');
                await waitFor(() => identities(w, 'bob') === 'Admin,Bob', 'bob takes entries meanwhile');
                assertTrue(await bootstrapping(b), 'and keeps bootstrapping');
                await appB.stop('default');
                await rejectsWith(() => appB.start('default'), "unknown column 'nope'", 'once the group is there, the column is checked');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST20] a removed host that has received its removal refuses a peer its list does not name',
        invoke: async () => {
            const w = await world(['bob', 'carol', 'eve']);
            try {
                const { app: appA, host: a } = await memberListAdmin(w);
                await addIdentity(a, 'bob', 'Bob');
                await addIdentity(a, 'carol', 'Carol');
                const bobCap = await grant(a, 'member', 'bob');
                await grant(a, 'member', 'carol');

                const appB = await open(w, 'bob', BY_MEMBER_CAP);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await waitFor(() => memberCaps(w, 'bob') === 3, 'bob synced while a member');
                await waitFor(async () => !(await bootstrapping(b)), 'and is done bootstrapping');

                // Carol's host serves everyone, so bob receives his removal from her.
                const appC = await open(w, 'carol', null);
                await appC.join(a.db.getId(), undefined, hostSetup());
                await appC.start('default');
                await waitFor(() => memberCaps(w, 'carol') === 3, 'carol synced');
                // Eve was never added. She has the database id, and gets the genesis from carol.
                const appE = await open(w, 'eve', null);
                const e = await appE.join(a.db.getId(), undefined, hostSetup());

                await revoke(a, bobCap);
                await waitFor(() => memberCaps(w, 'bob') === 2, 'bob has received his own removal');
                await appA.stop('default');
                await appC.stop('default');

                await appE.start('default');
                await sleep(SETTLE_MS);
                assertFalse(await bootstrapping(b), 'a removed host does not bootstrap again');
                assertFalse(peersOf(b).includes(keyOf(w, 'eve')), 'bob refuses eve');
                assertEquals(peersOf(e).length, 0, 'eve has no peer');

                await appB.stop('default');
                await appB.start('default');
                await sleep(SETTLE_MS);
                assertFalse(await bootstrapping(b), 'nor after a restart');
                assertFalse(peersOf(b).includes(keyOf(w, 'eve')), 'bob still refuses eve');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST21] a host that created its database, with an allow entry that matches nobody, refuses every peer',
        invoke: async () => {
            const w = await world(['carol', 'eve']);
            try {
                const { app: first, host: a0 } = await memberListAdmin(w);
                await addIdentity(a0, 'carol', 'Carol');
                await grant(a0, 'member', 'carol');
                const id = a0.db.getId();

                const appC = await open(w, 'carol', null);
                await appC.join(id, undefined, hostSetup());
                await appC.start('default');
                await waitFor(() => identities(w, 'carol') === 'Admin,Carol', 'carol synced');
                const appE = await open(w, 'eve', null);
                const e = await appE.join(id, undefined, hostSetup());
                await appC.stop('default');

                // The admin's own key is not on this list either.
                const appA = await reopen(w, first, 'admin', ["user.caps.grantee where label = 'membr'"]);
                await appA.start('default');
                const a = await appA.host('default');
                await appE.start('default');
                await sleep(SETTLE_MS);
                assertFalse(await bootstrapping(a), 'it does not bootstrap');
                assertEquals(peersOf(a).length, 0, 'it admits nobody');
                assertEquals(peersOf(e).length, 0, 'eve has no peer');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST22] a joined host bootstraps through starts with no peer, then records where it found its key',
        invoke: async () => {
            const w = await world(['bob']);
            try {
                const { app: appA, host: a } = await memberListAdmin(w);
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'member', 'bob');

                const appB = await open(w, 'bob', BY_MEMBER_CAP);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appA.stop('default');

                for (const attempt of ['first', 'second']) {
                    await appB.start('default');
                    await sleep(500);
                    assertTrue(await bootstrapping(b), `bootstrapping at the ${attempt} start with no peer`);
                    assertEquals((await storedRecord(w, 'bob'))?.listedAt, undefined, 'and nothing is written');
                    await appB.stop('default');
                }
                assertFalse(await bootstrapping(b), 'a stopped host is not bootstrapping');

                // The first write of the record fails.
                const hosts = platformOf(w, 'bob').hosts;
                const update = hosts.update.bind(hosts);
                let failures = 0;
                hosts.update = async (name, record) => {
                    if (failures === 0) {
                        failures++;
                        throw new Error('disk full');
                    }
                    return update(name, record);
                };
                const heard: HostStatus[] = [];
                const unwatch = b.onStatus((status) => { heard.push(status); });

                await appA.start('default');
                await appB.start('default');
                await waitFor(() => identities(w, 'bob') === 'Admin,Bob' && memberCaps(w, 'bob') === 2, 'bob synced');
                await waitFor(async () => !(await bootstrapping(b)), 'bob goes by his list for this run');
                await waitFor(async () => (await b.status()).lastError?.includes('disk full') === true, 'the failed write is his last error');
                assertEquals((await storedRecord(w, 'bob'))?.listedAt, undefined, 'the record was not written');
                await waitFor(() => heard.some((status) => status.running && status.bootstrapping !== true),
                    'whoever watches the status hears that bootstrapping ended');
                unwatch();

                // The next start writes it, with no peer around.
                await appB.stop('default');
                await appA.stop('default');
                await appB.start('default');
                assertFalse(await bootstrapping(b), 'done before any peer is admitted');
                const stored = await storedRecord(w, 'bob');
                assertEquals(stored?.listedAt?.group, 'user', 'the record names the group');
                assertTrue((stored?.listedAt?.frontier.length ?? 0) > 0, 'and a position in it');
                assertEquals((await b.status()).lastError, undefined, 'and the error is gone');
                assertFalse(stored?.created ?? true, 'the rest of the record is kept');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST23] a record that names an entry the replica does not hold makes the host bootstrap again',
        invoke: async () => {
            const w = await world(['bob']);
            try {
                const { app: appA, host: a } = await memberListAdmin(w);
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'member', 'bob');

                let appB = await open(w, 'bob', BY_MEMBER_CAP);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await waitFor(async () => (await storedRecord(w, 'bob'))?.listedAt !== undefined, 'bob recorded his position');
                assertFalse(await bootstrapping(b), 'and is done bootstrapping');
                await appA.stop('default');

                // As after putting back an older replica: the record is ahead of it.
                const missing = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
                const stored = await storedRecord(w, 'bob');
                await platformOf(w, 'bob').hosts.update('default', { ...stored!, listedAt: { group: 'user', frontier: [missing] } });
                appB = await reopen(w, appB, 'bob', BY_MEMBER_CAP);
                await appB.start('default');

                // His replica still names him, so the start finds the key again.
                const rewritten = (await storedRecord(w, 'bob'))?.listedAt;
                assertTrue(rewritten !== undefined && !rewritten.frontier.includes(missing), 'the record is rewritten');
                assertEquals(rewritten?.frontier.join(','), stored?.listedAt?.frontier.join(','), 'with the position the replica holds');
                assertFalse(await bootstrapping(await appB.host('default')), 'and the host is done bootstrapping');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST24] a host that is removed and added again catches up after a restart',
        invoke: async () => {
            const w = await world(['bob', 'carol']);
            try {
                const { host: a } = await memberListAdmin(w);
                await addIdentity(a, 'bob', 'Bob');
                await addIdentity(a, 'carol', 'Carol');
                const bobCap = await grant(a, 'member', 'bob');
                await grant(a, 'member', 'carol');
                await writePage(a, 'first');

                const appB = await open(w, 'bob', BY_MEMBER_CAP);
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await waitFor(() => pages(w, 'bob') === 'first' && memberCaps(w, 'bob') === 3, 'bob synced while a member');
                const appC = await open(w, 'carol', null);
                await appC.join(a.db.getId(), undefined, hostSetup());
                await appC.start('default');
                await waitFor(() => pages(w, 'carol') === 'first', 'carol synced');

                await revoke(a, bobCap);
                await waitFor(() => memberCaps(w, 'bob') === 2, 'bob has received his own removal');
                await appC.stop('default');
                await appB.stop('default');
                await appB.start('default');
                await sleep(SETTLE_MS);
                assertEquals(peersOf(b).length, 0, 'removed and restarted, bob has no peer');

                await grant(a, 'member', 'bob');
                await writePage(a, 'while-away');
                await sleep(SETTLE_MS);
                assertEquals(pages(w, 'bob'), 'first', 'added again, he does not catch up on his own');

                await appB.stop('default');
                await appB.start('default');
                await waitFor(() => pages(w, 'bob') === 'first,while-away' && memberCaps(w, 'bob') === 3, 'he catches up after a restart');
                assertFalse(await bootstrapping(b), 'without bootstrapping');
                assertTrue(peersOf(b).includes(keyOf(w, 'admin')), 'from the admin, a member he knew');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST25] a list may name a group that a later release added',
        invoke: async () => {
            const { v100, v110 } = await withGroup();
            const w = await world(['bob'], [v100]);
            try {
                let appA = await open(w, 'admin', BY_IDENTITY);
                let a = await appA.create(undefined, hostSetup());
                await appA.start('default');
                await asAuthor(a);
                await addIdentity(a, 'bob', 'Bob');
                const id = a.db.getId();

                platformOf(w, 'admin').releases = [v100, v110];
                appA = await reopen(w, appA, 'admin', BY_IDENTITY);
                const [report] = await appA.update();
                assertEquals(report?.deployed?.version, '1.1.0', 'the admin upgrades the database');
                await appA.start('default');
                a = await appA.host('default');
                assertTrue((await a.db.getMemberGroupNames()).has('notes'), 'which now has the new group');

                // Bob's host has only the genesis, which names the groups of 1.0.0.
                platformOf(w, 'bob').releases = [v110];
                let appB = await open(w, 'bob', ['user.identities.keyId', 'notes.pages.title']);
                const b = await appB.join(id, undefined, hostSetup());
                assertFalse((await b.db.getMemberGroupNames()).has('notes'), 'bob does not have the new group yet');
                await appB.start('default');
                await waitFor(async () => identities(w, 'bob') === 'Admin,Bob' && (await b.db.getMemberGroupNames()).has('notes'),
                    'bob syncs, and the upgrade arrives');
                await appB.stop('default');
                await appB.start('default');
                await appB.stop('default');

                appB = await reopen(w, appB, 'bob', ['user.identities.keyId', 'nots.pages.title']);
                await rejectsWith(() => appB.start('default'), "Unknown group 'nots'", 'a group that no release has');
            } finally {
                await closeAll(w);
            }
        },
    },
    {
        name: '[RHOST26] a host removed while its key is being found leaves no record behind',
        invoke: async () => {
            const w = await world(['bob']);
            try {
                const { host: a } = await memberListAdmin(w);
                await addIdentity(a, 'bob', 'Bob');
                await grant(a, 'member', 'bob');

                const appB = await open(w, 'bob', BY_MEMBER_CAP);
                await appB.join(a.db.getId(), undefined, hostSetup());

                // The write of the record waits until the test lets it go.
                const hosts = platformOf(w, 'bob').hosts;
                const update = hosts.update.bind(hosts);
                let reached = false;
                let release: () => void = () => {};
                const held = new Promise<void>((resolve) => { release = resolve; });
                const outcome: { error?: unknown; done: boolean } = { done: false };
                hosts.update = async (name, record) => {
                    reached = true;
                    await held;
                    try {
                        await update(name, record);
                    } catch (err) {
                        outcome.error = err;
                        throw err;
                    } finally {
                        outcome.done = true;
                    }
                };

                await appB.start('default');
                await waitFor(() => reached, 'bob found his key and is writing his record');
                // The host goes away while the write is still waiting.
                await Promise.race([
                    appB.remove('default'),
                    sleep(5000).then(() => { throw new Error('remove waited for the record to be written'); }),
                ]);
                release();
                await waitFor(() => outcome.done, 'the write was let go');

                assertTrue(String(outcome.error).includes("no host 'default'"), 'the store refuses to update a host it does not have');
                assertEquals((await hosts.list()).length, 0, 'no host is left');
                assertEquals(await hosts.read('default'), undefined, 'and no record');
            } finally {
                await closeAll(w);
            }
        },
    },
];
