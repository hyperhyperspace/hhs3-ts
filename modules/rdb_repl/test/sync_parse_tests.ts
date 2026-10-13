import { allowIsEveryone, formatAllow } from "@hyper-hyper-space/hhs3_rhost";

import { parseSyncCommand } from "../src/sync/parse.js";

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
    assert(actual === expected, `${message}: expected ${String(expected)}, got ${String(actual)}`);
}

export async function runSyncParseTests(): Promise<void> {
    const everyone = parseSyncCommand('start app as santi {"scope":"localhost"}');
    assert(everyone.kind === 'start', 'omitted allow is start');
    if (everyone.kind !== 'start') return;
    assertEqual(everyone.database, 'app', 'database');
    assertEqual(everyone.localId, 'santi', 'local-id');
    assertEqual(everyone.mesh.scope, 'localhost', 'scope');
    assert(allowIsEveryone(everyone.sources), 'omitted allow is everyone');
    assertEqual(formatAllow(everyone.sources), 'everyone', 'format omitted allow');

    const explicit = parseSyncCommand('start app as santi allow everyone {"scope":"internet"}');
    assert(explicit.kind === 'start' && allowIsEveryone(explicit.sources), 'bare everyone');

    const pathEveryone = parseSyncCommand('start app as santi allow users.everyone.id {"scope":"localhost"}');
    assert(pathEveryone.kind === 'start', 'users.everyone.id is start');
    if (pathEveryone.kind === 'start') {
        assert(!allowIsEveryone(pathEveryone.sources), 'users.everyone.id is not the everyone token');
        assertEqual(formatAllow(pathEveryone.sources), 'users.everyone.id', 'dotted everyone path');
        const src = pathEveryone.sources[0];
        assert(src !== undefined && src.type === 'column', 'path is a column source');
        if (src.type === 'column') {
            assertEqual(src.group, 'users', 'group');
            assertEqual(src.table, 'everyone', 'table named everyone');
            assertEqual(src.column, 'id', 'column');
        }
    }

    const where = parseSyncCommand("start app as santi allow users.caps.grantee where label='member' {\"scope\":\"localhost\"}");
    assert(where.kind === 'start', 'where is start');
    if (where.kind === 'start') {
        const src = where.sources[0];
        assert(src !== undefined && src.type === 'column' && src.where === "label='member'", 'where clause captured');
    }

    const onInString = parseSyncCommand("start app as santi allow users.caps.grantee where status='on' {\"scope\":\"internet\"}");
    assert(onInString.kind === 'start' && onInString.mesh.scope === 'internet', 'on inside quotes is not the scope keyword');
    if (onInString.kind === 'start') {
        const src = onInString.sources[0];
        assert(src !== undefined && src.type === 'column' && src.where === "status='on'", 'quoted on stays in where');
    }

    const union = parseSyncCommand(
        "start app as santi allow [users.caps.grantee where label='member', users.identities.keyId] {\"scope\":\"internet\",\"discovery\":[{\"type\":\"tracker\",\"url\":\"wss://other:4610\"}]}",
    );
    assert(union.kind === 'start', 'union is start');
    if (union.kind === 'start') {
        assertEqual(union.sources.length, 2, 'two sources');
        assert(!allowIsEveryone(union.sources), 'union of columns is not everyone');
        const tracker = union.mesh.discovery?.find(source => source.type === 'tracker');
        assertEqual(tracker?.type === 'tracker' ? tracker.url : undefined, 'wss://other:4610', 'tracker url');
        assertEqual(
            formatAllow(union.sources),
            "[users.caps.grantee where label='member', users.identities.keyId]",
            'format union',
        );
        const first = union.sources[0];
        assert(first !== undefined && first.type === 'column' && first.where === "label='member'", 'union where');
        const second = union.sources[1];
        assert(second !== undefined && second.type === 'column' && second.column === 'keyId', 'union second column');
    }

    const unionEveryone = parseSyncCommand('start app as santi allow [everyone, users.identities.keyId]');
    assert(unionEveryone.kind === 'start' && allowIsEveryone(unionEveryone.sources), 'union containing everyone is open');
    if (unionEveryone.kind === 'start') {
        assertEqual(formatAllow(unionEveryone.sources), 'everyone', 'status prints allow everyone');
    }

    const flags = parseSyncCommand(
        'start app as santi {"scope":"localhost","discovery":[{"type":"tracker","url":"ws://127.0.0.1:9","key":"KEY"}],"transports":{"ws":{"listen":"ws://127.0.0.1:1"}}}',
    );
    assert(flags.kind === 'start', 'flags is start');
    if (flags.kind === 'start') {
        const tracker = flags.mesh.discovery?.[0];
        assert(tracker?.type === 'tracker', 'tracker entry');
        if (tracker?.type === 'tracker') {
            assertEqual(tracker.url, 'ws://127.0.0.1:9', 'tracker url');
            assertEqual(tracker.key, 'KEY', 'tracker key');
        }
        const ws = flags.mesh.transports?.ws;
        assert(ws !== undefined && ws !== false && ws.listen === 'ws://127.0.0.1:1', 'ws listen');
    }

    const status = parseSyncCommand('status app');
    assert(status.kind === 'status' && status.database === 'app', 'status filter');
    const statusAll = parseSyncCommand('');
    assert(statusAll.kind === 'status' && statusAll.database === undefined, 'bare remainder is status');
    const stop = parseSyncCommand('stop 3');
    assert(stop.kind === 'stop' && stop.id === 3, 'stop id');
    const peers = parseSyncCommand('peers 12');
    assert(peers.kind === 'peers' && peers.id === 12, 'peers id');

    let threw = false;
    const FULL_ID = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
    const fetched = parseSyncCommand(`fetch #${FULL_ID} as santi {"scope":"localhost"}`);
    assert(fetched.kind === 'fetch', 'fetch is fetch');
    if (fetched.kind === 'fetch') {
        assertEqual(fetched.rdbId, FULL_ID, 'fetch rdb id without #');
        assertEqual(fetched.localId, 'santi', 'fetch local-id');
        assertEqual(fetched.mesh.scope, 'localhost', 'fetch scope');
    }

    const fetchFlags = parseSyncCommand(
        `fetch #${FULL_ID} as santi {"scope":"internet","discovery":[{"type":"tracker","url":"wss://other:4610","key":"KEY"}],"transports":{"ws":{"listen":"ws://127.0.0.1:1"}}}`,
    );
    assert(fetchFlags.kind === 'fetch', 'fetch flags is fetch');
    if (fetchFlags.kind === 'fetch') {
        const tracker = fetchFlags.mesh.discovery?.[0];
        assert(tracker?.type === 'tracker' && tracker.url === 'wss://other:4610', 'fetch tracker');
        assert(tracker?.type === 'tracker' && tracker.key === 'KEY', 'fetch tracker key');
        const ws = fetchFlags.mesh.transports?.ws;
        assert(ws !== undefined && ws !== false && ws.listen === 'ws://127.0.0.1:1', 'fetch ws listen');
    }

    threw = false;
    try { parseSyncCommand(`fetch #${FULL_ID} as santi allow everyone`); } catch { threw = true; }
    assert(threw, 'fetch rejects allow');

    threw = false;
    try { parseSyncCommand('fetch shopdb as santi'); } catch (err) {
        threw = true;
        assert(err instanceof Error && err.message.includes('full #hash'), 'fetch name is rejected as not a hash');
    }
    assert(threw, 'fetch name is rejected');

    threw = false;
    try { parseSyncCommand('fetch #AAAAAAAA as santi'); } catch (err) {
        threw = true;
        assert(err instanceof Error && err.message.includes('full object hash'), 'fetch prefix is rejected');
    }
    assert(threw, 'fetch prefix is rejected');

    threw = false;
    try { parseSyncCommand(`fetch #${FULL_ID}`); } catch { threw = true; }
    assert(threw, 'fetch without as is rejected');

    threw = false;
    const bareFetch = parseSyncCommand(`fetch #${FULL_ID} as santi`);
    assert(bareFetch.kind === 'fetch' && bareFetch.mesh.scope === 'internet', 'fetch without mesh JSON is internet');

    threw = false;
    try { parseSyncCommand('start app as santi {"scope":"lan"}'); } catch { threw = true; }
    assert(threw, 'unknown scope is rejected');

    threw = false;
    try { parseSyncCommand('stop x'); } catch { threw = true; }
    assert(threw, 'non-numeric stop id is rejected');
}
