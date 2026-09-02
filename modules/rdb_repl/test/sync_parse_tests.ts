import { createAllowAuthorizer } from "../src/sync/authorizer.js";
import {
    allowIsEveryone,
    formatAllow,
    parseSyncCommand,
    type AllowSource,
} from "../src/sync/parse.js";
import {
    DEFAULT_INTERNET_TRACKER,
    DEFAULT_INTERNET_TRACKER_KEY,
    DEFAULT_LOCAL_TRACKER,
    resolveTrackerConfig,
} from "../src/sync/discovery.js";

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
    assert(actual === expected, `${message}: expected ${String(expected)}, got ${String(actual)}`);
}

export async function runSyncParseTests(): Promise<void> {
    const everyone = parseSyncCommand('start app as santi on localhost');
    assert(everyone.kind === 'start', 'omitted allow is start');
    if (everyone.kind !== 'start') return;
    assertEqual(everyone.database, 'app', 'database');
    assertEqual(everyone.localId, 'santi', 'local-id');
    assertEqual(everyone.scope, 'localhost', 'scope');
    assert(allowIsEveryone(everyone.sources), 'omitted allow is everyone');
    assertEqual(formatAllow(everyone.sources), 'everyone', 'format omitted allow');

    const explicit = parseSyncCommand('start app as santi allow everyone on internet');
    assert(explicit.kind === 'start' && allowIsEveryone(explicit.sources), 'bare everyone');

    const pathEveryone = parseSyncCommand('start app as santi allow users.everyone.id on localhost');
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

    const where = parseSyncCommand("start app as santi allow users.caps.grantee where label='member' on localhost");
    assert(where.kind === 'start', 'where is start');
    if (where.kind === 'start') {
        const src = where.sources[0];
        assert(src !== undefined && src.type === 'column' && src.where === "label='member'", 'where clause captured');
    }

    const onInString = parseSyncCommand("start app as santi allow users.caps.grantee where status='on' on internet");
    assert(onInString.kind === 'start' && onInString.scope === 'internet', 'on inside quotes is not the scope keyword');
    if (onInString.kind === 'start') {
        const src = onInString.sources[0];
        assert(src !== undefined && src.type === 'column' && src.where === "status='on'", 'quoted on stays in where');
    }

    const union = parseSyncCommand(
        "start app as santi allow [users.caps.grantee where label='member', users.identities.keyId] on internet --tracker wss://other:4610",
    );
    assert(union.kind === 'start', 'union is start');
    if (union.kind === 'start') {
        assertEqual(union.sources.length, 2, 'two sources');
        assert(!allowIsEveryone(union.sources), 'union of columns is not everyone');
        assertEqual(union.tracker, 'wss://other:4610', 'tracker flag');
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

    const unionEveryone = parseSyncCommand('start app as santi allow [everyone, users.identities.keyId] on localhost');
    assert(unionEveryone.kind === 'start' && allowIsEveryone(unionEveryone.sources), 'union containing everyone is open');
    if (unionEveryone.kind === 'start') {
        assertEqual(formatAllow(unionEveryone.sources), 'everyone', 'status prints allow everyone');
    }

    const flags = parseSyncCommand(
        'start app as santi on localhost --tracker ws://127.0.0.1:9 --tracker-key KEY --listen ws://127.0.0.1:1',
    );
    assert(flags.kind === 'start', 'flags is start');
    if (flags.kind === 'start') {
        assertEqual(flags.tracker, 'ws://127.0.0.1:9', '--tracker');
        assertEqual(flags.trackerKey, 'KEY', '--tracker-key');
        assertEqual(flags.listen, 'ws://127.0.0.1:1', '--listen');
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
    const fetched = parseSyncCommand(`fetch #${FULL_ID} as santi on localhost`);
    assert(fetched.kind === 'fetch', 'fetch is fetch');
    if (fetched.kind === 'fetch') {
        assertEqual(fetched.rdbId, FULL_ID, 'fetch rdb id without #');
        assertEqual(fetched.localId, 'santi', 'fetch local-id');
        assertEqual(fetched.scope, 'localhost', 'fetch scope');
    }

    const fetchFlags = parseSyncCommand(
        `fetch #${FULL_ID} as santi on internet --tracker wss://other:4610 --tracker-key KEY --listen ws://127.0.0.1:1`,
    );
    assert(fetchFlags.kind === 'fetch', 'fetch flags is fetch');
    if (fetchFlags.kind === 'fetch') {
        assertEqual(fetchFlags.tracker, 'wss://other:4610', 'fetch --tracker');
        assertEqual(fetchFlags.trackerKey, 'KEY', 'fetch --tracker-key');
        assertEqual(fetchFlags.listen, 'ws://127.0.0.1:1', 'fetch --listen');
    }

    threw = false;
    try { parseSyncCommand(`fetch #${FULL_ID} as santi allow everyone on localhost`); } catch { threw = true; }
    assert(threw, 'fetch rejects allow');

    threw = false;
    try { parseSyncCommand('fetch shopdb as santi on localhost'); } catch (err) {
        threw = true;
        assert(err instanceof Error && err.message.includes('full #hash'), 'fetch name is rejected as not a hash');
    }
    assert(threw, 'fetch name is rejected');

    threw = false;
    try { parseSyncCommand('fetch #AAAAAAAA as santi on localhost'); } catch (err) {
        threw = true;
        assert(err instanceof Error && err.message.includes('full object hash'), 'fetch prefix is rejected');
    }
    assert(threw, 'fetch prefix is rejected');

    threw = false;
    try { parseSyncCommand(`fetch #${FULL_ID} on localhost`); } catch { threw = true; }
    assert(threw, 'fetch without as is rejected');

    threw = false;
    try { parseSyncCommand(`fetch #${FULL_ID} as santi`); } catch { threw = true; }
    assert(threw, 'fetch without on is rejected');

    threw = false;
    try { parseSyncCommand('start app as santi on lan'); } catch { threw = true; }
    assert(threw, 'unknown scope is rejected');

    threw = false;
    try { parseSyncCommand('stop x'); } catch { threw = true; }
    assert(threw, 'non-numeric stop id is rejected');

    const local = resolveTrackerConfig('localhost', {});
    assertEqual(local.address, DEFAULT_LOCAL_TRACKER, 'localhost tracker default');
    assert(local.keyId === undefined, 'localhost tracker is TOFU');

    const internet = resolveTrackerConfig('internet', {});
    assertEqual(internet.address, DEFAULT_INTERNET_TRACKER, 'internet tracker default');
    assertEqual(internet.keyId, DEFAULT_INTERNET_TRACKER_KEY, 'internet tracker is pinned');

    const override = resolveTrackerConfig('internet', { tracker: 'wss://other:4610' });
    assertEqual(override.address, 'wss://other:4610', 'flag overrides tracker URL');
    assert(override.keyId === undefined, 'overridden tracker is TOFU unless keyed');

    const envWinsOverDefault = resolveTrackerConfig('localhost', {}, { tracker: 'ws://env:1' });
    assertEqual(envWinsOverDefault.address, 'ws://env:1', 'env overrides scope default');

    const flagWinsOverEnv = resolveTrackerConfig('localhost', { tracker: 'ws://flag:1' }, { tracker: 'ws://env:1' });
    assertEqual(flagWinsOverEnv.address, 'ws://flag:1', 'flag overrides env');
}

export async function runSyncAuthorizerTests(): Promise<void> {
    const alice = 'alice-key';
    const bob = 'bob-key';
    const eve = 'eve-key';
    const sources: AllowSource[] = [
        { type: 'column', group: 'users', table: 'caps', column: 'grantee' },
        { type: 'column', group: 'users', table: 'identities', column: 'keyId' },
    ];
    const auth = createAllowAuthorizer(sources, async (source) => {
        if (source.column === 'grantee') return [alice];
        return [bob];
    });
    assert(auth !== undefined, 'column union has an authorizer');
    assert(await auth!.authorize(alice), 'first column match');
    assert(await auth!.authorize(bob), 'second column match');
    assert(!(await auth!.authorize(eve)), 'neither column match');

    assert(
        createAllowAuthorizer([{ type: 'everyone' }], async () => []) === undefined,
        'everyone short-circuits to no authorizer',
    );
    assert(
        createAllowAuthorizer(
            [{ type: 'everyone' }, { type: 'column', group: 'u', table: 't', column: 'c' }],
            async () => [eve],
        ) === undefined,
        'union containing everyone is open',
    );
}
