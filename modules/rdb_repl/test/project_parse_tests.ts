import { parseProjectCommand } from "../src/projection/parse.js";

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
    assert(actual === expected, `${message}: expected ${String(expected)}, got ${String(actual)}`);
}

function assertThrows(fn: () => void, message: string, match?: string): void {
    let threw = false;
    try {
        fn();
    } catch (err) {
        threw = true;
        if (match !== undefined) {
            assert(err instanceof Error && err.message.includes(match), `${message}: ${String(err)}`);
        }
    }
    assert(threw, message);
}

export async function runProjectParseTests(): Promise<void> {
    const start = parseProjectCommand('start shopdb as alice to ./shop.sqlite');
    assert(start.kind === 'start', 'start is start');
    if (start.kind === 'start') {
        assertEqual(start.database, 'shopdb', 'database');
        assertEqual(start.localId, 'alice', 'local-id');
        assertEqual(start.path, './shop.sqlite', 'path');
    }

    const memory = parseProjectCommand('start shopdb as alice to :memory:');
    assert(memory.kind === 'start' && memory.path === ':memory:', 'to :memory:');

    const quoted = parseProjectCommand('start shopdb as alice to "./my shop.sqlite"');
    assert(quoted.kind === 'start' && quoted.path === './my shop.sqlite', 'quoted path with spaces');

    const quotedSingle = parseProjectCommand("start shopdb as alice to '/tmp/proj.sqlite'");
    assert(quotedSingle.kind === 'start' && quotedSingle.path === '/tmp/proj.sqlite', 'single-quoted path');

    const status = parseProjectCommand('status shopdb');
    assert(status.kind === 'status' && status.database === 'shopdb', 'status filter');
    const statusAll = parseProjectCommand('');
    assert(statusAll.kind === 'status' && statusAll.database === undefined, 'bare remainder is status');

    const stop = parseProjectCommand('stop 3');
    assert(stop.kind === 'stop' && stop.id === 3, 'stop id');
    const update = parseProjectCommand('update 12');
    assert(update.kind === 'update' && update.id === 12, 'update id');
    const events = parseProjectCommand('events 1');
    assert(events.kind === 'events' && events.id === 1, 'events id');

    const register = parseProjectCommand('register-key 2 kh pk');
    assert(register.kind === 'register-key', 'register-key');
    if (register.kind === 'register-key') {
        assertEqual(register.id, 2, 'register session id');
        assertEqual(register.keyHash, 'kh', 'keyHash');
        assertEqual(register.publicKey, 'pk', 'publicKey');
    }

    const resolve = parseProjectCommand('resolve-key 2 7');
    assert(resolve.kind === 'resolve-key' && resolve.id === 2 && resolve.token === '7', 'resolve-key');

    assertThrows(() => parseProjectCommand('start shopdb to :memory:'), 'start without as is rejected', "Expected 'as'");
    assertThrows(() => parseProjectCommand('start shopdb as alice'), 'start without to is rejected', "Expected 'to'");
    assertThrows(() => parseProjectCommand('start'), 'start without args is rejected');
    assertThrows(() => parseProjectCommand('stop shopdb'), 'non-numeric stop id is rejected', 'numeric session id');
    assertThrows(() => parseProjectCommand('update x'), 'non-numeric update id is rejected', 'numeric session id');
    assertThrows(() => parseProjectCommand('sync 1'), 'old sync subcommand is rejected');
    assertThrows(() => parseProjectCommand('start shopdb as alice to "./oops'), 'unclosed quote is rejected', 'Unclosed');
    assertThrows(() => parseProjectCommand('start shopdb as alice to :memory: extra'), 'trailing token is rejected', 'Unexpected token');
}
