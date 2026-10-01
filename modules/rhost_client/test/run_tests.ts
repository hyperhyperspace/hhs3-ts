import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    LineDecoder, decodeRequest, decodeServerMessage, encodeMessage, isHostStatus,
    type HostStatus, type StoppedStatus,
} from "../src/index.js";

function throwsWith(fn: () => unknown, fragment: string, what: string): void {
    try {
        fn();
    } catch (e) {
        assertTrue(String(e).includes(fragment), `${what}: expected '${fragment}' in '${String(e)}'`);
        return;
    }
    throw new Error(`${what}: expected an error`);
}

const status: HostStatus = {
    host: 'default', database: 'DB', catalog: 'editor', created: true, adoptionRange: '<2.0.0',
    deployed: [{ hash: 'H', version: '1.0.0' }], adopted: [], held: [], members: [],
    upgradeRequired: false, hostBehind: true, running: true, peers: 2,
    notDeployed: '1.1.0 needs a value for :moderator (identity), which params don\'t set',
    missingParams: [{ name: 'moderator', type: 'identity' }],
};

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST_CLIENT01] messages are one JSON object per line; requests and replies decode with their ids',
        invoke: async () => {
            const line = encodeMessage({ id: 1, method: 'status' });
            assertTrue(line.endsWith('\n') && !line.slice(0, -1).includes('\n'), 'one line per message');
            assertEquals(decodeRequest(line).method, 'status', 'status round-trips');
            const unwatch = decodeRequest(encodeMessage({ id: 3, method: 'unwatch', watch: 2 }));
            assertTrue(unwatch.method === 'unwatch' && unwatch.watch === 2, 'unwatch carries the watch id');

            const reply = decodeServerMessage(encodeMessage({ id: 1, result: status }));
            assertTrue('result' in reply && reply.result?.peers === 2, 'a result carries the status');
            assertTrue('result' in reply && reply.result?.missingParams?.[0]?.name === 'moderator', 'and why the host is held');
            const push = decodeServerMessage(encodeMessage({ id: 2, event: status }));
            assertTrue('event' in push && push.id === 2, 'a push is tagged with its watch');
            const error = decodeServerMessage(encodeMessage({ id: 4, error: 'no' }));
            assertTrue('error' in error && error.error === 'no', 'an error');

            throwsWith(() => decodeRequest('{"id":1,"method":"deploy"}'), 'unknown method "deploy"', 'no signing methods');
            throwsWith(() => decodeRequest('{"method":"status"}'), 'integer id', 'an id is required');
            throwsWith(() => decodeRequest('[1]'), 'JSON object', 'an object is required');
            throwsWith(() => decodeRequest('nope'), 'not JSON', 'JSON is required');
            throwsWith(() => decodeRequest('{"id":1,"method":"unwatch"}'), 'unwatch needs', 'unwatch needs its watch');
        },
    },
    {
        name: '[RHOST_CLIENT02] the line decoder reassembles lines split across chunks',
        invoke: async () => {
            const decoder = new LineDecoder();
            assertEquals(decoder.push('{"id":1,').length, 0, 'no line yet');
            const lines = decoder.push('"method":"status"}\n{"id":2,"method":"watchStatus"}\n{"id"');
            assertEquals(lines.length, 2, 'two complete lines');
            assertEquals(decodeRequest(lines[1]!).method, 'watchStatus', 'the second line');
            assertEquals(decoder.push(':3,"method":"status"}\n').length, 1, 'the rest arrives later');
            assertEquals(decoder.push('\n\n').length, 0, 'blank lines are skipped');

            const stopped: StoppedStatus = { host: 'default', database: 'DB', catalog: 'editor', created: true, running: false };
            assertTrue(isHostStatus(status) && !isHostStatus(stopped), 'a full status and a stopped one are told apart');
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);

    console.log('Running tests for Hyper Hyper Space v3 rhost_client module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    console.log('rhost_client');

    for (const test of tests) {
        let match = true;
        for (const filter of filters) {
            match = match && test.name.indexOf(filter) >= 0;
        }

        if (match) {
            testing.exitIfFailed(await testing.run(test.name, test.invoke));
        } else {
            await testing.skip(test.name);
        }
    }

    console.log();
}

main();
