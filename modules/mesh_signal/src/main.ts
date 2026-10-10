// Reference signaling server. TLS is terminated upstream; this process listens
// on ws and checks delegations against the public wss origin.
//
//   node --import ../../register.mjs ./src/main.ts --origin wss://signal.example.com:443 --public
//   node --import ../../register.mjs ./src/main.ts --origin wss://signal.example.com:443 --allow <keyId>

import { SignalServer } from './signal_server.js';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';

function parseArgs(argv: string[]): { flags: Set<string>; values: Map<string, string[]> } {
    const flags = new Set<string>();
    const values = new Map<string, string[]>();
    for (let i = 0; i < argv.length; i++) {
        const key = argv[i];
        if (key === undefined || !key.startsWith('--')) continue;
        const name = key.slice(2);
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith('--')) {
            const list = values.get(name) ?? [];
            list.push(next);
            values.set(name, list);
            i++;
        } else {
            flags.add(name);
        }
    }
    return { flags, values };
}

async function main(): Promise<void> {
    const { flags, values } = parseArgs(process.argv.slice(2));
    const origin = values.get('origin')?.[0];
    if (origin === undefined) {
        console.error('missing --origin wss://host[:port][/mount]');
        process.exit(1);
    }
    const port = Number(values.get('port')?.[0] ?? '9443');
    const host = values.get('host')?.[0] ?? '0.0.0.0';
    const server = new SignalServer({
        host,
        port,
        publicOrigin: origin,
        publicMode: flags.has('public'),
        allow: (values.get('allow') ?? []) as KeyId[],
    });
    await server.start();
    console.log(`signaling on ws://${host}:${server.port} for ${origin} (${flags.has('public') ? 'public' : 'private'})`);
}

main().catch(err => {
    console.error(err instanceof Error ? err.message : err);
    process.exit(1);
});
