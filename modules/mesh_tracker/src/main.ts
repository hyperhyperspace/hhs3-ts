// CLI entry point for the HHSv3 tracker server. Supports sshd-like identity
// management: generate on first run, persist to disk, reload on subsequent
// starts. Run with: node --import ../../register.mjs ./src/main.ts

import {
    SIGNING_ED25519, KEM_X25519_HKDF,
    type SigningName, type KemName,
} from '@hyper-hyper-space/hhs3_crypto';
import { createNoiseAuthenticator } from '@hyper-hyper-space/hhs3_mesh';
import { WsTransportProvider } from '@hyper-hyper-space/hhs3_mesh_ws';
import { loadOrCreateIdentity, generateIdentity, saveIdentity, identityKeyId } from './identity.js';
import { TrackerServer } from './tracker_server.js';

function parseArgs(argv: string[]): Record<string, string | true> {
    const args: Record<string, string | true> = {};
    for (let i = 0; i < argv.length; i++) {
        const key = argv[i];
        if (!key.startsWith('--')) continue;
        const name = key.slice(2);
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith('--')) {
            args[name] = next;
            i++;
        } else {
            args[name] = true;
        }
    }
    return args;
}

async function main() {
    const args = parseArgs(process.argv.slice(2));

    const signingName = (args['signing'] as string ?? SIGNING_ED25519) as SigningName;
    const kemName = (args['kem'] as string ?? KEM_X25519_HKDF) as KemName;

    // --generate-identity [path]
    if (args['generate-identity'] !== undefined) {
        const filePath = typeof args['generate-identity'] === 'string'
            ? args['generate-identity']
            : './tracker-identity.json';
        const identity = await generateIdentity(signingName);
        await saveIdentity(filePath, identity);
        const keyId = identityKeyId(identity);
        console.log(`Identity generated: ${keyId}`);
        console.log(`Saved to: ${filePath}`);
        return;
    }

    // Load or create identity
    const identityPath = typeof args['identity'] === 'string'
        ? args['identity']
        : './tracker-identity.json';
    const identity = await loadOrCreateIdentity(identityPath, signingName);
    const keyId = identityKeyId(identity);

    const listenAddress = typeof args['listen'] === 'string'
        ? args['listen']
        : 'ws://0.0.0.0:4433';

    const ttlMin = typeof args['ttl-min'] === 'string' ? parseInt(args['ttl-min'], 10) : undefined;
    const ttlMax = typeof args['ttl-max'] === 'string' ? parseInt(args['ttl-max'], 10) : undefined;

    const authenticator = createNoiseAuthenticator({
        localKey: { publicKey: identity.publicKey, secretKey: identity.secretKey },
        signingName,
        kemPrefs: [kemName],
    });

    const transportProvider = new WsTransportProvider();

    const server = new TrackerServer({
        transportProvider,
        authenticator,
        listenAddress,
        ttlMin,
        ttlMax,
    });

    await server.start();
    console.log(`HHSv3 Tracker Server`);
    console.log(`  KeyId:   ${keyId}`);
    console.log(`  Listen:  ${listenAddress}`);
    console.log(`  Signing: ${signingName}`);
    console.log(`  KEM:     ${kemName}`);

    process.on('SIGINT', () => {
        console.log('\nShutting down...');
        server.stop();
        process.exit(0);
    });
    process.on('SIGTERM', () => {
        server.stop();
        process.exit(0);
    });
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
