# mesh_tracker

Tracker server for HHSv3 mesh peer discovery. Accepts authenticated connections from `mesh_tracker_client` peers, maintains an in-memory registry of topic-to-peer mappings with TTL-based expiry, and responds to ANNOUNCE / QUERY / LEAVE requests.

## Identity management

The tracker has its own cryptographic identity (signing keypair), following an sshd-like model:

- **First run:** generates a keypair, saves it to `./tracker-identity.json`, and prints the `KeyId`
- **Subsequent runs:** loads the existing keypair from disk
- **Explicit generation:** `--generate-identity [path]` creates a new keypair and exits
- **Explicit path:** `--identity <path>` loads a specific identity file

Clients can verify the tracker's identity by its `KeyId`, preventing impersonation.

## Running

```bash
# Default mode (generate identity on first run, listen on ws://0.0.0.0:4433)
npm start

# Custom options
node --import ../../register.mjs ./src/main.ts \
    --listen ws://0.0.0.0:9000 \
    --identity /path/to/identity.json \
    --signing ed25519 \
    --kem x25519-hkdf \
    --ttl-min 60 \
    --ttl-max 600
```

## TrackerServer API

The server is also usable as a library:

```typescript
import { TrackerServer } from '@hyper-hyper-space/hhs3_mesh_tracker';

const server = new TrackerServer({
    transportProvider: wsProvider,
    authenticator: noiseAuth,
    listenAddress: 'ws://0.0.0.0:4433',
    ttlMin: 60,      // seconds
    ttlMax: 600,      // seconds
    sweepInterval: 30000,  // ms
});

await server.start();
// ...
server.stop();
```

## Anti-spoofing

The server verifies that the `keyId` in every ANNOUNCE request matches the authenticated identity of the connection (established via the Noise handshake). This prevents a peer from announcing on behalf of another.

## TTL and expiry

Clients request a TTL with each ANNOUNCE. The server clamps it to `[ttlMin, ttlMax]` and returns the confirmed value. A background sweep removes expired entries periodically.

## Building

```
npm install
npm run build
```

## Testing

```
npm run test
```
