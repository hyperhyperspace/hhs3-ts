# Mesh Signal

Reference signaling server for the `rtc://` mesh transport. It checks listener delegations and possession proofs, then forwards opaque SDP between a persistent listener socket and short-lived dialer sockets.

TLS terminates upstream. This process listens on plain WebSocket. `--origin` is the public `wss://` origin listeners must sign.

```
node --import ../../register.mjs ./src/main.ts --origin wss://signal.example.com:443 --port 9443 --public
node --import ../../register.mjs ./src/main.ts --origin wss://signal.example.com:443 --allow <keyId>
```

The default is private: only `--allow` key ids may register. `--public` accepts any valid delegation.

## Building

```
npm install
npm run build
```
