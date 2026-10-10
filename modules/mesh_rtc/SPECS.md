# rtc:// signaling

Status: Initial version v0.1

The mesh dials one scheme, `rtc`. The address names a signaling server and a stable endpoint id. Mesh bytes ride a reliable, ordered WebRTC data channel. The signaling socket is only for setup.

## Address

```
rtc://<host>[:<port>][/<mount>]/<endpointId>
```

The provider opens signaling at `wss://` with the same host, port, mount, and endpoint id. The endpoint id is stable across delegation renewals. The delegation itself is not part of the address.

## Delegation

The listener signs, with its mesh signing key:

```
hhs3-rtc-delegate-v1
<base64 serialized public key>
<signaling origin>
<endpointId>
<expiry unix seconds>
```

Lines are joined with `\n`. The signaling origin is the `wss://` URL without the endpoint id.

A dialer checks the signature, that the origin is the server it opened, that the expiry is still ahead (60 seconds of clock skew), and that the public key's key id is the peer discovery named. It does this before creating a peer connection or sending an ICE candidate.

## Possession

On each attach, and whenever the server sends a fresh nonce, the listener signs:

```
hhs3-rtc-register-v1
<nonce base64>
<endpointId>
<signaling origin>
```

An honest server accepts the listen socket only when this proof matches the current nonce and the delegation is valid. Private servers also require the key id on an allowlist. Public servers accept any key that passes those checks.

## Messages

JSON text frames: `hello`, `register`, `registered`, `renew`, `dial`, `warrant`, `signal`, `bye`, `reject`. `signal` carries an opaque offer, answer, or ICE candidate. Topics are not part of this protocol.

The listener keeps one socket. The dialer opens a socket, reads the warrant, exchanges signals, and closes that socket when the data channel opens. A dead data channel is a closed mesh transport. The swarm dials again.

## Data channel

One ordered, reliable channel labeled `hhs3`. Messages larger than 16 KiB are framed inside the transport. `remoteAddress` on the resulting mesh transport is the remote `rtc://` address. When both sides dial at once, the lower endpoint id drops its outbound attempt and answers the inbound one.
