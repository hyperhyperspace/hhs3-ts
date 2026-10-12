# Mesh RTC Node

Node `TransportProvider` for `rtc://`. The peer connection is [werift](https://github.com/shinyoshiaki/werift-webrtc). Signaling uses the `ws` package.

Pass `signalBase` (a `wss://` URL with no endpoint id) to listen. Without it the provider can still dial.

`npm run test:live --workspace=modules/mesh_rtc_node` exchanges one mesh message through `wss://signal.mypeer.net:4612`. It is not part of `npm test`.

`npm run test:stun --workspace=modules/mesh_rtc_node` is the same pair twice, once with `stun:stun.l.google.com:19302` and once with `stun:stun.mypeer.net:3478`, and no injected loopback address. It is not part of `npm test`.
