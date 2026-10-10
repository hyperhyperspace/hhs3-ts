# Mesh RTC Browser

Browser `TransportProvider` for `rtc://`, using `RTCPeerConnection` and `WebSocket`.

Pass `signalBase` (a `wss://` URL with no endpoint id) to listen. Without it the provider can still dial addresses discovered from other peers.
