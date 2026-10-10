# Mesh RTC Node

Node `TransportProvider` for `rtc://`. The peer connection is [werift](https://github.com/shinyoshiaki/werift-webrtc). Signaling uses the `ws` package.

Pass `signalBase` (a `wss://` URL with no endpoint id) to listen. Without it the provider can still dial.
