# Mesh RTC

Shared WebRTC pieces for the HHSv3 mesh: the `rtc://` address, listener delegations, the signaling codec, and the session that turns a data channel into a mesh `Transport`.

Browser and Node providers live in `mesh_rtc_browser` and `mesh_rtc_node`. The reference signaling server is `mesh_signal`. See [SPECS.md](./SPECS.md).

## Building

```
npm install
npm run build
```
