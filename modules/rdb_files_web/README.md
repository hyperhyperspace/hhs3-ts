# rdb_files_web

`HandleDirectory`: a [file mount](../rdb_projection#file-mounts)'s folder in the browser, over a `FileSystemDirectoryHandle`.

```ts
import { HandleDirectory } from '@hyper-hyper-space/hhs3_rdb_files_web';

const media = await HandleDirectory.opfs(['hosts', 'default', 'media']);   // the origin private file system
// or a folder the user picked, after asking for readwrite access in a user gesture:
const picked = new HandleDirectory(await showDirectoryPicker({ mode: 'readwrite' }));

await projection.reconcileFiles([{ name: 'media', path: 'media' }], () => media);
```

- **`write`** uses `createWritable()`, which stages into a swap file and commits on close, so a reader sees the old bytes or the new ones; a failed write is aborted. Safari writes this way from version 26 on (older Safari reads OPFS but can't write from the main thread).
- **`rename`** uses `FileSystemHandle.move()` where the browser has it, and otherwise copies then removes.
- **`remove`** and **`rename`** prune the folders they leave empty, never the root or a folder passed to **`preserve`**.
- **`ensureDir`** makes a folder and its missing parents; a file in the way is an error.
- **`watch`** uses `FileSystemObserver` where it exists (Chromium). Elsewhere there is no hint, and the mount's periodic scan sees every change.

`showDirectoryPicker()` is Chromium-only; OPFS works in every current browser.

## Test

```
npm test
```

The tests run in Node against an in-memory stand-in for the handles (swap-file writables, optional `move`), and check that `HandleDirectory` and `MemoryDirectory` agree.
