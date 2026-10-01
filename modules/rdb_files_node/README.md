# rdb_files_node

`NodeDirectory`: a [file mount](../rdb_projection#file-mounts)'s folder on the local filesystem, the `FileDirectory` [rhost_node](../rhost_node) hands to each `projection.files` entry.

```ts
import { NodeDirectory } from '@hyper-hyper-space/hhs3_rdb_files_node';

const media = await NodeDirectory.open('hosts/default/files/media');   // creates the folder
await projection.reconcileFiles([{ name: 'media', path: 'files/media' }], () => media);
```

- **`write`** streams into `.hhs/tmp/` under the root, fsyncs, then renames over the target, so a reader sees the old file or the new one. Missing parent folders are created. A failed write leaves the old file and no staging file.
- **`remove`** and **`rename`** prune the folders they leave empty, never the root or a folder passed to **`preserve`**.
- **`ensureDir`** makes a folder and its missing parents; a file in the way is an error.
- **`list`** walks regular files only; symlinks and special files are skipped. Paths are relative and `/`-separated on every OS.
- **`watch`** is a recursive `fs.watch` hint with the changed path when the OS gives one. Where recursive watching fails there is no hint, and the mount's periodic scan still sees every change.

Paths with empty, `.` or `..` segments, backslashes or a leading `/` are refused, so nothing escapes the root.

## Test

```
npm test
```

The tests run in `test-tmp/` and check writes, atomicity, pruning, the watch hint, and that `NodeDirectory` and `MemoryDirectory` agree on the same operations.
