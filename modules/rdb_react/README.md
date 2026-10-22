# React bindings (`@hyper-hyper-space/hhs3_rdb_react`)

React 18+ bindings for [reactive projection queries](../rdb_adapter#reactive-queries). Components subscribe to the queries they render over a projected database (`RdbProjection` or a standalone `QueryEngine` on `SqliteTarget` / `IdbTarget` / `MemoryTarget`) via `useSyncExternalStore`.

```tsx
import { ProjectionProvider, useProjection, useQuery } from '@hyper-hyper-space/hhs3_rdb_react';

function RoomMessages({ roomId, beforeId }: { roomId: number; beforeId?: number }) {
  const source = useProjection();

  let messages = source
    .selectFrom('chat_messages')
    .select('body', 'author_key_id', 'sent_at')
    .where('room_id = :roomId AND deleted = FALSE', { roomId })
    .orderBy('sent_at', 'desc')
    .limit(50);

  if (beforeId !== undefined) messages = messages.before(beforeId);

  const profiles = source
    .selectFrom('user_profiles')
    .select('key_id', 'display_name')
    .where('key_id IN :keys', { keys: messages.col('author_key_id') });

  const { data, loading, error } = useQuery({ messages, profiles });
  if (loading) return null;
  if (error !== undefined) throw error;

  // data.messages and data.profiles are ProjectedRow[]
  // ...
}
```

## API

- **`<ProjectionProvider source={source}>`** puts any `QuerySource` in React context — an in-process `RdbProjection`, or `engine.source` from a `QueryEngine` opened over a local target.
- **`useProjection(): QuerySource`** returns that `QuerySource` (throws if rendered outside `ProjectionProvider`).
- **`useQuery(query)`** takes either a single `TableQuery` or a `{ [name: string]: TableQuery }` query set and returns its `QuerySnapshot`:
  - `data`: `ProjectedRow[]` for a single query, or `{ [name: string]: ProjectedRow[] }` for a query set;
  - `events` is populated for a single `TableQuery`: ingestion failures and concurrency void/reinstate flips whose row is in that delivery. A query set returns `events: []`. A member's events stay on a monitor of that member alone;
  - `loading`: `true` until the initial query completes;
  - `error`: populated if the query failed.

## How it behaves on render

- **Keyed by value fingerprint, not object identity.** `TableQuery` instances and parameter arrays or sets can be built inline during render. `useQuery` computes the canonical value fingerprint of the query (or query set) and retains the same `QuerySubscription` as long as the fingerprint does not change. Changing a bound parameter or `.before(id)` / `.after(id)` switches to the monitor for the new fingerprint and releases the old one.
- **Strict Mode safe.** Monitor reference counts are incremented in `subscribe` and decremented in its cleanup function, so React 18 Strict Mode's double mount/unmount settles cleanly on one active monitor.
- **Referentially stable snapshots and rows.** When a local write is later confirmed by a matching `apply()` project-back, or when an unrelated table or off-page row changes, the snapshot reference does not change and the component does not re-render. In a query set `{ messages, profiles }`, if `messages` updates without changing the set of `author_key_id` values, `data.profiles` keeps the exact same array reference.

## Test

```
npm test
```
