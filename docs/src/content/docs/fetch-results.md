---
title: Fetch results and batch writes
description: Read fetch metadata, skip caching individual results with SwrNoCache, and write related keys in one Redis commit with SwrWriteBatch.
---

A cache's `fetch(context, meta)` returns the value to cache. It can also return
`new SwrNoCache(value)` to skip caching that result, or
`new SwrWriteBatch({ primary, ttl?, secondary? })` to override the TTL and write
related keys together. Examples build on `swr` from
[getting started](/swr-redis/).

## Fetch metadata

The second argument to `fetch` is `SwrFetchMeta<Context>`:

| Field | Description |
| --- | --- |
| `key` | The cache's `cache.key` function. |
| `signal` | The caller's `AbortSignal`, or `undefined` during background `'swr'` refreshes. |
| `cacheWrite` | `'attempt'` for normal fetches, or `'skip'` during a [fallback fetch](/swr-redis/consistency/#failure-strategies) that writes nothing. |

`'attempt'` does not guarantee the write will land (for instance, if the lock is
lost or Redis fails). Check `cacheWrite === 'skip'` to avoid work that only
exists to populate extra cache keys.

## Skip caching with `SwrNoCache`

Return `new SwrNoCache(value)` to deliver a value to the caller without caching
it:

```ts
import { SwrNoCache } from 'swr-redis'

interface Profile {
  id: string
  provisional: boolean
}

const profileCache = swr.cache<Profile | null, string>({
  name: 'profiles',
  key: (id) => `profile:${id}`,
  fetch: async (id, { signal }) => {
    const profile = await db.loadProfile(id, { signal })
    if (profile?.provisional) return new SwrNoCache(profile)
    return profile
  },
})
```

Nothing is written to Redis or memory, existing entries are left untouched, the
`ttl` callback does not run, and no `refresh` event is emitted. In background
`'swr'` refreshes, the caller already received the stale value, so the
`SwrNoCache` result is discarded.

## Write related keys with `SwrWriteBatch`

Return a `SwrWriteBatch` to cache the primary value along with related entries.
Each `secondary` entry takes a logical `key` (built with the target cache's
`cache.key`), a `value`, and optional `ttl` and `codec` overrides:

```ts
import { SwrWriteBatch } from 'swr-redis'

const accountCache = swr.cache({
  name: 'accounts',
  key: (id: string) => `account:${id}`,
  fetch: (id) => db.getAccount(id),
})

const userGraphCache = swr.cache({
  name: 'user-graphs',
  key: (id: string) => `user-graph:${id}`,
  fetch: async (id, { cacheWrite, signal }) => {
    const { graph, account } = await db.loadUserGraph(id, { signal })
    if (cacheWrite === 'skip') return graph

    return new SwrWriteBatch({
      primary: graph,
      ttl: 60,
      secondary: [
        { key: accountCache.key(account.id), value: account },
        { key: `audit:last-seen:${id}`, value: Date.now(), ttl: 30 },
      ],
    })
  },
})
```

## Batch TTLs, codecs, and validation

- **TTL precedence.** `batch.ttl` overrides the cache's `ttl` and skips its
  callback; otherwise the cache resolves its `ttl` once for `primary`. Secondary
  entries inherit that primary TTL unless they specify their own `ttl`.
- **Codec precedence.** `primary` uses the producing cache's `codec`. Secondary
  entries default to the **client's** `codec` unless they specify `codec` — pass
  the target cache's codec when it differs. TypeScript does not check that a
  secondary `value` matches its `codec`.
- **Shared settings.** Every entry in the batch shares the producing cache's
  `compression`, `staleMultiplier`, and write timestamp.
- **Validation.** Empty keys, keys with lone UTF-16 surrogates, duplicate keys
  (including duplicating the primary key), and non-positive or non-finite TTLs
  throw `SwrValidationError` before anything reaches Redis.

## Commit behavior

All entries are encoded first and written in one Redis script under the primary
key's lock:

- **Only the primary key is locked.** Secondary keys are not locked, so
  concurrent writes to a secondary key apply in the order Redis executes them.
- **No Redis rollback.** If a later `SET` fails mid-script (for example when
  Redis runs out of memory), Redis does not roll back earlier entries written in
  the same batch.

Post-fetch commit errors follow the normal
[after-fetch behavior](/swr-redis/consistency/#failure-strategies): the
caller still receives `primary`, and a `redis_error_fallback` event lists every
key in the batch.
