---
title: Stale-while-revalidate caching in Redis
description: Get started with swr-redis to define typed Redis caches that serve stale values while one process refreshes them.
head:
  - tag: meta
    attrs:
      property: og:type
      content: website
---

`swr-redis` caches the results of your data loaders in Redis. When an
entry goes stale, callers keep getting the cached value while one process
refreshes it. On a miss, one process fetches and concurrent callers wait instead
of querying your database in parallel.

## Install

```sh
npm install swr-redis ioredis bson@7
```

If you use MongoDB driver 6, install `bson@6` in that command. BSON values
such as `ObjectId` must come from the same BSON major as the default codec.

The package is ESM-only, requires Node.js 24 or newer, and supports `ioredis`
`^5.11.1 || ^6.0.0`. It works with standalone Redis and Redis Sentinel. Redis
Cluster is not supported.

## Create a client

A client holds your Redis connection and defaults shared across its caches.
Create one per connection and prefix.

```ts
import { Redis } from 'ioredis'
import { createSwrClient } from 'swr-redis'

const redis = new Redis(process.env.REDIS_URL!)

const swr = createSwrClient({
  redis,
  prefix: 'my-app:prod',
  ttl: 120,
  staleMultiplier: 5,
})
```

`prefix` namespaces every key the client writes; use a distinct prefix per
application and environment sharing a Redis server. Entries stay fresh for `ttl`
seconds and are served stale until `ttl * staleMultiplier` seconds.

The client does not open or close `redis`. Close the connection when your
application shuts down.

## Define a cache

Pair a key builder with a loader. In the examples, `db` stands for your
application's data access layer.

```ts
interface User {
  id: string
  name: string
}

const userCache = swr.cache({
  name: 'users',
  key: (userId: string) => `user:${userId}`,
  fetch: (userId, { signal }): Promise<User | null> => db.getUser(userId, { signal }),
})
```

TypeScript infers the context argument from `key` and the cached value type from
`fetch`. `name` labels the cache in telemetry events; see
[observability](/swr-redis/observability/).

For a single-entry cache, pass a string `key`:

```ts
const settingsCache = swr.cache({
  name: 'settings',
  key: 'settings:v1',
  ttl: 300,
  fetch: () => db.getSettings(),
})

const settings = await settingsCache.get()
```

`get()`, `invalidate()`, and `key()` then take no context argument. Pass
`undefined` when supplying a second argument, such as
`settingsCache.set(undefined, value)` or
`settingsCache.get(undefined, { memory: false })`.

## Read cached values

`get` returns a fresh cached value, handles stale entries according to `mode`,
or fetches on a miss while concurrent callers wait. Pass a `signal` to stop
waiting:

```ts
const user = await userCache.get('u_123', {
  signal: AbortSignal.timeout(2_500),
})
```

## Update cached values

Always update your source of truth before updating or invalidating the cache:

```ts
const updatedUser = await db.updateUser('u_123', { name: 'Ada' })
await userCache.set('u_123', updatedUser)
```

Or remove the entry so the next read fetches it:

```ts
await db.updateUser('u_123', { name: 'Ada' })
await userCache.invalidate('u_123')
```

To remove multiple keys in one call, pass them to `swr.invalidateKeys` using
`cache.key`:

```ts
await swr.invalidateKeys(userCache.key('u_123'), userCache.key('u_456'))
```

## Before you deploy

- **Put every input in the key.** Include every value that changes the result,
  such as tenant, permission scope, locale, or filter.
- **Version your keys.** Processes sharing a key must use the same `codec`. Bump
  a version segment (such as `:v2`) when the cached shape or codec changes.
- **Update the source before the cache.** A concurrent read that started before
  your `set` or `invalidate` can still fetch and write what it reads. See
  [write ordering](/swr-redis/consistency/#write-ordering).
- **Wire background refresh lifecycles.** Under the default `'swr'` mode, pass
  `waitUntil` in serverless runtimes and call `await swr.flush()` before process
  shutdown. See
  [background refresh lifecycle](/swr-redis/consistency/#background-refresh-lifecycle).
- **Memory caching is per process.** When `memory` is enabled, invalidations in
  one process do not notify other processes. See
  [cache values in memory](/swr-redis/configuration/#cache-values-in-memory).

## Next steps

- [Configuration](/swr-redis/configuration/) — option defaults, dynamic
  TTLs, in-memory caching, codecs, compression, and timeouts.
- [Fetch results](/swr-redis/fetch-results/) — skip caching a result or
  write related keys together.
- [Pagination](/swr-redis/pagination/) — cache offset-paginated queries
  and prefetch following pages.
- [Consistency and failures](/swr-redis/consistency/) — revalidation
  modes, failure strategies, and write ordering.
- [Observability and errors](/swr-redis/observability/) — `onEvent`
  telemetry and typed errors.
