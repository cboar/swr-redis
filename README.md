# swr-redis

Stale-while-revalidate caching in Redis for Node.js, with typed caches, distributed fetch locking, and an optional in-memory LRU cache.

By default, callers get cached values while one process refreshes stale entries. On a miss, one process fetches and concurrent callers wait instead of flooding your database.

[Documentation](https://cboar.github.io/swr-redis/) · [npm package](https://www.npmjs.com/package/swr-redis) · [GitHub](https://github.com/cboar/swr-redis)

> [!IMPORTANT]
> This branch is **v2**, which is in development and not yet published to npm. `npm install swr-redis` installs v1, documented on the [`v1` branch](https://github.com/cboar/swr-redis/tree/v1#readme).

## Install

```sh
npm install swr-redis ioredis bson@7
```

If your application uses MongoDB driver 6, install `bson@6` in that command so the default codec can serialize the driver's `ObjectId` values.

## Define a client and cache

In this example, `db` is your application’s data access layer.

```ts
import { Redis } from 'ioredis'
import { createSwrClient } from 'swr-redis'

interface User {
  id: string
  name: string
}

const redis = new Redis(process.env.REDIS_URL!)

const swr = createSwrClient({
  redis,
  prefix: 'my-app:prod',
  ttl: 120,
  stale: 480,
})

const userCache = swr.cache({
  name: 'users',
  key: (userId: string) => `user:${userId}`,
  fetch: (userId, { signal }): Promise<User | null> => db.getUser(userId, { signal }),
})
```

`prefix` namespaces Redis keys. Entries stay fresh for `ttl` seconds and can be served stale for `stale` seconds after that (default `({ ttl }) => ttl * 4`).

## Read and update cached values

TypeScript infers the argument to `get()` from `key` and the return type from `fetch`.

```ts
const user = await userCache.get('u_123') // User | null
```

Call `getWithMeta()` when you need the value's source and cache timing alongside the result.

Update the source of truth first, then update the cached value.

```ts
const updatedUser = await db.updateUser('u_123', { name: 'Ada' })
await userCache.set('u_123', updatedUser)
```

To let the next read fetch the updated value, call `userCache.invalidate('u_123')` after updating the source. To invalidate several entries, pass an array of their logical keys from `cache.key()` to `swr.invalidateKeys()`.

## Choose how stale entries are refreshed

- By default (`staleWait: 0`, `runtime: 'server'`), stale reads return immediately while one process refreshes in the background.
- Set `staleWait` (on the client, cache, or `get()`) to wait up to that many seconds for fresh data before falling back to the stale value.
- In serverless runtimes, set `runtime: 'serverless'` (the refreshing caller waits inline) or pass `waitUntil` to keep background refreshes alive. On long-lived servers, call `await swr.drain()` before closing Redis. [Stale handling & lifecycle](https://cboar.github.io/swr-redis/consistency/#stale-handling)

## Choose what happens when Redis fails

When Redis fails before a fetch, both strategies return an unexpired stale value if one is available. Otherwise, `'availability'` (default) runs one shared direct fetch per key per cache instance without caching, while `failureStrategy: 'protect-source'` rejects the request to limit load on the source. [Failure strategies](https://cboar.github.io/swr-redis/consistency/#failure-strategies)

## Before you deploy

- Include every input that changes the result in the cache key, such as tenant, permission scope, or locale.
- Version your keys when the cached shape or codec changes.
- If you enable memory caching, each client has a local copy. Invalidations in another process can leave that copy readable until it expires. [Memory configuration](https://cboar.github.io/swr-redis/configuration/#cache-values-in-memory)

## Compatibility

The package is ESM-only, ships TypeScript declarations, and requires Node.js 24+, `ioredis ^5.11.1 || ^6.0.0`, and `bson ^6.10.4 || ^7.0.0`. Standalone Redis and Redis Sentinel are supported. Redis Cluster is unsupported. Your application owns the Redis connection.

## Documentation

- [Getting started](https://cboar.github.io/swr-redis/) — reads, updates, key design, source-first writes, and deployment caveats.
- [Configuration](https://cboar.github.io/swr-redis/configuration/) — option defaults and scopes, dynamic TTLs, in-memory LRU caching, codecs, compression, and timeouts.
- [Fetch results](https://cboar.github.io/swr-redis/fetch-results/) — fetch metadata, skipping cache writes with `SwrNoCache`, and multi-key writes with `SwrWriteBatch`.
- [Pagination](https://cboar.github.io/swr-redis/pagination/) — offset-paginated caches with optional page prefetching via `swr-redis/pagination`.
- [Consistency & failures](https://cboar.github.io/swr-redis/consistency/) — stale handling, timeouts, `waitUntil` / `drain`, failure strategies, and write ordering.
- [Observability & errors](https://cboar.github.io/swr-redis/observability/) — `onEvent` telemetry, typed error classes, and fatal error handling.
