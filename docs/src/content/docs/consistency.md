---
title: Consistency and failure handling
description: Choose a revalidation mode, wire background refresh lifecycles, handle Redis failures, and avoid stale writes after mutations.
---

A Redis lock coordinates fetches for each key. Concurrent callers can reuse a
stale value or wait for a refresh, depending on the mode.

## Revalidation modes

Set `mode` on the client or per cache to choose what callers receive when an
entry is stale (past `ttl`, before `ttl * staleMultiplier`):

| `mode` | Lock winner | Other callers | Best for |
| --- | --- | --- | --- |
| `'swr'` (default) | Returns stale and starts a background refresh. | Return stale. | Lowest latency when background work can outlive the response. |
| `'swr-serverless'` | Refreshes inline and returns the fresh value. | Return stale. | Serverless runtimes without `waitUntil`, or bounded in-request refreshes. |
| `'wait-on-stale'` | Refreshes inline and returns the fresh value. | Wait up to `waitTimeout` for the fresh value, then return stale. | Stricter freshness where brief latency is preferable to stale data. |

On a miss, one caller fetches while others wait for the result. Pass a
`signal` to `get()` to cancel waiting and let your fetcher cancel its work.
Background refreshes run independently of the caller's signal.

Inline refreshes can fall back to the stale value if the fetch fails. Use
[`isFatal`](/swr-redis/observability/#mark-fatal-fetch-errors) for errors
that should reach the caller.

## Background refresh lifecycle

In `'swr'` mode, a refresh can outlive the request. Pass `waitUntil` in
serverless runtimes, or call `flush()` before closing Redis on shutdown.

```ts
// Serverless global hook (e.g. Vercel waitUntil or Next.js after)
const swr = createSwrClient({ redis, prefix: 'my-app:prod', waitUntil })

// Or per-request hook (e.g. Cloudflare Workers ctx.waitUntil) — overrides client waitUntil
const user = await userCache.get('u_123', {
  waitUntil: (promise) => ctx.waitUntil(promise),
})

// Long-running servers: wait for in-flight refreshes before closing Redis on shutdown
await swr.flush()
await redis.quit()
```

## Failure strategies

Set `failureStrategy` on the client or per cache to choose how `swr-redis`
behaves when Redis is unavailable or a cold-miss wait exceeds `waitTimeout`:

- **`'availability'`** (default) serves a stale value when available or fetches
  directly from the source without caching the result. Failed explicit cache
  writes and invalidations are reported through `onEvent`.
- **`'protect-source'`** rejects the request when Redis fails before a fetch or
  a cold-miss wait times out. It also rejects failed explicit cache writes and
  invalidations. See [typed errors](/swr-redis/observability/#errors).

In either strategy, callers waiting in `'wait-on-stale'` can fall back to the
stale value when their wait times out. If a successfully prepared fetch result
fails to reach Redis, the caller still receives the fetched value and
`onEvent` reports the failure.

## Write ordering

Always update your source of truth **before** calling `set`, `invalidate`, or
`invalidateKeys`.

A concurrent fetch can repopulate the cache after your mutation. Make sure
`fetch` reads from a source that reflects the update. A lagging replica can
still return old data.
