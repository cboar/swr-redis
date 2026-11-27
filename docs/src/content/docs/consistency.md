---
title: Consistency and failure handling
description: Configure stale handling, timeouts, serverless lifecycles, Redis failure strategies, and write ordering.
---

A Redis lock coordinates fetches for each key. On a miss, one caller fetches
while others wait. When an entry is stale (past `ttl`, before `ttl + stale`),
`staleWait` and `runtime` control whether callers wait for fresh data or return
stale immediately.

## Stale handling

| Settings | Lock winner | Other callers | Best for |
| --- | --- | --- | --- |
| Defaults (`staleWait: 0`, `runtime: 'server'`) | Returns stale immediately; refreshes in background. | Return stale immediately. | Lowest latency on long-lived servers. |
| `staleWait: 0.5` | Waits up to `0.5s`, then returns stale; refresh continues. | Wait up to `0.5s`, then return stale. | Stricter freshness with a bounded wait. |
| `runtime: 'serverless'` (no `waitUntil`) | Refreshes inline (ignores `staleWait`). | Return stale (or wait up to `staleWait`). | Serverless runtimes without a keep-alive hook. |
| `runtime: 'serverless'` + `waitUntil` | Waits up to `staleWait`; hook keeps refresh alive. | Wait up to `staleWait`. | Serverless with a keep-alive hook. |

`staleWait` can be set on the client, per cache, or per `get()` call. For
example, a detail route can pass `staleWait: 0.2` while a listing route sharing
the same cache passes `staleWait: 0`.

If an attached refresh fails or hits `fetchTimeout` before the stale entry
expires, the caller receives the stale value. Use
[`isFatal`](/swr-redis/observability/#mark-fatal-fetch-errors) for errors that
should reach the waiting winner instead.

In serverless runtimes without a `waitUntil` hook, the lock winner waits for its
full refresh (up to `fetchTimeout`) even when holding stale data. Provide a
`waitUntil` hook or size `fetchTimeout` to what the request can afford.

## Timeouts, cancellation, and cooldowns

- **Source limit (`fetchTimeout`).** Every source call, including background
  refreshes, is bounded by `fetchTimeout` (default `30s`). On expiry,
  `meta.signal` aborts and late results are discarded. Waiters also wait for at
  most `fetchTimeout`. Without a usable stale value, callers receive
  `SwrTimeoutError` (`phase: 'fetch'` or `'wait'`).
- **Caller deadlines (`signal`).** Pass `signal: AbortSignal.timeout(ms)` to
  `get()` to bound a request's wait (a cold call without a signal can wait up to
  `fetchTimeout` and then take over a crashed fetcher's lock for another
  `fetchTimeout`). Aborting rejects with `signal.reason` (never serves stale)
  and detaches the caller without aborting the shared fetch, so the cache still
  fills. Cancellation is checked between Redis commands; set
  [`commandTimeout`](/swr-redis/configuration/#set-ioredis-command-timeouts) on
  `ioredis` to bound stalled Redis calls.
- **Failure cooldown (`failureCooldown`).** When a shared fetch fails or times
  out, the key rests for `failureCooldown` (default `1s`) so a failing source
  sees roughly one attempt per cooldown per key across all processes. Waiting
  callers holding stale data return it immediately; cold waiters keep waiting
  until the cooldown expires. `set()` and `invalidate()` clear any active
  cooldown.

## Background refresh lifecycle

When a refresh can outlive the request, pass `waitUntil` in serverless runtimes
or call `drain()` before closing Redis on shutdown.

```ts
// AWS Lambda (no keep-alive hook): refreshing caller waits inline, bounded by fetchTimeout
const lambdaSwr = createSwrClient({ redis, runtime: 'serverless', fetchTimeout: 7 })

// Vercel / Next.js: global hook keeps refreshes alive after the response
const vercelSwr = createSwrClient({ redis, runtime: 'serverless', waitUntil })

// Cloudflare Workers: per-request hook (routes that omit it refresh inline)
const workerSwr = createSwrClient({ redis, runtime: 'serverless' })
const user = await userCache.get('u_123', {
  waitUntil: (promise) => ctx.waitUntil(promise),
})

// Long-running servers: stop accepting requests, drain refreshes, then close Redis
await swr.drain()
await redis.quit()
```

Without a `waitUntil` hook in serverless, work left behind after a caller's
`signal` aborts may freeze once the response is sent; its lock expires after
`lockTtl` so the next caller retries.

## Failure strategies

Set `failureStrategy` on the client or per cache to choose how `swr-redis`
behaves when Redis fails and no stale value can be served:

- **`'availability'`** (default) fetches directly from the source without
  caching the result (returned with `source: 'direct'` from `getWithMeta()`),
  sharing one in-flight fetch per key within each cache instance (aborted if
  every attached caller leaves). Failed explicit writes and invalidations are
  reported through `onEvent`.
- **`'protect-source'`** rejects with `SwrRedisError`, as well as rejecting
  failed explicit writes and invalidations. See
  [typed errors](/swr-redis/observability/#errors).

Waiting timeouts while Redis is healthy never fall back to direct fetches under
either strategy. Callers receive stale data if available, or `SwrTimeoutError`.
Under either strategy, if a refresh or wait fails, an unexpired stale value is
returned (emitting `stale_fallback`, or `timeout` when a refresh hits
`fetchTimeout`; a wait that times out and returns stale emits neither). If the
value expires mid-wait or mid-refresh, the call falls back to the cold-miss or
error path. Cancellation and
[fatal fetch errors](/swr-redis/observability/#mark-fatal-fetch-errors) still
reject the caller.

If a successfully prepared fetch result fails to reach Redis, the caller still
receives the fetched value and `onEvent` reports the failure.

Entries written in an unknown or unsupported format are treated as misses on
every read path (never served stale): a caller takes the lock, refetches, and
overwrites the entry while emitting an `unreadable_entry` warning.

## Write ordering

Always update your source of truth **before** calling `set`, `invalidate`, or
`invalidateKeys`.

A concurrent fetch can repopulate the cache after your mutation. Make sure
`fetch` reads from a source that reflects the update. A lagging replica can
still return old data.
