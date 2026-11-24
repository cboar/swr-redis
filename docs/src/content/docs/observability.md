---
title: Observability and errors
description: Instrument cache activity with onEvent, handle typed error classes, and mark fatal fetch errors.
---

Clients report cache activity through `onEvent` and throw typed errors you can
match with `instanceof`. Examples build on `redis` and `swr` from
[getting started](/swr-redis/).

## Subscribe to events

Pass `onEvent` to `createSwrClient` and give each cache a low-cardinality `name`
so metrics can group by `event.cache` instead of by key:

```ts
import { createSwrClient } from 'swr-redis'

const swr = createSwrClient({
  redis,
  prefix: 'my-app:prod',
  onEvent: (event) => {
    const cache = event.cache ?? 'unnamed'
    switch (event.type) {
      case 'hit':
        metrics.inc('swr_hits_total', { cache, source: event.source, stale: String(event.stale) })
        break
      case 'miss':
        metrics.inc('swr_misses_total', { cache, action: event.action })
        break
      case 'refresh':
        metrics.observe('swr_refresh_duration_ms', event.durationMs, {
          cache,
          trigger: event.trigger,
        })
        break
      case 'error':
        logger[event.severity](event, `swr.${event.reason}`)
        break
    }
  },
})
```

`metrics` and `logger` stand for your application's telemetry clients. Errors
from `onEvent` are ignored.

## Errors

All library errors extend `SwrCacheError` (which extends `Error`):

| Class | Thrown when | Useful fields |
| --- | --- | --- |
| `SwrValidationError` | An option, key, `ttl` callback result, `SwrWriteBatch`, or pagination input is invalid. | |
| `SwrRedisError` | Under `'protect-source'`, Redis fails before a fetch with no stale value to serve, or during `set`, `invalidate`, or `invalidateKeys`. | `key`, `keys`, `cause` |
| `SwrTimeoutError` | Under `'protect-source'`, waiting exceeds `waitTimeout` with no stale value to serve. | `key`, `elapsedMs` |

## Mark fatal fetch errors

When an inline stale refresh fails (`'swr-serverless'` or the lock winner in
`'wait-on-stale'`), the caller receives the stale value by default. Pass
`isFatal` to rethrow non-transient errors that should surface to the refreshing
caller and emit a `'fatal'` error event instead of being masked by stale data
(such as an upstream schema mismatch or invalid configuration):

```ts
import { SwrRedisError, SwrTimeoutError } from 'swr-redis'

const strictUsers = swr.cache({
  name: 'strict-users',
  mode: 'swr-serverless',
  failureStrategy: 'protect-source',
  isFatal: (err) => err instanceof SchemaMismatchError,
  key: (id: string) => `user:${id}`,
  fetch: (id, { signal }) => db.getUser(id, { signal }),
})

try {
  await strictUsers.get('u_123')
} catch (err) {
  if (err instanceof SwrRedisError) {
    console.error(err.keys, err.cause)
  } else if (err instanceof SwrTimeoutError) {
    console.error(err.key, err.elapsedMs)
  }
  throw err
}
```

`SchemaMismatchError` stands for an application error class. Any `SwrCacheError`
thrown inside `fetch` is always fatal.

`isFatal` only rejects the caller running the inline refresh (the lock winner);
it does not invalidate the cached entry or reject concurrent callers. Concurrent
callers still receive the stale value immediately in `'swr-serverless'` and fall
back to it in `'wait-on-stale'` once the lock releases. Do not rely on `isFatal`
for security boundaries such as revoked access — enforce authorization outside
the cache. In `'swr'` mode, refreshes run in
the background, so a fatal error emits a `'fatal'` error event without rejecting
the caller.

## Events

`SwrEvent` is a discriminated union on `type`. Every event includes `key` (the
logical key) and optional `cache` (the cache `name`, omitted for
`swr.invalidateKeys` errors).

| `type` | Fields | Emitted when |
| --- | --- | --- |
| `'hit'` | `source` (`'memory' \| 'redis'`), `stale` (`boolean`), `startedRevalidation` (`boolean`) | A fresh entry is served from memory or Redis, or a stale entry is found in Redis. `startedRevalidation` is `true` only for the caller that takes the refresh lock. |
| `'miss'` | `action` (`'fetch' \| 'wait'`) | A key is missing or expired: `'fetch'` when this caller took the lock, `'wait'` when another caller holds it. |
| `'poll'` | `outcome`, `attempts`, `durationMs` | A caller finishes waiting for another caller's fetch. `outcome` is `'resolved'`, `'lock_stolen'`, `'stale_fallback'`, `'timeout'`, `'cancelled'`, or `'error'`. |
| `'refresh'` | `trigger` (`'cold_miss' \| 'stale_revalidation' \| 'lock_stolen'`), `durationMs` | A fetch result is committed to Redis. Omitted for `SwrNoCache`, fallback fetches, lost locks, and failed commits. |
| `'error'` | `reason`, `severity` (`'warn' \| 'error'`), plus reason-specific fields | A Redis, read, or fetch failure, wait timeout, or lock issue occurs (see below). |

### Error event reasons

| `reason` | `severity` | Extra fields | Meaning |
| --- | --- | --- | --- |
| `redis_error` | `'error'` | `keys`, `err` | Redis error under `'protect-source'` that rejects the request. |
| `redis_error_fallback` | `'warn'` | `keys`, `err` | Redis error handled without rejecting: direct fetch or mutation failure under `'availability'`, or a post-fetch commit error under either strategy. |
| `fetch_cold_failure` | `'error'` | `err` | `fetch` failed without a stale value to fall back to (excluding caller abort). |
| `fatal` | `'error'` | `err` | `fetch` threw a fatal error during a stale refresh. |
| `stale_fallback` | `'warn'` | `stage`, `err` | A stale value was returned after a Redis error (`'redis'`), cache read error (`'read'`), or non-fatal fetch error (`'refresh'`). |
| `timeout` | `'error'` | `err`, `durationMs` | Wait timed out under `'protect-source'` with no stale value to serve. |
| `lock_lost` | `'warn'` | `durationMs` | Fetch finished after its lock expired or was cleared; write skipped. |
| `lock_renewal_failure` | `'warn'` | `err` | Background lock renewal failed while `fetch` ran (retried while budget remains). |
| `lock_release_failure` | `'warn'` | `err` | Releasing the lock failed after an error, cancellation, `SwrNoCache`, or failed write. |

On Redis error events, `keys` lists every affected key (including secondary keys
of a failed `SwrWriteBatch`) and `key` is `keys[0]`.
