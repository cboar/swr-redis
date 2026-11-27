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
| `SwrValidationError` | An option, key, `ttl` or `stale` callback result, `SwrWriteBatch`, or pagination input is invalid. | |
| `SwrRedisError` | Under `'protect-source'`, Redis fails before a fetch with no stale value to serve, or during `set`, `invalidate`, or `invalidateKeys`. | `key?`, `keys`, `cause` |
| `SwrTimeoutError` | The waiting limit (`'wait'`) or `fetchTimeout` (`'fetch'`) expires with no usable stale value to serve. | `key`, `elapsedMs`, `phase` |

Note that `get(ctx, { signal: AbortSignal.timeout(ms) })` rejects with the
signal's `TimeoutError` (not `SwrTimeoutError`) and emits no `timeout` event.

## Mark fatal fetch errors

When a stale refresh fails while its lock winner is still waiting
(`staleWait > 0`, or `runtime: 'serverless'` without `waitUntil`), the caller
receives the stale value by default. Pass `isFatal` to rethrow non-transient
errors to that waiting caller and emit a `'fatal'` error event instead of
masking them with stale data (such as an upstream schema mismatch or invalid
configuration):

```ts
import { SwrRedisError, SwrTimeoutError } from 'swr-redis'

const strictUsers = swr.cache({
  name: 'strict-users',
  staleWait: Infinity,
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
    console.error(err.phase, err.key, err.elapsedMs)
  }
  throw err
}
```

Any `SwrCacheError` other than `SwrTimeoutError` thrown inside `fetch` is always
fatal. `isFatal` only rejects the lock winner while it is still attached; it does
not invalidate the entry or reject other callers, so don't use it for security
boundaries such as revoked access.

## Events

`SwrEvent` is a discriminated union on `type`. Events include the logical `key`
and the cache `name` as `cache` when set.

| `type` | Fields | Emitted when |
| --- | --- | --- |
| `'hit'` | `source` (`'memory' \| 'redis'`), `stale` (`boolean`), `startedRevalidation` (`boolean`) | A fresh entry is served from memory or Redis, or a stale entry is found in Redis. `startedRevalidation` is `true` only for the caller that takes the refresh lock. |
| `'miss'` | `action` (`'fetch' \| 'wait'`) | A key is missing or expired: `'fetch'` when this caller took the lock, `'wait'` when another caller holds it. |
| `'wait'` | `outcome`, `durationMs` | A caller finishes waiting for another caller's fetch. `outcome` is `'resolved'`, `'lock_takeover'`, `'stale_fallback'`, `'timeout'`, `'cancelled'`, or `'error'`. |
| `'refresh'` | `trigger` (`'cold_miss' \| 'stale_revalidation' \| 'lock_takeover'`), `durationMs` | A fetch result is committed to Redis. Omitted for `SwrNoCache`, fallback fetches, lost locks, and failed commits. |
| `'error'` | `reason`, `severity` (`'warn' \| 'error'`), plus reason-specific fields | A Redis, read, or fetch failure, wait timeout, or lock issue occurs (see below). |

### Error event reasons

| `reason` | `severity` | Extra fields | Meaning |
| --- | --- | --- | --- |
| `redis_error` | `'error'` | `keys`, `err` | Redis error under `'protect-source'` that rejects the request. |
| `redis_error_fallback` | `'warn'` | `keys`, `err` | Redis error handled without rejecting: direct fetch or mutation failure under `'availability'`, or a post-fetch commit error under either strategy. |
| `fetch_cold_failure` | `'error'` | `err` | `fetch` failed without a usable stale value remaining. |
| `fatal` | `'error'` | `err` | `fetch` threw a fatal error during a stale refresh. |
| `stale_fallback` | `'warn'` | `stage`, `err` | A stale value was returned after a Redis error (`'redis'`), cache read error (`'read'`), or non-fatal fetch error (`'refresh'`). |
| `timeout` | `'warn' \| 'error'` | `err`, `durationMs` | `fetchTimeout` expired (`'warn'` when usable stale data remains), or a cold wait hit the waiting limit. |
| `lock_lost` | `'warn'` | `durationMs` | Lock renewal found the lock gone, or `fetch` finished after its lock expired or was cleared; write skipped. |
| `lock_renewal_failure` | `'warn'` | `err` | Background lock renewal failed while `fetch` ran. |
| `lock_release_failure` | `'warn'` | `err` | Releasing the lock or writing the cooldown marker failed. |
| `unreadable_entry` | `'warn'` | | A cached entry used an unknown format and was treated as a miss. |

On Redis error events, `keys` lists every affected key, including secondary keys
of a failed `SwrWriteBatch`.
