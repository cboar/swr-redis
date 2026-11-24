import type { Redis } from 'ioredis'
import type { SwrTimeoutError } from './errors.ts'
import type { SwrNoCache, SwrWriteBatch } from './wrappers.ts'

/**
 * How stale entries (past `ttl`, before `ttl * staleMultiplier`) are handled:
 * - `'swr'`: Lock winner returns stale and refreshes in the background; other callers return stale.
 * - `'swr-serverless'`: Lock winner refreshes inline and returns the fresh value; other callers return stale.
 * - `'wait-on-stale'`: Lock winner refreshes inline; other callers wait up to `waitTimeout` for the fresh value, then fall back to stale.
 */
export type SwrMode = 'swr' | 'swr-serverless' | 'wait-on-stale'

/**
 * Behavior when Redis fails or a wait exceeds `waitTimeout` and no stale value can be served.
 * Both strategies return an unexpired stale value when one is available.
 * - `'availability'`: Fetch from source without caching; report failed `set`/`invalidate` calls via `onEvent`.
 * - `'protect-source'`: Reject on pre-fetch Redis errors, failed explicit writes or invalidations, and wait timeouts.
 */
export type SwrFailureStrategy = 'availability' | 'protect-source'

export interface SwrCodec<T = unknown> {
  encode: (value: T) => Uint8Array
  decode: (buffer: Uint8Array) => T
}

/**
 * zlib compression settings for Redis payloads:
 * - `true` / `undefined`: Compress payloads of at least 256 bytes at level 1.
 * - `false`: Disable compression.
 * - `{ level?, minBytes? }`: Custom zlib settings.
 */
export type SwrCompressionOption =
  | boolean
  | {
      /** zlib compression level (`0` = none, `1` = fastest, `9` = smallest). Default: `1`. */
      level?: number
      /** Minimum encoded payload size in bytes to compress. Default: `256`. */
      minBytes?: number
    }

/** Configuration for the process-local in-memory LRU cache (serves fresh entries only). */
export interface SwrMemoryOptions {
  /** Maximum entries in the LRU cache. Default: `1000`. */
  maxEntries?: number
  /** Maximum retention in seconds from the original write, capped by the entry's fresh `ttl`. Default: entry fresh `ttl`. */
  maxTtl?: number
  /**
   * Clone values with `structuredClone` on memory read and write.
   * Does not preserve `Buffer`, `ObjectId`, or custom classes. Default: `false`.
   */
  clone?: boolean
}

export interface SwrTtlInput<T = unknown, Context = void> {
  context: Context
  key: string
  value: T
}

export type SwrTtlOption<T = unknown, Context = void> =
  number | ((input: SwrTtlInput<T, Context>) => number)

export interface SwrSharedOptions {
  /** Fresh lifetime in seconds. Default: `120`. */
  ttl?: number
  /** Multiplier on `ttl` for total Redis lifetime (`ttl * staleMultiplier`). Set `1` to disable the stale window. Must be >= 1. Default: `5`. */
  staleMultiplier?: number
  /** Stale revalidation mode. Default: `'swr'`. */
  mode?: SwrMode
  /** Behavior on Redis errors or wait timeouts when no stale value is available. Default: `'availability'`. */
  failureStrategy?: SwrFailureStrategy
  /** zlib compression settings for Redis payloads. Default: `{ level: 1, minBytes: 256 }`. */
  compression?: SwrCompressionOption
  /** Initial fetch lock TTL in seconds, renewed automatically while `fetch` runs. Default: `3`. */
  lockTtl?: number
  /** Maximum duration in seconds a single fetch keeps renewing its lock (Redis command latency can extend the lock slightly past this). Default: `Math.max(30, lockTtl)`. */
  maxLockTtl?: number
  /** Time budget in seconds for polling while another caller holds the lock. Default: `10`. */
  waitTimeout?: number
  /** Base interval in milliseconds between poll checks while waiting (plus up to 30% jitter). Default: `80`. */
  pollIntervalMs?: number
  /** Predicate marking fetch errors that should throw instead of falling back to a stale value (only rejects the refreshing caller). */
  isFatal?: (err: unknown) => boolean
}

export interface SwrClientOptions extends SwrSharedOptions {
  /** ioredis connection (standalone or Sentinel; Redis Cluster is not supported). */
  redis: Redis
  /** Namespace prefix isolating every Redis key written by this client. Default: `'default'`. */
  prefix?: string
  /** Process-local in-memory LRU cache shared across caches on this client (`true` uses default options). Default: `false`. */
  memory?: boolean | SwrMemoryOptions
  /** Default codec for caches on this client. Default: BSON. */
  codec?: SwrCodec<unknown>
  /** Telemetry callback for client and cache events. Errors thrown or rejected by this callback are ignored. */
  onEvent?: (event: SwrEvent) => void
  /**
   * Serverless lifecycle hook for background `'swr'` refreshes (e.g. Vercel `waitUntil`,
   * Next.js `after`). Overridden per call by `SwrGetOptions.waitUntil`.
   */
  waitUntil?: (promise: Promise<unknown>) => void
}

export interface SwrGetOptions {
  /** Cancels waiting and is forwarded to inline `fetch` calls. Ignored by background `'swr'` refreshes. */
  signal?: AbortSignal
  /** Skip memory cache reads and writes for this call. */
  memory?: false
  /**
   * Per-request lifecycle hook (e.g. Cloudflare `ctx.waitUntil`) for a background `'swr'` refresh
   * started by this call. Overrides the client `waitUntil`.
   */
  waitUntil?: (promise: Promise<unknown>) => void
}

export interface SwrFetchMeta<Context = void> {
  key: (context: Context) => string
  /** The caller's `AbortSignal`, or `undefined` during background `'swr'` refreshes. */
  signal?: AbortSignal
  /** `'attempt'` for normal fetches; `'skip'` during fallback fetches that write nothing to cache. */
  cacheWrite: 'attempt' | 'skip'
}

/**
 * A secondary entry written alongside the primary entry in one Redis commit.
 * Targets any logical key; pairing between `value` and `codec` is not type-checked.
 */
export interface SwrCacheWrite {
  key: string
  value: unknown
  /** Fresh lifetime in seconds. Default: primary entry's resolved TTL. */
  ttl?: number
  /**
   * Codec of the cache that reads `key` when it differs from the client's `codec`.
   * Defaults to the client's `codec` (not the producing cache's).
   */
  codec?: { encode: (value: never) => Uint8Array }
}

export interface SwrWriteBatchInput<T> {
  primary: T
  /** Fresh lifetime in seconds for this batch. Overrides the cache `ttl` and skips its callback. */
  ttl?: number
  /** Additional entries committed with `primary` under the primary key's lock. */
  secondary?: readonly SwrCacheWrite[]
}

/** A fetched value, optionally wrapped in `SwrWriteBatch` or `SwrNoCache`. */
export type SwrFetchResult<T> = T | SwrWriteBatch<T> | SwrNoCache<T>

// Keep the fetched result separate from the cached value so inference can unwrap it afterwards.
export interface SwrCacheDefinition<T, Context, Result> extends Omit<SwrSharedOptions, 'ttl'> {
  key: string | ((context: Context) => string)
  /** May return a value, `SwrNoCache`, or `SwrWriteBatch`. */
  fetch: (context: Context, meta: SwrFetchMeta<Context>) => Promise<Result>
  /** Fresh lifetime in seconds, or a synchronous callback resolved on write. Default: client `ttl` (`120`). */
  ttl?: SwrTtlOption<NoInfer<T>, NoInfer<Context>>
  /** Disable the client's in-memory cache for this cache. */
  memory?: false
  /** Optional low-cardinality label reported as `event.cache`. */
  name?: string
  /** Codec for this cache. Default: client `codec` (BSON). */
  codec?: SwrCodec<T>
}

export interface SwrCacheOptions<T = unknown, Context = void> extends SwrCacheDefinition<
  T,
  Context,
  SwrFetchResult<T>
> {}

export interface SwrCache<T = unknown, Context = void> {
  get: (context: Context, options?: SwrGetOptions) => Promise<T>
  /**
   * Writes `value` to Redis, then updates the client's memory cache (stores when enabled for this
   * cache, or evicts when `memory: false`). Evicts from memory on Redis failure; `'availability'`
   * emits a warning event and resolves, while `'protect-source'` throws `SwrRedisError`.
   */
  set: (context: Context, value: T) => Promise<void>
  invalidate: (context: Context) => Promise<void>
  key: (context: Context) => string
}

export type SwrUnwrapFetchResult<R> = R extends SwrNoCache<infer V> | SwrWriteBatch<infer V> ? V : R

export interface SwrClient {
  cache: {
    // Infer the complete result before unwrapping; plain objects resembling wrappers stay intact.
    <Result, Context = void>(
      options: SwrCacheDefinition<SwrUnwrapFetchResult<Result>, Context, Result>,
    ): SwrCache<SwrUnwrapFetchResult<Result>, Context>
    // Explicit type arguments describe the cached value, allowing SwrNoCache and SwrWriteBatch in fetch.
    <T = unknown, Context = void>(options: SwrCacheOptions<T, Context>): SwrCache<T, Context>
  }
  /** Removes one or more logical keys and their locks from memory and Redis. */
  invalidateKeys: (...keys: string[]) => Promise<void>
  /** Waits for all in-flight background refreshes started by this client to settle. */
  flush: () => Promise<void>
}

/** One or more logical keys; the first is reported as `key`. */
export type SwrNonEmptyKeys = readonly [string, ...string[]]

export type SwrErrorEvent = { type: 'error'; cache?: string; key: string } & (
  | { reason: 'redis_error'; severity: 'error'; keys: SwrNonEmptyKeys; err: unknown }
  | { reason: 'redis_error_fallback'; severity: 'warn'; keys: SwrNonEmptyKeys; err: unknown }
  | { reason: 'fetch_cold_failure' | 'fatal'; severity: 'error'; err: unknown }
  | {
      reason: 'stale_fallback'
      severity: 'warn'
      stage: 'redis' | 'read' | 'refresh'
      err: unknown
    }
  | {
      reason: 'lock_release_failure' | 'lock_renewal_failure'
      severity: 'warn'
      err: unknown
    }
  | { reason: 'timeout'; severity: 'error'; err: SwrTimeoutError; durationMs: number }
  | { reason: 'lock_lost'; severity: 'warn'; durationMs: number }
)

export type SwrErrorReason = SwrErrorEvent['reason']

export type SwrEvent =
  | {
      type: 'hit'
      cache?: string
      key: string
      source: 'memory' | 'redis'
      stale: boolean
      startedRevalidation: boolean
    }
  | {
      type: 'miss'
      cache?: string
      key: string
      action: 'fetch' | 'wait'
    }
  | {
      type: 'poll'
      cache?: string
      key: string
      outcome: 'resolved' | 'lock_stolen' | 'timeout' | 'stale_fallback' | 'cancelled' | 'error'
      attempts: number
      durationMs: number
    }
  | {
      type: 'refresh'
      cache?: string
      key: string
      trigger: 'cold_miss' | 'stale_revalidation' | 'lock_stolen'
      durationMs: number
    }
  | SwrErrorEvent
