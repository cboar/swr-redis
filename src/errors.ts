import type { SwrTimeoutPhase } from './types.ts'

/** Base class for errors thrown by `swr-redis`. Treated as fatal if thrown from `fetch`, except `SwrTimeoutError`. */
export class SwrCacheError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'SwrCacheError'
  }
}

export function isFatalError(err: unknown, customIsFatal?: (err: unknown) => boolean) {
  // A timeout from a nested cache is an ordinary failure; a custom isFatal can still match it.
  if (err instanceof SwrCacheError && !(err instanceof SwrTimeoutError)) return true
  return customIsFatal?.(err) ?? false
}

/** Thrown when the waiting limit (`'wait'`) or `fetchTimeout` (`'fetch'`) expires with no usable stale value to serve. */
export class SwrTimeoutError extends SwrCacheError {
  readonly key: string
  readonly elapsedMs: number
  readonly phase: SwrTimeoutPhase

  constructor(key: string, elapsedMs: number, phase: SwrTimeoutPhase) {
    super(`SWR ${phase} timeout exceeded (${elapsedMs}ms) for key: "${key}"`)
    this.name = 'SwrTimeoutError'
    this.key = key
    this.elapsedMs = elapsedMs
    this.phase = phase
  }
}

/** Thrown under `'protect-source'` when Redis fails before a fetch with no stale value to serve, or during `set`, `invalidate`, or `invalidateKeys`. */
export class SwrRedisError extends SwrCacheError {
  readonly key?: string
  readonly keys: readonly string[]

  constructor(keys: readonly string[], cause: unknown) {
    const [key] = keys
    const reason = cause instanceof Error ? cause.message : String(cause)
    const target =
      keys.length === 0
        ? ''
        : keys.length === 1
          ? ` for key "${key}"`
          : ` for ${keys.length} keys ("${key}", ...)`
    super(`SWR Redis error${target}: ${reason}`, { cause })
    this.name = 'SwrRedisError'
    this.key = key
    this.keys = keys
  }
}

/** Thrown when an option, key, `ttl` or `stale` callback result, `SwrWriteBatch`, or pagination input is invalid. */
export class SwrValidationError extends SwrCacheError {
  constructor(message: string) {
    super(message)
    this.name = 'SwrValidationError'
  }
}
