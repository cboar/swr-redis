import type { SwrNonEmptyKeys } from './types.ts'

/** Base class for errors thrown by `swr-redis`. Always treated as fatal if thrown from `fetch`. */
export class SwrCacheError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'SwrCacheError'
  }
}

export function isFatalError(err: unknown, customIsFatal?: (err: unknown) => boolean): boolean {
  if (err instanceof SwrCacheError) return true
  return customIsFatal?.(err) ?? false
}

/** Thrown under `'protect-source'` when waiting on a cold miss exceeds `waitTimeout`. */
export class SwrTimeoutError extends SwrCacheError {
  readonly key: string
  readonly elapsedMs: number

  constructor(key: string, elapsedMs: number) {
    super(`SWR wait timeout exceeded (${elapsedMs}ms) for key: "${key}"`)
    this.name = 'SwrTimeoutError'
    this.key = key
    this.elapsedMs = elapsedMs
  }
}

/** Thrown under `'protect-source'` when Redis fails before a fetch or during `set`, `invalidate`, or `invalidateKeys`. */
export class SwrRedisError extends SwrCacheError {
  readonly key: string
  readonly keys: SwrNonEmptyKeys

  constructor(keys: SwrNonEmptyKeys, cause: unknown) {
    const [key] = keys
    const reason = cause instanceof Error ? cause.message : String(cause)
    const target = keys.length === 1 ? `key "${key}"` : `${keys.length} keys ("${key}", ...)`
    super(`SWR Redis error for ${target}: ${reason}`, { cause })
    this.name = 'SwrRedisError'
    this.key = key
    this.keys = keys
  }
}

/** Thrown when an option, key, `ttl` callback result, `SwrWriteBatch`, or pagination input is invalid. */
export class SwrValidationError extends SwrCacheError {
  constructor(message: string) {
    super(message)
    this.name = 'SwrValidationError'
  }
}
