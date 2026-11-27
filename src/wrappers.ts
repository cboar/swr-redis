import type { SwrCacheWrite, SwrWriteBatchInput } from './types.ts'

/** Return from `fetch` to deliver `value` to the caller without writing to Redis or memory. */
export class SwrNoCache<T> {
  declare private readonly _brand: void
  readonly value: T

  constructor(value: T) {
    this.value = value
  }
}

/** Return from `fetch` to override write options or commit secondary keys with `primary` under the same lock. */
export class SwrWriteBatch<T> {
  declare private readonly _brand: void
  readonly primary: T
  readonly ttl?: number
  readonly stale?: number
  readonly secondary: readonly SwrCacheWrite[]

  constructor(input: SwrWriteBatchInput<T>) {
    this.primary = input.primary
    if (input.ttl !== undefined) this.ttl = input.ttl
    if (input.stale !== undefined) this.stale = input.stale
    this.secondary = input.secondary ?? []
  }
}
