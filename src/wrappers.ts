import type { SwrWriteBatchInput } from './types.ts'

/** Return from `fetch` to deliver `value` to the caller without writing to Redis or memory. */
export class SwrNoCache<T> {
  declare private readonly _brand: void
  readonly value: T

  constructor(value: T) {
    this.value = value
  }
}

/** Return from `fetch` to override the TTL or commit secondary keys with `primary` under the same lock. */
export class SwrWriteBatch<T> {
  declare private readonly _brand: void
  readonly primary: T
  readonly ttl?: number
  readonly secondary: NonNullable<SwrWriteBatchInput<T>['secondary']>

  constructor(input: SwrWriteBatchInput<T>) {
    this.primary = input.primary
    if (input.ttl !== undefined) this.ttl = input.ttl
    this.secondary = input.secondary ?? []
  }
}
