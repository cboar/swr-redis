import { SwrValidationError } from './errors.ts'

// The hash tag keeps a key's data and lock in one Cluster slot; the base64url prefix has no braces.
export function createRedisKeys(prefix: string) {
  const ns = Buffer.from(prefix).toString('base64url')
  return {
    data: (key: string) => `SWR2:D:${ns}:{${key}}`,
    lock: (key: string) => `SWR2:L:${ns}:{${key}}`,
  }
}

export type RedisKeys = ReturnType<typeof createRedisKeys>

export function requireNonEmpty(value: string, label: 'prefix' | 'name' | 'key') {
  if (value.length === 0) throw new SwrValidationError(`${label} must be a non-empty string`)
  // UTF-8 encoding replaces lone surrogates with U+FFFD, causing distinct values to collide in Redis.
  if (label !== 'name' && !value.isWellFormed())
    throw new SwrValidationError(`${label} must not contain lone surrogates`)
  return value
}
