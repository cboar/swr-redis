import { SwrValidationError } from './errors.ts'

export function createRedisKeys(prefix: string) {
  const namespace = `swr:v1:${Buffer.from(prefix).toString('base64url')}`
  return {
    data: (key: string) => `${namespace}:data:${key}`,
    lock: (key: string) => `${namespace}:lock:${key}`,
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
