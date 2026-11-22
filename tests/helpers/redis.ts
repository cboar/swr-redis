import type { TestContext } from 'node:test'
import { randomUUID } from 'node:crypto'
import { Redis, type RedisOptions } from 'ioredis'

// Register cleanup before connecting so failed or partial setup also closes sockets.
export async function connectRedis(t: TestContext, options: RedisOptions = {}) {
  const redis = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
    lazyConnect: true,
    retryStrategy: () => null,
    maxRetriesPerRequest: 1,
    ...options,
  })
  redis.on('error', () => {})
  const prefix = options.keyPrefix
  t.after(async () => {
    try {
      if (prefix && redis.status === 'ready') {
        // KEYS returns physical names; UNLINK on this client applies its prefix.
        const keys = await redis.keys(`${prefix}*`)
        if (keys.length) await redis.unlink(...keys.map((key) => key.slice(prefix.length)))
      }
    } finally {
      redis.disconnect()
    }
  })
  try {
    await redis.connect()
    await redis.ping()
    return redis
  } catch (error) {
    if (process.env.REQUIRE_REDIS === '1') throw error
    t.skip('Redis unavailable; set REQUIRE_REDIS=1 to require integration coverage')
    return undefined
  }
}

export function uniquePrefix(label: string) {
  return `${label}:${randomUUID()}:`
}
