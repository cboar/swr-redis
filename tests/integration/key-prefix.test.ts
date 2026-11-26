import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient, SwrWriteBatch } from '../../src/index.ts'
import { createRedisKeys } from '../../src/keys.ts'
import { connectRedis, uniquePrefix } from '../helpers/redis.ts'

test(
  'Integration: ioredis keyPrefix applies to data keys, lock keys, batch commits, and polling',
  { timeout: 5000 },
  async (t) => {
    const keyPrefix = uniquePrefix('it-prefix')
    const swrPrefix = uniquePrefix('tenant-a')
    const { data: dataKey, lock: lockKey } = createRedisKeys(swrPrefix)
    const observer = await connectRedis(t)
    if (!observer) return
    const redis = await connectRedis(t, { keyPrefix })
    if (!redis) return

    const waiting = Promise.withResolvers<void>()
    const swr = createSwrClient({
      redis,
      prefix: swrPrefix,
      ttl: 30,
      pollInterval: 0.005,
      onEvent: (event) => {
        if (event.type === 'miss' && event.action === 'wait') waiting.resolve()
      },
    })
    let fetches = 0
    const orders = swr.cache({
      key: (id: string) => `order${id}`,
      fetch: async () => {
        fetches++
        return new SwrWriteBatch({
          primary: 'primary',
          secondary: [{ key: 'order:next', value: 'next', ttl: 60 }],
        })
      },
    })

    assert.equal(await orders.get(''), 'primary')
    assert.equal(await orders.get(':next'), 'next')
    assert.equal(fetches, 1)
    assert.ok(await observer.getBuffer(`${keyPrefix}${dataKey('order')}`))
    assert.ok(await observer.getBuffer(`${keyPrefix}${dataKey('order:next')}`))
    assert.equal(await observer.getBuffer(dataKey('order')), null)
    assert.equal(await observer.get(`${keyPrefix}${lockKey('order')}`), null)
    const nextTtl = await observer.ttl(`${keyPrefix}${dataKey('order:next')}`)
    assert.ok(nextTtl > 290 && nextTtl <= 300, `Expected ~300s, got ${nextTtl}`)

    // A waiter polls the prefixed data and lock keys with one MGET and resolves on commit.
    await observer.set(`${keyPrefix}${lockKey('polled')}`, 'other-owner', 'PX', 5000)
    const polled = swr.cache<string>({
      key: 'polled',
      fetch: async () => assert.fail('waiter must not fetch'),
    })
    const pending = polled.get()
    await waiting.promise
    await polled.set(undefined, 'committed')
    assert.equal(await pending, 'committed')
  },
)
