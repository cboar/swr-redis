import { connectRedis, uniquePrefix } from '../helpers/redis.ts'
import { createRedisKeys } from '../../src/keys.ts'
import { DEFAULTS } from '../../src/options.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient } from '../../src/index.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

test(
  'Integration: cache coordinates 50 simultaneous cold misses with real Redis',
  { timeout: 5000 },
  async (t) => {
    const redis = await connectRedis(t, { keyPrefix: uniquePrefix('concurrency') })
    if (!redis) return
    const testId = 'shared'
    let upstreamFetchCount = 0
    let waiters = 0
    const allWaiting = Promise.withResolvers<void>()
    const options = {
      redis,
      ttl: 5,
      staleMultiplier: 6,
      pollIntervalMs: 15,
      onEvent: (event: import('../../src/types.ts').SwrEvent) => {
        if (event.type === 'miss' && event.action === 'wait' && ++waiters === 49)
          allWaiting.resolve()
      },
    }
    const fetchValue = async (id: string) => {
      upstreamFetchCount++
      await allWaiting.promise
      return `data-for-${id}-fetch-${upstreamFetchCount}`
    }

    const workerA = createSwrClient(options).cache({
      key: (id: string) => `stress:${id}`,
      fetch: fetchValue,
    })
    const workerB = createSwrClient(options).cache({
      key: (id: string) => `stress:${id}`,
      fetch: fetchValue,
    })

    const testKey = workerA.key(String(testId))
    await redis.unlink(toDataKey(testKey), toLockKey(testKey))

    const promises = Array.from({ length: 50 }, (_, index) =>
      (index % 2 === 0 ? workerA : workerB).get(String(testId)),
    )
    const results = await Promise.all(promises)

    assert.equal(results.length, 50)
    for (const result of results) {
      assert.equal(result, `data-for-${testId}-fetch-1`)
    }

    assert.equal(upstreamFetchCount, 1)
    assert.equal(await redis.get(toLockKey(testKey)), null)
  },
)
