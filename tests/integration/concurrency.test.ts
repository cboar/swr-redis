import { connectRedis, uniquePrefix } from '../helpers/redis.ts'
import { createRedisKeys } from '../../src/keys.ts'
import { COOLDOWN_MARKER } from '../../src/lock.ts'
import { DEFAULTS } from '../../src/options.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient, type SwrEvent } from '../../src/index.ts'
import { encodeEnvelope } from '../helpers/envelope.ts'

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
      pollInterval: 0.015,
      onEvent: (event: SwrEvent) => {
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

for (const phase of ['cold', 'stale'] as const) {
  test(
    `Integration: a failed ${phase} fetch leaves a cooldown marker that another process ${phase === 'cold' ? 'waits out before the next attempt' : 'answers with stale data'}`,
    { timeout: 5000 },
    async (t) => {
      const redis = await connectRedis(t, { keyPrefix: uniquePrefix('cooldown') })
      if (!redis) return
      const lockKey = toLockKey('cooling')
      await redis.unlink(toDataKey('cooling'), lockKey)
      if (phase === 'stale') {
        const now = Date.now()
        const timing = {
          cachedAtMs: now - 2000,
          freshUntilMs: now - 1000,
          expiresAtMs: now + 60_000,
        }
        await redis.set(toDataKey('cooling'), encodeEnvelope({ value: 'stale', ...timing }))
      }
      const failure = new Error('source down')
      const waits: string[] = []
      // The stale phase never waits for expiry; the cold phase waits it out once.
      const cooldownMs = phase === 'cold' ? 1000 : 5000
      const options = {
        redis,
        failureCooldown: cooldownMs / 1000,
        pollInterval: 0.015,
        staleWait: Infinity,
        onEvent: (event: SwrEvent) => {
          if (event.type === 'miss' && event.action === 'wait') waits.push(event.key)
        },
      }
      const failingSwr = createSwrClient(options)
      const failing = failingSwr.cache({
        key: 'cooling',
        fetch: async () => {
          throw failure
        },
      })
      let fetches = 0
      const other = createSwrClient(options).cache({
        key: 'cooling',
        fetch: async () => {
          fetches++
          return 'recovered'
        },
      })

      if (phase === 'cold') await assert.rejects(failing.get(), (err) => err === failure)
      else {
        // An attached winner serves stale data after its refresh fails.
        assert.equal(await failing.get(), 'stale')
        await failingSwr.flush()
      }
      assert.equal(await redis.get(lockKey), COOLDOWN_MARKER)
      const pttl = await redis.pttl(lockKey)
      assert.ok(
        pttl > 0 && pttl <= cooldownMs,
        `expected the marker to expire within ${cooldownMs}ms, got ${pttl}`,
      )

      if (phase === 'cold') {
        assert.equal(await other.get(), 'recovered')
        assert.deepEqual(waits, ['cooling'])
        assert.equal(fetches, 1)
        assert.equal(await redis.get(lockKey), null)
      } else {
        assert.equal(await other.get(), 'stale')
        assert.equal(fetches, 0)
        assert.equal(await redis.get(lockKey), COOLDOWN_MARKER)
      }
    },
  )
}
