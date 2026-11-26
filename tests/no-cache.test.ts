import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient, SwrNoCache } from '../src/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)

test(
  'SwrNoCache: unwraps values, skips Redis/memory/ttl/codec/refresh, and releases lock',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const swr = createSwrClient({ redis, memory: true })
    const samples: unknown[] = [{ ok: false, reason: 'rate-limited' }, null, undefined, 0]
    let fetches = 0
    const cache = swr.cache<unknown, number>({
      key: (id) => `nocache:${id}`,
      ttl: () => assert.fail('SwrNoCache must not resolve TTL'),
      codec: {
        encode: () => assert.fail('SwrNoCache must not encode'),
        decode: () => assert.fail('SwrNoCache must not decode'),
      },
      fetch: async (id, { cacheWrite }) => {
        assert.equal(cacheWrite, 'attempt')
        fetches++
        return new SwrNoCache(samples[id])
      },
    })

    for (const [id, expected] of samples.entries()) {
      assert.deepEqual(await cache.get(id), expected)
      assert.deepEqual(await cache.get(id), expected)
      assert.equal(await redis.getBuffer(dataKey(`nocache:${id}`)), null)
      assert.equal(await redis.get(lockKey(`nocache:${id}`)), null)
    }
    assert.equal(fetches, samples.length * 2)
  },
)

test(
  'SwrNoCache: concurrent cold waiter proceeds to fetch when winner returns SwrNoCache',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const waiting = Promise.withResolvers<void>()
    const swr = createSwrClient({
      redis,
      ttl: 10,
      pollInterval: 0.005,
      onEvent: (e) => {
        if (e.type === 'miss' && e.action === 'wait') waiting.resolve()
      },
    })

    let call = 0
    let releaseWinner!: () => void
    let winnerStarted!: () => void
    const entered = new Promise<void>((resolve) => {
      winnerStarted = resolve
    })

    const cache = swr.cache({
      key: 'nocache-waiter',
      fetch: async () => {
        call++
        if (call === 1) {
          winnerStarted()
          await new Promise<void>((resolve) => {
            releaseWinner = resolve
          })
          return new SwrNoCache('uncached-first')
        }
        return 'cached-second'
      },
    })

    const pWinner = cache.get()
    await entered
    const pWaiter = cache.get()

    await waiting.promise
    releaseWinner()
    const [winnerRes, waiterRes] = await Promise.all([pWinner, pWaiter])

    assert.equal(winnerRes, 'uncached-first')
    assert.equal(waiterRes, 'cached-second')
    assert.equal(call, 2)
    assert.equal(await cache.get(), 'cached-second')
    assert.equal(call, 2)
  },
)

// Detached: the winner returns stale. Inline: the winner waits for its refresh. Waiter: a second
// stale caller polls for the refresh.
for (const stale of ['detached', 'inline', 'inline with waiter'] as const) {
  test(
    `SwrNoCache: a stale refresh (${stale}) keeps the stale entry and releases the lock without a cooldown`,
    { timeout: 5000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      let waiterStarted!: () => void
      const waiting = new Promise<void>((resolve) => {
        waiterStarted = resolve
      })
      const swr = createSwrClient({
        redis,
        memory: true,
        ttl: 1,
        staleMultiplier: 10,
        runtime: stale === 'detached' ? 'server' : 'serverless',
        staleWait: stale === 'inline with waiter' ? 10 : 0,
        pollInterval: 0.005,
        onEvent: (event) => {
          if (event.type === 'hit' && event.stale && !event.startedRevalidation) waiterStarted()
        },
      })
      let calls = 0
      let fetchStarted!: () => void
      let releaseFetch!: () => void
      const entered = new Promise<void>((resolve) => {
        fetchStarted = resolve
      })
      const gate = new Promise<void>((resolve) => {
        releaseFetch = resolve
      })
      const cache = swr.cache({
        key: 'stale-nocache',
        fetch: async (_ctx, { cacheWrite }) => {
          assert.equal(cacheWrite, 'attempt')
          if (++calls === 1) return 'stored'
          fetchStarted()
          await gate
          return new SwrNoCache('transient')
        },
      })

      assert.equal(await cache.get(), 'stored')
      const rawBefore = await redis.getBuffer(dataKey('stale-nocache'))
      redis.advanceTime(1500)
      const winner = cache.get()
      await entered
      const waiter = stale === 'inline with waiter' ? cache.get() : undefined
      if (waiter) await waiting
      if (stale === 'detached') assert.equal(await winner, 'stored')
      releaseFetch()

      assert.equal(await winner, stale === 'detached' ? 'stored' : 'transient')
      if (waiter) assert.equal(await waiter, 'stored')
      await swr.flush()
      assert.equal(calls, 2)
      assert.deepEqual(await redis.getBuffer(dataKey('stale-nocache')), rawBefore)
      assert.equal(await redis.get(lockKey('stale-nocache')), null)
    },
  )
}
