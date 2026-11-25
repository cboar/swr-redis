import test from 'node:test'
import assert from 'node:assert/strict'
import { setImmediate } from 'node:timers/promises'
import { createSwrClient, SwrValidationError } from '../src/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)

test(
  'Mode swr (default): returns stale immediately, refreshes in background, waitUntil & flush track it',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const waitUntilPromises: Promise<unknown>[] = []
    let fetchCount = 0
    let releaseBg!: () => void
    let bgStarted!: () => void
    const bgGate = new Promise<void>((resolve) => {
      releaseBg = resolve
    })
    const bgEntered = new Promise<void>((resolve) => {
      bgStarted = resolve
    })

    const swr = createSwrClient({
      redis,
      waitUntil: (p) => waitUntilPromises.push(p),
    })

    let bgSignal: AbortSignal | undefined
    const cache = swr.cache({
      key: 'item',
      ttl: 5,
      fetch: async (_ctx, meta) => {
        assert.equal(meta.cacheWrite, 'attempt')
        fetchCount++
        if (fetchCount === 2) {
          bgSignal = meta.signal
          bgStarted()
          await bgGate
        }
        return `v-${fetchCount}`
      },
    })

    assert.equal(await cache.get(), 'v-1')
    redis.advanceTime(6_000)

    // Caller passes a short-lived request AbortController; background revalidation must NOT inherit it
    const callerAbort = new AbortController()
    const staleRes = await cache.get(undefined, { signal: callerAbort.signal })
    assert.equal(staleRes, 'v-1')

    await bgEntered
    callerAbort.abort()
    assert.equal(bgSignal?.aborted, false)
    // The cold fetch is tracked as well.
    assert.equal(waitUntilPromises.length, 2)

    // Second stale caller while background refresh is in-flight also gets stale immediately
    assert.equal(await cache.get(), 'v-1')
    assert.equal(fetchCount, 2)

    releaseBg()
    await swr.flush()

    // Now fresh hit returns v-2
    assert.equal(await cache.get(), 'v-2')
    assert.equal(fetchCount, 2)
  },
)

test(
  'Mode swr (default): a failed background refresh keeps the stale entry and starts a cooldown',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const entered = Promise.withResolvers<void>()
    const gate = Promise.withResolvers<void>()
    let calls = 0
    const swr = createSwrClient({ redis })
    const cache = swr.cache<string>({
      key: 'bg-failure',
      ttl: 5,
      fetch: async () => {
        if (++calls === 1) return 'v-1'
        entered.resolve()
        await gate.promise
        throw new Error('bg boom')
      },
    })

    assert.equal(await cache.get(), 'v-1')
    const stored = await redis.getBuffer(dataKey('bg-failure'))
    redis.advanceTime(6_000)
    assert.equal(await cache.get(), 'v-1')
    await entered.promise
    assert.ok(await redis.get(lockKey('bg-failure')), 'lock is held while the refresh runs')

    gate.resolve()
    await swr.flush()
    assert.equal(await redis.get(lockKey('bg-failure')), 'cooldown')
    assert.deepEqual(await redis.getBuffer(dataKey('bg-failure')), stored)
  },
)

test(
  'swr-serverless: winner awaits refresh while loser returns stale',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const entered = Promise.withResolvers<void>()
    const gate = Promise.withResolvers<void>()
    let calls = 0
    const cache = createSwrClient({
      redis,
      runtime: 'serverless',
    }).cache({
      key: 'item',
      ttl: 5,
      fetch: async () => {
        calls++
        if (calls === 2) {
          entered.resolve()
          await gate.promise
        }
        return `v${calls}`
      },
    })
    assert.equal(await cache.get(), 'v1')
    redis.advanceTime(6000)
    const winner = cache.get()
    await entered.promise
    assert.equal(await cache.get(), 'v1')
    gate.resolve()
    assert.equal(await winner, 'v2')
    assert.equal(calls, 2)
    assert.equal(await redis.get(lockKey('item')), null)
  },
)

for (const mode of ['swr-serverless', 'wait-on-stale'] as const) {
  for (const { name, outcome, fatal, isFatal } of [
    { name: 'ordinary Error', outcome: new Error('transient'), fatal: false, isFatal: () => false },
    {
      name: 'custom fatal match',
      outcome: new RangeError('schema mismatch'),
      fatal: true,
      isFatal: (error: unknown) => error instanceof RangeError,
    },
    {
      name: 'built-in error overrides isFatal false',
      outcome: new SwrValidationError('contract violation'),
      fatal: true,
      isFatal: () => false,
    },
  ]) {
    test(`Stale failure ${mode}: ${name}`, { timeout: 5000 }, async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      let calls = 0
      const cache = createSwrClient({
        redis,
        runtime: 'serverless',
        staleWait: mode === 'wait-on-stale' ? 10 : 0,
      }).cache({
        key: 'failure',
        ttl: 5,
        isFatal,
        fetch: async () => {
          if (++calls === 1) return 'stale'
          throw outcome
        },
      })
      await cache.get()
      const stored = await redis.getBuffer(dataKey('failure'))
      redis.advanceTime(6000)
      if (fatal) {
        await assert.rejects(cache.get(), (error) => error === outcome)
      } else {
        assert.equal(await cache.get(), 'stale')
      }
      assert.deepEqual(await redis.getBuffer(dataKey('failure')), stored)
      assert.equal(await redis.get(lockKey('failure')), 'cooldown')
    })
  }
}

test('wait-on-stale: loser waits for the winner commit', { timeout: 2000 }, async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const waiting = Promise.withResolvers<void>()
  let calls = 0
  const cache = createSwrClient({
    redis,
    runtime: 'serverless',
    staleWait: 10,
    pollIntervalMs: 1,
    onEvent: (event) => {
      if (event.type === 'hit' && event.stale && !event.startedRevalidation) waiting.resolve()
    },
  }).cache({
    key: 'wait',
    ttl: 5,
    fetch: async (_ctx, { cacheWrite }) => {
      assert.equal(cacheWrite, 'attempt')
      if (++calls === 2) {
        entered.resolve()
        await release.promise
      }
      return `v${calls}`
    },
  })
  await cache.get()
  redis.advanceTime(6000)
  const winner = cache.get()
  await entered.promise
  const loser = cache.get()
  await waiting.promise
  release.resolve()
  assert.deepEqual(await Promise.all([winner, loser]), ['v2', 'v2'])
  assert.equal(calls, 2)
})

for (const fatal of [false, true]) {
  test(
    `wait-on-stale: loser falls back after ${fatal ? 'fatal' : 'nonfatal'} winner failure starts a cooldown`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const entered = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const waiting = Promise.withResolvers<void>()
      const failure = fatal ? new SwrValidationError('fatal') : new Error('transient')
      let calls = 0
      const cache = createSwrClient({
        redis,
        runtime: 'serverless',
        staleWait: 10,
        pollIntervalMs: 1,
        onEvent: (event) => {
          if (event.type === 'hit' && event.stale && !event.startedRevalidation) waiting.resolve()
        },
      }).cache({
        key: 'failed-winner',
        ttl: 5,
        fetch: async () => {
          if (++calls === 1) return 'stale'
          entered.resolve()
          await release.promise
          throw failure
        },
      })
      await cache.get()
      redis.advanceTime(6000)
      const winner = cache.get()
      const winnerResult = fatal
        ? assert.rejects(winner, (error) => error === failure)
        : winner.then((value) => assert.equal(value, 'stale'))
      await entered.promise
      const loser = cache.get()
      await waiting.promise
      release.resolve()
      await winnerResult
      assert.equal(await loser, 'stale')
      assert.equal(await redis.get(lockKey('failed-winner')), 'cooldown')
      assert.equal(calls, 2)
    },
  )
}

test(
  'wait-on-stale: protect-source loser returns stale on timeout while winner keeps fetching',
  { timeout: 2000 },
  async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    t.mock.method(Math, 'random', () => 0)
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const waiting = Promise.withResolvers<void>()
    let calls = 0
    const cache = createSwrClient({
      redis,
      runtime: 'serverless',
      failureStrategy: 'protect-source',
      staleWait: 0.04,
      pollIntervalMs: 10,
      onEvent: (event) => {
        if (event.type === 'hit' && event.stale && !event.startedRevalidation) waiting.resolve()
      },
    }).cache({
      key: 'timeout',
      ttl: 5,
      fetch: async () => {
        if (++calls === 1) return 'stale'
        entered.resolve()
        await release.promise
        return 'late'
      },
    })
    await cache.get()
    redis.advanceTime(6000)
    const winner = cache.get()
    await entered.promise
    const loser = cache.get()
    await waiting.promise
    redis.advanceTime(40)
    t.mock.timers.tick(40)
    await setImmediate()
    assert.equal(await loser, 'stale')
    assert.ok(await redis.get(lockKey('timeout')))
    release.resolve()
    assert.equal(await winner, 'late')
  },
)

test('Mode swr: a per-get waitUntil replaces the client waitUntil for that call', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const clientTracked: Promise<unknown>[] = []
  const getTracked: Promise<unknown>[] = []
  const swr = createSwrClient({ redis, waitUntil: (p) => clientTracked.push(p) })
  let version = 0
  const cache = swr.cache({ key: 'per-get-wait-until', ttl: 2, fetch: async () => ++version })

  await cache.set(undefined, 0)
  redis.advanceTime(3000)
  assert.equal(await cache.get(undefined, { waitUntil: (p) => getTracked.push(p) }), 0)
  assert.equal(getTracked.length, 1)
  assert.equal(clientTracked.length, 0)
  await getTracked[0]

  redis.advanceTime(3000)
  assert.equal(await cache.get(), 1)
  assert.equal(clientTracked.length, 1)
  assert.equal(getTracked.length, 1)
  await swr.flush()
  assert.equal(await cache.get(), 2)
})
