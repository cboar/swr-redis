import test from 'node:test'
import assert from 'node:assert/strict'
import { setImmediate } from 'node:timers/promises'
import { pollDelayMs } from '../src/cache.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createSwrClient, SwrTimeoutError } from '../src/index.ts'
import { encodeEnvelope } from './helpers/envelope.ts'
import { holdLock, interceptAcquire } from './helpers/acquire.ts'
import { createFakeRedis } from './fake-redis.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)
const envelope = (value: string | null, freshUntilMs: number, expiresAtMs = 10_000) =>
  encodeEnvelope({ value, cachedAtMs: 1000, freshUntilMs, expiresAtMs })

// Elapsed-time tests advance both clocks; setImmediate drains command continuations.
async function tick(t: test.TestContext, redis: ReturnType<typeof createFakeRedis>, ms: number) {
  redis.advanceTime(ms)
  t.mock.timers.tick(ms)
  await setImmediate()
}

test('pollDelayMs: adds up to 30% jitter and never sleeps past the remaining wait budget', (t) => {
  let random = 0
  t.mock.method(Math, 'random', () => random)
  assert.equal(pollDelayMs(100, 1000, 0), 100)
  random = 0.999999
  assert.equal(pollDelayMs(100, 1000, 0), 130)
  assert.equal(pollDelayMs(100, 1000, 950), 50)
  assert.equal(pollDelayMs(100, 1000, 1200), 1)
})

test(
  'Cold winner and waiter: one fetch commits and the waiter resolves from Redis',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const waiting = Promise.withResolvers<void>()
    let fetches = 0
    const cache = createSwrClient({
      redis,
      pollIntervalMs: 1,
      onEvent: (event) => {
        if (event.type === 'miss' && event.action === 'wait') waiting.resolve()
      },
    }).cache({
      key: 'cold',
      fetch: async (_ctx, { cacheWrite }) => {
        assert.equal(cacheWrite, 'attempt')
        fetches++
        entered.resolve()
        await release.promise
        return 'winner'
      },
    })
    const winner = cache.get()
    await entered.promise
    const waiter = cache.get()
    await waiting.promise
    release.resolve()
    assert.deepEqual(await Promise.all([winner, waiter]), ['winner', 'winner'])
    assert.equal(fetches, 1)
    assert.equal(await redis.get(lockKey('cold')), null)
  },
)

test(
  'Cold polling: sleeps first and uses one atomic mgetBuffer per tick',
  { timeout: 2000 },
  async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    t.mock.method(Math, 'random', () => 0)
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    await holdLock(redis, lockKey('passive'), 5000)
    const evals = redis.evalCallCount
    const sets = redis.setCallCount
    let initialReads = 0
    let pollSnapshots = 0
    let lockGets = 0
    const getBuffer = redis.getBuffer.bind(redis)
    const mgetBuffer = redis.mgetBuffer.bind(redis)
    const get = redis.get.bind(redis)
    redis.getBuffer = async (key) => {
      initialReads++
      return getBuffer(key)
    }
    redis.mgetBuffer = (async (...keys: string[]) => {
      pollSnapshots++
      return mgetBuffer(...keys)
    }) as typeof redis.mgetBuffer
    redis.get = async (key) => {
      lockGets++
      return get(key)
    }
    const waiting = Promise.withResolvers<void>()
    const cache = createSwrClient({
      redis,
      pollIntervalMs: 10,
      onEvent: (event) => {
        if (event.type === 'miss' && event.action === 'wait') waiting.resolve()
      },
    }).cache({ key: 'passive', fetch: async () => assert.fail('passive waiter must not fetch') })
    const pending = cache.get()
    await waiting.promise
    assert.equal(initialReads, 1)
    assert.equal(pollSnapshots, 0)
    await tick(t, redis, 9)
    assert.equal(pollSnapshots, 0)
    await tick(t, redis, 1)
    assert.equal(pollSnapshots, 1)
    await tick(t, redis, 10)
    assert.equal(pollSnapshots, 2)
    assert.equal(lockGets, 0)
    assert.equal(redis.evalCallCount - evals, 1)
    assert.equal(redis.setCallCount - sets, 0)
    await redis.set(dataKey('passive'), envelope('committed', 5000))
    await redis.unlink(lockKey('passive'))
    await tick(t, redis, 10)
    assert.equal(await pending, 'committed')
    assert.equal(pollSnapshots, 3)
    assert.equal(redis.evalCallCount - evals, 1)
    assert.equal(redis.setCallCount - sets, 1, 'the test write')
  },
)

test(
  'Lock stealing: a waiter takes over an expired lock, fetches once, and releases it',
  { timeout: 2000 },
  async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    t.mock.method(Math, 'random', () => 0)
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    await holdLock(redis, lockKey('steal'), 30, 'crashed')
    const waiting = Promise.withResolvers<void>()
    let fetches = 0
    const cache = createSwrClient({
      redis,
      pollIntervalMs: 10,
      waitTimeout: 0.03,
      failureStrategy: 'protect-source',
      onEvent: (event) => {
        if (event.type === 'miss' && event.action === 'wait') waiting.resolve()
      },
    }).cache({
      key: 'steal',
      fetch: async (_ctx, { cacheWrite }) => {
        assert.equal(cacheWrite, 'attempt')
        fetches++
        return 'stolen'
      },
    })
    const pending = cache.get()
    await waiting.promise
    await tick(t, redis, 30)
    assert.equal(await pending, 'stolen')
    assert.equal(fetches, 1)
    assert.equal(await redis.get(lockKey('steal')), null)
    assert.ok(await redis.getBuffer(dataKey('steal')))
  },
)

for (const failureStrategy of ['availability', 'protect-source'] as const) {
  test(
    `Cold wait timeout ${failureStrategy}: policy and accurate attempt count`,
    { timeout: 2000 },
    async (t) => {
      t.mock.timers.enable({ apis: ['setTimeout'] })
      t.mock.method(Math, 'random', () => 0)
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      await holdLock(redis, lockKey('timeout'), 5000)
      let pollSnapshots = 0
      const origMget = redis.mgetBuffer.bind(redis)
      redis.mgetBuffer = (async (...keys: string[]) => {
        pollSnapshots++
        return origMget(...keys)
      }) as typeof redis.mgetBuffer
      const waiting = Promise.withResolvers<void>()
      let fetches = 0
      const cache = createSwrClient({
        redis,
        failureStrategy,
        waitTimeout: 0.03,
        pollIntervalMs: 10,
        onEvent: (event) => {
          if (event.type === 'miss' && event.action === 'wait') waiting.resolve()
        },
      }).cache({
        key: 'timeout',
        fetch: async (_ctx, { cacheWrite }) => {
          assert.equal(cacheWrite, 'skip')
          fetches++
          return 'direct'
        },
      })
      const pending = cache.get()
      const result =
        failureStrategy === 'protect-source'
          ? assert.rejects(
              pending,
              (error) => error instanceof SwrTimeoutError && error.key === 'timeout',
            )
          : pending.then((value) => assert.equal(value, 'direct'))
      await waiting.promise
      await tick(t, redis, 30)
      await result
      assert.equal(fetches, failureStrategy === 'availability' ? 1 : 0)
      assert.equal(pollSnapshots, 1)
    },
  )
}

for (const { name, freshUntilMs, expiresAtMs, accepted, inMemory } of [
  {
    name: 'fresh writer deadline',
    freshUntilMs: 5000,
    expiresAtMs: 10_000,
    accepted: true,
    inMemory: true,
  },
  {
    name: 'exact freshness boundary',
    freshUntilMs: 1000,
    expiresAtMs: 5000,
    accepted: true,
    inMemory: false,
  },
  {
    name: 'exact expiry boundary',
    freshUntilMs: 900,
    expiresAtMs: 1000,
    accepted: false,
    inMemory: false,
  },
  { name: 'past expiry', freshUntilMs: 800, expiresAtMs: 999, accepted: false, inMemory: false },
]) {
  test(`Cold poll stored deadlines: ${name}`, { timeout: 2000 }, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    await holdLock(redis, lockKey('deadline'), 5000)
    const stored = envelope(null, freshUntilMs, expiresAtMs)
    let pollCalls = 0
    const mgetBuffer = redis.mgetBuffer.bind(redis)
    redis.mgetBuffer = (async (...keys: string[]) => {
      pollCalls++
      if (pollCalls === 1) {
        // First poll observes `stored` while lock is still held; queue replacement for tick 2.
        await redis.set(dataKey('deadline'), envelope('replacement', 5000))
        await redis.unlink(lockKey('deadline'))
        return [stored, Buffer.from('owner')]
      }
      return mgetBuffer(...keys)
    }) as typeof redis.mgetBuffer
    const cache = createSwrClient({
      redis,
      memory: true,
      pollIntervalMs: 1,
    }).cache({
      key: 'deadline',
      ttl: () => assert.fail('read must not resolve reader TTL'),
      fetch: async () => assert.fail('waiter must not fetch'),
    })
    assert.equal(await cache.get(), accepted ? null : 'replacement')

    let reads = 0
    const getBuffer = redis.getBuffer.bind(redis)
    redis.getBuffer = async (key) => {
      reads++
      return getBuffer(key)
    }
    await redis.set(dataKey('deadline'), stored)
    if (accepted && !inMemory) {
      redis.getBuffer = async () => {
        reads++
        return envelope('redis', 5000)
      }
    }
    assert.equal(
      await cache.get(),
      accepted && inMemory ? null : accepted ? 'redis' : 'replacement',
    )
    assert.equal(reads, accepted && !inMemory ? 1 : 0)
  })
}

test(
  'Cold poll: fresh writer entry beyond memory cap stays eligible for Redis reads only',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(4000)
    useRedisClock(t, redis)
    await holdLock(redis, lockKey('cap'), 5000)
    const stored = envelope(null, 5000, 11_000)
    let reads = 0
    redis.getBuffer = async () => {
      reads++
      return reads === 1 ? null : stored
    }
    redis.mgetBuffer = async () => [stored, null]
    const cache = createSwrClient({
      redis,
      memory: { maxTtl: 1 },
      pollIntervalMs: 1,
    }).cache({
      key: 'cap',
      ttl: () => assert.fail('read TTL'),
      fetch: async () => assert.fail('unexpected fetch'),
    })
    assert.equal(await cache.get(), null)
    const before = reads
    assert.equal(await cache.get(), null)
    assert.equal(reads, before + 1)
  },
)

for (const { name, freshUntilMs, expected } of [
  { name: 'accepts fresh writer deadline', freshUntilMs: 7001, expected: null },
  { name: 'rejects exact freshness boundary', freshUntilMs: 7000, expected: 'stale' },
  { name: 'rejects already stale writer deadline', freshUntilMs: 6900, expected: 'stale' },
]) {
  test(`Stale atomic MGET poll: ${name}`, { timeout: 2000 }, async (t) => {
    const redis = createFakeRedis(7000)
    useRedisClock(t, redis)
    const key = 'stale-deadline'
    await redis.set(dataKey(key), envelope('stale', 6000, 20_000))
    await holdLock(redis, lockKey(key), 5000)
    const replacement = envelope(null, freshUntilMs, 10_000)
    let pollSnapshots = 0
    const mgetBuffer = redis.mgetBuffer.bind(redis)
    redis.mgetBuffer = (async (...keys: string[]) => {
      pollSnapshots++
      if (pollSnapshots === 1) {
        await redis.set(dataKey(key), replacement)
        await redis.unlink(lockKey(key))
      }
      return mgetBuffer(...keys)
    }) as typeof redis.mgetBuffer
    const cache = createSwrClient({
      redis,
      mode: 'wait-on-stale',
      pollIntervalMs: 1,
    }).cache({
      key,
      ttl: () => assert.fail('stored deadlines bypass reader TTL'),
      fetch: async () => assert.fail('loser must not fetch'),
    })
    assert.equal(await cache.get(), expected)
    // Atomic MGET resolves or falls back in a single snapshot with no extra re-read.
    assert.equal(pollSnapshots, 1)
  })
}

for (const scenario of ['timeout', 'unlocked', 'redis_error'] as const) {
  test(
    `Stale poll with cached undefined preserves stale policy on ${scenario}`,
    { timeout: 2000 },
    async (t) => {
      if (scenario === 'timeout') {
        t.mock.timers.enable({ apis: ['setTimeout'] })
        t.mock.method(Math, 'random', () => 0)
      }
      const redis = createFakeRedis(7000)
      useRedisClock(t, redis)
      const key = `stale-undef:${scenario}`
      await redis.set(
        dataKey(key),
        encodeEnvelope({
          value: undefined,
          cachedAtMs: 1000,
          freshUntilMs: 6000,
          expiresAtMs: 20_000,
        }),
      )
      await holdLock(redis, lockKey(key), 5000)

      const waiting = Promise.withResolvers<void>()
      let acquireCalls = 0
      interceptAcquire(redis, (acquire, ...args) => {
        acquireCalls++
        return acquire(...args)
      })

      const origMget = redis.mgetBuffer.bind(redis)
      redis.mgetBuffer = (async (...keys: string[]) => {
        if (scenario === 'unlocked') {
          await redis.unlink(lockKey(key))
          return origMget(...keys)
        }
        if (scenario === 'redis_error') {
          throw new Error('mget failed')
        }
        return origMget(...keys)
      }) as typeof redis.mgetBuffer

      const cache = createSwrClient({
        redis,
        mode: 'wait-on-stale',
        failureStrategy: scenario === 'timeout' ? 'protect-source' : 'availability',
        waitTimeout: 0.03,
        pollIntervalMs: scenario === 'timeout' ? 10 : 1,
        onEvent: (event) => {
          if (event.type === 'hit' && event.stale && !event.startedRevalidation) waiting.resolve()
        },
      }).cache<string | undefined>({
        key,
        fetch: async () => assert.fail('stale waiter must not fetch'),
      })

      const pending = cache.get()
      if (scenario === 'timeout') {
        await waiting.promise
        await tick(t, redis, 30)
      }
      assert.equal(await pending, undefined)
      assert.equal(acquireCalls, 1, 'stale waiter must not attempt lock steal')
    },
  )
}
