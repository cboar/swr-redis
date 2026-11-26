import test from 'node:test'
import assert from 'node:assert/strict'
import {
  createSwrClient,
  SwrTimeoutError,
  SwrValidationError,
  SwrWriteBatch,
  type SwrEvent,
} from '../src/index.ts'
import { registerLockCommands } from '../src/lock.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { tick, useRedisClock } from './helpers/clock.ts'

const { lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

function recorder() {
  const events: SwrEvent[] = []
  return { events, onEvent: (event: SwrEvent) => void events.push(event) }
}

test('Events: read paths emit hit, miss, and refresh, stamped with the cache name', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const { events, onEvent } = recorder()
  const swr = createSwrClient({
    redis,
    memory: true,
    runtime: 'serverless',
    onEvent,
  })
  const named = swr.cache({
    name: 'users',
    key: (id: string) => `u:${id}`,
    ttl: 5,
    fetch: async (id) => id,
  })
  const unnamed = swr.cache({ key: 'plain', fetch: async () => 'ok' })

  await named.get('1')
  await named.get('1')
  await named.get('1', { memory: false })
  redis.advanceTime(6000)
  await named.get('1', { memory: false })
  await unnamed.get()

  assert.deepEqual(events, [
    { type: 'miss', cache: 'users', key: 'u:1', action: 'fetch' },
    { type: 'refresh', cache: 'users', key: 'u:1', trigger: 'cold_miss', durationMs: 0 },
    {
      type: 'hit',
      cache: 'users',
      key: 'u:1',
      source: 'memory',
      stale: false,
      startedRevalidation: false,
    },
    {
      type: 'hit',
      cache: 'users',
      key: 'u:1',
      source: 'redis',
      stale: false,
      startedRevalidation: false,
    },
    {
      type: 'hit',
      cache: 'users',
      key: 'u:1',
      source: 'redis',
      stale: true,
      startedRevalidation: true,
    },
    { type: 'refresh', cache: 'users', key: 'u:1', trigger: 'stale_revalidation', durationMs: 0 },
    { type: 'miss', key: 'plain', action: 'fetch' },
    { type: 'refresh', key: 'plain', trigger: 'cold_miss', durationMs: 0 },
  ])
})

test('Events: error reasons and severities', { timeout: 5000 }, async (t) => {
  const errors = async (
    run: (
      redis: ReturnType<typeof createFakeRedis>,
      onEvent: (e: SwrEvent) => void,
    ) => Promise<unknown>,
  ) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const { events, onEvent } = recorder()
    await run(redis, onEvent)
    // Every redis_error* event names all affected keys.
    return events.flatMap((e) =>
      e.type !== 'error'
        ? []
        : e.reason === 'redis_error' || e.reason === 'redis_error_fallback'
          ? [`${e.reason}:${e.severity}:${e.keys.join()}`]
          : e.reason === 'stale_fallback'
            ? [`${e.reason}:${e.severity}:${e.stage}`]
            : [`${e.reason}:${e.severity}`],
    )
  }
  const failing = () => {
    throw new Error('boom')
  }

  // With staleWait 0, stale refresh failures only differ by event: transient vs fatal (built-in or
  // isFatal). A SwrTimeoutError from a nested cache is an ordinary failure.
  class CustomFatal extends Error {}
  for (const [failure, expected] of [
    [new Error('transient'), 'stale_fallback:warn:refresh'],
    [new SwrTimeoutError('nested', 1, 'fetch'), 'stale_fallback:warn:refresh'],
    [new CustomFatal('schema mismatch'), 'fatal:error'],
    [new SwrValidationError('contract violation'), 'fatal:error'],
  ] as const) {
    assert.deepEqual(
      await errors(async (redis, onEvent) => {
        const swr = createSwrClient({ redis, onEvent, isFatal: (e) => e instanceof CustomFatal })
        const cache = swr.cache<string>({
          key: 'k',
          ttl: 1,
          fetch: async () => {
            throw failure
          },
        })
        await cache.set(undefined, 'stale')
        redis.advanceTime(1500)
        assert.equal(await cache.get(), 'stale')
        await swr.flush()
      }),
      [expected],
    )
  }

  // A fetchTimeout is a warning when stale data is served instead, whether or not a caller waits.
  t.mock.timers.enable({ apis: ['setTimeout'] })
  for (const [phase, expected] of [
    ['cold', 'timeout:error'],
    ['stale', 'timeout:warn'],
  ] as const) {
    assert.deepEqual(
      await errors(async (redis, onEvent) => {
        const entered = Promise.withResolvers<void>()
        const swr = createSwrClient({ redis, onEvent, fetchTimeout: 1 })
        const cache = swr.cache<string>({
          key: 'k',
          ttl: 1,
          fetch: (_ctx, { signal }) => {
            entered.resolve()
            return new Promise((_resolve, reject) => {
              signal.addEventListener('abort', () => reject(signal.reason))
            })
          },
        })
        if (phase === 'stale') {
          await cache.set(undefined, 'stale')
          redis.advanceTime(1500)
        }
        const result =
          phase === 'cold'
            ? assert.rejects(cache.get(), SwrTimeoutError)
            : cache.get().then((value) => assert.equal(value, 'stale'))
        await entered.promise
        await tick(t, redis, 1000)
        await result
        await swr.flush()
      }),
      [expected],
    )
  }
  t.mock.timers.reset()

  assert.deepEqual(
    await errors((redis, onEvent) => {
      registerLockCommands(redis).swrCommitAndRelease = async () => '0'
      return createSwrClient({ redis, onEvent })
        .cache({ key: 'k', fetch: async () => 'v' })
        .get()
    }),
    ['lock_lost:warn'],
  )

  for (const failureStrategy of ['availability', 'protect-source'] as const) {
    // A failed commit falls back to the fetched value under either strategy, naming the whole batch.
    assert.deepEqual(
      await errors(async (redis, onEvent) => {
        registerLockCommands(redis).swrCommitAndRelease = async () => failing()
        const cache = createSwrClient({ redis, onEvent, failureStrategy }).cache({
          key: 'p',
          fetch: async () =>
            new SwrWriteBatch({
              primary: 'v',
              secondary: [
                { key: 's1', value: 1 },
                { key: 's2', value: 2 },
              ],
            }),
        })
        assert.equal(await cache.get(), 'v')
      }),
      ['redis_error_fallback:warn:p,s1,s2'],
    )
  }
})

test(
  'Events: observer failures are isolated and refresh observers see committed memory',
  { timeout: 5000 },
  async () => {
    // Sync throws and async rejections never reach the cache or leak unhandled rejections.
    const redis = createFakeRedis(1000)
    registerLockCommands(redis).swrCommitAndRelease = async () => {
      throw new Error('commit failed')
    }
    const throwing = createSwrClient({
      redis,
      onEvent: (event) => {
        if (event.type === 'miss') throw new Error('sync boom')
        return Promise.reject(new Error('async boom')) as unknown as void
      },
    }).cache({ key: 'isolated', fetch: async () => 'value' })
    assert.equal(await throwing.get(), 'value')
    assert.equal(await redis.get(toLockKey('isolated')), null)
    await new Promise<void>((resolve) => setImmediate(resolve))

    // A synchronous refresh observer reads the committed memory entry and can mutate safely.
    const redis2 = createFakeRedis(1000)
    let reads = 0
    const getBuffer = redis2.getBuffer.bind(redis2)
    redis2.getBuffer = async (key) => {
      reads++
      return getBuffer(key)
    }
    let observedGet: Promise<string> | undefined
    let observedSet: Promise<void> | undefined
    let cache!: ReturnType<typeof swr.cache<string>>
    const swr = createSwrClient({
      redis: redis2,
      memory: true,
      onEvent: (event) => {
        if (event.type === 'refresh') {
          observedGet = cache.get()
          observedSet = cache.set(undefined, 'observer-override')
        }
      },
    })
    cache = swr.cache<string>({ key: 'refresh-order', fetch: async () => 'fetched' })

    assert.equal(await cache.get(), 'fetched')
    assert.equal(await observedGet, 'fetched')
    assert.equal(reads, 1, 'observer read came from memory')
    await observedSet
    assert.equal(await cache.get(), 'observer-override')
    assert.equal(await cache.get(undefined, { memory: false }), 'observer-override')
  },
)

test('Events: heartbeat renewal failures emit lock_renewal_failure stamped with key and cache', async () => {
  const redis = createFakeRedis()
  registerLockCommands(redis).swrExtendLock = async () => {
    throw new Error('renew failed')
  }
  const { events, onEvent } = recorder()
  const cache = createSwrClient({ redis, onEvent, lockTtl: 0.02 }).cache({
    name: 'slow',
    key: 'renew',
    fetch: async () => {
      await new Promise((resolve) => setTimeout(resolve, 60))
      return 'v'
    },
  })
  assert.equal(await cache.get(), 'v')
  const renewals = events.filter((e) => e.type === 'error' && e.reason === 'lock_renewal_failure')
  assert.ok(renewals.length >= 1)
  for (const event of renewals) {
    assert.ok(event.type === 'error' && event.reason === 'lock_renewal_failure')
    assert.equal(event.key, 'renew')
    assert.equal(event.cache, 'slow')
    assert.equal(event.severity, 'warn')
    assert.equal((event.err as Error).message, 'renew failed')
  }
  // Without renewals the short lock expires before the commit, which is rejected.
  assert.ok(events.some((e) => e.type === 'error' && e.reason === 'lock_lost'))
})
