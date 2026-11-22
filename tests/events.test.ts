import test from 'node:test'
import assert from 'node:assert/strict'
import {
  createSwrClient,
  SwrRedisError,
  SwrTimeoutError,
  SwrValidationError,
  SwrWriteBatch,
  type SwrCodec,
  type SwrEvent,
} from '../src/index.ts'
import { registerLockCommands } from '../src/lock.ts'
import { failAcquire } from './helpers/acquire.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { encodeEnvelope } from './helpers/envelope.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

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
    mode: 'swr-serverless',
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

test(
  'Events: waiters emit miss(wait) and one terminal poll per wait',
  { timeout: 2000 },
  async () => {
    // waitTimeout elapses on the wall clock.
    const redis = createFakeRedis()
    const pollOutcomes = async (
      setup: () => Promise<void>,
      run: (swr: ReturnType<typeof createSwrClient>, waiting: Promise<void>) => Promise<unknown>,
      options = {},
    ) => {
      redis.clear()
      await setup()
      const { events, onEvent } = recorder()
      const waiting = Promise.withResolvers<void>()
      const observe = (event: SwrEvent) => {
        onEvent(event)
        const staleWait = event.type === 'hit' && event.stale && !event.startedRevalidation
        if ((event.type === 'miss' && event.action === 'wait') || staleWait) waiting.resolve()
      }
      await run(
        createSwrClient({
          redis,
          pollIntervalMs: 1,
          waitTimeout: 0.02,
          onEvent: observe,
          ...options,
        }),
        waiting.promise,
      )
      for (const e of events) {
        // Every poll that took a snapshot reports it; cancellation can land before the first one.
        if (e.type === 'poll' && e.outcome !== 'cancelled')
          assert.ok(e.attempts >= 1 && e.durationMs >= 0, `${e.outcome} attempts`)
      }
      return events
        .filter(
          (e) =>
            e.type === 'miss' || e.type === 'poll' || e.type === 'refresh' || e.type === 'error',
        )
        .map((e) =>
          e.type === 'poll'
            ? `poll:${e.outcome}`
            : e.type === 'miss'
              ? `miss:${e.action}`
              : e.type === 'refresh'
                ? `refresh:${e.trigger}`
                : `error:${e.reason}`,
        )
    }
    const holdLock = (key: string, ms: number) => async () => {
      await redis.set(toLockKey(key), 'other-owner', 'PX', ms)
    }

    // Lock released with data committed -> resolved.
    assert.deepEqual(
      await pollOutcomes(holdLock('resolved', 5000), async (swr, waiting) => {
        const cache = swr.cache({ key: 'resolved', fetch: async () => 'unused' })
        const pending = cache.get()
        await waiting
        await cache.set(undefined, 'committed')
        assert.equal(await pending, 'committed')
      }),
      ['miss:wait', 'poll:resolved'],
    )
    // Lock disappears without data -> waiter steals it.
    assert.deepEqual(
      await pollOutcomes(holdLock('stolen', 5000), async (swr, waiting) => {
        const pending = swr.cache({ key: 'stolen', fetch: async () => 'v' }).get()
        await waiting
        await redis.unlink(toLockKey('stolen'))
        assert.equal(await pending, 'v')
      }),
      ['miss:wait', 'poll:lock_stolen', 'refresh:lock_stolen'],
    )
    // Lock held past waitTimeout under protect-source -> timeout.
    assert.deepEqual(
      await pollOutcomes(
        holdLock('timeout', 5000),
        (swr) =>
          assert.rejects(
            swr.cache({ key: 'timeout', fetch: async () => 'v' }).get(),
            SwrTimeoutError,
          ),
        { failureStrategy: 'protect-source' },
      ),
      ['miss:wait', 'poll:timeout', 'error:timeout'],
    )
    // Caller abort while waiting -> cancelled, with no error events.
    assert.deepEqual(
      await pollOutcomes(holdLock('cancelled', 5000), async (swr, waiting) => {
        const controller = new AbortController()
        const pending = swr
          .cache({ key: 'cancelled', fetch: async () => 'v' })
          .get(undefined, { signal: controller.signal })
        await waiting
        controller.abort(new Error('stop'))
        await assert.rejects(pending, { message: 'stop' })
      }),
      ['miss:wait', 'poll:cancelled'],
    )
    // Stale waiter sees the lock released without a fresh value -> stale_fallback.
    const now = Date.now()
    assert.deepEqual(
      await pollOutcomes(
        async () => {
          await holdLock('stale', 5000)()
          await redis.set(
            toDataKey('stale'),
            encodeEnvelope({
              value: 'old',
              cachedAtMs: now - 2000,
              freshUntilMs: now - 1000,
              expiresAtMs: now + 60_000,
            }),
          )
        },
        async (swr, waiting) => {
          const pending = swr.cache({ key: 'stale', fetch: async () => 'v' }).get()
          await waiting
          await redis.unlink(toLockKey('stale'))
          assert.equal(await pending, 'old')
        },
        { mode: 'wait-on-stale' },
      ),
      ['poll:stale_fallback'],
    )
    // A trusted-envelope decode failure during polling -> poll error, propagated without an error event.
    const failingCodec: SwrCodec<string> = {
      encode: (value) => Buffer.from(value),
      decode: () => {
        throw new Error('cannot decode')
      },
    }
    assert.deepEqual(
      await pollOutcomes(holdLock('corrupt', 5000), async (swr, waiting) => {
        const pending = swr
          .cache({ key: 'corrupt', codec: failingCodec, fetch: async () => 'v' })
          .get()
        await waiting
        await redis.set(
          toDataKey('corrupt'),
          encodeEnvelope(
            { value: 'x', cachedAtMs: now, freshUntilMs: now + 60_000, expiresAtMs: now + 60_000 },
            failingCodec,
          ),
        )
        await assert.rejects(pending, { message: 'cannot decode' })
      }),
      ['miss:wait', 'poll:error'],
    )
  },
)

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
          : [`${e.reason}:${e.severity}`],
    )
  }
  const failing = () => {
    throw new Error('boom')
  }

  assert.deepEqual(
    await errors((redis, onEvent) =>
      assert.rejects(
        createSwrClient({ redis, onEvent })
          .cache({ key: 'k', fetch: async () => failing() })
          .get(),
      ),
    ),
    ['fetch_cold_failure:error'],
  )

  // Release failure is a warning on top of the fetch failure.
  assert.deepEqual(
    await errors((redis, onEvent) => {
      registerLockCommands(redis).swrReleaseLock = async () => failing()
      return assert.rejects(
        createSwrClient({ redis, onEvent })
          .cache({ key: 'k', fetch: async () => failing() })
          .get(),
      )
    }),
    ['lock_release_failure:warn', 'fetch_cold_failure:error'],
  )

  // In 'swr' mode stale refresh failures only differ by event: transient vs fatal (built-in or isFatal).
  class CustomFatal extends Error {}
  for (const [failure, expected] of [
    [new Error('transient'), 'fetch_stale_fallback:warn'],
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

  // A cooperative fetch aborted by the caller is not a fetch failure.
  assert.deepEqual(
    await errors(async (redis, onEvent) => {
      const controller = new AbortController()
      const cache = createSwrClient({ redis, onEvent }).cache<string>({
        key: 'k',
        fetch: async (_ctx, { signal }) => {
          controller.abort(new Error('cancelled'))
          throw signal!.reason
        },
      })
      await assert.rejects(cache.get(undefined, { signal: controller.signal }), {
        message: 'cancelled',
      })
    }),
    [],
  )

  // A stale hit is still reported when the revalidation lock cannot be acquired.
  {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const { events, onEvent } = recorder()
    const cache = createSwrClient({ redis, onEvent }).cache({
      key: 'k',
      ttl: 1,
      fetch: async () => 'v',
    })
    await cache.set(undefined, 'stale')
    redis.advanceTime(1500)
    failAcquire(redis)
    assert.equal(await cache.get(), 'stale')
    assert.deepEqual(
      events.map((e) =>
        e.type === 'hit'
          ? `hit:${e.stale}:${e.startedRevalidation}`
          : e.type === 'error'
            ? `error:${e.reason}`
            : e.type,
      ),
      ['hit:true:false', 'error:redis_error_fallback'],
    )
  }

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
    assert.deepEqual(
      await errors((redis, onEvent) => {
        redis.getBuffer = async () => failing()
        const get = createSwrClient({ redis, onEvent, failureStrategy })
          .cache({ key: 'k', fetch: async () => 'v' })
          .get()
        return failureStrategy === 'availability' ? get : assert.rejects(get, SwrRedisError)
      }),
      [failureStrategy === 'availability' ? 'redis_error_fallback:warn:k' : 'redis_error:error:k'],
    )

    // set() and invalidate() report the cache key.
    for (const operation of ['set', 'invalidate'] as const) {
      assert.deepEqual(
        await errors((redis, onEvent) => {
          registerLockCommands(redis).swrSetAndClearLock = async () => failing()
          redis.unlink = async () => failing()
          const cache = createSwrClient({ redis, onEvent, failureStrategy }).cache({
            key: 'k',
            fetch: async () => 'v',
          })
          const run = operation === 'set' ? cache.set(undefined, 'v') : cache.invalidate()
          return failureStrategy === 'availability' ? run : assert.rejects(run, SwrRedisError)
        }),
        [
          failureStrategy === 'availability'
            ? 'redis_error_fallback:warn:k'
            : 'redis_error:error:k',
        ],
      )
    }

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

    // invalidateKeys reports a single client-level event covering every key.
    const failure = new Error('unlink failed')
    const redis = createFakeRedis()
    useRedisClock(t, redis)
    redis.unlink = async () => {
      throw failure
    }
    const { events, onEvent } = recorder()
    const swr = createSwrClient({ redis, failureStrategy, onEvent })
    if (failureStrategy === 'protect-source') {
      await assert.rejects(
        swr.invalidateKeys('a', 'b', 'c'),
        (err: unknown) =>
          err instanceof SwrRedisError &&
          err.key === 'a' &&
          err.keys.join() === 'a,b,c' &&
          err.cause === failure &&
          err.message.includes('3 keys'),
      )
    } else {
      await swr.invalidateKeys('a', 'b', 'c')
    }
    assert.deepEqual(events, [
      {
        type: 'error',
        reason: failureStrategy === 'protect-source' ? 'redis_error' : 'redis_error_fallback',
        severity: failureStrategy === 'protect-source' ? 'error' : 'warn',
        key: 'a',
        keys: ['a', 'b', 'c'],
        err: failure,
      },
    ])
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
  const cache = createSwrClient({ redis, onEvent, lockTtl: 0.02, maxLockTtl: 1 }).cache({
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
