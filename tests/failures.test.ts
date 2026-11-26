import test from 'node:test'
import assert from 'node:assert/strict'
import { setImmediate } from 'node:timers/promises'
import { createSwrClient, SwrRedisError, SwrTimeoutError, type SwrEvent } from '../src/index.ts'
import { COOLDOWN_MARKER, registerLockCommands } from '../src/lock.ts'
import { failAcquire, interceptAcquire, type Acquire } from './helpers/acquire.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { gateCommand } from './helpers/command-gate.ts'
import { encodeEnvelope } from './helpers/envelope.ts'
import { tick, useRedisClock } from './helpers/clock.ts'
import { memoryHarness } from './helpers/memory-harness.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

for (const swapFails of [false, true]) {
  test(
    `Cold fetch failure rethrows the upstream error and ${swapFails ? 'reports a failed cooldown swap' : 'starts a cooldown'}`,
    { timeout: 5000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const failure = new Error('Database down')
      const swapFailure = new Error('Redis connection lost during the cooldown swap')
      const events: SwrEvent[] = []
      if (swapFails) {
        registerLockCommands(redis).swrCooldownLock = async () => {
          throw swapFailure
        }
      }
      let fetches = 0
      const cache = createSwrClient({ redis, onEvent: (event) => events.push(event) }).cache({
        key: 'cold-fail',
        fetch: async () => {
          fetches++
          throw failure
        },
      })
      await assert.rejects(cache.get(), (error) => error === failure)
      assert.equal(fetches, 1)
      if (!swapFails) assert.equal(await redis.get(toLockKey('cold-fail')), COOLDOWN_MARKER)
      assert.deepEqual(
        events.filter((event) => event.type === 'error'),
        [
          ...(swapFails
            ? [
                {
                  type: 'error',
                  key: 'cold-fail',
                  reason: 'lock_release_failure',
                  severity: 'warn',
                  err: swapFailure,
                },
              ]
            : []),
          {
            type: 'error',
            key: 'cold-fail',
            reason: 'fetch_cold_failure',
            severity: 'error',
            err: failure,
          },
        ],
      )
    },
  )
}

test(
  'Stale fallback returns an unexpired snapshot even when the cooldown swap fails',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const cmds = registerLockCommands(redis)
    let calls = 0
    const cache = createSwrClient({ redis, runtime: 'serverless' }).cache({
      key: 'stale-release',
      ttl: 5,
      fetch: async () => {
        if (++calls === 1) return 'initial'
        redis.advanceTime(1_000)
        throw new Error('Upstream service unavailable')
      },
    })
    assert.equal(await cache.get(), 'initial')
    redis.advanceTime(7_000)
    cmds.swrCooldownLock = async () => {
      throw new Error('Redis connection lost during the cooldown swap')
    }
    assert.equal(await cache.get(), 'initial')
    assert.equal(calls, 2)
  },
)

test(
  'failureStrategy: set, invalidate, and invalidateKeys swallow Redis errors under availability and throw under protect-source',
  { timeout: 5000 },
  async () => {
    for (const failureStrategy of ['availability', 'protect-source'] as const) {
      for (const operation of ['set', 'invalidate', 'invalidateKeys'] as const) {
        const redis = createFakeRedis()
        redis.eval = async () => {
          throw new Error('eval down')
        }
        failAcquire(redis)
        redis.unlink = async () => {
          throw new Error('unlink down')
        }
        const client = createSwrClient({ redis, failureStrategy })
        const cache = client.cache({ key: 'k', fetch: async () => 'v' })
        const run = () =>
          operation === 'set'
            ? cache.set(undefined, 'v')
            : operation === 'invalidate'
              ? cache.invalidate()
              : client.invalidateKeys(['k'])
        if (failureStrategy === 'protect-source')
          await assert.rejects(run, SwrRedisError, `${failureStrategy} ${operation}`)
        else await run()
        // An empty array sends no command, so it resolves even with Redis down.
        if (operation === 'invalidateKeys') await client.invalidateKeys([])
      }
    }
    // Client-wide operations have no logical key.
    const keyless = new SwrRedisError([], new Error('unlink down'))
    assert.equal(keyless.message, 'SWR Redis error: unlink down')
    assert.equal(keyless.key, undefined)
    assert.deepEqual(keyless.keys, [])
  },
)

test('Inline refresh propagates the fetch error when the snapshot expires during the fetch', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const failure = new Error('upstream unavailable')
  const events: SwrEvent[] = []
  let fetches = 0
  const cache = createSwrClient({
    redis,
    runtime: 'serverless',
    onEvent: (event) => events.push(event),
  }).cache<string>({
    key: 'inline-expiry',
    ttl: 1,
    stale: ({ ttl }) => ttl,
    fetch: async () => {
      fetches++
      redis.advanceTime(1000)
      throw failure
    },
  })
  await cache.set(undefined, 'stale')
  redis.advanceTime(1000)
  await assert.rejects(cache.get(), (err) => err === failure)
  assert.equal(fetches, 1)
  // The refresh judges its own outcome when it settles, by which time no stale value is usable.
  assert.deepEqual(
    events.flatMap((e) => (e.type === 'error' ? [`${e.reason}:${e.severity}`] : [])),
    ['fetch_cold_failure:error'],
  )
})

for (const stage of ['acquire', 'poll'] as const) {
  test(`Expired snapshot cannot mask a Redis ${stage} failure`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const cache = createSwrClient({
      redis,
      staleWait: 10,
      failureStrategy: 'protect-source',
      pollInterval: 0.001,
    }).cache({ key: 'redis-expiry', ttl: 1, stale: ({ ttl }) => ttl, fetch: async () => 'fresh' })
    await cache.set(undefined, 'stale')
    redis.advanceTime(1000)
    const failAtExpiry = async () => {
      redis.advanceTime(1000)
      throw new Error('Redis unavailable')
    }
    if (stage === 'acquire') interceptAcquire(redis, failAtExpiry)
    else {
      await redis.set(toLockKey('redis-expiry'), 'owner', 'PX', 5000)
      redis.mgetBuffer = failAtExpiry
    }
    await assert.rejects(cache.get(), SwrRedisError)
  })
}

for (const failure of ['read', 'acquire', 'poll'] as const) {
  for (const failureStrategy of ['availability', 'protect-source'] as const) {
    test(
      `failureStrategy ${failureStrategy}: cold ${failure} error`,
      { timeout: 5000 },
      async () => {
        const redis = createFakeRedis()
        const err = new Error('Command timed out')
        const fail = async () => {
          throw err
        }
        if (failure === 'read') redis.getBuffer = fail
        else if (failure === 'acquire') failAcquire(redis, err)
        else {
          await redis.set(toLockKey('redis-failure'), 'other-owner', 'PX', 5000)
          redis.mgetBuffer = fail
        }
        let fetches = 0
        const events: SwrEvent[] = []
        const cache = createSwrClient({
          redis,
          failureStrategy,
          pollInterval: 0.001,
          onEvent: (event) => events.push(event),
        }).cache({
          key: 'redis-failure',
          ttl: () => assert.fail('direct fallback bypasses TTL resolution'),
          fetch: async (_ctx, { cacheWrite }) => {
            assert.equal(cacheWrite, 'skip')
            fetches++
            return 'direct'
          },
        })
        if (failureStrategy === 'availability') assert.equal(await cache.get(), 'direct')
        else
          await assert.rejects(
            cache.get(),
            (err) => err instanceof SwrRedisError && err.key === 'redis-failure',
          )
        assert.equal(fetches, failureStrategy === 'availability' ? 1 : 0)
        assert.deepEqual(
          events.filter((event) => event.type === 'error'),
          [
            {
              type: 'error',
              key: 'redis-failure',
              keys: ['redis-failure'],
              err,
              reason: failureStrategy === 'availability' ? 'redis_error_fallback' : 'redis_error',
              severity: failureStrategy === 'availability' ? 'warn' : 'error',
            },
          ],
        )
      },
    )
  }
}

for (const failureStrategy of ['availability', 'protect-source'] as const) {
  for (const operation of ['stale acquire', 'stale poll snapshot'] as const) {
    test(
      `failureStrategy ${failureStrategy}: ${operation} error serves the stale snapshot without fetching`,
      { timeout: 2000 },
      async (t) => {
        const redis = createFakeRedis(1000)
        useRedisClock(t, redis)
        const events: SwrEvent[] = []
        let fetches = 0
        let acquireCalls = 0
        const polling = operation === 'stale poll snapshot'
        const snapshot = polling ? undefined : 'stale'
        const cache = createSwrClient({
          redis,
          failureStrategy,
          staleWait: polling ? 10 : 0,
          pollInterval: 0.001,
          onEvent: (e) => events.push(e),
        }).cache<string | undefined>({
          key: 'stale-failure',
          ttl: 1,
          fetch: async () => `fetched-${++fetches}`,
        })
        await cache.set(undefined, snapshot)
        redis.advanceTime(1500)
        const failure = new Error('Redis unavailable')
        if (!polling) {
          interceptAcquire(redis, async (acquire, ...args) => {
            assert.equal(await acquire(...args), 'acquired')
            throw failure
          })
        } else {
          await redis.set(toLockKey('stale-failure'), 'owner', 'PX', 5000)
          interceptAcquire(redis, (acquire, ...args) => {
            acquireCalls++
            return acquire(...args)
          })
          redis.mgetBuffer = async () => {
            throw failure
          }
        }
        assert.equal(await cache.get(), snapshot)
        assert.equal(fetches, 0)
        assert.equal(await redis.get(toLockKey('stale-failure')), polling ? 'owner' : null)
        if (polling) assert.equal(acquireCalls, 1)
        assert.deepEqual(
          events.map((e) =>
            e.type === 'hit'
              ? `hit:${e.stale}:${e.startedRevalidation}`
              : e.type === 'wait'
                ? `wait:${e.outcome}`
                : e.type,
          ),
          polling ? ['hit:true:false', 'wait:error', 'error'] : ['hit:true:false', 'error'],
        )
        assert.deepEqual(events.at(-1), {
          type: 'error',
          reason: 'stale_fallback',
          severity: 'warn',
          key: 'stale-failure',
          stage: 'redis',
          err: failure,
        })
      },
    )
  }
}

for (const failureStrategy of ['availability', 'protect-source'] as const) {
  test(
    `Commit failure ${failureStrategy}: returns primary, releases lock, and next caller acquires`,
    { timeout: 5000 },
    async () => {
      const redis = createFakeRedis(1000)
      const cmds = registerLockCommands(redis)
      let fetchCount = 0
      const cache = createSwrClient({ redis, lockTtl: 10, failureStrategy }).cache({
        key: 'commit-err-release',
        fetch: async () => `val-${++fetchCount}`,
      })
      const origCommit = cmds.swrCommitAndRelease.bind(redis)
      cmds.swrCommitAndRelease = async () => {
        cmds.swrCommitAndRelease = origCommit
        throw new Error('Redis commit failed')
      }

      assert.equal(await cache.get(), 'val-1')
      assert.equal(await redis.get(toLockKey('commit-err-release')), null)
      // The next caller acquires immediately instead of waiting out the 10s lock.
      assert.equal(await cache.get(), 'val-2')
      assert.equal(fetchCount, 2)
    },
  )
}

test('Commit and release failure: still returns the fetched value', { timeout: 5000 }, async () => {
  const redis = createFakeRedis(1000)
  const cmds = registerLockCommands(redis)
  const cache = createSwrClient({ redis }).cache({
    key: 'commit-and-release-fail',
    fetch: async () => 'fetched-ok',
  })
  cmds.swrCommitAndRelease = async () => {
    throw new Error('Command timed out during commit')
  }
  cmds.swrReleaseLock = async () => {
    throw new Error('Command timed out during release')
  }
  assert.equal(await cache.get(), 'fetched-ok')
})

test(
  'Commit failure: token-checked release preserves the replacement owner lock',
  { timeout: 5000 },
  async () => {
    const redis = createFakeRedis(1000)
    const cmds = registerLockCommands(redis)
    const cache = createSwrClient({ redis, lockTtl: 5 }).cache({
      key: 'replacement-owner',
      fetch: async () => 'primary-ok',
    })
    const lockKey = toLockKey('replacement-owner')
    cmds.swrCommitAndRelease = async () => {
      await redis.set(lockKey, 'new-owner-token', 'PX', 5000)
      throw new Error('commit timed out')
    }
    assert.equal(await cache.get(), 'primary-ok')
    assert.equal(await redis.get(lockKey), 'new-owner-token')
  },
)

test('A throwing isFatal in a background refresh reports fatal without rejecting the tracked refresh', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const failure = new Error('classification failed')
  const events: SwrEvent[] = []
  let tracked: Promise<unknown> | undefined
  const swr = createSwrClient({
    redis,
    ttl: 2,
    onEvent: (event) => events.push(event),
    isFatal() {
      throw failure
    },
    waitUntil(promise) {
      tracked = promise
    },
  })
  const cache = swr.cache<string>({
    key: 'bg-reject-inflight',
    fetch: async () => {
      throw new Error('source failed')
    },
  })
  await cache.set(undefined, 'original')
  redis.advanceTime(3000)
  assert.equal(await cache.get(), 'original')
  assert.ok(tracked)
  await tracked
  await swr.drain()
  assert.equal(await redis.get(toLockKey('bg-reject-inflight')), COOLDOWN_MARKER)
  assert.deepEqual(
    events.filter((e) => e.type === 'error'),
    [
      {
        type: 'error',
        reason: 'fatal',
        severity: 'error',
        key: 'bg-reject-inflight',
        err: failure,
      },
    ],
  )
})

for (const memory of ['cold', 'warm'] as const) {
  test(
    `AbortSignal: a pre-aborted get rejects with the original reason before Redis I/O, with ${memory} memory`,
    { timeout: 5000 },
    async () => {
      const redis = createFakeRedis()
      const cache = createSwrClient({ redis, memory: true }).cache({
        key: 'pre-abort',
        fetch: async () => 'cached',
      })
      if (memory === 'warm') assert.equal(await cache.get(), 'cached')
      let reads = 0
      let scripts = 0
      redis.getBuffer = async () => {
        reads++
        return null
      }
      redis.eval = async () => {
        scripts++
        return null
      }
      const reason = new Error('already aborted')
      await assert.rejects(
        cache.get(undefined, { signal: AbortSignal.abort(reason) }),
        (error) => error === reason,
      )
      assert.equal(reads, 0)
      assert.equal(scripts, 0)
    },
  )
}

// The refresh reports its own outcome once, whether or not its caller is still attached.
const DETACHED_EVENTS = {
  cold: { resolves: ['miss', 'refresh'], rejects: ['miss', 'fetch_cold_failure:error'] },
  stale: { resolves: ['hit', 'refresh'], rejects: ['hit', 'stale_fallback:warn'] },
}

for (const phase of ['cold', 'stale'] as const) {
  for (const outcome of ['resolves', 'rejects'] as const) {
    test(
      `AbortSignal: a caller leaving a ${phase} inline refresh rejects without aborting it, and the refresh ${outcome === 'resolves' ? 'still commits' : 'still cools down'}`,
      { timeout: 2000 },
      async (t) => {
        const redis = createFakeRedis(1000)
        useRedisClock(t, redis)
        const entered = Promise.withResolvers<AbortSignal>()
        const release = Promise.withResolvers<void>()
        const failure = new Error('source failed')
        let fetches = 0
        const events: SwrEvent[] = []
        const swr = createSwrClient({
          redis,
          runtime: 'serverless',
          onEvent: (event) => events.push(event),
        })
        const cache = swr.cache({
          key: 'inline-abort',
          ttl: 1,
          fetch: async (_ctx, { signal }) => {
            if (++fetches === 1 && phase === 'stale') return 'stale'
            entered.resolve(signal)
            await release.promise
            if (outcome === 'rejects') throw failure
            return 'fresh'
          },
        })
        if (phase === 'stale') {
          await cache.get()
          redis.advanceTime(1500)
          events.length = 0
        }
        const controller = new AbortController()
        const reason = new Error('caller cancelled')
        const pending = cache.get(undefined, { signal: controller.signal })
        const fetchSignal = await entered.promise
        controller.abort(reason)
        await assert.rejects(pending, (error) => error === reason)
        assert.equal(fetchSignal.aborted, false)
        const lock = await redis.get(toLockKey('inline-abort'))
        assert.ok(lock && lock !== COOLDOWN_MARKER, 'the refresh still holds the lock')

        release.resolve()
        await swr.drain()
        assert.deepEqual(
          events.map((e) => (e.type === 'error' ? `${e.reason}:${e.severity}` : e.type)),
          DETACHED_EVENTS[phase][outcome],
        )
        assert.equal(
          await redis.get(toLockKey('inline-abort')),
          outcome === 'resolves' ? null : COOLDOWN_MARKER,
        )
        if (outcome === 'resolves') assert.equal(await cache.get(), 'fresh')
        assert.equal(fetches, phase === 'stale' ? 2 : 1)
      },
    )
  }
}

for (const phase of ['cold', 'stale'] as const) {
  test(
    `AbortSignal: cancels ${phase} polling without fetching or deleting another owner lock`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const waiting = Promise.withResolvers<void>()
      const events: SwrEvent[] = []
      const cache = createSwrClient({
        redis,
        staleWait: 10,
        pollInterval: 0.001,
        onEvent: (event) => {
          events.push(event)
          if (
            (event.type === 'miss' && event.action === 'wait') ||
            (event.type === 'hit' && event.stale && !event.startedRevalidation)
          ) {
            waiting.resolve()
          }
        },
      }).cache<string>({
        key: 'poll-abort',
        ttl: 1,
        fetch: async () => assert.fail('aborted waiter must not fetch'),
      })
      if (phase === 'stale') {
        await cache.set(undefined, 'stale')
        redis.advanceTime(1500)
      }
      await redis.set(toLockKey('poll-abort'), 'other-owner', 'PX', 5000)
      const controller = new AbortController()
      const pending = cache.get(undefined, { signal: controller.signal })
      await waiting.promise
      const reason = new Error('stop waiting')
      controller.abort(reason)
      await assert.rejects(pending, (error) => error === reason)
      assert.equal(await redis.get(toLockKey('poll-abort')), 'other-owner')
      assert.deepEqual(
        events.map((event) => (event.type === 'wait' ? event.outcome : event.type)),
        [phase === 'cold' ? 'miss' : 'hit', 'cancelled'],
      )
    },
  )
}

// A successful MGET and a failed one hit separate abort checks; each case would otherwise
// steal the lock, serve a hit, or serve stale data.
for (const [phase, mgetOutcome] of [
  ['cold', 'unlocked'],
  ['stale', 'hit'],
  ['stale', 'redis_error'],
] as const) {
  test(
    `AbortSignal: abort during an in-flight ${phase} poll MGET (${mgetOutcome}) awaits the reply, then rejects without acting on it`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const key = `abort-mget:${phase}:${mgetOutcome}`
      const events: SwrEvent[] = []
      const cache = createSwrClient({
        redis,
        staleWait: 10,
        pollInterval: 0.001,
        onEvent: (event) => events.push(event),
      }).cache<string>({
        key,
        ttl: 5,
        fetch: async () => assert.fail('aborted waiter must not fetch'),
      })

      if (phase === 'stale') {
        await redis.set(
          toDataKey(key),
          encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 10_000 }),
        )
      }
      await redis.set(toLockKey(key), 'other-owner', 'PX', 5000)
      let acquireCalls = 0
      interceptAcquire(redis, (acquire, ...args) => {
        acquireCalls++
        return acquire(...args)
      })

      const fresh = encodeEnvelope({
        value: 'fresh',
        cachedAtMs: 1000,
        freshUntilMs: 6000,
        expiresAtMs: 20_000,
      })
      const gatedMget = gateCommand(async () => {
        if (mgetOutcome === 'redis_error') throw new Error('mget failed')
        return mgetOutcome === 'hit' ? [fresh, null] : [null, null]
      }, 'before execution')
      redis.mgetBuffer = gatedMget.command as typeof redis.mgetBuffer

      const controller = new AbortController()
      const reason = new Error(`cancelled-${phase}-${mgetOutcome}`)
      const pending = cache.get(undefined, { signal: controller.signal })
      await gatedMget.entered
      controller.abort(reason)
      let settled = false
      void pending.catch(() => (settled = true))
      await setImmediate()
      assert.equal(settled, false, 'the in-flight reply is awaited')
      gatedMget.release()

      await assert.rejects(pending, (err) => err === reason)
      assert.equal(acquireCalls, 1, 'only the initial acquire ran')
      assert.deepEqual(
        events.map((event) => (event.type === 'wait' ? `wait:${event.outcome}` : event.type)),
        [phase === 'cold' ? 'miss' : 'hit', 'wait:cancelled'],
      )
    },
  )
}

for (const acquireOutcome of ['acquired', 'contended', 'lost_reply'] as const) {
  test(
    `AbortSignal: abort during in-flight cold lock reacquisition (${acquireOutcome}) cancels cleanly`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const key = `abort-steal:${acquireOutcome}`
      const cache = createSwrClient({
        redis,
        failureStrategy: 'availability',
        pollInterval: 0.001,
      }).cache<string>({
        key,
        ttl: 5,
        fetch: async () => assert.fail('aborted waiter must not fetch'),
      })

      await redis.set(toLockKey(key), 'expired-owner', 'PX', 5000)
      const origMget = redis.mgetBuffer.bind(redis)
      redis.mgetBuffer = (async (...keys: string[]) => {
        await redis.unlink(toLockKey(key))
        return origMget(...keys)
      }) as typeof redis.mgetBuffer

      let acquireCalls = 0
      interceptAcquire(redis, (acquire, lockKey, token, ttlMs) => {
        if (++acquireCalls === 1) return acquire(lockKey, token, ttlMs)
        return gatedTakeover.command(acquire, lockKey, token, ttlMs)
      })
      const gatedTakeover = gateCommand(
        async (acquire: Acquire, lockKey: string, token: Buffer, ttlMs: number) => {
          if (acquireOutcome === 'contended') {
            await redis.set(lockKey, 'other-owner', 'PX', 5000)
            return 'busy'
          }
          const acquired = await acquire(lockKey, token, ttlMs)
          if (acquireOutcome === 'lost_reply') throw new Error('reply lost after acquisition')
          return acquired
        },
        'before execution',
      )

      const controller = new AbortController()
      const reason = new Error(`cancelled-steal-${acquireOutcome}`)
      const pending = cache.get(undefined, { signal: controller.signal })
      await gatedTakeover.entered

      controller.abort(reason)
      gatedTakeover.release()

      await assert.rejects(pending, (err) => err === reason)
      assert.equal(acquireCalls, 2)
      assert.equal(
        await redis.get(toLockKey(key)),
        acquireOutcome === 'contended' ? 'other-owner' : null,
      )
    },
  )
}

for (const readOutcome of ['fresh', 'redis_error'] as const) {
  test(
    `AbortSignal: abort during the initial getBuffer (${readOutcome}) awaits the reply, then rejects without locking, fetching, or events`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const key = `abort-read:${readOutcome}`
      const events: SwrEvent[] = []
      const cache = createSwrClient({ redis, onEvent: (e) => events.push(e) }).cache<string>({
        key,
        ttl: 5,
        fetch: async () => assert.fail('aborted initial read must not fetch'),
      })
      await redis.set(
        toDataKey(key),
        encodeEnvelope({
          value: 'fresh',
          cachedAtMs: 1000,
          freshUntilMs: 6000,
          expiresAtMs: 20_000,
        }),
      )
      let acquireCalls = 0
      interceptAcquire(redis, (acquire, ...args) => {
        acquireCalls++
        return acquire(...args)
      })
      const getBuffer = redis.getBuffer.bind(redis)
      const gatedRead = gateCommand(async (k: string) => {
        if (readOutcome === 'redis_error') throw new Error('getBuffer failed')
        return getBuffer(k)
      }, 'before execution')
      redis.getBuffer = gatedRead.command as typeof redis.getBuffer

      const controller = new AbortController()
      const reason = new Error(`cancelled-read-${readOutcome}`)
      const pending = cache.get(undefined, { signal: controller.signal })
      await gatedRead.entered
      controller.abort(reason)
      let settled = false
      void pending.catch(() => (settled = true))
      await setImmediate()
      assert.equal(settled, false, 'the in-flight reply is awaited')
      gatedRead.release()

      await assert.rejects(pending, (err) => err === reason)
      assert.equal(acquireCalls, 0)
      assert.deepEqual(events, [])
    },
  )
}

// Both outcomes leave our token in Redis, so a leaked lock shows up as a non-null lock key.
for (const [phase, acquireOutcome] of [
  ['cold', 'acquired'],
  ['stale', 'lost_reply'],
] as const) {
  test(
    `AbortSignal: abort during initial ${phase} lock.acquire (${acquireOutcome}) releases the lock and starts nothing`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const key = `abort-init-lock:${phase}`
      const events: SwrEvent[] = []
      let waitUntilCalls = 0
      let fetches = 0
      const cache = createSwrClient({
        redis,
        onEvent: (e) => events.push(e),
        waitUntil: () => {
          waitUntilCalls++
        },
      }).cache<string>({
        key,
        ttl: 5,
        fetch: async () => `fetched-${++fetches}`,
      })

      if (phase === 'stale') {
        await redis.set(
          toDataKey(key),
          encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 20_000 }),
        )
      }
      redis.mgetBuffer = async () => assert.fail('aborted initial lock must not poll')

      const gatedAcquire = gateCommand(
        async (acquire: Acquire, lockKey: string, token: Buffer, ttlMs: number) => {
          const acquired = await acquire(lockKey, token, ttlMs)
          if (acquireOutcome === 'lost_reply') throw new Error('reply lost after acquire')
          return acquired
        },
        'before execution',
      )
      interceptAcquire(redis, (acquire, lockKey, token, ttlMs) =>
        gatedAcquire.command(acquire, lockKey, token, ttlMs),
      )

      const controller = new AbortController()
      const reason = new Error(`cancelled-${phase}-${acquireOutcome}`)
      const pending = cache.get(undefined, { signal: controller.signal })
      await gatedAcquire.entered

      controller.abort(reason)
      let settled = false
      void pending.catch(() => (settled = true))
      await setImmediate()
      assert.equal(settled, false, 'the in-flight reply is awaited')
      gatedAcquire.release()

      await assert.rejects(pending, (err) => err === reason)
      assert.equal(waitUntilCalls, 0)
      assert.deepEqual(events, [])
      assert.equal(fetches, 0)
      assert.equal(await redis.get(toLockKey(key)), null)
    },
  )
}

test(
  'AbortSignal: release failure on aborted initial lock.acquire preserves abort reason and emits only lock_release_failure',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const releaseErr = new Error('release failed')
    registerLockCommands(redis).swrReleaseLock = async () => {
      throw releaseErr
    }
    const events: SwrEvent[] = []
    const cache = createSwrClient({
      redis,
      failureStrategy: 'protect-source',
      onEvent: (e) => events.push(e),
    }).cache<string>({
      key: 'abort-release-fail',
      ttl: 5,
      fetch: async () => assert.fail('aborted initial lock must not fetch'),
    })

    const gatedAcquire = gateCommand(
      async (acquire: Acquire, lockKey: string, token: Buffer, ttlMs: number) =>
        acquire(lockKey, token, ttlMs),
      'before execution',
    )
    interceptAcquire(redis, (acquire, lockKey, token, ttlMs) =>
      gatedAcquire.command(acquire, lockKey, token, ttlMs),
    )

    const controller = new AbortController()
    const reason = new Error('abort-reason')
    const pending = cache.get(undefined, { signal: controller.signal })
    await gatedAcquire.entered

    controller.abort(reason)
    gatedAcquire.release()

    await assert.rejects(pending, (err) => err === reason)
    assert.equal(events.length, 1)
    const [event] = events
    assert.ok(event?.type === 'error')
    assert.equal(event.reason, 'lock_release_failure')
    assert.equal(event.err, releaseErr)
  },
)

for (const failureStrategy of ['availability', 'protect-source'] as const) {
  test(
    `A lost acquire reply under ${failureStrategy} settles the caller at once while drain and waitUntil wait for the release`,
    { timeout: 2000 },
    async () => {
      const redis = createFakeRedis()
      const commands = registerLockCommands(redis)
      const release = gateCommand(commands.swrReleaseLock.bind(redis), 'before execution')
      commands.swrReleaseLock = release.command
      interceptAcquire(redis, async (acquire, ...args) => {
        await acquire(...args)
        throw new Error('reply lost after lock grant')
      })
      const tracked: Promise<unknown>[] = []
      const swr = createSwrClient({
        redis,
        failureStrategy,
        waitUntil: (promise) => tracked.push(promise),
      })
      const cache = swr.cache({
        key: 'lost-grant',
        fetch: async (_context, { cacheWrite }) => cacheWrite,
      })

      const pending = cache.get()
      await release.entered
      if (failureStrategy === 'availability') assert.equal(await pending, 'skip')
      else await assert.rejects(pending, SwrRedisError)
      assert.ok(await redis.get(toLockKey('lost-grant')))
      assert.equal(tracked.length, 1)

      let drained = false
      const draining = swr.drain().then(() => (drained = true))
      await setImmediate()
      assert.equal(drained, false, 'drain waits for the pending release')
      release.release()
      await draining
      await tracked[0]
      assert.equal(await redis.get(toLockKey('lost-grant')), null)
    },
  )
}

test(
  'AbortSignal: an onEvent abort after lock acquisition skips the fetch and releases the lock',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const controller = new AbortController()
    const reason = new Error('cancelled before fetch')
    const cache = createSwrClient({
      redis,
      onEvent: (e) => e.type === 'miss' && e.action === 'fetch' && controller.abort(reason),
    }).cache({ key: 'pre-fetch-abort', fetch: async () => assert.fail('fetch must not run') })
    await assert.rejects(
      cache.get(undefined, { signal: controller.signal }),
      (err) => err === reason,
    )
    assert.equal(await redis.get(toLockKey('pre-fetch-abort')), null)
  },
)

test(
  'AbortSignal: an onEvent abort before the direct fallback fetch skips the fetch',
  { timeout: 2000 },
  async () => {
    const redis = createFakeRedis()
    redis.getBuffer = async () => {
      throw new Error('Redis down')
    }
    const controller = new AbortController()
    const reason = new Error('cancelled before direct fetch')
    const cache = createSwrClient({
      redis,
      failureStrategy: 'availability',
      onEvent: (e) =>
        e.type === 'error' && e.reason === 'redis_error_fallback' && controller.abort(reason),
    }).cache({ key: 'pre-direct-abort', fetch: async () => assert.fail('fetch must not run') })
    await assert.rejects(
      cache.get(undefined, { signal: controller.signal }),
      (err) => err === reason,
    )
  },
)

for (const phase of ['cold', 'stale'] as const) {
  test(`fetchTimeout: a hung ${phase} fetch is aborted, the key cools down, and the late result is discarded`, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] })
    const events: SwrEvent[] = []
    const entered = Promise.withResolvers<AbortSignal>()
    const late = Promise.withResolvers<string>()
    let fetches = 0
    const { redis, memory, cache } = memoryHarness<string>(
      t,
      {
        key: 'hung',
        fetch: async (_ctx, { signal }) => {
          fetches++
          entered.resolve(signal)
          return late.promise
        },
      },
      { runtime: 'serverless', fetchTimeout: 5, onEvent: (event) => events.push(event) },
    )
    const staleTiming = { cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 60_000 }
    if (phase === 'stale') {
      await redis.set(toDataKey('hung'), encodeEnvelope({ value: 'stale', ...staleTiming }))
    }
    const before = await redis.getBuffer(toDataKey('hung'))

    const pending = cache.getWithMeta()
    const settled =
      phase === 'cold'
        ? assert.rejects(
            pending,
            (err) => err instanceof SwrTimeoutError && err.phase === 'fetch' && err.key === 'hung',
          )
        : pending.then((result) =>
            assert.deepEqual(result, { value: 'stale', source: 'redis', timing: staleTiming }),
          )
    const signal = await entered.promise
    // Step through renewals so the lock outlives the default 3s lockTtl.
    for (let elapsed = 0; elapsed < 5000; elapsed += 1000) await tick(t, redis, 1000)
    await settled
    assert.ok(signal.aborted && signal.reason instanceof SwrTimeoutError)

    late.resolve('late')
    await setImmediate()
    assert.equal(fetches, 1)
    assert.deepEqual(await redis.getBuffer(toDataKey('hung')), before)
    assert.equal(await redis.get(toLockKey('hung')), COOLDOWN_MARKER)
    assert.equal(memory.entryCount, 0)
    // Serving stale data downgrades the timeout to a warning.
    assert.deepEqual(
      events.flatMap((e) => (e.type === 'error' ? [`${e.reason}:${e.severity}`] : [])),
      [`timeout:${phase === 'cold' ? 'error' : 'warn'}`],
    )
  })
}

test('failureCooldown: the marker expires failureCooldown after the failure, not after the fetch start', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const failure = new Error('slow failure')
  const cache = createSwrClient({ redis, failureCooldown: 1 }).cache({
    key: 'slow-fail',
    fetch: async () => {
      redis.advanceTime(1500)
      throw failure
    },
  })
  await assert.rejects(cache.get(), (err) => err === failure)
  assert.equal(await redis.get(toLockKey('slow-fail')), COOLDOWN_MARKER)
  assert.equal(await redis.pttl(toLockKey('slow-fail')), 1000)
})

test('failureCooldown 0: a failed fetch releases the lock, so the next caller fetches at once', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const failure = new Error('transient')
  let fetches = 0
  const cache = createSwrClient({ redis, failureCooldown: 0 }).cache({
    key: 'no-cooldown',
    fetch: async () => {
      if (++fetches === 1) throw failure
      return 'recovered'
    },
  })
  await assert.rejects(cache.get(), (err) => err === failure)
  assert.equal(await redis.get(toLockKey('no-cooldown')), null)
  assert.equal(await cache.get(), 'recovered')
  assert.equal(fetches, 2)
})

for (const write of ['set', 'invalidate'] as const) {
  test(`Cooldown: ${write}() clears an active marker`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    let fetches = 0
    const cache = createSwrClient({ redis }).cache({
      key: 'cleared',
      fetch: async () => {
        if (++fetches === 1) throw new Error('broken source')
        return 'fixed'
      },
    })
    await assert.rejects(cache.get())
    assert.equal(await redis.get(toLockKey('cleared')), COOLDOWN_MARKER)
    if (write === 'set') await cache.set(undefined, 'explicit')
    else await cache.invalidate()
    assert.equal(await redis.get(toLockKey('cleared')), null)
    assert.equal(await cache.get(), write === 'set' ? 'explicit' : 'fixed')
    assert.equal(fetches, write === 'set' ? 1 : 2)
  })

  test(`Cooldown swap racing ${write}(): the write clears the lock first, so no marker is written`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const commands = registerLockCommands(redis)
    const swap = gateCommand(commands.swrCooldownLock.bind(redis), 'before execution')
    let swapReply: Promise<unknown> | undefined
    commands.swrCooldownLock = (...args) => (swapReply = swap.command(...args))
    const failure = new Error('broken source')
    const events: SwrEvent[] = []
    let fetches = 0
    const cache = createSwrClient({ redis, onEvent: (event) => events.push(event) }).cache({
      key: 'swap-race',
      fetch: async () => {
        if (++fetches === 1) throw failure
        return 'fixed'
      },
    })

    const pending = cache.get()
    await swap.entered
    if (write === 'set') await cache.set(undefined, 'explicit')
    else await cache.invalidate()
    swap.release()
    await assert.rejects(pending, (err) => err === failure)
    assert.equal(await swapReply, '0')
    assert.equal(await redis.get(toLockKey('swap-race')), null)
    assert.deepEqual(
      events.flatMap((e) => (e.type === 'error' ? [e.reason] : [])),
      ['fetch_cold_failure'],
    )
    assert.equal(await cache.get(), write === 'set' ? 'explicit' : 'fixed')
    assert.equal(fetches, write === 'set' ? 1 : 2)
  })
}

for (const outcome of ['resolves', 'rejects', 'times out'] as const) {
  test(`Direct fallback: concurrent callers share one fetch per key and cache instance, which ${outcome} once for all of them`, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    redis.getBuffer = async () => {
      throw new Error('Redis down')
    }
    const release = Promise.withResolvers<void>()
    const events: SwrEvent[] = []
    const calls: string[] = []
    const swr = createSwrClient({ redis, fetchTimeout: 5, onEvent: (event) => events.push(event) })
    const fetchFor = (instance: string) => async (id: string) => {
      calls.push(`${instance}:${id}`)
      await release.promise
      if (outcome === 'rejects') throw new Error(`source failed for ${id}`)
      return { id }
    }
    const cache = swr.cache({ key: (id: string) => id, fetch: fetchFor('cache') })
    const otherInstance = swr.cache({ key: (id: string) => id, fetch: fetchFor('other') })

    const results = Promise.allSettled([
      cache.get('A'),
      cache.get('A'),
      cache.get('B'),
      otherInstance.get('A'),
    ])
    await setImmediate()
    assert.deepEqual(calls.sort(), ['cache:A', 'cache:B', 'other:A'])
    if (outcome === 'times out') await tick(t, redis, 5000)
    else release.resolve()

    const settled = await results
    assert.deepEqual(
      settled.map((result) => result.status),
      Array(4).fill(outcome === 'resolves' ? 'fulfilled' : 'rejected'),
    )
    const [a1, a2, b, otherA] = settled.map((result) =>
      result.status === 'fulfilled' ? result.value : result.reason,
    )
    if (outcome === 'resolves') {
      assert.deepEqual([a1, b, otherA], [{ id: 'A' }, { id: 'B' }, { id: 'A' }])
    } else if (outcome === 'rejects') {
      assert.deepEqual(
        [a1.message, b.message, otherA.message],
        ['source failed for A', 'source failed for B', 'source failed for A'],
      )
    } else {
      for (const [err, key] of [
        [a1, 'A'],
        [b, 'B'],
        [otherA, 'A'],
      ]) {
        assert.ok(err instanceof SwrTimeoutError && err.phase === 'fetch' && err.key === key)
      }
    }
    assert.equal(a2, a1, 'callers of one key share the same result')
    assert.notEqual(otherA, a1, 'cache instances never share a fetch')
    const failureEvent = {
      resolves: [],
      rejects: ['fetch_cold_failure:error'],
      'times out': ['timeout:error'],
    }
    assert.deepEqual(
      events.flatMap((e) =>
        e.type === 'error' && e.reason !== 'redis_error_fallback'
          ? [`${e.reason}:${e.severity}`]
          : [],
      ),
      Array(3).fill(failureEvent[outcome]).flat(),
      'one failure event per fetch, not per caller',
    )
  })
}

test('Direct fallback: the last caller leaving aborts the fetch, and its late failure is not reported and leaves the replacement shared', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  redis.getBuffer = async () => {
    throw new Error('Redis down')
  }
  const events: SwrEvent[] = []
  const signals: AbortSignal[] = []
  const results: PromiseWithResolvers<string>[] = []
  const cache = createSwrClient({ redis, onEvent: (event) => events.push(event) }).cache({
    key: 'direct-leave',
    // Ignores its signal, so the aborted fetch settles after its replacement starts.
    fetch: async (_ctx, { signal }) => {
      signals.push(signal)
      const result = Promise.withResolvers<string>()
      results.push(result)
      return result.promise
    },
  })

  const a = new AbortController()
  const b = new AbortController()
  const first = cache.get(undefined, { signal: a.signal })
  const second = cache.get(undefined, { signal: b.signal })
  await setImmediate()
  assert.equal(signals.length, 1)
  a.abort(new Error('a left'))
  await assert.rejects(first, { message: 'a left' })
  assert.equal(signals[0]!.aborted, false, 'b is still attached')
  b.abort(new Error('b left'))
  await assert.rejects(second, { message: 'b left' })
  assert.equal(signals[0]!.aborted, true)

  const replacement = cache.getWithMeta()
  await setImmediate()
  assert.equal(signals.length, 2)
  results[0]!.reject(new Error('abandoned fetch failed late'))
  await setImmediate()
  const joined = cache.getWithMeta()
  await setImmediate()
  assert.equal(signals.length, 2, 'a late settle does not remove the replacement')
  results[1]!.resolve('replacement')
  // A caller joining another caller's direct fetch also gets source 'direct'.
  assert.deepEqual(
    await Promise.all([replacement, joined]),
    Array(2).fill({ value: 'replacement', source: 'direct' }),
  )
  assert.deepEqual(
    events.flatMap((e) => (e.type === 'error' && e.reason !== 'redis_error_fallback' ? [e] : [])),
    [],
  )
})
