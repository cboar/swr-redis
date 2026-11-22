import test from 'node:test'
import assert from 'node:assert/strict'
import {
  createSwrClient,
  SwrNoCache,
  SwrRedisError,
  SwrWriteBatch,
  type SwrCodec,
  type SwrEvent,
} from '../src/index.ts'
import { registerLockCommands } from '../src/lock.ts'
import { failAcquire, interceptAcquire, type Acquire } from './helpers/acquire.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { gateCommand } from './helpers/command-gate.ts'
import { encodeEnvelope } from './helpers/envelope.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

for (const releaseFails of [false, true]) {
  test(
    `Cold fetch failure rethrows the upstream error${releaseFails ? ' even when lock release fails' : ' and releases the lock'}`,
    { timeout: 5000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const failure = new Error('Database down')
      if (releaseFails) {
        registerLockCommands(redis).swrReleaseLock = async () => {
          throw new Error('Redis connection lost during release')
        }
      }
      const cache = createSwrClient({ redis }).cache({
        key: 'cold-fail',
        fetch: async () => {
          throw failure
        },
      })
      await assert.rejects(cache.get(), (error) => error === failure)
      if (!releaseFails) assert.equal(await redis.get(toLockKey('cold-fail')), null)
    },
  )
}

test(
  'Stale fallback returns the snapshot read at get() start, even past expiry and when lock release fails',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const cmds = registerLockCommands(redis)
    let calls = 0
    const cache = createSwrClient({ redis, mode: 'swr-serverless' }).cache({
      key: 'stale-release',
      ttl: 5,
      fetch: async () => {
        if (++calls === 1) return 'initial'
        redis.advanceTime(20_000)
        throw new Error('Upstream service unavailable')
      },
    })
    assert.equal(await cache.get(), 'initial')
    redis.advanceTime(7_000)
    cmds.swrReleaseLock = async () => {
      throw new Error('Redis connection lost during release')
    }
    assert.equal(await cache.get(), 'initial')
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
              : client.invalidateKeys('k')
        if (failureStrategy === 'protect-source')
          await assert.rejects(run, SwrRedisError, `${failureStrategy} ${operation}`)
        else await run()
      }
    }
  },
)

for (const failure of ['read', 'acquire'] as const) {
  for (const failureStrategy of ['availability', 'protect-source'] as const) {
    test(
      `failureStrategy ${failureStrategy}: cold ${failure} error`,
      { timeout: 5000 },
      async () => {
        const redis = createFakeRedis()
        const fail = async () => {
          throw new Error('Command timed out')
        }
        if (failure === 'read') redis.getBuffer = fail
        else failAcquire(redis, new Error('Command timed out'))
        let fetches = 0
        const cache = createSwrClient({ redis, failureStrategy }).cache({
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
      },
    )
  }
}

test(
  'Availability fallback unwraps SwrNoCache and SwrWriteBatch results',
  { timeout: 5000 },
  async () => {
    const redis = createFakeRedis()
    redis.getBuffer = async () => {
      throw new Error('Redis down')
    }
    const swr = createSwrClient({ redis })
    const noCache = swr.cache({ key: 'nc', fetch: async () => new SwrNoCache('direct-nc') })
    const batch = swr.cache({
      key: 'batch',
      fetch: async () => new SwrWriteBatch({ primary: 'direct-batch' }),
    })
    assert.equal(await noCache.get(), 'direct-nc')
    assert.equal(await batch.get(), 'direct-batch')
  },
)

for (const failureStrategy of ['availability', 'protect-source'] as const) {
  for (const operation of ['stale acquire', 'stale poll snapshot'] as const) {
    test(
      `failureStrategy ${failureStrategy}: ${operation} error preserves the stale fallback policy`,
      { timeout: 2000 },
      async (t) => {
        const redis = createFakeRedis(1000)
        useRedisClock(t, redis)
        const cache = createSwrClient({
          redis,
          failureStrategy,
          mode: 'wait-on-stale',
          pollIntervalMs: 1,
        }).cache<string>({
          key: 'stale-failure',
          ttl: 1,
          fetch: async () => assert.fail('stale Redis error must not fetch directly'),
        })
        await cache.set(undefined, 'stale')
        redis.advanceTime(1500)
        const fail = async () => {
          throw new Error('Redis unavailable')
        }
        if (operation === 'stale acquire') failAcquire(redis, new Error('Redis unavailable'))
        else {
          await redis.set(toLockKey('stale-failure'), 'owner', 'PX', 5000)
          redis.mgetBuffer = fail
        }
        if (failureStrategy === 'availability') assert.equal(await cache.get(), 'stale')
        else await assert.rejects(cache.get(), SwrRedisError)
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

test('flush handles a rejected background refresh without an unhandled cleanup promise', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const failure = new Error('classification failed')
  let tracked: Promise<unknown> | undefined
  const swr = createSwrClient({
    redis,
    ttl: 2,
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
  await assert.rejects(tracked, (err) => err === failure)
  await swr.flush()
  await new Promise<void>((resolve) => setImmediate(resolve))
  assert.equal(await redis.get(toLockKey('bg-reject-inflight')), null)
})

test(
  'AbortSignal: pre-aborted get rejects with the original reason before Redis I/O',
  { timeout: 5000 },
  async () => {
    const redis = createFakeRedis()
    redis.getBuffer = async () => assert.fail('pre-aborted get must not read Redis')
    redis.eval = async () => assert.fail('pre-aborted get must not run a script')
    const cache = createSwrClient({ redis }).cache({
      key: 'pre-abort',
      fetch: async () => assert.fail('pre-aborted fetch'),
    })
    const reason = new Error('already aborted')
    await assert.rejects(
      cache.get(undefined, { signal: AbortSignal.abort(reason) }),
      (error) => error === reason,
    )
  },
)

for (const phase of ['cold', 'stale'] as const) {
  test(
    `AbortSignal: cooperative ${phase} inline fetch releases its lock`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const entered = Promise.withResolvers<void>()
      let calls = 0
      const cache = createSwrClient({ redis, mode: 'swr-serverless' }).cache({
        key: 'inline-abort',
        ttl: 1,
        fetch: async (_ctx, { signal }) => {
          if (++calls === 1 && phase === 'stale') return 'stale'
          entered.resolve()
          await new Promise<void>((_resolve, reject) => {
            signal!.addEventListener('abort', () => reject(signal!.reason), { once: true })
          })
          return 'unexpected'
        },
      })
      if (phase === 'stale') {
        await cache.get()
        redis.advanceTime(1500)
      }
      const controller = new AbortController()
      const reason = new Error('caller cancelled')
      const pending = cache.get(undefined, { signal: controller.signal })
      await entered.promise
      controller.abort(reason)
      await assert.rejects(pending, (error) => error === reason)
      assert.equal(await redis.get(toLockKey('inline-abort')), null)
    },
  )

  test(
    `AbortSignal: cancels ${phase} polling without fetching or deleting another owner lock`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const waiting = Promise.withResolvers<void>()
      const cache = createSwrClient({
        redis,
        mode: 'wait-on-stale',
        pollIntervalMs: 1,
        onEvent: (event) => {
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
    },
  )
}

for (const phase of ['cold', 'stale'] as const) {
  for (const mgetOutcome of ['hit', 'unlocked', 'redis_error'] as const) {
    test(
      `AbortSignal: abort during in-flight ${phase} poll MGET (${mgetOutcome}) cancels without hit, fallback, or steal`,
      { timeout: 2000 },
      async (t) => {
        const redis = createFakeRedis(1000)
        useRedisClock(t, redis)
        let decodes = 0
        const codec: SwrCodec<string> = {
          encode: (value) => Buffer.from(value),
          decode: (bytes) => {
            decodes++
            return Buffer.from(bytes).toString()
          },
        }
        const key = `abort-mget:${phase}:${mgetOutcome}`
        const cache = createSwrClient({
          redis,
          memory: true,
          mode: 'wait-on-stale',
          failureStrategy: 'availability',
          pollIntervalMs: 1,
        }).cache<string>({
          key,
          ttl: 5,
          codec,
          fetch: async () => assert.fail('aborted waiter must not fetch'),
        })

        if (phase === 'stale') {
          await redis.set(
            toDataKey(key),
            encodeEnvelope(
              { value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 10_000 },
              codec,
            ),
          )
        }
        await redis.set(toLockKey(key), 'other-owner', 'PX', 5000)

        const freshBuf = encodeEnvelope(
          { value: 'fresh', cachedAtMs: 1000, freshUntilMs: 6000, expiresAtMs: 20_000 },
          codec,
        )
        let acquireCalls = 0
        interceptAcquire(redis, (acquire, ...args) => {
          acquireCalls++
          return acquire(...args)
        })

        const gatedMget = gateCommand(async () => {
          if (mgetOutcome === 'redis_error') throw new Error('mget failed')
          if (mgetOutcome === 'hit') return [freshBuf, null]
          return [null, null]
        }, 'before execution')
        redis.mgetBuffer = gatedMget.command as typeof redis.mgetBuffer

        const controller = new AbortController()
        const reason = new Error(`cancelled-${phase}-${mgetOutcome}`)
        const pending = cache.get(undefined, { signal: controller.signal })
        await gatedMget.entered

        const decodesBeforeAbort = decodes
        controller.abort(reason)
        gatedMget.release()

        await assert.rejects(pending, (err) => err === reason)
        assert.equal(acquireCalls, 1, 'aborted waiter must not attempt lock steal')
        assert.equal(decodes, decodesBeforeAbort, 'aborted MGET must skip payload decode')

        if (mgetOutcome === 'hit') {
          // The aborted hit must not have populated memory: the next read goes to Redis.
          await redis.set(toDataKey(key), freshBuf)
          let reads = 0
          const getBuffer = redis.getBuffer.bind(redis)
          redis.getBuffer = async (k) => {
            reads++
            return getBuffer(k)
          }
          assert.equal(await cache.get(), 'fresh')
          assert.equal(reads, 1)
        }
      },
    )
  }
}

for (const acquireOutcome of ['acquired', 'contended', 'redis_error', 'lost_reply'] as const) {
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
        pollIntervalMs: 1,
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
        return gatedSteal.command(acquire, lockKey, token, ttlMs)
      })
      const gatedSteal = gateCommand(
        async (acquire: Acquire, lockKey: string, token: string, ttlMs: number) => {
          if (acquireOutcome === 'redis_error') throw new Error('acquire failed')
          if (acquireOutcome === 'contended') {
            await redis.set(lockKey, 'other-owner', 'PX', 5000)
            return false
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
      await gatedSteal.entered

      controller.abort(reason)
      gatedSteal.release()

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
    `AbortSignal: abort during initial getBuffer (${readOutcome}) skips decode, lock, events, and memory`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      let decodes = 0
      const codec: SwrCodec<string> = {
        encode: (value) => Buffer.from(value),
        decode: (bytes) => {
          decodes++
          return Buffer.from(bytes).toString()
        },
      }
      const key = `abort-read:${readOutcome}`
      const events: SwrEvent[] = []
      const cache = createSwrClient({
        redis,
        memory: true,
        onEvent: (e) => events.push(e),
      }).cache<string>({
        key,
        ttl: 5,
        codec,
        fetch: async () => assert.fail('aborted initial read must not fetch'),
      })
      await redis.set(
        toDataKey(key),
        encodeEnvelope(
          { value: 'fresh', cachedAtMs: 1000, freshUntilMs: 6000, expiresAtMs: 20_000 },
          codec,
        ),
      )

      let acquireCalls = 0
      interceptAcquire(redis, (acquire, ...args) => {
        acquireCalls++
        return acquire(...args)
      })

      const origGetBuffer = redis.getBuffer.bind(redis)
      const gatedRead = gateCommand(async (k: string) => {
        if (readOutcome === 'redis_error') throw new Error('getBuffer failed')
        return origGetBuffer(k)
      }, 'before execution')
      redis.getBuffer = gatedRead.command as typeof redis.getBuffer

      const controller = new AbortController()
      const reason = new Error(`cancelled-read-${readOutcome}`)
      const pending = cache.get(undefined, { signal: controller.signal })
      await gatedRead.entered

      controller.abort(reason)
      gatedRead.release()

      await assert.rejects(pending, (err) => err === reason)
      assert.equal(decodes, 0)
      assert.equal(acquireCalls, 0)
      assert.deepEqual(events, [])

      if (readOutcome === 'fresh') {
        let reads = 0
        redis.getBuffer = async (k) => {
          reads++
          return origGetBuffer(k)
        }
        assert.equal(await cache.get(), 'fresh')
        assert.equal(reads, 1, 'aborted fresh read must not populate memory')
      }
    },
  )
}

// Both outcomes leave our token in Redis, so a leaked lock shows up as a non-null lock key.
for (const [phase, acquireOutcome] of [
  ['cold', 'replayed_acquire'],
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
      const cache = createSwrClient({
        redis,
        mode: 'swr',
        onEvent: (e) => events.push(e),
        waitUntil: () => {
          waitUntilCalls++
        },
      }).cache<string>({
        key,
        ttl: 5,
        fetch: async () => assert.fail('aborted initial lock must not fetch'),
      })

      if (phase === 'stale') {
        await redis.set(
          toDataKey(key),
          encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 20_000 }),
        )
      }
      redis.mgetBuffer = async () => assert.fail('aborted initial lock must not poll')

      const gatedAcquire = gateCommand(
        async (acquire: Acquire, lockKey: string, token: string, ttlMs: number) => {
          await acquire(lockKey, token, ttlMs)
          if (acquireOutcome === 'lost_reply') throw new Error('reply lost after acquire')
          return acquire(lockKey, token, ttlMs)
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
      gatedAcquire.release()

      await assert.rejects(pending, (err) => err === reason)
      assert.equal(waitUntilCalls, 0)
      assert.deepEqual(events, [])
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
      async (acquire: Acquire, lockKey: string, token: string, ttlMs: number) =>
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

test(
  'A lost acquire reply starts cleanup without delaying the fallback fetch',
  { timeout: 2000 },
  async () => {
    const redis = createFakeRedis()
    const commands = registerLockCommands(redis)
    const release = gateCommand(commands.swrReleaseLock.bind(redis), 'before execution')
    const released = Promise.withResolvers<void>()
    commands.swrReleaseLock = async (...args) => {
      const result = await release.command(...args)
      released.resolve()
      return result
    }

    interceptAcquire(redis, async (acquire, ...args) => {
      await acquire(...args)
      throw new Error('reply lost after lock grant')
    })
    const cache = createSwrClient({ redis }).cache({
      key: 'lost-grant',
      fetch: async (_context, { cacheWrite }) => cacheWrite,
    })

    const pending = cache.get()
    await release.entered
    try {
      assert.equal(await pending, 'skip')
      assert.ok(await redis.get(toLockKey('lost-grant')))
    } finally {
      release.release()
      await released.promise
    }
    assert.equal(await redis.get(toLockKey('lost-grant')), null)
  },
)
