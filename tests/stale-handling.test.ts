import test from 'node:test'
import assert from 'node:assert/strict'
import { setImmediate } from 'node:timers/promises'
import {
  createSwrClient,
  SwrTimeoutError,
  SwrValidationError,
  type SwrEvent,
  type SwrRuntime,
} from '../src/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { COOLDOWN_MARKER, registerLockCommands } from '../src/lock.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { holdLock } from './helpers/acquire.ts'
import { tick, useRedisClock } from './helpers/clock.ts'
import { gateCommand } from './helpers/command-gate.ts'
import { encodeEnvelope } from './helpers/envelope.ts'

const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)

const errorEvents = (events: SwrEvent[]) =>
  events.flatMap((e) => (e.type === 'error' ? [`${e.reason}:${e.severity}`] : []))

test(
  'Defaults (staleWait 0): the winner returns stale at once; its refresh ignores the caller signal and is tracked by waitUntil and flush',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const waitUntilPromises: Promise<unknown>[] = []
    let fetchCount = 0
    const bgEntered = Promise.withResolvers<AbortSignal>()
    const bgGate = Promise.withResolvers<void>()
    const swr = createSwrClient({
      redis,
      waitUntil: (p) => waitUntilPromises.push(p),
    })
    const cache = swr.cache({
      key: 'item',
      ttl: 5,
      fetch: async (_ctx, meta) => {
        assert.equal(meta.cacheWrite, 'attempt')
        fetchCount++
        if (fetchCount === 2) {
          bgEntered.resolve(meta.signal)
          await bgGate.promise
        }
        return `v-${fetchCount}`
      },
    })

    assert.equal(await cache.get(), 'v-1')
    redis.advanceTime(6_000)

    const callerAbort = new AbortController()
    assert.deepEqual(await cache.getWithMeta(undefined, { signal: callerAbort.signal }), {
      value: 'v-1',
      source: 'redis',
      timing: { cachedAtMs: 1000, freshUntilMs: 6000, expiresAtMs: 26_000 },
    })
    const bgSignal = await bgEntered.promise
    callerAbort.abort()
    assert.equal(bgSignal.aborted, false)
    // The cold fetch is tracked as well.
    assert.equal(waitUntilPromises.length, 2)

    assert.equal(await cache.get(), 'v-1')
    assert.equal(fetchCount, 2)

    bgGate.resolve()
    await swr.flush()
    assert.equal(await cache.get(), 'v-2')
    assert.equal(fetchCount, 2)
  },
)

test(
  'Defaults: a failed background refresh keeps the stale entry, reports stale_fallback once, and cools the key down',
  { timeout: 5000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const entered = Promise.withResolvers<void>()
    const gate = Promise.withResolvers<void>()
    const events: SwrEvent[] = []
    let calls = 0
    const swr = createSwrClient({ redis, onEvent: (event) => events.push(event) })
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
    assert.equal(await redis.get(lockKey('bg-failure')), COOLDOWN_MARKER)
    assert.deepEqual(await redis.getBuffer(dataKey('bg-failure')), stored)
    assert.deepEqual(errorEvents(events), ['stale_fallback:warn'])

    assert.equal(await cache.get(), 'v-1')
    await swr.flush()
    assert.equal(calls, 2)
  },
)

for (const hook of [false, true]) {
  test(
    `runtime serverless ${hook ? 'with' : 'without'} a waitUntil hook: the winner ${hook ? 'returns stale while the hook keeps the refresh alive' : 'refreshes inline'}; other stale callers return stale at once`,
    { timeout: 2000 },
    async (t) => {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      const entered = Promise.withResolvers<void>()
      const gate = Promise.withResolvers<void>()
      const tracked: Promise<unknown>[] = []
      let calls = 0
      const cache = createSwrClient({ redis, runtime: 'serverless' }).cache({
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
      const winner = cache.getWithMeta(
        undefined,
        hook ? { waitUntil: (p) => tracked.push(p) } : undefined,
      )
      await entered.promise
      if (hook) assert.equal((await winner).source, 'redis')
      assert.equal(await cache.get(), 'v1')
      gate.resolve()
      if (hook) await Promise.all(tracked)
      const result = await winner
      assert.equal(result.value, hook ? 'v1' : 'v2')
      assert.equal(result.source, hook ? 'redis' : 'fetch')
      assert.equal(tracked.length, hook ? 1 : 0)
      assert.equal(calls, 2)
      assert.equal(await redis.get(lockKey('item')), null)
      assert.equal(await cache.get(), 'v2')
    },
  )
}

const STALE_WAIT_CASES: {
  name: string
  runtime: SwrRuntime
  staleWait: number
  hook?: boolean
  // Which step outlasts staleWait: the source call, or the commit reply after the source succeeds.
  slow?: 'source' | 'commit'
  source: 'redis' | 'fetch'
}[] = [
  {
    name: 'staleWait 0 returns stale even when the refresh settles within a microtask',
    runtime: 'server',
    staleWait: 0,
    source: 'redis',
  },
  {
    name: 'a refresh within staleWait returns the fetched value',
    runtime: 'server',
    staleWait: 0.5,
    source: 'fetch',
  },
  {
    name: 'a refresh slower than staleWait returns stale and keeps refreshing',
    runtime: 'server',
    staleWait: 0.5,
    slow: 'source',
    source: 'redis',
  },
  {
    name: 'a commit reply slower than staleWait returns stale while flush and the hook wait for the write',
    runtime: 'server',
    staleWait: 0.5,
    hook: true,
    slow: 'commit',
    source: 'redis',
  },
  {
    name: 'serverless without a hook ignores staleWait and waits for its refresh',
    runtime: 'serverless',
    staleWait: 0.5,
    slow: 'source',
    source: 'fetch',
  },
  {
    name: 'serverless with a waitUntil hook honours staleWait',
    runtime: 'serverless',
    staleWait: 0.5,
    hook: true,
    slow: 'source',
    source: 'redis',
  },
]

for (const { name, runtime, staleWait, hook, slow, source } of STALE_WAIT_CASES) {
  test(`staleWait winner: ${name}`, { timeout: 2000 }, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const commands = registerLockCommands(redis)
    const commit = gateCommand(commands.swrCommitAndRelease.bind(redis), 'after execution')
    if (slow === 'commit') commands.swrCommitAndRelease = commit.command
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const tracked: Promise<unknown>[] = []
    let calls = 0
    const swr = createSwrClient({
      redis,
      runtime,
      memory: true,
      waitUntil: hook ? (promise) => tracked.push(promise) : undefined,
    })
    const cache = swr.cache({
      key: 'stale-wait',
      ttl: 1,
      fetch: async () => {
        calls++
        entered.resolve()
        if (slow === 'source') await release.promise
        return 'fresh'
      },
    })
    await cache.set(undefined, 'stale')
    redis.advanceTime(1500)

    const pending = cache.getWithMeta(undefined, { staleWait })
    let settled = false
    void pending.then(() => (settled = true))
    if (slow) {
      await (slow === 'commit' ? commit.entered : entered.promise)
      await tick(t, redis, 500)
      assert.equal(settled, source === 'redis')
      let flushed = false
      const flushing = swr.flush().then(() => (flushed = true))
      assert.equal(tracked.length, hook ? 1 : 0)
      let hookSettled = false
      const hookDone = hook ? tracked[0]!.then(() => (hookSettled = true)) : undefined
      await setImmediate()
      assert.equal(flushed, false, 'flush waits for the refresh')
      assert.equal(hookSettled, false, 'the hook promise waits for the refresh')
      if (slow === 'commit') commit.release()
      else release.resolve()
      await flushing
      await hookDone
    }
    const result = await pending
    assert.equal(result.source, source)
    assert.equal(result.value, source === 'redis' ? 'stale' : 'fresh')

    await swr.flush()
    assert.equal(calls, 1)
    assert.equal(tracked.length, hook ? 1 : 0)
    assert.equal(await redis.get(lockKey('stale-wait')), null)
    const after = await cache.getWithMeta()
    assert.equal(after.value, 'fresh')
    assert.equal(after.source, 'memory')
  })
}

for (const { failure, outcome, isFatal, event } of [
  {
    failure: new Error('transient'),
    outcome: 'stale',
    isFatal: () => false,
    event: 'stale_fallback:warn',
  },
  {
    failure: new RangeError('schema mismatch'),
    outcome: 'rejects',
    isFatal: (error: unknown) => error instanceof RangeError,
    event: 'fatal:error',
  },
  {
    failure: new SwrValidationError('contract violation'),
    outcome: 'rejects',
    isFatal: () => false,
    event: 'fatal:error',
  },
  {
    failure: new SwrTimeoutError('nested', 5, 'fetch'),
    outcome: 'stale',
    isFatal: () => false,
    event: 'stale_fallback:warn',
  },
] as const) {
  test(`Stale refresh failure for an attached winner: ${failure.name} ${outcome === 'stale' ? 'serves stale' : 'is fatal'}`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const events: SwrEvent[] = []
    let calls = 0
    const cache = createSwrClient({
      redis,
      staleWait: 10,
      onEvent: (event) => events.push(event),
    }).cache({
      key: 'failure',
      ttl: 5,
      isFatal,
      fetch: async () => {
        if (++calls === 1) return 'stale'
        throw failure
      },
    })
    await cache.get()
    const stored = await redis.getBuffer(dataKey('failure'))
    redis.advanceTime(6000)
    if (outcome === 'rejects') {
      await assert.rejects(cache.get(), (error) => error === failure)
    } else {
      assert.equal(await cache.get(), 'stale')
    }
    assert.equal(calls, 2)
    assert.deepEqual(await redis.getBuffer(dataKey('failure')), stored)
    assert.equal(await redis.get(lockKey('failure')), COOLDOWN_MARKER)
    assert.deepEqual(errorEvents(events), [event])
  })
}

test(
  'staleWait: a stale caller that finds the lock busy waits for the winner commit',
  { timeout: 2000 },
  async (t) => {
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
      fetch: async () => {
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
  },
)

test(
  'staleWait: a polling stale caller returns stale as soon as it sees the cooldown, even after a fatal winner failure',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const waiting = Promise.withResolvers<void>()
    const failure = new SwrValidationError('fatal')
    const polls: string[] = []
    let calls = 0
    const cache = createSwrClient({
      redis,
      runtime: 'serverless',
      staleWait: 10,
      pollIntervalMs: 1,
      onEvent: (event) => {
        if (event.type === 'poll') polls.push(event.outcome)
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
    const winner = assert.rejects(cache.get(), (error) => error === failure)
    await entered.promise
    const loser = cache.get()
    await waiting.promise
    release.resolve()
    await winner
    assert.equal(await loser, 'stale')
    assert.equal(await redis.get(lockKey('failed-winner')), COOLDOWN_MARKER)
    assert.deepEqual(polls, ['stale_fallback'])
    assert.equal(calls, 2)
  },
)

test(
  'Cooldown: a stale caller whose acquire reports cooldown returns stale at once without polling, whatever staleWait',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    await redis.set(lockKey('cooling'), COOLDOWN_MARKER, 'PX', 1000)
    await redis.set(
      dataKey('cooling'),
      encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 10_000 }),
    )
    redis.mgetBuffer = async () => assert.fail('must not poll')
    const events: SwrEvent[] = []
    const cache = createSwrClient({
      redis,
      staleWait: Infinity,
      onEvent: (event) => events.push(event),
    }).cache({ key: 'cooling', fetch: async () => assert.fail('must not fetch') })
    assert.equal(await cache.get(), 'stale')
    assert.deepEqual(events, [
      { type: 'hit', key: 'cooling', source: 'redis', stale: true, startedRevalidation: false },
    ])
  },
)

for (const { name, staleWait, fetchTimeout } of [
  { name: 'staleWait ends', staleWait: 0.04, fetchTimeout: 30 },
  { name: 'the waiting limit caps staleWait: Infinity', staleWait: Infinity, fetchTimeout: 0.04 },
]) {
  test(
    `staleWait: a polling stale caller returns stale when ${name}, emitting no timeout event`,
    { timeout: 2000 },
    async (t) => {
      t.mock.timers.enable({ apis: ['setTimeout'] })
      t.mock.method(Math, 'random', () => 0)
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      await holdLock(redis, lockKey('stale-timeout'), 5000, 'winner')
      await redis.set(
        dataKey('stale-timeout'),
        encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 10_000 }),
      )
      const waiting = Promise.withResolvers<void>()
      const events: SwrEvent[] = []
      const cache = createSwrClient({
        redis,
        staleWait,
        fetchTimeout,
        pollIntervalMs: 10,
        onEvent: (event) => {
          events.push(event)
          if (event.type === 'hit') waiting.resolve()
        },
      }).cache({ key: 'stale-timeout', fetch: async () => assert.fail('must not fetch') })
      const pending = cache.get()
      await waiting.promise
      for (let elapsed = 0; elapsed < 40; elapsed += 10) await tick(t, redis, 10)
      assert.equal(await pending, 'stale')
      assert.equal(await redis.get(lockKey('stale-timeout')), 'winner')
      assert.deepEqual(events.slice(1), [
        { type: 'poll', key: 'stale-timeout', outcome: 'timeout', attempts: 4, durationMs: 40 },
      ])
    },
  )
}

for (const role of ['winner', 'poller'] as const) {
  test(
    `Stale data expiring while the ${role} waits: it continues as a cold ${role === 'winner' ? 'caller' : 'waiter'} and gets the fetched value`,
    { timeout: 2000 },
    async (t) => {
      t.mock.timers.enable({ apis: ['setTimeout'] })
      t.mock.method(Math, 'random', () => 0)
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      await redis.set(
        dataKey('expiring'),
        encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 1100 }),
      )
      if (role === 'poller') await holdLock(redis, lockKey('expiring'), 5000, 'winner')
      const entered = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const waiting = Promise.withResolvers<void>()
      const polls: string[] = []
      let calls = 0
      const cache = createSwrClient({
        redis,
        staleWait: role === 'winner' ? 0.5 : 10,
        pollIntervalMs: 10,
        onEvent: (event) => {
          if (event.type === 'poll') polls.push(event.outcome)
          if (event.type === 'hit') waiting.resolve()
        },
      }).cache({
        key: 'expiring',
        fetch: async () => {
          calls++
          entered.resolve()
          await release.promise
          return 'fresh'
        },
      })

      const pending = cache.getWithMeta()
      let settled = false
      void pending.then(() => (settled = true))
      if (role === 'winner') {
        await entered.promise
        await tick(t, redis, 500)
      } else {
        await waiting.promise
        await tick(t, redis, 100)
        assert.equal(calls, 0)
        // The winner gives up without writing; a usable snapshot would now be served.
        await redis.unlink(lockKey('expiring'))
        await tick(t, redis, 10)
        await entered.promise
      }
      assert.equal(settled, false, 'the expired snapshot is not served')
      release.resolve()
      const result = await pending
      assert.equal(result.value, 'fresh')
      assert.equal(result.source, 'fetch')
      assert.equal(calls, 1)
      assert.deepEqual(polls, role === 'winner' ? [] : ['lock_stolen'])
    },
  )
}

test('waitUntil: a per-get hook replaces the client hook for that call', async (t) => {
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
