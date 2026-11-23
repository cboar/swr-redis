import { lockContract } from './helpers/lock-contract.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createFakeRedis } from './fake-redis.ts'
import { useRedisClock } from './helpers/clock.ts'
import { RedisLock, registerLockCommands } from '../src/lock.ts'

lockContract(
  'RedisLock (fake Redis)',
  async () => createFakeRedis(1000),
  async (redis, _key, ms) => {
    ;(redis as unknown as ReturnType<typeof createFakeRedis>).advanceTime(ms)
  },
)

function createHeartbeatTracker(redis: ReturnType<typeof createFakeRedis>) {
  const cmds = registerLockCommands(redis)
  const origExtend = cmds.swrExtendLock.bind(redis)
  const ttls: number[] = []
  let pendingPromise: Promise<unknown> | null = null

  cmds.swrExtendLock = async (...args) => {
    ttls.push(args[2])
    const p = origExtend(...args)
    pendingPromise = p
    return await p
  }

  return {
    cmds,
    get calls() {
      return ttls.length
    },
    get ttls() {
      return ttls
    },
    async advanceAndAwaitRenewal(
      t: test.TestContext,
      timerMs: number,
      clockMs = timerMs,
    ): Promise<void> {
      pendingPromise = null
      redis.advanceTime(clockMs)
      t.mock.timers.tick(timerMs)
      if (pendingPromise) {
        await (pendingPromise as Promise<unknown>).catch(() => {})
      }
      await new Promise<void>((resolve) => setImmediate(resolve))
    },
  }
}

test('RedisLock.hold: renews normal lock while fetch runs and cleans up on completion', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lockKey = 'lock:heartbeat'
  const lock = new RedisLock(tracker.cmds, lockKey)
  const token = lock.token

  await lock.acquire(250)

  let releaseFetch!: () => void
  const run = lock.hold(
    { lockTtlMs: 250, maxLockTtlMs: 1000 },
    () => assert.fail('no renewal error expected'),
    async () => {
      await new Promise<void>((resolve) => {
        releaseFetch = resolve
      })
      return 'done'
    },
  )

  // Three 100ms intervals = 300ms (> initial 250ms lockTtlMs)
  await tracker.advanceAndAwaitRenewal(t, 100)
  assert.equal(tracker.calls, 1)
  assert.equal(await redis.get(lockKey), token)

  await tracker.advanceAndAwaitRenewal(t, 100)
  assert.equal(tracker.calls, 2)
  assert.equal(await redis.get(lockKey), token)

  await tracker.advanceAndAwaitRenewal(t, 100)
  assert.equal(tracker.calls, 3)
  assert.equal(await redis.get(lockKey), token)

  releaseFetch()
  assert.equal(await run, 'done')

  // No further renewals occur after fetch completion
  await tracker.advanceAndAwaitRenewal(t, 100)
  assert.equal(tracker.calls, 3)

  const committed = await lock.commit([
    { dataKey: 'data:heartbeat', buffer: Buffer.from('ok'), ttlSeconds: 60 },
  ])
  assert.equal(committed, true)
})

test('RedisLock.hold: stops at maxLockTtl ceiling while fetch pending, expires lock, and rejects late commit', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lockKey = 'lock:ceiling'
  const lock = new RedisLock(tracker.cmds, lockKey)
  const token = lock.token

  await lock.acquire(100)

  let releaseFetch!: () => void
  const run = lock.hold(
    { lockTtlMs: 100, maxLockTtlMs: 220 },
    () => assert.fail('no renewal error expected'),
    async () => {
      await new Promise<void>((resolve) => {
        releaseFetch = resolve
      })
      return 'late-done'
    },
  )

  // Renewals at 40ms (+100=140), 80ms (+100=180), 120ms (+100=220 -> hits exact ceiling and stops)
  for (let i = 1; i <= 3; i++) {
    await tracker.advanceAndAwaitRenewal(t, 40)
    assert.equal(tracker.calls, i)
    assert.equal(await redis.get(lockKey), token)
  }
  assert.deepEqual(tracker.ttls, [100, 100, 100])

  // At 160ms and 200ms: interval already cleared after hitting exact ceiling at 120ms
  await tracker.advanceAndAwaitRenewal(t, 40)
  await tracker.advanceAndAwaitRenewal(t, 40)
  assert.equal(tracker.calls, 3)
  assert.equal(await redis.get(lockKey), token)

  // At 220ms: last renewed lock (120 + 100 = 220) expires while fetch is still pending
  await tracker.advanceAndAwaitRenewal(t, 20)
  assert.equal(tracker.calls, 3)
  assert.equal(await redis.get(lockKey), null)

  releaseFetch()
  assert.equal(await run, 'late-done')

  // Late commit must be rejected and data key must remain unwritten
  const committed = await lock.commit([
    { dataKey: 'data:ceiling', buffer: Buffer.from('late'), ttlSeconds: 60 },
  ])
  assert.equal(committed, false)
  assert.equal(await redis.getBuffer('data:ceiling'), null)
})

test('RedisLock.hold: clamps final renewal under timer drift (100/220, tick at 125ms)', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lockKey = 'lock:drift'
  const lock = new RedisLock(tracker.cmds, lockKey)

  await lock.acquire(100)
  let releaseFetch!: () => void
  const run = lock.hold(
    { lockTtlMs: 100, maxLockTtlMs: 220 },
    () => assert.fail('no renewal error expected'),
    () => new Promise<string>((resolve) => (releaseFetch = () => resolve('done'))),
  )

  await tracker.advanceAndAwaitRenewal(t, 40)
  await tracker.advanceAndAwaitRenewal(t, 40)
  // Clock drifts +5ms ahead of interval timer so the 3rd tick fires at elapsed=125ms.
  await tracker.advanceAndAwaitRenewal(t, 40, 45)
  assert.deepEqual(tracker.ttls, [100, 100, 95])
  assert.equal(await redis.get(lockKey), lock.token)

  await tracker.advanceAndAwaitRenewal(t, 40)
  await tracker.advanceAndAwaitRenewal(t, 40)
  assert.equal(tracker.calls, 3)
  assert.equal(await redis.get(lockKey), lock.token)

  // At elapsed=220ms (205 + 15): lock expires at ceiling.
  await tracker.advanceAndAwaitRenewal(t, 15)
  assert.equal(tracker.calls, 3)
  assert.equal(await redis.get(lockKey), null)

  releaseFetch()
  assert.equal(await run, 'done')
})

test('RedisLock.hold: stops without sending when a tick lands at or past exhausted budget', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lock = new RedisLock(tracker.cmds, 'lock:exhausted')

  await lock.acquire(100)
  let releaseFetch!: () => void
  const run = lock.hold(
    { lockTtlMs: 100, maxLockTtlMs: 250 },
    () => assert.fail('no renewal error expected'),
    () => new Promise<string>((resolve) => (releaseFetch = () => resolve('done'))),
  )

  // Clock jumps to exact ceiling (remainingMs === 0) before the first tick fires.
  await tracker.advanceAndAwaitRenewal(t, 40, 250)
  assert.deepEqual(tracker.ttls, [])

  releaseFetch()
  assert.equal(await run, 'done')
})

test('RedisLock.hold: failed clamped renewal retries on next tick with smaller TTL', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const cmds = registerLockCommands(redis)
  const origExtend = cmds.swrExtendLock.bind(redis)
  const lockKey = 'lock:retry-clamped'
  const lock = new RedisLock(cmds, lockKey)

  await lock.acquire(100)
  const ttls: number[] = []
  const reported: unknown[] = []
  cmds.swrExtendLock = async (key, token, ttlMs) => {
    ttls.push(ttlMs)
    if (ttls.length === 2) throw new Error('transient clamped failure')
    return await origExtend(key, token, ttlMs)
  }

  let releaseFetch!: () => void
  const run = lock.hold(
    { lockTtlMs: 100, maxLockTtlMs: 170 },
    (err) => reported.push((err as Error).message),
    () => new Promise<string>((resolve) => (releaseFetch = () => resolve('done'))),
  )

  for (let elapsed = 40; elapsed <= 200; elapsed += 40) {
    redis.advanceTime(40)
    t.mock.timers.tick(40)
    await new Promise<void>((resolve) => setImmediate(resolve))
    // The lease would expire at 140ms without the successful retry.
    if (elapsed === 160) assert.equal(await redis.get(lockKey), lock.token)
  }
  assert.deepEqual(ttls, [100, 90, 50])
  assert.deepEqual(reported, ['transient clamped failure'])
  assert.equal(await redis.get(lockKey), null)

  releaseFetch()
  assert.equal(await run, 'done')
})

test('RedisLock.hold: cleans up interval on fetch rejection', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lock = new RedisLock(tracker.cmds, 'lock:reject')

  await lock.acquire(100)

  let rejectFetch!: (err: Error) => void
  const run = lock.hold(
    { lockTtlMs: 100, maxLockTtlMs: 500 },
    () => assert.fail('no renewal error expected'),
    async () => {
      await new Promise<void>((_resolve, reject) => {
        rejectFetch = reject
      })
      return 'unreachable'
    },
  )

  await tracker.advanceAndAwaitRenewal(t, 40)
  assert.equal(tracker.calls, 1)

  rejectFetch(new Error('upstream failed'))
  await assert.rejects(run, { message: 'upstream failed' })

  // Timer must be cleared so subsequent ticks do not call swrExtendLock
  await tracker.advanceAndAwaitRenewal(t, 40)
  await tracker.advanceAndAwaitRenewal(t, 40)
  assert.equal(tracker.calls, 1)
})

test('RedisLock.hold: reports renewal errors while active, keeps renewing, and stays silent after settling', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const cmds = registerLockCommands(redis)
  const lock = new RedisLock(cmds, 'lock:renew-error')
  await lock.acquire(100)

  let attempts = 0
  const lateRenewal = Promise.withResolvers<never>()
  cmds.swrExtendLock = async () => {
    attempts++
    if (attempts === 3) return lateRenewal.promise
    throw new Error(`renew failed ${attempts}`)
  }
  const reported: unknown[] = []
  let releaseFetch!: () => void
  const run = lock.hold(
    { lockTtlMs: 100, maxLockTtlMs: 1000 },
    (err) => reported.push((err as Error).message),
    () => new Promise<string>((resolve) => (releaseFetch = () => resolve('done'))),
  )
  const tick = async () => {
    redis.advanceTime(40)
    t.mock.timers.tick(40)
    await new Promise<void>((resolve) => setImmediate(resolve))
  }

  await tick()
  await tick()
  assert.deepEqual(reported, ['renew failed 1', 'renew failed 2'])

  // A renewal still in flight when the fetch settles must not report afterwards.
  await tick()
  assert.equal(attempts, 3)
  releaseFetch()
  assert.equal(await run, 'done')
  lateRenewal.reject(new Error('late failure'))
  await new Promise<void>((resolve) => setImmediate(resolve))
  assert.deepEqual(reported, ['renew failed 1', 'renew failed 2'])
})
