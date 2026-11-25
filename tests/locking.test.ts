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

async function hold<R>(
  lock: RedisLock,
  lockTtlMs: number,
  onRenewError: (err: unknown) => void,
  fn: () => Promise<R>,
): Promise<R> {
  const renewal = lock.startRenewal(lockTtlMs, { onRenewError, onLockLost: () => {} })
  try {
    return await fn()
  } finally {
    renewal.stop()
  }
}

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

test('RedisLock.startRenewal: renews normal lock while fetch runs and cleans up on completion', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lockKey = 'lock:heartbeat'
  const lock = new RedisLock(tracker.cmds, lockKey)
  const token = lock.token

  await lock.acquire(250)

  let releaseFetch!: () => void
  const run = hold(
    lock,
    250,
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

test('RedisLock.startRenewal: cleans up interval on fetch rejection', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const tracker = createHeartbeatTracker(redis)
  const lock = new RedisLock(tracker.cmds, 'lock:reject')

  await lock.acquire(100)

  let rejectFetch!: (err: Error) => void
  const run = hold(
    lock,
    100,
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

test('RedisLock.startRenewal: reports renewal errors while active, keeps renewing, and stays silent after settling', async (t) => {
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
  const run = hold(
    lock,
    100,
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
