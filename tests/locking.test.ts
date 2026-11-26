import { lockContract } from './helpers/lock-contract.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { setImmediate } from 'node:timers/promises'
import { createSwrClient, SwrTimeoutError, type SwrEvent } from '../src/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { COOLDOWN_MARKER, RedisLock, registerLockCommands } from '../src/lock.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { gateCommand } from './helpers/command-gate.ts'
import { tick, useRedisClock } from './helpers/clock.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

lockContract(
  'RedisLock (fake Redis)',
  async () => createFakeRedis(1000),
  async (redis, _key, ms) => {
    ;(redis as unknown as ReturnType<typeof createFakeRedis>).advanceTime(ms)
  },
)

function trackRenewals(redis: ReturnType<typeof createFakeRedis>) {
  const cmds = registerLockCommands(redis)
  const extend = cmds.swrExtendLock.bind(redis)
  const ttls: number[] = []
  cmds.swrExtendLock = (...args) => {
    ttls.push(args[2])
    return extend(...args)
  }
  return { cmds, ttls }
}

const unexpected = (what: string) => () => assert.fail(`unexpected ${what}`)

test('RedisLock.startRenewal: renews the full lockTtl every 40% of it, without a cap, until stopped', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const { cmds, ttls } = trackRenewals(redis)
  const lock = new RedisLock(cmds, 'lock:heartbeat')
  await lock.acquire(100)

  const renewal = lock.startRenewal(100, {
    onRenewError: unexpected('renewal error'),
    onLockLost: unexpected('lock loss'),
  })
  for (let i = 0; i < 10; i++) await tick(t, redis, 40)
  assert.deepEqual(ttls, Array(10).fill(100))
  assert.deepEqual(await redis.getBuffer(lock.key), lock.token)
  assert.equal(renewal.lost, false)

  renewal.stop()
  await tick(t, redis, 40)
  await tick(t, redis, 40)
  assert.equal(ttls.length, 10)
  assert.equal(
    await lock.commit([{ dataKey: 'data:heartbeat', buffer: Buffer.from('ok'), ttlMs: 60_000 }]),
    true,
  )
})

test('RedisLock.startRenewal: reports renewal errors while active, keeps renewing, and stays silent after stop', async (t) => {
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
  const renewal = lock.startRenewal(100, {
    onRenewError: (err) => reported.push((err as Error).message),
    onLockLost: unexpected('lock loss'),
  })

  await tick(t, redis, 40)
  await tick(t, redis, 40)
  assert.deepEqual(reported, ['renew failed 1', 'renew failed 2'])

  // A renewal still in flight when the fetch settles must not report afterwards.
  await tick(t, redis, 40)
  assert.equal(attempts, 3)
  renewal.stop()
  lateRenewal.reject(new Error('late failure'))
  await setImmediate()
  assert.deepEqual(reported, ['renew failed 1', 'renew failed 2'])
  assert.equal(renewal.lost, false)
})

test('RedisLock.startRenewal: a negative reply while active marks the lock lost once and stops renewing', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const { cmds, ttls } = trackRenewals(redis)
  const lock = new RedisLock(cmds, 'lock:lost')
  await lock.acquire(100)

  let lost = 0
  const renewal = lock.startRenewal(100, {
    onRenewError: unexpected('renewal error'),
    onLockLost: () => lost++,
  })
  await tick(t, redis, 40)
  await redis.set(lock.key, 'other-owner', 'PX', 5000)
  await tick(t, redis, 40)
  assert.equal(renewal.lost, true)
  assert.equal(lost, 1)

  await tick(t, redis, 40)
  await tick(t, redis, 40)
  assert.equal(ttls.length, 2)
  assert.equal(lost, 1)
  assert.equal(await redis.get(lock.key), 'other-owner')
})

for (const outcome of ['resolves', 'rejects'] as const) {
  test(`Lease renewal: a renewal that finds the lock taken reports lock_lost once, and a fetch that ${outcome} writes nothing`, async (t) => {
    t.mock.timers.enable({ apis: ['setInterval'] })
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const events: SwrEvent[] = []
    const entered = Promise.withResolvers<AbortSignal>()
    const release = Promise.withResolvers<void>()
    const failure = new Error('source failed')
    let fetches = 0
    const cache = createSwrClient({
      redis,
      lockTtl: 0.1,
      onEvent: (event) => events.push(event),
    }).cache({
      key: 'renew-lost',
      fetch: async (_ctx, { signal }) => {
        fetches++
        entered.resolve(signal)
        await release.promise
        if (outcome === 'rejects') throw failure
        return 'fetched'
      },
    })

    const pending = cache.getWithMeta()
    const signal = await entered.promise
    // The lease expired unnoticed and another process took the key.
    await redis.set(toLockKey('renew-lost'), 'other-owner', 'PX', 5000)
    await tick(t, redis, 40)
    await tick(t, redis, 40)
    assert.equal(signal.aborted, false, 'losing the lock does not abort the fetch')

    release.resolve()
    if (outcome === 'resolves')
      assert.deepEqual(await pending, { value: 'fetched', source: 'fetch' })
    else await assert.rejects(pending, (err) => err === failure)
    assert.equal(fetches, 1)
    assert.equal(await redis.getBuffer(toDataKey('renew-lost')), null)
    // Neither a commit nor a cooldown swap replaces the new owner's lock.
    assert.equal(await redis.get(toLockKey('renew-lost')), 'other-owner')
    assert.deepEqual(
      events.flatMap((e) => (e.type === 'error' ? [`${e.reason}:${e.severity}`] : [])),
      outcome === 'resolves' ? ['lock_lost:warn'] : ['lock_lost:warn', 'fetch_cold_failure:error'],
    )
  })
}

test('Lease renewal: a renewal reply delayed past the commit is not reported as lock_lost', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const cmds = registerLockCommands(redis)
  const gate = gateCommand(cmds.swrExtendLock.bind(redis), 'before execution')
  let reply: Promise<unknown> | undefined
  cmds.swrExtendLock = (...args) => (reply = gate.command(...args))
  const events: SwrEvent[] = []
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const cache = createSwrClient({
    redis,
    lockTtl: 0.1,
    onEvent: (event) => events.push(event),
  }).cache({
    key: 'renew-late',
    fetch: async () => {
      entered.resolve()
      await release.promise
      return 'fetched'
    },
  })

  const pending = cache.get()
  await entered.promise
  await tick(t, redis, 40)
  await gate.entered
  release.resolve()
  assert.equal(await pending, 'fetched')
  assert.equal(await redis.get(toLockKey('renew-late')), null)

  gate.release()
  assert.equal(await reply, '0')
  await setImmediate()
  assert.ok(await redis.getBuffer(toDataKey('renew-late')))
  assert.deepEqual(
    events.map((e) => (e.type === 'error' ? e.reason : e.type)),
    ['miss', 'refresh'],
  )
})

test('fetchTimeout equal to lockTtl: renewals hold the lease until the timeout, so the cooldown swap succeeds', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] })
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const events: SwrEvent[] = []
  const entered = Promise.withResolvers<AbortSignal>()
  const cache = createSwrClient({
    redis,
    lockTtl: 0.1,
    fetchTimeout: 0.1,
    onEvent: (event) => events.push(event),
  }).cache({
    key: 'timeout-lease',
    fetch: async (_ctx, { signal }) => {
      entered.resolve(signal)
      return new Promise<string>(() => {})
    },
  })

  const pending = cache.get()
  const rejected = assert.rejects(
    pending,
    (err) => err instanceof SwrTimeoutError && err.phase === 'fetch',
  )
  const signal = await entered.promise
  for (let elapsed = 0; elapsed < 100; elapsed += 20) await tick(t, redis, 20)
  await rejected
  assert.ok(signal.aborted)
  assert.equal(await redis.get(toLockKey('timeout-lease')), COOLDOWN_MARKER)
  assert.equal(await redis.pttl(toLockKey('timeout-lease')), 1000)
  assert.deepEqual(
    events.flatMap((e) => (e.type === 'error' ? [`${e.reason}:${e.severity}`] : [])),
    ['timeout:error'],
  )
})
