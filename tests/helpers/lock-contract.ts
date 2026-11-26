import test from 'node:test'
import assert from 'node:assert/strict'
import type { Redis } from 'ioredis'
import { COOLDOWN_MARKER, RedisLock, registerLockCommands } from '../../src/lock.ts'

// Shared command contract: the fake checks callers; real Redis executes the Lua.
export function lockContract(
  label: string,
  createRedis: (t: test.TestContext) => Promise<Redis | undefined>,
  expire: (redis: Redis, key: string, ms: number) => Promise<void>,
) {
  const contract = (name: string, fn: (t: test.TestContext) => Promise<void>) =>
    test(`${label}: ${name}`, fn)
  contract(
    'RedisLock.acquire: acquires only an unowned lock without replacing its owner',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const alpha = new RedisLock(redis, 'lock:user:123')
      const beta = new RedisLock(redis, 'lock:user:123')

      assert.equal(await alpha.acquire(5000), 'acquired')
      assert.deepEqual(await redis.getBuffer(alpha.key), alpha.token)
      await assertTtl(redis, alpha.key, 5000)

      assert.equal(await beta.acquire(5000), 'busy')
      assert.deepEqual(await redis.getBuffer(alpha.key), alpha.token)
    },
  )

  contract(
    'RedisLock.acquire: replayed acquire with the same token recovers ownership and restores TTL',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const owner = new RedisLock(redis, 'lock:replay:1')
      const other = new RedisLock(redis, 'lock:replay:1')

      assert.equal(await owner.acquire(2000), 'acquired')
      assert.equal(await owner.acquire(5000), 'acquired')
      assert.deepEqual(await redis.getBuffer(owner.key), owner.token)
      await assertTtl(redis, owner.key, 5000)

      assert.equal(await other.acquire(5000), 'busy')
      assert.deepEqual(await redis.getBuffer(owner.key), owner.token)
    },
  )

  contract('COMMIT_AND_RELEASE_SCRIPT: atomically sets data key and unlinks lock', async (t) => {
    const raw = await createRedis(t)
    if (!raw) return
    const redis = registerLockCommands(raw)
    const lock = new RedisLock(redis, 'lock:dataset:1')

    assert.equal(await lock.acquire(10000), 'acquired')

    const buf = Buffer.from('payload-a')
    const result = await lock.commit([{ dataKey: 'dataset:1:a', buffer: buf, ttlMs: 120_000 }])

    assert.equal(result, true)
    assert.equal(await redis.get(lock.key), null)
    assert.deepEqual(await redis.getBuffer('dataset:1:a'), buf)

    await assertTtl(redis, 'dataset:1:a', 120_000)
    await expire(redis, 'dataset:1:a', 120_000)
    assert.equal(await redis.getBuffer('dataset:1:a'), null)
  })

  contract(
    'COMMIT_AND_RELEASE_SCRIPT: overwrites secondary entries with per-entry PX',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const lock = new RedisLock(redis, 'lock:dataset:warm')

      await redis.set('dataset:warm:existing', Buffer.from('old'), 'EX', 300)
      assert.equal(await lock.acquire(10000), 'acquired')

      const primaryBuf = Buffer.from('primary-payload')
      const replacementBuf = Buffer.from('replacement-payload')
      const absentBuf = Buffer.from('absent-payload')
      const result = await lock.commit([
        { dataKey: 'dataset:warm:primary', buffer: primaryBuf, ttlMs: 120_000 },
        { dataKey: 'dataset:warm:existing', buffer: replacementBuf, ttlMs: 45_000 },
        { dataKey: 'dataset:warm:absent', buffer: absentBuf, ttlMs: 180_000 },
      ])

      assert.equal(result, true)
      assert.equal(await redis.get(lock.key), null)
      assert.deepEqual(await redis.getBuffer('dataset:warm:primary'), primaryBuf)
      assert.deepEqual(await redis.getBuffer('dataset:warm:existing'), replacementBuf)
      assert.deepEqual(await redis.getBuffer('dataset:warm:absent'), absentBuf)
      await assertTtl(redis, 'dataset:warm:primary', 120_000)
      await assertTtl(redis, 'dataset:warm:existing', 45_000)
      await assertTtl(redis, 'dataset:warm:absent', 180_000)
    },
  )

  contract(
    'COMMIT_AND_RELEASE_SCRIPT: aborts primary and secondary writes if lock ownership lost',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const owner = new RedisLock(redis, 'lock:dataset:2')
      const stale = new RedisLock(redis, 'lock:dataset:2')

      await owner.acquire(5000)

      const result = await stale.commit([
        { dataKey: 'dataset:2', buffer: Buffer.from('should-not-be-written'), ttlMs: 60_000 },
        {
          dataKey: 'dataset:2:warm',
          buffer: Buffer.from('warm-should-not-be-written'),
          ttlMs: 30_000,
        },
      ])

      assert.equal(result, false)
      assert.equal(await redis.get('dataset:2'), null)
      assert.equal(await redis.get('dataset:2:warm'), null)
      assert.deepEqual(await redis.getBuffer(owner.key), owner.token)
    },
  )

  contract(
    'COMMIT_AND_RELEASE_SCRIPT: rejects on mid-script Redis error without rollback',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const lock = new RedisLock(redis, 'lock:dataset:script-err')

      assert.equal(await lock.acquire(10000), 'acquired')

      // Omit the second secondary's PX so Redis errors mid-script after writing primary and secondary 0.
      await assert.rejects(() =>
        redis.swrCommitAndRelease(4, [
          lock.key,
          'dataset:err:primary',
          'dataset:err:ok',
          'dataset:err:bad',
          lock.token,
          Buffer.from('primary-ok'),
          120_000,
          Buffer.from('warm-ok'),
          60_000,
          Buffer.from('warm-missing-px'),
        ]),
      )

      assert.deepEqual(await redis.getBuffer(lock.key), lock.token)
      assert.deepEqual(await redis.getBuffer('dataset:err:primary'), Buffer.from('primary-ok'))
      assert.deepEqual(await redis.getBuffer('dataset:err:ok'), Buffer.from('warm-ok'))
      assert.equal(await redis.getBuffer('dataset:err:bad'), null)
    },
  )

  contract('RELEASE_LOCK_SCRIPT: compare-and-unlink releases only if token matches', async (t) => {
    const raw = await createRedis(t)
    if (!raw) return
    const redis = registerLockCommands(raw)
    const owner = new RedisLock(redis, 'lock:order:55')
    const other = new RedisLock(redis, 'lock:order:55')

    await owner.acquire(10000)

    assert.equal(await other.release(), false)
    assert.deepEqual(await redis.getBuffer(owner.key), owner.token)

    assert.equal(await owner.release(), true)
    assert.equal(await redis.get(owner.key), null)

    assert.equal(await owner.release(), false)
  })

  contract(
    'COOLDOWN_LOCK_SCRIPT: swaps only the owner token for the marker, which no token can renew or release',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const owner = new RedisLock(redis, 'lock:cooldown:1')
      const other = new RedisLock(redis, 'lock:cooldown:1')

      assert.equal(await owner.acquire(10_000), 'acquired')
      assert.equal(await other.cooldown(5000), false)
      assert.deepEqual(await redis.getBuffer(owner.key), owner.token)

      assert.equal(await owner.cooldown(5000), true)
      assert.equal(await redis.get(owner.key), COOLDOWN_MARKER)
      await assertTtl(redis, owner.key, 5000)

      assert.equal(await owner.cooldown(5000), false)
      assert.equal(await owner.release(), false)
      assert.equal(await redis.swrExtendLock(owner.key, owner.token, 50_000), '0')
      assert.equal(await redis.get(owner.key), COOLDOWN_MARKER)
      await assertTtl(redis, owner.key, 5000)
    },
  )

  contract(
    'COOLDOWN_LOCK_SCRIPT: writes no marker once the lock was cleared or expired',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const lock = new RedisLock(redis, 'lock:cooldown:cleared')

      assert.equal(await lock.acquire(10_000), 'acquired')
      await redis.unlink(lock.key)
      assert.equal(await lock.cooldown(5000), false)
      assert.equal(await redis.get(lock.key), null)
    },
  )

  contract(
    'ACQUIRE_LOCK_SCRIPT: reports cooldown, even to the previous owner, until the marker expires',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const owner = new RedisLock(redis, 'lock:cooldown:acquire')
      const next = new RedisLock(redis, 'lock:cooldown:acquire')

      assert.equal(await owner.acquire(10_000), 'acquired')
      assert.equal(await owner.cooldown(5000), true)
      assert.equal(await next.acquire(10_000), 'cooldown')
      assert.equal(await owner.acquire(10_000), 'cooldown')
      assert.equal(await redis.get(owner.key), COOLDOWN_MARKER)
      await assertTtl(redis, owner.key, 5000)

      await expire(redis, owner.key, 5000)
      assert.equal(await next.acquire(10_000), 'acquired')
      assert.deepEqual(await redis.getBuffer(next.key), next.token)
    },
  )

  contract('EXTEND_LOCK_SCRIPT: extends TTL only if token matches', async (t) => {
    const raw = await createRedis(t)
    if (!raw) return
    const redis = registerLockCommands(raw)
    const lock = new RedisLock(redis, 'lock:extend:1')

    await lock.acquire(10_000)
    const before = Number(await redis.pttl(lock.key))
    assert.equal(await redis.swrExtendLock(lock.key, Buffer.from('other-token'), 50_000), '0')
    const afterWrongToken = Number(await redis.pttl(lock.key))
    assert.ok(afterWrongToken <= before && afterWrongToken >= before - 2_000)
    assert.equal(await redis.swrExtendLock(lock.key, lock.token, 50_000), '1')
    await assertTtl(redis, lock.key, 50_000)
    // A shorter TTL never shortens the lock, and ownership is still checked first.
    assert.equal(await redis.swrExtendLock(lock.key, Buffer.from('other-token'), 10_000), '0')
    assert.equal(await redis.swrExtendLock(lock.key, lock.token, 10_000), '1')
    await assertTtl(redis, lock.key, 50_000)
    await expire(redis, lock.key, 50_000)
    assert.equal(await redis.get(lock.key), null)
  })

  contract(
    'SET_AND_CLEAR_LOCK_SCRIPT: sets data key and unlinks an owner lock or a cooldown marker',
    async (t) => {
      const raw = await createRedis(t)
      if (!raw) return
      const redis = registerLockCommands(raw)
      const owner = new RedisLock(redis, 'lock:k1')
      await owner.acquire(5000)

      const buf = Buffer.from('primed-value')
      await redis.swrSetAndClearLock('data:k1', 'lock:k1', buf, 1_500)

      assert.deepEqual(await redis.getBuffer('data:k1'), buf)
      await assertTtl(redis, 'data:k1', 1_500)
      assert.equal(await redis.get('lock:k1'), null)

      await owner.acquire(5000)
      await owner.cooldown(5000)
      await redis.swrSetAndClearLock('data:k1', 'lock:k1', Buffer.from('fixed'), 60_000)
      assert.equal(await redis.get('lock:k1'), null)
      assert.equal(await new RedisLock(redis, 'lock:k1').acquire(5000), 'acquired')
    },
  )
}

async function assertTtl(redis: Redis, key: string, expected: number) {
  const remaining = await redis.pttl(key)
  assert.ok(
    remaining > expected - 2_000 && remaining <= expected,
    `${key}: expected TTL near ${expected}ms, got ${remaining}ms`,
  )
}
