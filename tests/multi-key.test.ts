import test from 'node:test'
import assert from 'node:assert/strict'
import { createFakeRedis } from './fake-redis.ts'
import {
  createSwrClient,
  SwrNoCache,
  SwrValidationError,
  SwrWriteBatch,
  type SwrCodec,
} from '../src/index.ts'
import { COOLDOWN_MARKER, registerLockCommands } from '../src/lock.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS } from '../src/options.ts'
import { decodeEnvelope } from './helpers/envelope.ts'
import { gateCommand } from './helpers/command-gate.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

test('SwrWriteBatch: writes different value types and codecs to different cache keys', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, memory: true })
  const countsCodec: SwrCodec<Map<string, number>> = {
    encode: (counts) => Buffer.from(JSON.stringify([...counts])),
    decode: (bytes) => new Map(JSON.parse(Buffer.from(bytes).toString())),
  }
  const counts = swr.cache({
    key: (category: string) => `counts:${category}`,
    codec: countsCodec,
    fetch: async () => {
      throw new Error('counts should have been warmed')
    },
  })
  const lastFetched = swr.cache({
    key: (category: string) => `last-fetched:${category}`,
    fetch: async () => {
      throw new Error('timestamp should have been warmed')
    },
  })
  const products = swr.cache({
    key: (category: string) => `products:${category}`,
    ttl: 60,
    staleMultiplier: 2,
    codec: {
      encode: (value: string[]) => Buffer.from(value.join(',')),
      decode: (bytes) => Buffer.from(bytes).toString().split(','),
    },
    fetch: async (category) =>
      new SwrWriteBatch({
        primary: ['sneaker'],
        secondary: [
          {
            key: counts.key(category),
            value: new Map([['sneaker', 4]]),
            codec: countsCodec,
            ttl: 30,
          },
          { key: lastFetched.key(category), value: redis.getTime() },
        ],
      }),
  })

  assert.deepEqual(await products.get('shoes'), ['sneaker'])
  assert.deepEqual(await counts.get('shoes'), new Map([['sneaker', 4]]))
  assert.deepEqual(await counts.get('shoes', { memory: false }), new Map([['sneaker', 4]]))
  assert.equal(await lastFetched.get('shoes', { memory: false }), 1000)

  const entry = decodeEnvelope((await redis.getBuffer(toDataKey('counts:shoes')))!, countsCodec)
  assert.equal(entry.cachedAtMs, 1000)
  assert.equal(entry.freshUntilMs, 31_000)
  assert.equal(entry.expiresAtMs, 61_000)
  assert.equal(await redis.pttl(toDataKey('products:shoes')), 120_000)
  assert.equal(await redis.pttl(toDataKey('counts:shoes')), 60_000)
  assert.equal(await redis.pttl(toDataKey('last-fetched:shoes')), 120_000)

  assert.deepEqual(new SwrWriteBatch({ primary: 'only', ttl: 10 }).secondary, [])
})

test('SwrWriteBatch: TTL precedence (batch override vs dynamic callback vs secondary override)', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, staleMultiplier: 3 })

  // 1. Dynamic TTL callback evaluated once with primary params/value; secondaries inherit or override
  const callbackInputs: Array<{ context: string; key: string; value: string | null }> = []
  const dynCache = swr.cache<string | null, string>({
    key: (id) => `dyn:${id}`,
    ttl: (input) => {
      callbackInputs.push(input)
      return input.value === null ? 1 : 10
    },
    fetch: async () =>
      new SwrWriteBatch<string | null>({
        primary: null,
        secondary: [
          { key: 'dyn:inherit', value: 'inherits-1s' },
          { key: 'dyn:long', value: 'override-10s', ttl: 10 },
          { key: 'dyn:short', value: 'override-0.2s', ttl: 0.2 },
        ],
      }),
  })

  assert.equal(await dynCache.get('primary'), null)
  assert.deepEqual(callbackInputs, [{ context: 'primary', key: 'dyn:primary', value: null }])

  const inheritEnv = decodeEnvelope<string>((await redis.getBuffer(toDataKey('dyn:inherit')))!)
  const longEnv = decodeEnvelope<string>((await redis.getBuffer(toDataKey('dyn:long')))!)
  const shortEnv = decodeEnvelope<string>((await redis.getBuffer(toDataKey('dyn:short')))!)
  assert.deepEqual(inheritEnv, {
    value: 'inherits-1s',
    cachedAtMs: 1000,
    freshUntilMs: 2000,
    expiresAtMs: 4000,
  })
  assert.deepEqual(longEnv, {
    value: 'override-10s',
    cachedAtMs: 1000,
    freshUntilMs: 11_000,
    expiresAtMs: 31_000,
  })
  assert.deepEqual(shortEnv, {
    value: 'override-0.2s',
    cachedAtMs: 1000,
    freshUntilMs: 1200,
    expiresAtMs: 1600,
  })
  assert.equal(await redis.pttl(toDataKey('dyn:primary')), 3000)
  assert.equal(await redis.pttl(toDataKey('dyn:inherit')), 3000)
  assert.equal(await redis.pttl(toDataKey('dyn:long')), 30_000)
  assert.equal(await redis.pttl(toDataKey('dyn:short')), 1000)

  // 2. Batch TTL overrides configured TTL without invoking callback and supplies secondary default
  const overrideCache = swr.cache({
    key: (id: string) => `override:${id}`,
    staleMultiplier: 2,
    ttl: () => {
      throw new Error('TTL callback should be skipped when batch.ttl is set')
    },
    fetch: async () =>
      new SwrWriteBatch({
        primary: 'primary-val',
        ttl: 10,
        secondary: [
          { key: 'override:inherited', value: 'sec-inherited' },
          { key: 'override:explicit', value: 'sec-explicit', ttl: 30 },
        ],
      }),
  })

  assert.equal(await overrideCache.get('primary'), 'primary-val')
  for (const [key, ttl] of [
    ['override:primary', 10],
    ['override:inherited', 10],
    ['override:explicit', 30],
  ] as const) {
    const env = decodeEnvelope<string>((await redis.getBuffer(toDataKey(key)))!)
    assert.equal(env.cachedAtMs, 1000)
    assert.equal(env.freshUntilMs, 1000 + ttl * 1000)
    assert.equal(env.expiresAtMs, 1000 + ttl * 2000)
    assert.equal(await redis.pttl(toDataKey(key)), ttl * 2000)
  }
})

test('SwrWriteBatch: rejects empty keys, primary collisions, and duplicate secondary keys before Redis and starts a cooldown', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, memory: true })

  // 1. Primary collision
  const primaryDup = swr.cache({
    key: (id: string) => `dup:${id}`,
    fetch: async (id) =>
      new SwrWriteBatch({
        primary: 'valueA',
        secondary: [{ key: `dup:${id}`, value: 'valueB' }],
      }),
  })
  await assert.rejects(
    () => primaryDup.get('p1'),
    (err: unknown) =>
      err instanceof SwrValidationError &&
      err.message.includes('Duplicate cache write key: "dup:p1"'),
  )
  assert.equal(await redis.get(toLockKey('dup:p1')), COOLDOWN_MARKER)
  assert.equal(await redis.getBuffer(toDataKey('dup:p1')), null)

  // 2. Duplicate secondary keys
  const normalizedDup = swr.cache({
    key: (params: { slug: string; tag: string }) => `norm:${params.slug.toLowerCase()}`,
    fetch: async () =>
      new SwrWriteBatch({
        primary: 'primaryVal',
        secondary: [
          { key: 'norm:alpha', value: 'val1' },
          { key: 'norm:alpha', value: 'val2' },
        ],
      }),
  })
  await assert.rejects(
    () => normalizedDup.get({ slug: 'root', tag: 'main' }),
    (err: unknown) =>
      err instanceof SwrValidationError &&
      err.message.includes('Duplicate cache write key: "norm:alpha"'),
  )
  assert.equal(await redis.get(toLockKey('norm:root')), COOLDOWN_MARKER)
  assert.equal(await redis.getBuffer(toDataKey('norm:root')), null)
  assert.equal(await redis.getBuffer(toDataKey('norm:alpha')), null)

  // 3. Empty secondary key
  const emptySec = swr.cache({
    key: (id: string) => id,
    fetch: async () =>
      new SwrWriteBatch({
        primary: 'primaryVal',
        secondary: [{ key: '', value: 'secondaryVal' }],
      }),
  })
  await assert.rejects(() => emptySec.get('valid-primary'), SwrValidationError)
  assert.equal(await redis.get(toLockKey('valid-primary')), COOLDOWN_MARKER)
  assert.equal(await redis.getBuffer(toDataKey('valid-primary')), null)
})

test('SwrWriteBatch: invalid batch or secondary TTL rejects before any Redis/memory write and starts a cooldown', async (t) => {
  const badTtls = [
    0,
    -2,
    NaN,
    Infinity,
    -Infinity,
    '10',
    Math.floor(Number.MAX_SAFE_INTEGER / 1000),
  ] as unknown as number[]

  for (const badTtl of badTtls) {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const swr = createSwrClient({ redis, memory: true, staleMultiplier: 2 })

    const badBatchTtl = swr.cache({
      key: (id: string) => `bad-batch:${id}`,
      ttl: () => {
        throw new Error('configured ttl must not be called when batch.ttl is invalid')
      },
      fetch: async () =>
        new SwrWriteBatch({
          primary: 'p',
          ttl: badTtl,
          secondary: [{ key: 'bad-batch:sec', value: 's' }],
        }),
    })
    await assert.rejects(
      () => badBatchTtl.get('p'),
      (err: unknown) => err instanceof SwrValidationError && err.message.includes('ttl'),
    )
    assert.equal(await redis.get(toLockKey('bad-batch:p')), COOLDOWN_MARKER)
    assert.equal(await redis.getBuffer(toDataKey('bad-batch:p')), null)
    assert.equal(await redis.getBuffer(toDataKey('bad-batch:sec')), null)

    const badSecTtl = swr.cache({
      key: (id: string) => `bad-sec:${id}`,
      ttl: () => 30,
      fetch: async () =>
        new SwrWriteBatch({
          primary: 'p',
          secondary: [
            { key: 'bad-sec:valid-before', value: 'should-not-write', ttl: 10 },
            { key: 'bad-sec:invalid', value: 'bad', ttl: badTtl },
          ],
        }),
    })
    await assert.rejects(
      () => badSecTtl.get('p'),
      (err: unknown) => err instanceof SwrValidationError && err.message.includes('ttl'),
    )
    assert.equal(await redis.get(toLockKey('bad-sec:p')), COOLDOWN_MARKER)
    assert.equal(await redis.getBuffer(toDataKey('bad-sec:p')), null)
    assert.equal(await redis.getBuffer(toDataKey('bad-sec:valid-before')), null)
    assert.equal(await redis.getBuffer(toDataKey('bad-sec:invalid')), null)
  }
})

test('SwrWriteBatch: encoding error on secondary fails preparation before Redis and starts a cooldown', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, memory: true })
  const encodeError = new Error('cannot encode secondary')
  let commitCalls = 0
  const commands = registerLockCommands(redis)
  const origCommit = commands.swrCommitAndRelease.bind(redis)
  commands.swrCommitAndRelease = async (...args) => {
    commitCalls++
    return origCommit(...args)
  }

  const codec: SwrCodec<string> = {
    encode: () => {
      throw encodeError
    },
    decode: (bytes) => Buffer.from(bytes).toString(),
  }
  const cache = swr.cache({
    key: (id: string) => `enc:${id}`,
    fetch: async () =>
      new SwrWriteBatch({
        primary: 'ok-primary',
        secondary: [
          { key: 'enc:good', value: 'ok-sec' },
          { key: 'enc:bad', value: 'poison', codec },
        ],
      }),
  })

  await assert.rejects(
    () => cache.get('primary'),
    (err) => err === encodeError,
  )
  assert.equal(commitCalls, 0)
  assert.equal(await redis.get(toLockKey('enc:primary')), COOLDOWN_MARKER)
  assert.equal(await redis.getBuffer(toDataKey('enc:primary')), null)
  assert.equal(await redis.getBuffer(toDataKey('enc:good')), null)
})

test('SwrWriteBatch: lock loss aborts primary and secondary writes in Redis and memory', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({
    redis,
    memory: true,
    lockTtl: 1,
  })

  const cache = swr.cache({
    key: (id: string) => `lock:${id}`,
    fetch: async () => {
      redis.advanceTime(1500) // Expire lock before commit
      return new SwrWriteBatch({
        primary: 'primary-val',
        secondary: [{ key: 'lock:sec', value: 'sec-val', ttl: 30 }],
      })
    },
  })

  assert.equal(await cache.get('p'), 'primary-val')
  assert.equal(await redis.getBuffer(toDataKey('lock:p')), null)
  assert.equal(await redis.getBuffer(toDataKey('lock:sec')), null)
})

test(
  'SwrWriteBatch: ambiguous commit reply evicts every batch key, and a set during the commit still wins in Redis',
  { timeout: 2000 },
  async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const swr = createSwrClient({ redis, memory: true })
    const commands = registerLockCommands(redis)
    const origCommit = commands.swrCommitAndRelease.bind(redis)

    const cache = swr.cache({
      key: (id: string) => `ambig:${id}`,
      fetch: async () =>
        new SwrWriteBatch({
          primary: 'p-new',
          secondary: [
            { key: 'ambig:s1', value: 's1-new' },
            { key: 'ambig:s2', value: 's2-new' },
          ],
        }),
    })

    await cache.set('s1', 's1-old')
    await cache.set('s2', 's2-old')

    const gated = gateCommand(origCommit, 'after execution')
    commands.swrCommitAndRelease = async (...args) => {
      await gated.command(...args)
      throw new Error('connection dropped after commit')
    }

    const pending = cache.get('p')
    await gated.entered
    commands.swrCommitAndRelease = origCommit

    await cache.set('s2', 's2-explicit')
    gated.release()

    assert.equal(await pending, 'p-new')

    const reads = countReads(redis)
    assert.equal(await cache.get('s1'), 's1-new')
    assert.equal(await cache.get('s2'), 's2-explicit')
    assert.equal(reads.count, 2)
  },
)

test('SwrWriteBatch: direct cacheWrite:"skip" fallback validates the batch and skips TTL callbacks and serialization', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  redis.getBuffer = async () => {
    throw new Error('Redis unavailable')
  }

  let keyCalls = 0
  let ttlCalls = 0
  let encodeCalls = 0
  const swr = createSwrClient({ redis, failureStrategy: 'availability' })
  const cache = swr.cache({
    key: (id: string) => {
      keyCalls++
      return `skip:${id}`
    },
    ttl: () => {
      ttlCalls++
      return 30
    },
    codec: {
      encode: (val: string) => {
        encodeCalls++
        return Buffer.from(val)
      },
      decode: (bytes) => Buffer.from(bytes).toString(),
    },
    fetch: async (id, meta) => {
      assert.equal(meta.cacheWrite, 'skip')
      if (id === 'invalid') {
        return new SwrWriteBatch({ primary: 'invalid', secondary: [{ key: '', value: 'ignored' }] })
      }
      return new SwrWriteBatch({
        primary: 'direct-primary',
        secondary: [{ key: 'skip:other', value: 42 }],
      })
    },
  })

  assert.equal(await cache.get('main'), 'direct-primary')
  await assert.rejects(() => cache.get('invalid'), SwrValidationError)
  assert.equal(keyCalls, 2)
  assert.equal(ttlCalls, 0)
  assert.equal(encodeCalls, 0)
})

test('SwrWriteBatch: union with SwrNoCache and plain return values works in one cache', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, memory: true })
  let calls = 0

  const cache = swr.cache({
    key: (id: string) => `mix:${id}`,
    fetch: async (id) => {
      calls++
      if (id === 'nocache') return new SwrNoCache(`nc:${calls}`)
      if (id === 'batch') {
        return new SwrWriteBatch({
          primary: 'b-primary',
          secondary: [{ key: 'mix:warmed', value: 'b-warmed', ttl: 45 }],
        })
      }
      return `plain:${id}`
    },
  })

  assert.equal(await cache.get('nocache'), 'nc:1')
  assert.equal(await cache.get('nocache'), 'nc:2')
  assert.equal(await cache.get('batch'), 'b-primary')
  assert.equal(await cache.get('warmed'), 'b-warmed')
  assert.equal(await cache.get('plain'), 'plain:plain')
  assert.equal(calls, 4)

  // Wrappers are nominal: a plain batch-shaped value is cached as an ordinary value.
  const plainBatchShaped = {
    primary: 'not-a-batch',
    ttl: 5,
    secondary: [{ key: 'other', value: 'ignored' }],
  }
  const plain = swr.cache({ key: 'plain-shape', fetch: async () => plainBatchShaped })
  assert.deepEqual(await plain.get(), plainBatchShaped)
  assert.equal(await redis.getBuffer(toDataKey('other')), null)
})

function countReads(redis: ReturnType<typeof createFakeRedis>) {
  const reads = { count: 0 }
  const getBuffer = redis.getBuffer.bind(redis)
  redis.getBuffer = async (key) => {
    reads.count++
    return getBuffer(key)
  }
  return reads
}
