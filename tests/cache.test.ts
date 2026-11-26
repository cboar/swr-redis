import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient, SwrNoCache, SwrValidationError } from '../src/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { COOLDOWN_MARKER } from '../src/lock.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { decodeEnvelope, encodeEnvelope } from './helpers/envelope.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

test('Redis keys: lone surrogates in prefixes and keys are rejected so they cannot collide with U+FFFD', async () => {
  const redis = createFakeRedis()
  const swr = createSwrClient({ redis })
  assert.throws(
    () => createSwrClient({ redis, prefix: 'acct:\uD800' }),
    (err: unknown) => err instanceof SwrValidationError && err.message.includes('prefix'),
  )
  for (const bad of ['acct:\uD800', 'acct:\uDFFF', '\uDC00acct']) {
    const cache = swr.cache({ key: (id: string) => id, fetch: async () => 'x' })
    await assert.rejects(cache.get(bad), SwrValidationError)
    await assert.rejects(cache.set(bad, 'x'), SwrValidationError)
    assert.throws(() => swr.cache({ key: bad, fetch: async () => 'x' }), SwrValidationError)
  }
  const ok = createSwrClient({ redis, prefix: 'acct:\uFFFD' }).cache({
    key: (id: string) => id,
    fetch: async () => 'ok',
  })
  assert.equal(await ok.get('acct:\uFFFD'), 'ok')
  assert.equal(await ok.get('acct:\uD83D\uDE00'), 'ok')
})

// Redis Cluster hashes only the text between the first `{` and the next `}`, when nonempty.
function hashTag(name: string) {
  const open = name.indexOf('{')
  const close = name.indexOf('}', open + 1)
  return open !== -1 && close > open + 1 ? name.slice(open + 1, close) : name
}

test('Redis keys: type letter first and hash tag last, so data and lock share a Cluster slot', () => {
  const keys = createRedisKeys('app')
  assert.equal(keys.data('user:42'), 'SWR2:D:YXBw:{user:42}')
  assert.equal(keys.lock('user:42'), 'SWR2:L:YXBw:{user:42}')
  for (const key of ['user:42', 'a{b}c', 'a}b', '{x}']) {
    assert.equal(hashTag(keys.data(key)), hashTag(keys.lock(key)), key)
    assert.notEqual(hashTag(keys.data(key)), keys.data(key), key)
  }
})

test('Redis keys: client prefixes isolate entries without delimiter collisions', async () => {
  const redis = createFakeRedis()
  const pairs: readonly [string, string][] = [
    ['a', 'b:c'],
    ['a:b', 'c'],
  ]
  for (const [index, [prefix, key]] of pairs.entries()) {
    const cache = createSwrClient({ redis, prefix }).cache({ key, fetch: async () => index })
    assert.equal(await cache.get(), index)
    assert.equal(await cache.get(), index)
  }
})

for (const { value, ttl } of [
  { value: null, ttl: 0.25 },
  { value: 'found', ttl: 2.1 },
]) {
  test(`TTL callback: cold fetch resolves value ${String(value)} once and writes ms-precision PX`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const inputs: unknown[] = []
    const cache = createSwrClient({ redis, stale: ({ ttl }) => ttl }).cache<string | null, string>({
      key: (id) => `ttl:${id}`,
      fetch: async () => value,
      ttl: (input) => {
        inputs.push(input)
        return ttl
      },
    })
    assert.equal(await cache.get('id'), value)
    assert.deepEqual(inputs, [{ context: 'id', key: 'ttl:id', value }])
    assert.deepEqual(decodeEnvelope((await redis.getBuffer(toDataKey('ttl:id')))!), {
      value,
      cachedAtMs: 1000,
      freshUntilMs: 1000 + ttl * 1000,
      expiresAtMs: 1000 + ttl * 2000,
    })
    assert.equal(await redis.pttl(toDataKey('ttl:id')), Math.ceil(ttl * 2000))
    assert.equal(await redis.get(toLockKey('ttl:id')), null)
    assert.equal(await cache.get('id'), value)
    assert.equal(inputs.length, 1)
  })

  test(`TTL callback: set value ${String(value)} resolves context and writes ms-precision PX`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const inputs: unknown[] = []
    const cache = createSwrClient({ redis, stale: ({ ttl }) => ttl, memory: true }).cache<
      string | null,
      string
    >({
      key: (id) => `set:${id}`,
      fetch: async () => assert.fail('set must populate'),
      ttl: (input) => {
        inputs.push(input)
        return ttl
      },
    })
    await cache.set('id', value)
    assert.deepEqual(inputs, [{ context: 'id', key: 'set:id', value }])
    assert.equal(await cache.get('id'), value)
    assert.equal(inputs.length, 1)
    assert.equal(await redis.pttl(toDataKey('set:id')), Math.ceil(ttl * 2000))
    const stored = decodeEnvelope((await redis.getBuffer(toDataKey('set:id')))!)
    assert.equal(stored.freshUntilMs, 1000 + ttl * 1000)
    assert.equal(stored.expiresAtMs, 1000 + ttl * 2000)
  })
}

test('TTL callback: stale refresh resolves its new value lifetime with static undefined context', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const inputs: unknown[] = []
  let calls = 0
  const swr = createSwrClient({ redis })
  const cache = swr.cache<string | null>({
    key: 'static-ttl',
    stale: ({ ttl }) => ttl * 2,
    fetch: async () => (++calls === 1 ? null : 'recovered'),
    ttl: (input) => {
      inputs.push(input)
      return input.value === null ? 0.2 : 5
    },
  })
  assert.equal(await cache.get(), null)
  redis.advanceTime(300)
  assert.equal(await cache.get(), null)
  await swr.drain()
  assert.equal(await cache.get(), 'recovered')
  assert.deepEqual(inputs, [
    { context: undefined, key: 'static-ttl', value: null },
    { context: undefined, key: 'static-ttl', value: 'recovered' },
  ])
  assert.deepEqual(decodeEnvelope((await redis.getBuffer(toDataKey('static-ttl')))!), {
    value: 'recovered',
    cachedAtMs: 1300,
    freshUntilMs: 6300,
    expiresAtMs: 16_300,
  })
})

test('stale: 0 hard cutoff is a cold miss at the exact fractional deadline while Redis still holds the key', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  let calls = 0
  const failure = new Error('source unavailable')
  const cache = createSwrClient({ redis }).cache({
    key: 'hard-cutoff',
    ttl: () => 0.25,
    stale: 0,
    fetch: async () => {
      if (++calls > 1) throw failure
      return null
    },
  })
  assert.equal(await cache.get(), null)
  assert.equal(await redis.pttl(toDataKey('hard-cutoff')), 250)
  // Simulate Redis expiry lagging the header (command latency or reader clock skew).
  await redis.pexpire(toDataKey('hard-cutoff'), 5000)
  redis.advanceTime(249)
  assert.equal(await cache.get(), null)
  assert.equal(calls, 1)
  redis.advanceTime(1)
  assert.ok(await redis.getBuffer(toDataKey('hard-cutoff')))
  // A stale hit would return null with the default staleWait 0; a cold miss surfaces the failure.
  await assert.rejects(cache.get(), (error) => error === failure)
  assert.equal(await redis.get(toLockKey('hard-cutoff')), COOLDOWN_MARKER)
})

for (const phase of ['cold', 'stale', 'set'] as const) {
  test(`Lifetime validation: ${phase} rejects invalid ttl and stale callback results before changing Redis or memory`, async (t) => {
    const invalidResults = [-1, NaN, Infinity, -Infinity, '5', undefined, null]
    for (const [option, invalid] of [
      ...[0, ...invalidResults].map((invalid) => ['ttl', invalid] as const),
      ...invalidResults.map((invalid) => ['stale', invalid] as const),
    ] as unknown as ['ttl' | 'stale', number][]) {
      const redis = createFakeRedis(1000)
      useRedisClock(t, redis)
      let invalidResult = phase === 'cold'
      const resolve = (name: 'ttl' | 'stale') => () =>
        invalidResult && option === name ? invalid : 1
      const cache = createSwrClient({
        redis,
        memory: true,
        runtime: 'serverless',
      }).cache({
        key: 'invalid-ttl',
        ttl: resolve('ttl'),
        stale: resolve('stale'),
        fetch: async () => 'existing',
      })
      if (phase !== 'cold') await cache.set(undefined, 'existing')
      const before = await redis.getBuffer(toDataKey('invalid-ttl'))
      if (phase === 'stale') redis.advanceTime(1500)
      invalidResult = true
      await assert.rejects(
        phase === 'set' ? cache.set(undefined, 'replacement') : cache.get(),
        SwrValidationError,
        `${option}=${String(invalid)}`,
      )
      assert.deepEqual(await redis.getBuffer(toDataKey('invalid-ttl')), before)
      // A failure after the fetch still cools the key down.
      const lock = phase === 'set' ? null : COOLDOWN_MARKER
      assert.equal(await redis.get(toLockKey('invalid-ttl')), lock)
      if (phase === 'set') {
        assert.equal(await cache.get(), 'existing')
        assert.equal(await cache.get(undefined, { memory: false }), 'existing')
      }
    }
  })

  test(`TTL callback throws: ${phase} follows fetch failure semantics without mutation`, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const failure = new Error('TTL callback failed')
    let shouldThrow = phase === 'cold'
    const cache = createSwrClient({
      redis,
      memory: true,
      runtime: 'serverless',
    }).cache({
      key: 'throw-ttl',
      ttl: () => {
        if (shouldThrow) throw failure
        return 1
      },
      fetch: async () => 'existing',
    })
    if (phase !== 'cold') await cache.set(undefined, 'existing')
    const before = await redis.getBuffer(toDataKey('throw-ttl'))
    if (phase === 'stale') redis.advanceTime(1500)
    shouldThrow = true
    if (phase === 'stale') assert.equal(await cache.get(), 'existing')
    else
      await assert.rejects(
        phase === 'set' ? cache.set(undefined, 'new') : cache.get(),
        (error) => error === failure,
      )
    assert.deepEqual(await redis.getBuffer(toDataKey('throw-ttl')), before)
    const lock = phase === 'set' ? null : COOLDOWN_MARKER
    assert.equal(await redis.get(toLockKey('throw-ttl')), lock)
    if (phase === 'set') assert.equal(await cache.get(), 'existing')
  })
}

// With the default staleWait 0 a stale read returns the stored value and refreshes in the background,
// so fresh, stale, and expired reads are distinguishable by result and fetch count.
for (const { name, now, expected, fetches } of [
  { name: 'before freshness boundary', now: 1999, expected: 'writer', fetches: 0 },
  { name: 'at freshness boundary', now: 2000, expected: 'writer', fetches: 1 },
  { name: 'before expiry boundary', now: 3999, expected: 'writer', fetches: 1 },
  { name: 'at expiry boundary', now: 4000, expected: 'origin', fetches: 1 },
]) {
  test(`Redis read stored deadlines: ${name} ignores reader TTL`, async (t) => {
    const redis = createFakeRedis(now)
    useRedisClock(t, redis)
    await redis.set(
      toDataKey('writer-owned'),
      encodeEnvelope({ value: 'writer', cachedAtMs: 1000, freshUntilMs: 2000, expiresAtMs: 4000 }),
    )
    let calls = 0
    const swr = createSwrClient({ redis })
    const cache = swr.cache({
      key: 'writer-owned',
      ttl: () => assert.fail('reader TTL must not run'),
      stale: ({ ttl }) => ttl * 19,
      fetch: async () => {
        calls++
        return new SwrNoCache('origin')
      },
    })
    assert.equal(await cache.get(), expected)
    await swr.drain()
    assert.equal(calls, fetches)
  })
}

test('Oversized dynamic resolved Redis TTL (including stale) throws SwrValidationError without mutating existing entry', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, memory: true, stale: ({ ttl }) => ttl })
  const seed = swr.cache({ key: 'oversized-ttl', ttl: 10, fetch: async () => 'existing' })
  await seed.set(undefined, 'existing')
  const before = await redis.getBuffer(toDataKey('oversized-ttl'))

  const oversized = swr.cache({
    key: 'oversized-ttl',
    ttl: () => Math.floor(Number.MAX_SAFE_INTEGER / 2000) + 1,
    fetch: async () => 'replacement',
  })
  await assert.rejects(oversized.set(undefined, 'replacement'), SwrValidationError)
  assert.deepEqual(await redis.getBuffer(toDataKey('oversized-ttl')), before)
  assert.equal(await seed.get(), 'existing')
})

test('getWithMeta reports source and timing across fetch, memory, Redis, and stale hits', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  let calls = 0
  const swr = createSwrClient({ redis, memory: true, ttl: 2, stale: ({ ttl }) => ttl * 2 })
  const cache = swr.cache({ key: 'meta', fetch: async () => `v${++calls}` })
  const expectedTiming = { cachedAtMs: 1000, freshUntilMs: 3000, expiresAtMs: 7000 }

  assert.deepEqual(await cache.getWithMeta(), {
    value: 'v1',
    source: 'fetch',
    timing: expectedTiming,
  })

  redis.advanceTime(500)
  assert.deepEqual(await cache.getWithMeta(), {
    value: 'v1',
    source: 'memory',
    timing: expectedTiming,
  })

  assert.deepEqual(await cache.getWithMeta(undefined, { memory: false }), {
    value: 'v1',
    source: 'redis',
    timing: expectedTiming,
  })

  redis.advanceTime(2000)
  assert.deepEqual(await cache.getWithMeta(), {
    value: 'v1',
    source: 'redis',
    timing: expectedTiming,
  })
  await swr.drain()
})

test('getWithMeta omits timing for uncached SwrNoCache and reports fallback fetches as direct', async () => {
  const redis = createFakeRedis()
  const swr = createSwrClient({ redis })
  const noCache = swr.cache({
    key: 'no-cache',
    fetch: async () => new SwrNoCache('uncached'),
  })
  assert.deepEqual(await noCache.getWithMeta(), {
    value: 'uncached',
    source: 'fetch',
  })

  redis.getBuffer = async () => {
    throw new Error('redis down')
  }
  const fallback = swr.cache({ key: 'fallback', fetch: async () => 'direct' })
  assert.deepEqual(await fallback.getWithMeta(), {
    value: 'direct',
    source: 'direct',
  })
  const noCacheFallback = swr.cache({
    key: 'no-cache-fallback',
    fetch: async () => new SwrNoCache('direct-nc'),
  })
  assert.deepEqual(await noCacheFallback.getWithMeta(), { value: 'direct-nc', source: 'direct' })
})
