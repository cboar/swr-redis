import { decodeEnvelope } from './helpers/envelope.ts'
import { createRedisKeys } from '../src/keys.ts'
import { DEFAULTS, MAX_TIMER_MS } from '../src/options.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient, SwrValidationError } from '../src/index.ts'
import type { SwrSharedOptions } from '../src/types.ts'
import { createFakeRedis } from './fake-redis.ts'
import { useRedisClock } from './helpers/clock.ts'

// Largest interval whose jittered sleep (p + floor(0.3p)) still fits a Node timer.
const MAX_POLL_INTERVAL_MS = 1_651_910_498

interface InvalidOptionCase {
  label: string
  options: SwrSharedOptions
  expectedFragment: string
}

const INVALID_OPTION_CASES: InvalidOptionCase[] = [
  ...(
    [0, -10, NaN, Infinity, -Infinity, Math.floor(Number.MAX_SAFE_INTEGER / 1000) + 1] as const
  ).map((ttl) => ({
    label: `ttl=${String(ttl)}`,
    options: { ttl },
    expectedFragment: 'ttl',
  })),
  {
    label: 'same-layer oversized ttl * staleMultiplier',
    options: { ttl: Math.floor(Number.MAX_SAFE_INTEGER / 2000) + 1, staleMultiplier: 2 },
    expectedFragment: 'ttl',
  },
  ...([0.5, 0, -1, NaN, Infinity] as const).map((staleMultiplier) => ({
    label: `staleMultiplier=${String(staleMultiplier)}`,
    options: { staleMultiplier },
    expectedFragment: 'staleMultiplier',
  })),
  ...([0, -1, NaN, Infinity, -Infinity, MAX_TIMER_MS / 1000 + 0.001] as const).map((lockTtl) => ({
    label: `lockTtl=${String(lockTtl)}`,
    options: { lockTtl },
    expectedFragment: 'lockTtl',
  })),
  ...([0, -1, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER / 1000 + 1] as const).map(
    (maxLockTtl) => ({
      label: `maxLockTtl=${String(maxLockTtl)}`,
      options: { maxLockTtl },
      expectedFragment: 'maxLockTtl',
    }),
  ),
  {
    label: 'same-layer maxLockTtl < lockTtl',
    options: { lockTtl: 10, maxLockTtl: 5 },
    expectedFragment: 'maxLockTtl',
  },
  ...([0, -1, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER / 1000 + 1] as const).map(
    (waitTimeout) => ({
      label: `waitTimeout=${String(waitTimeout)}`,
      options: { waitTimeout },
      expectedFragment: 'waitTimeout',
    }),
  ),
  ...(
    [0, -1, 1.5, NaN, Infinity, MAX_POLL_INTERVAL_MS + 1, Number.MAX_SAFE_INTEGER + 1] as const
  ).map((pollIntervalMs) => ({
    label: `pollIntervalMs=${String(pollIntervalMs)}`,
    options: { pollIntervalMs },
    expectedFragment: 'pollIntervalMs',
  })),
  ...([-1, 10, 1.5, NaN, Infinity] as const).map((level) => ({
    label: `compression.level=${String(level)}`,
    options: { compression: { level } },
    expectedFragment: 'compression.level',
  })),
  ...([-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1] as const).map((minBytes) => ({
    label: `compression.minBytes=${String(minBytes)}`,
    options: { compression: { minBytes } },
    expectedFragment: 'compression.minBytes',
  })),
]

test('Options validation: rejects invalid options on both client defaults and per-cache overrides', () => {
  const redis = createFakeRedis()
  const baseClient = createSwrClient({ redis })

  for (const { label, options, expectedFragment } of INVALID_OPTION_CASES) {
    assert.throws(
      () => createSwrClient({ redis, ...options }),
      (err: unknown) => err instanceof SwrValidationError && err.message.includes(expectedFragment),
      `Expected client validation failure for ${label}`,
    )

    assert.throws(
      () => baseClient.cache({ key: 'k', fetch: async () => 'v', ...options }),
      (err: unknown) => err instanceof SwrValidationError && err.message.includes(expectedFragment),
      `Expected cache override validation failure for ${label}`,
    )
  }
})

test('Options validation: rejects empty prefixes, cache names, and keys', async () => {
  const redis = createFakeRedis()
  const keys = createRedisKeys(DEFAULTS.prefix)
  const rejects = (fragment: string) => (err: unknown) =>
    err instanceof SwrValidationError && err.message.includes(fragment)

  assert.throws(() => createSwrClient({ redis, prefix: '' }), rejects('prefix'))
  const swr = createSwrClient({ redis })
  assert.throws(() => swr.cache({ name: '', key: 'k', fetch: async () => 'v' }), rejects('name'))
  assert.throws(() => swr.cache({ key: '', fetch: async () => 'v' }), rejects('key'))
  const dynamic = swr.cache({ key: () => '', fetch: async () => 'v' })
  assert.throws(() => dynamic.key(), rejects('key'))
  await assert.rejects(dynamic.get(), rejects('key'))
  await assert.rejects(swr.invalidateKeys(''), rejects('key'))

  // Every key is validated before any eviction.
  const withMemory = createSwrClient({ redis, memory: true })
  const a = withMemory.cache({ key: 'a', fetch: async () => 'fetched' })
  await a.set(undefined, 'kept')
  await redis.unlink(keys.data('a'))
  await assert.rejects(withMemory.invalidateKeys('a', ''), rejects('key'))
  assert.equal(await a.get(), 'kept')
})

test('Options validation: enforces merged cross-layer maxLockTtl >= lockTtl in both directions', () => {
  const redis = createFakeRedis()

  const clientWithHighLockTtl = createSwrClient({ redis, lockTtl: 10 })
  assert.throws(
    () =>
      clientWithHighLockTtl.cache({
        key: 'cross-1',
        maxLockTtl: 5,
        fetch: async () => 'v',
      }),
    (err: unknown) => err instanceof SwrValidationError && err.message.includes('maxLockTtl'),
  )
  clientWithHighLockTtl.cache({
    key: 'cross-1-eq',
    maxLockTtl: 10,
    fetch: async () => 'v',
  })

  const clientWithLowMaxLockTtl = createSwrClient({ redis, maxLockTtl: 5 })
  assert.throws(
    () =>
      clientWithLowMaxLockTtl.cache({
        key: 'cross-2',
        lockTtl: 10,
        fetch: async () => 'v',
      }),
    (err: unknown) => err instanceof SwrValidationError && err.message.includes('maxLockTtl'),
  )
  clientWithLowMaxLockTtl.cache({
    key: 'cross-2-eq',
    lockTtl: 5,
    fetch: async () => 'v',
  })

  // A client maxLockTtl below the default lockTtl stays valid for caches that lower lockTtl.
  createSwrClient({ redis, maxLockTtl: 2 }).cache({
    key: 'cross-3',
    lockTtl: 2,
    fetch: async () => 'v',
  })

  // lockTtl exceeding default ceiling (30s) without explicit maxLockTtl uses Math.max(30, lockTtl)
  const clientWithLongLockTtl = createSwrClient({ redis, lockTtl: 45 })
  clientWithLongLockTtl.cache({
    key: 'cross-default-max',
    fetch: async () => 'v',
  })
})

test('Options validation: enforces merged cross-layer resolved Redis TTL ceiling', () => {
  const redis = createFakeRedis()
  const halfMaxPlusOne = Math.floor(Number.MAX_SAFE_INTEGER / 2000) + 1

  const clientWithHighTtl = createSwrClient({ redis, ttl: halfMaxPlusOne })
  assert.throws(
    () => clientWithHighTtl.cache({ key: 'cross-ttl-default-mult', fetch: async () => 'v' }),
    SwrValidationError,
  )
  assert.throws(
    () =>
      clientWithHighTtl.cache({
        key: 'cross-ttl-mult-2',
        staleMultiplier: 2,
        fetch: async () => 'v',
      }),
    SwrValidationError,
  )
  clientWithHighTtl.cache({
    key: 'cross-ttl-mult-1',
    staleMultiplier: 1,
    fetch: async () => 'v',
  })

  const clientWithMult2 = createSwrClient({ redis, staleMultiplier: 2 })
  assert.throws(
    () =>
      clientWithMult2.cache({
        key: 'cross-mult-high-ttl',
        ttl: halfMaxPlusOne,
        fetch: async () => 'v',
      }),
    SwrValidationError,
  )
  clientWithMult2.cache({
    key: 'cross-mult-max-ttl',
    ttl: Math.floor(Number.MAX_SAFE_INTEGER / 2000),
    fetch: async () => 'v',
  })
})

test('Options: omitting memory or setting it to false uses Redis on each read', async (t) => {
  for (const options of [{}, { memory: false }] as const) {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    let reads = 0
    const getBuffer = redis.getBuffer.bind(redis)
    redis.getBuffer = (key: string) => {
      reads++
      return getBuffer(key)
    }
    let fetches = 0
    const cache = createSwrClient({ redis, ttl: 60, ...options }).cache({
      key: 'no-memory',
      fetch: async () => `val-${++fetches}`,
    })
    assert.equal(await cache.get(), 'val-1')
    assert.equal(await cache.get(), 'val-1')
    assert.deepEqual([fetches, reads], [1, 2])
  }
})

test('Options validation: validates client-only memory options', () => {
  const redis = createFakeRedis()

  for (const maxEntries of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(
      () => createSwrClient({ redis, memory: { maxEntries } }),
      (err: unknown) =>
        err instanceof SwrValidationError && err.message.includes('memory.maxEntries'),
      `Expected failure for memory.maxEntries=${String(maxEntries)}`,
    )
  }

  for (const maxTtl of [0, -5, NaN, Infinity, -Infinity]) {
    assert.throws(
      () => createSwrClient({ redis, memory: { maxTtl } }),
      (err: unknown) => err instanceof SwrValidationError && err.message.includes('memory.maxTtl'),
      `Expected failure for memory.maxTtl=${String(maxTtl)}`,
    )
  }
})

test('Options validation: accepts valid boundary values on client and cache overrides', () => {
  const redis = createFakeRedis()

  const boundaryOptions: SwrSharedOptions[] = [
    {
      ttl: 0.1,
      staleMultiplier: 1,
      lockTtl: 0.05,
      maxLockTtl: 0.05,
      waitTimeout: 0.05,
      pollIntervalMs: 1,
      compression: false,
    },
    {
      ttl: 1,
      staleMultiplier: 2,
      compression: true,
    },
    {
      ttl: Math.floor(Number.MAX_SAFE_INTEGER / 1000),
      staleMultiplier: 1,
    },
    {
      compression: { level: 0, minBytes: 0 },
    },
    {
      compression: { level: 9, minBytes: Number.MAX_SAFE_INTEGER },
      pollIntervalMs: MAX_POLL_INTERVAL_MS,
      lockTtl: MAX_TIMER_MS / 1000,
      maxLockTtl: Math.floor(Number.MAX_SAFE_INTEGER / 1000),
      waitTimeout: Math.floor(Number.MAX_SAFE_INTEGER / 1000),
    },
  ]

  for (const [idx, options] of boundaryOptions.entries()) {
    const client = createSwrClient({
      redis,
      memory: { maxEntries: 1, maxTtl: 0.1, clone: true },
      ...options,
    })
    client.cache({
      key: `boundary-${idx}`,
      fetch: async () => 'ok',
      ...options,
    })
  }

  // A dynamic ttl callback is accepted without being invoked at registration time.
  createSwrClient({ redis }).cache({
    key: 'dynamic-ttl-config',
    ttl: () => assert.fail('ttl callback must not run at registration'),
    fetch: async () => 'ok',
  })
})

test('Options inheritance: numeric client TTL and per-cache dynamic override set independent deadlines', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, ttl: 0.2, staleMultiplier: 1 })
  const inherited = swr.cache({ key: 'inherited', fetch: async () => null })
  let calls = 0
  const overridden = swr.cache({
    key: 'overridden',
    ttl: ({ value }) => {
      calls++
      assert.equal(value, null)
      return 1
    },
    fetch: async () => null,
  })
  await inherited.get()
  await overridden.get()
  assert.equal(calls, 1)
  const keys = createRedisKeys(DEFAULTS.prefix)
  for (const [key, freshUntilMs] of [
    ['inherited', 1200],
    ['overridden', 2000],
  ] as const) {
    const stored = decodeEnvelope((await redis.getBuffer(keys.data(key)))!)!
    assert.equal(stored.freshUntilMs, freshUntilMs)
    assert.equal(stored.expiresAtMs, freshUntilMs)
  }
})
