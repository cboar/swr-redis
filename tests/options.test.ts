import { decodeEnvelope } from './helpers/envelope.ts'
import { createRedisKeys } from '../src/keys.ts'
import {
  DEFAULTS,
  MAX_TIMER_MS,
  resolveCacheOptions,
  resolveClientOptions,
} from '../src/options.ts'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createSwrClient, SwrValidationError } from '../src/index.ts'
import type { SwrSharedOptions } from '../src/types.ts'
import { createFakeRedis } from './fake-redis.ts'
import { useRedisClock } from './helpers/clock.ts'

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
    label: 'same-layer oversized ttl + stale',
    options: {
      ttl: Math.floor(Number.MAX_SAFE_INTEGER / 2000) + 1,
      stale: Math.floor(Number.MAX_SAFE_INTEGER / 2000) + 1,
    },
    expectedFragment: 'ttl',
  },
  {
    label: 'oversized stale',
    options: { stale: Math.floor(Number.MAX_SAFE_INTEGER / 1000) + 1 },
    expectedFragment: 'ttl',
  },
  ...([-1, NaN, Infinity, -Infinity] as const).map((stale) => ({
    label: `stale=${String(stale)}`,
    options: { stale },
    expectedFragment: 'stale must',
  })),
  ...([0, -1, NaN, Infinity, -Infinity, MAX_TIMER_MS / 1000 + 0.001] as const).map((lockTtl) => ({
    label: `lockTtl=${String(lockTtl)}`,
    options: { lockTtl },
    expectedFragment: 'lockTtl',
  })),
  ...([0, -1, NaN, Infinity, -Infinity, MAX_TIMER_MS / 1000 + 0.001] as const).map(
    (fetchTimeout) => ({
      label: `fetchTimeout=${String(fetchTimeout)}`,
      options: { fetchTimeout },
      expectedFragment: 'fetchTimeout',
    }),
  ),
  ...([-1, NaN, -Infinity, MAX_TIMER_MS / 1000 + 0.001] as const).map((staleWait) => ({
    label: `staleWait=${String(staleWait)}`,
    options: { staleWait },
    expectedFragment: 'staleWait',
  })),
  ...([-1, NaN, Infinity, -Infinity, MAX_TIMER_MS / 1000 + 0.001] as const).map(
    (failureCooldown) => ({
      label: `failureCooldown=${String(failureCooldown)}`,
      options: { failureCooldown },
      expectedFragment: 'failureCooldown',
    }),
  ),
  ...([0, -1, NaN, Infinity, MAX_TIMER_MS / 1000 + 0.001] as const).map((pollInterval) => ({
    label: `pollInterval=${String(pollInterval)}`,
    options: { pollInterval },
    expectedFragment: 'pollInterval',
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
  await assert.rejects(swr.invalidateKeys(['']), rejects('key'))

  // Every key is validated before any eviction.
  const withMemory = createSwrClient({ redis, memory: true })
  const a = withMemory.cache({ key: 'a', fetch: async () => 'fetched' })
  await a.set(undefined, 'kept')
  await redis.unlink(keys.data('a'))
  await assert.rejects(withMemory.invalidateKeys(['a', '']), rejects('key'))
  assert.equal(await a.get(), 'kept')
})

test('Options validation: enforces merged cross-layer resolved Redis TTL ceiling', () => {
  const redis = createFakeRedis()
  const halfMaxPlusOne = Math.floor(Number.MAX_SAFE_INTEGER / 2000) + 1

  const clientWithHighTtl = createSwrClient({ redis, ttl: halfMaxPlusOne })
  assert.throws(
    () =>
      clientWithHighTtl.cache({
        key: 'cross-ttl-high-stale',
        stale: halfMaxPlusOne,
        fetch: async () => 'v',
      }),
    SwrValidationError,
  )
  clientWithHighTtl.cache({
    key: 'cross-ttl-no-stale',
    stale: 0,
    fetch: async () => 'v',
  })

  const clientWithHighStale = createSwrClient({
    redis,
    stale: Math.floor(Number.MAX_SAFE_INTEGER / 2000),
  })
  assert.throws(
    () =>
      clientWithHighStale.cache({
        key: 'cross-mult-high-ttl',
        ttl: halfMaxPlusOne,
        fetch: async () => 'v',
      }),
    SwrValidationError,
  )
  clientWithHighStale.cache({
    key: 'cross-mult-max-ttl',
    ttl: Math.floor(Number.MAX_SAFE_INTEGER / 2000),
    fetch: async () => 'v',
  })

  // A callback ttl is always positive, so an oversized stale alone fails every write.
  assert.throws(
    () =>
      createSwrClient({ redis }).cache({
        key: 'callback-ttl-oversized-stale',
        ttl: () => 1,
        stale: Math.floor(Number.MAX_SAFE_INTEGER / 1000) + 1,
        fetch: async () => 'v',
      }),
    SwrValidationError,
  )
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
      stale: 0,
      lockTtl: 0.05,
      fetchTimeout: 0.05,
      staleWait: 0,
      failureCooldown: 0,
      pollInterval: 0.001,
      compression: false,
    },
    {
      ttl: 1,
      stale: 1,
      compression: true,
    },
    {
      ttl: Math.floor(Number.MAX_SAFE_INTEGER / 1000),
      stale: 0,
    },
    {
      compression: { level: 0, minBytes: 0 },
    },
    {
      compression: { level: 9, minBytes: Number.MAX_SAFE_INTEGER },
      pollInterval: MAX_TIMER_MS / 1000,
      lockTtl: MAX_TIMER_MS / 1000,
      fetchTimeout: MAX_TIMER_MS / 1000,
      staleWait: Infinity,
      failureCooldown: MAX_TIMER_MS / 1000,
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

test('Options inheritance: client ttl and stale, per-cache overrides, and the default stale window set independent deadlines', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, ttl: 0.2, stale: 0 })
  const inherited = swr.cache({ key: 'inherited', fetch: async () => null })
  let calls = 0
  const overridden = swr.cache({
    key: 'overridden',
    ttl: ({ value }) => {
      calls++
      assert.equal(value, null)
      return 1
    },
    stale: 3,
    fetch: async () => null,
  })
  // The default stale window is 4 * ttl, so an entry stays usable for 5 * ttl.
  const defaulted = createSwrClient({ redis, ttl: 2 }).cache({
    key: 'defaulted',
    fetch: async () => null,
  })
  await inherited.get()
  await overridden.get()
  await defaulted.get()
  assert.equal(calls, 1)
  const keys = createRedisKeys(DEFAULTS.prefix)
  for (const [key, freshUntilMs, expiresAtMs] of [
    ['inherited', 1200, 1200],
    ['overridden', 2000, 5000],
    ['defaulted', 3000, 11_000],
  ] as const) {
    const stored = decodeEnvelope((await redis.getBuffer(keys.data(key)))!)!
    assert.equal(stored.freshUntilMs, freshUntilMs, key)
    assert.equal(stored.expiresAtMs, expiresAtMs, key)
    assert.equal(await redis.pttl(keys.data(key)), expiresAtMs - 1000, key)
  }
})

test('Options validation: a per-call staleWait is validated even when memory would serve the call', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const cache = createSwrClient({ redis, memory: true }).cache({
    key: 'per-call',
    fetch: async () => 'value',
  })
  assert.equal(await cache.get(), 'value')
  for (const staleWait of [-1, NaN, -Infinity, MAX_TIMER_MS / 1000 + 0.001]) {
    await assert.rejects(
      cache.get(undefined, { staleWait }),
      (err: unknown) => err instanceof SwrValidationError && err.message.includes('staleWait'),
      `staleWait=${staleWait}`,
    )
  }
  for (const staleWait of [0, 0.5, Infinity]) {
    assert.equal(await cache.get(undefined, { staleWait }), 'value')
  }
})

test('Options inheritance: timing fields merge independently, and zero disables staleWait and failureCooldown', () => {
  const client = resolveClientOptions({ fetchTimeout: 5, staleWait: 0.25, failureCooldown: 0 })
  const cache = resolveCacheOptions(client, {
    key: 'k',
    fetch: async () => 'v',
    staleWait: 0,
    lockTtl: 0.5,
  })
  assert.equal(cache.fetchTimeoutMs, 5000)
  assert.equal(cache.staleWaitMs, 0)
  assert.equal(cache.failureCooldownMs, 0)
  assert.equal(cache.lockTtlMs, 500)

  const defaults = resolveCacheOptions(resolveClientOptions({}), {
    key: 'k',
    fetch: async () => 'v',
    staleWait: Infinity,
  })
  assert.equal(defaults.fetchTimeoutMs, DEFAULTS.fetchTimeout * 1000)
  assert.equal(defaults.staleWaitMs, Infinity)
  assert.equal(defaults.failureCooldownMs, DEFAULTS.failureCooldown * 1000)
  assert.equal(defaults.lockTtlMs, DEFAULTS.lockTtl * 1000)
})
