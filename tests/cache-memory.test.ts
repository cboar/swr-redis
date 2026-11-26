import test, { type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { SwrCacheImpl } from '../src/cache.ts'
import { createSwrClient, SwrRedisError, SwrWriteBatch } from '../src/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { COOLDOWN_MARKER, registerLockCommands } from '../src/lock.ts'
import { DEFAULTS } from '../src/options.ts'
import type { SwrCodec, SwrEvent } from '../src/types.ts'
import { createFakeRedis } from './fake-redis.ts'
import { gateCommand } from './helpers/command-gate.ts'
import { useRedisClock } from './helpers/clock.ts'
import { decodeEnvelope, encodeEnvelope } from './helpers/envelope.ts'
import { createLruMemory, memoryHarness } from './helpers/memory-harness.ts'

const { data: toDataKey, lock: toLockKey } = createRedisKeys(DEFAULTS.prefix)

const stored = <T>(value: T, freshUntilMs = 5000, codec?: SwrCodec<T>) =>
  encodeEnvelope({ value, cachedAtMs: 1000, freshUntilMs, expiresAtMs: 10_000 }, codec)

function countReads(redis: ReturnType<typeof createFakeRedis>) {
  const reads = { count: 0 }
  const getBuffer = redis.getBuffer.bind(redis)
  redis.getBuffer = async (key) => {
    reads.count++
    return getBuffer(key)
  }
  return reads
}

function fakeClient(t: TestContext) {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  return { redis, reads: countReads(redis) }
}

test('Memory: caches on a client share memory, kept consistent by set, invalidate and invalidateKeys', async (t) => {
  const { redis, reads } = fakeClient(t)
  const swr = createSwrClient({ redis, memory: { maxEntries: 50 } })
  let aCalls = 0
  const cacheA = swr.cache({
    key: (id: string) => `shared:${id}`,
    fetch: async (id) => `A-${id}-${++aCalls}`,
  })
  let bCalls = 0
  const cacheB = swr.cache({
    key: (id: string) => `shared:${id}`,
    fetch: async (id) => `B-${id}-${++bCalls}`,
  })

  assert.equal(await cacheA.get('1'), 'A-1-1')
  assert.equal(await cacheB.get('1'), 'A-1-1')
  assert.equal(reads.count, 1)

  await cacheA.set('1', 'manual-1')
  assert.equal(await cacheB.get('1'), 'manual-1')
  assert.equal(reads.count, 1)

  await cacheB.invalidate('1')
  assert.equal(await cacheA.get('1'), 'A-1-2')

  await swr.invalidateKeys(['shared:1', 'other:key'])
  assert.equal(await redis.getBuffer(toDataKey('shared:1')), null)
  assert.equal(await cacheB.get('1'), 'B-1-1')
})

test('Memory: caches on a client share a single LRU budget', async (t) => {
  const { redis, reads } = fakeClient(t)
  const swr = createSwrClient({ redis, memory: { maxEntries: 2 }, ttl: 60 })
  const cacheA = swr.cache<string, string>({
    key: (id) => `a:${id}`,
    fetch: async (id) => `A-${id}`,
  })
  const cacheB = swr.cache<string, string>({
    key: (id) => `b:${id}`,
    fetch: async (id) => `B-${id}`,
  })

  assert.equal(await cacheA.get('1'), 'A-1')
  assert.equal(await cacheB.get('1'), 'B-1')
  assert.equal(await cacheA.get('1'), 'A-1') // b:1 is now least recently used
  assert.equal(await cacheB.get('2'), 'B-2')
  const before = reads.count
  assert.equal(await cacheA.get('1'), 'A-1')
  assert.equal(reads.count, before)
  assert.equal(await cacheB.get('1'), 'B-1')
  assert.equal(reads.count, before + 1)
})

test('Memory: readers honor the writer-owned freshness without calling their own ttl', async (t) => {
  const { redis, reads } = fakeClient(t)
  const swr = createSwrClient({
    redis,
    memory: { maxEntries: 50, maxTtl: 10 },
  })
  let writerTtlCalls = 0
  const writer = swr.cache<string | null>({
    key: 'shared-memory-dyn',
    stale: ({ ttl }) => ttl * 2,
    ttl: ({ value }) => {
      writerTtlCalls++
      return value === null ? 1 : 5
    },
    fetch: async () => null,
  })
  let readerTtlCalls = 0
  const reader = swr.cache<string | null>({
    key: 'shared-memory-dyn',
    ttl: () => {
      readerTtlCalls++
      return 0.1
    },
    fetch: async () => assert.fail('reader must not fetch'),
  })

  assert.equal(await writer.get(), null)
  assert.equal(writerTtlCalls, 1)
  redis.advanceTime(500)
  const before = reads.count
  assert.equal(await reader.get(), null)
  assert.equal(reads.count, before)
  assert.equal(await reader.get(undefined, { memory: false }), null)
  assert.equal(reads.count, before + 1)
  assert.equal(readerTtlCalls, 0)
})

test('Memory lifetime wiring: delayed Redis read uses the remaining non-sliding memory cap', async (t) => {
  const { redis, reads } = fakeClient(t)
  const writer = createSwrClient({ redis, memory: true }).cache<string | null>({
    key: 'dyn-memory-delay',
    stale: ({ ttl }) => ttl,
    ttl: ({ value }) => (value === null ? 10 : 1),
    fetch: async () => null,
  })
  assert.equal(await writer.get(), null)
  assert.equal(reads.count, 1)

  redis.advanceTime(1500)
  const reader = createSwrClient({ redis, memory: { maxEntries: 10, maxTtl: 2 } }).cache<
    string | null
  >({
    key: 'dyn-memory-delay',
    ttl: () => assert.fail('Redis read must not resolve reader TTL'),
    fetch: async () => 'unexpected',
  })
  assert.equal(await reader.get(), null)
  assert.equal(reads.count, 2)
  redis.advanceTime(499)
  assert.equal(await reader.get(), null)
  assert.equal(reads.count, 2)
  redis.advanceTime(1)
  assert.equal(await reader.get(), null)
  assert.equal(await reader.get(), null)
  assert.equal(reads.count, 4)
})

test('Memory lifetime wiring: stored freshness expires a memory entry before the memory cap', async (t) => {
  const { redis, reads } = fakeClient(t)
  const swr = createSwrClient({
    redis,
    memory: { maxEntries: 50, maxTtl: 0.5 },
    runtime: 'serverless',
  })
  let fetchCalls = 0
  const cache = swr.cache<string | null>({
    key: 'dyn-memory-short',
    stale: ({ ttl }) => ttl,
    ttl: ({ value }) => (value === null ? 0.05 : 2),
    fetch: async () => (++fetchCalls === 1 ? null : 'recovered'),
  })
  assert.equal(await cache.get(), null)
  redis.advanceTime(49)
  assert.equal(await cache.get(), null)
  assert.equal(reads.count, 1)
  redis.advanceTime(1)
  assert.equal(await cache.get(), 'recovered')
  assert.equal(reads.count, 2)
})

const scopes = ['cache', 'request'] as const
type Scope = (typeof scopes)[number]

function optOutClient(
  t: TestContext,
  scope: Scope,
  options: { ttl?: number; runtime?: 'server' } = {},
) {
  const { redis, reads } = fakeClient(t)
  const swr = createSwrClient({
    redis,
    memory: true,
    runtime: 'serverless',
    pollInterval: 0.001,
    ...options,
  })
  const getOptions = scope === 'request' ? { memory: false as const } : undefined
  return {
    redis,
    reads,
    swr,
    getOptions,
    cacheMemory: scope === 'cache' ? (false as const) : undefined,
  }
}

test('Memory opt-out (cache and request): reads skip memory and do not populate it', async (t) => {
  for (const scope of scopes) {
    const { reads, swr, getOptions, cacheMemory } = optOutClient(t, scope)
    let fetches = 0
    const fetch = async () => `v${++fetches}`
    const shared = swr.cache({ key: (id: string) => `opt:${id}`, fetch })
    const opted = swr.cache({ key: (id: string) => `opt:${id}`, memory: cacheMemory, fetch })
    assert.equal(await opted.get('cold', getOptions), 'v1', scope)
    assert.equal(await opted.get('cold', getOptions), 'v1', scope)
    assert.equal(reads.count, 2, scope)
    assert.equal(await shared.get('cold'), 'v1', scope)
    assert.equal(await shared.get('cold'), 'v1', scope)
    assert.equal(reads.count, 3, scope)
    assert.equal(await opted.get('cold', getOptions), 'v1', scope)
    assert.equal(reads.count, 4, scope)
    assert.equal(fetches, 1, scope)
  }
})

test('Memory opt-out (cache and request): fetch commits evict primary and batch secondary', async (t) => {
  for (const scope of scopes) {
    const { redis, reads, swr, getOptions, cacheMemory } = optOutClient(t, scope)
    let tag = 'old'
    const fetch = async () =>
      new SwrWriteBatch({ primary: `p-${tag}`, secondary: [{ key: 'sec', value: `sec-${tag}` }] })
    const shared = swr.cache({ key: (id: string) => id, fetch })
    const opted = swr.cache({ key: (id: string) => id, memory: cacheMemory, fetch })
    assert.equal(await shared.get('p'), 'p-old', scope)
    // Both memory entries stay fresh; only Redis loses the primary, forcing the opted-out fetch.
    await redis.unlink(toDataKey('p'))
    tag = 'new'
    assert.equal(await opted.get('p', getOptions), 'p-new', scope)
    const before = reads.count
    assert.equal(await shared.get('sec'), 'sec-new', scope)
    assert.equal(await shared.get('p'), 'p-new', scope)
    assert.equal(reads.count, before + 2, scope)
  }
})

test('Memory opt-out (cache and request): background refresh does not populate', async (t) => {
  for (const scope of scopes) {
    const { redis, reads, swr, getOptions, cacheMemory } = optOutClient(t, scope, {
      ttl: 2,
      runtime: 'server',
    })
    let value = 'old'
    const fetch = async () => value
    const shared = swr.cache({ key: 'background', fetch })
    const opted = swr.cache({ key: 'background', memory: cacheMemory, fetch })
    await shared.get()
    redis.advanceTime(3000)
    value = 'new'
    assert.equal(await opted.get(undefined, getOptions), 'old', scope)
    await swr.drain()
    const before = reads.count
    assert.equal(await shared.get(), 'new', scope)
    assert.equal(reads.count, before + 1, scope)
  }
})

test('Memory opt-out (cache and request): a cold-poll hit does not populate', async (t) => {
  for (const scope of scopes) {
    const { redis, reads, swr, getOptions, cacheMemory } = optOutClient(t, scope)
    const unexpected = async () => assert.fail('waiter must not fetch')
    const shared = swr.cache({ key: 'poll-opt', fetch: unexpected })
    const opted = swr.cache({ key: 'poll-opt', memory: cacheMemory, fetch: unexpected })
    await redis.set(toLockKey('poll-opt'), 'other-owner', 'PX', 5000)
    const mgetBuffer = redis.mgetBuffer.bind(redis)
    redis.mgetBuffer = (async (...keys: string[]) => {
      await redis.set(toDataKey('poll-opt'), stored('polled'))
      await redis.unlink(toLockKey('poll-opt'))
      return mgetBuffer(...keys)
    }) as typeof redis.mgetBuffer
    assert.equal(await opted.get(undefined, getOptions), 'polled', scope)
    const before = reads.count
    assert.equal(await shared.get(), 'polled', scope)
    assert.equal(reads.count, before + 1, scope)
  }
})

test('Memory opt-out (cache): set and invalidate evict shared memory', async (t) => {
  const { reads, swr } = optOutClient(t, 'cache', { ttl: 60 })
  let fetches = 0
  const shared = swr.cache({ key: 'p:1', fetch: async () => `origin-${++fetches}` })
  const opted = swr.cache({ key: 'p:1', memory: false, fetch: async () => 'unused' })
  assert.equal(await shared.get(), 'origin-1')

  await opted.set(undefined, 'p-set')
  let before = reads.count
  assert.equal(await shared.get(), 'p-set')
  assert.equal(await shared.get(), 'p-set')
  assert.equal(reads.count, before + 1)

  await opted.invalidate()
  before = reads.count
  assert.equal(await shared.get(), 'origin-2')
  assert.equal(await shared.get(), 'origin-2')
  assert.equal(reads.count, before + 1)
})

test('Failed writes evict memory and report the affected keys under both failure strategies', async (t) => {
  for (const failureStrategy of ['availability', 'protect-source'] as const) {
    for (const operation of ['set', 'invalidate', 'invalidateKeys'] as const) {
      const label = `${operation} under ${failureStrategy}`
      const { redis, reads } = fakeClient(t)
      const events: SwrEvent[] = []
      const swr = createSwrClient({
        redis,
        memory: true,
        failureStrategy,
        onEvent: (event) => events.push(event),
      })
      const cache = swr.cache({
        name: 'writer',
        key: (key: string) => key,
        fetch: async () => 'unexpected',
      })
      const keys = operation === 'invalidateKeys' ? ['a', 'b', 'c'] : ['a']
      for (const key of keys) {
        await cache.set(key, `old-${key}`)
        assert.equal(await cache.get(key), `old-${key}`, label)
      }
      assert.equal(reads.count, 0, label)
      events.length = 0

      const failure = new Error('Redis unavailable')
      const down = async () => {
        throw failure
      }
      if (operation === 'set') registerLockCommands(redis).swrSetAndClearLock = down
      else redis.unlink = down
      const run =
        operation === 'set'
          ? cache.set('a', 'new')
          : operation === 'invalidate'
            ? cache.invalidate('a')
            : swr.invalidateKeys(keys)
      const protectsSource = failureStrategy === 'protect-source'
      if (protectsSource) {
        await assert.rejects(
          run,
          (error: unknown) => {
            assert.ok(error instanceof SwrRedisError)
            assert.equal(error.cause, failure)
            assert.equal(error.key, 'a')
            assert.deepEqual(error.keys, keys)
            if (operation === 'invalidateKeys') assert.match(error.message, /3 keys/)
            return true
          },
          label,
        )
      } else await run
      assert.deepEqual(
        events,
        [
          {
            type: 'error',
            reason: protectsSource ? 'redis_error' : 'redis_error_fallback',
            severity: protectsSource ? 'error' : 'warn',
            ...(operation === 'invalidateKeys' ? {} : { cache: 'writer' }),
            key: 'a',
            keys,
            err: failure,
          },
        ],
        label,
      )
      for (const key of keys) assert.equal(await cache.get(key), `old-${key}`, label)
      assert.equal(reads.count, keys.length, label)
    }
  }
})

test('Memory: a rejected commit reply evicts every batch key, whether or not the commit executed', async (t) => {
  for (const executed of [false, true]) {
    const { redis, commands, cache } = memoryHarness<string, string>(t, {
      key: (key) => key,
      ttl: 60,
      fetch: async () =>
        new SwrWriteBatch({ primary: 'p-new', secondary: [{ key: 'sec', value: 'sec-new' }] }),
    })
    await cache.set('sec', 'sec-old')
    const execute = commands.swrCommitAndRelease.bind(redis)
    commands.swrCommitAndRelease = async (...args) => {
      commands.swrCommitAndRelease = execute
      if (executed) await execute(...args)
      throw new Error('commit reply lost')
    }
    assert.equal(await cache.get('p'), 'p-new')
    assert.equal(await redis.get(toLockKey('p')), null)
    const reads = countReads(redis)
    assert.equal(await cache.get('sec'), executed ? 'sec-new' : 'sec-old', `executed=${executed}`)
    assert.equal(reads.count, 1, `executed=${executed}`)
  }
})

test('Memory: a resent commit that reports lock_lost still evicts, and a delayed read cannot reinstall the old value', async (t) => {
  const events: SwrEvent[] = []
  const { redis, commands, cache } = memoryHarness<string, string>(
    t,
    {
      key: (key) => key,
      ttl: 60,
      fetch: async () =>
        new SwrWriteBatch({ primary: 'p-new', secondary: [{ key: 'sec', value: 'sec-new' }] }),
    },
    { onEvent: (event) => events.push(event) },
  )
  await redis.set(toDataKey('sec'), stored('sec-old'))
  const getBuffer = redis.getBuffer.bind(redis)
  const delayed = gateCommand(getBuffer, 'after execution')
  redis.getBuffer = delayed.command
  const delayedRead = cache.get('sec')
  await delayed.entered
  redis.getBuffer = getBuffer
  assert.equal(await cache.get('sec'), 'sec-old') // warms memory

  // The first run commits and releases the lock; its reply is lost and the resent run finds no lock.
  const execute = commands.swrCommitAndRelease.bind(redis)
  commands.swrCommitAndRelease = async (...args) => {
    await execute(...args)
    return execute(...args)
  }
  assert.equal(await cache.get('p'), 'p-new')
  assert.ok(events.some((e) => e.type === 'error' && e.reason === 'lock_lost'))

  delayed.release()
  assert.equal(await delayedRead, 'sec-old')
  const reads = countReads(redis)
  assert.equal(await cache.get('sec'), 'sec-new')
  assert.equal(await cache.get('p'), 'p-new')
  assert.equal(reads.count, 2)
})

test('Memory clone: a value structuredClone rejects fails like an encode error, before Redis', async (t) => {
  type Value = { tag: string; fn?: () => void }
  const codec: SwrCodec<Value> = {
    encode: (v) => Buffer.from(JSON.stringify({ tag: v.tag })),
    decode: (b) => JSON.parse(Buffer.from(b).toString()) as Value,
  }
  const uncloneable = { tag: 'new', fn: () => {} }
  for (const path of ['set', 'cold fetch', 'stale fetch', 'batch secondary'] as const) {
    const events: SwrEvent[] = []
    const { redis, cache } = memoryHarness<Value>(
      t,
      {
        key: 'clone',
        ttl: 5,
        codec,
        fetch: async () =>
          path === 'batch secondary'
            ? new SwrWriteBatch({
                primary: { tag: 'new' },
                secondary: [{ key: 'clone:sec', value: uncloneable, codec }],
              })
            : uncloneable,
      },
      { runtime: 'serverless', onEvent: (event) => events.push(event) },
      createLruMemory({ clone: true }),
    )
    if (path === 'set' || path === 'stale fetch') {
      await redis.set(
        toDataKey('clone'),
        stored({ tag: 'old' }, path === 'set' ? 5000 : 1000, codec),
      )
    }

    if (path === 'set') {
      assert.deepEqual(await cache.get(), { tag: 'old' })
      await assert.rejects(cache.set(undefined, uncloneable), { name: 'DataCloneError' })
      assert.deepEqual(await cache.get(), { tag: 'old' }, path)
    } else if (path === 'stale fetch') {
      assert.deepEqual(await cache.get(), { tag: 'old' }, path)
      assert.ok(events.some((e) => e.type === 'error' && e.reason === 'stale_fallback'))
    } else {
      await assert.rejects(cache.get(), { name: 'DataCloneError' }, path)
      assert.ok(events.some((e) => e.type === 'error' && e.reason === 'fetch_cold_failure'))
    }
    const expected = path === 'cold fetch' || path === 'batch secondary' ? null : { tag: 'old' }
    const data = await redis.getBuffer(toDataKey('clone'))
    assert.deepEqual(data && decodeEnvelope(data, codec).value, expected, path)
    assert.equal(await redis.getBuffer(toDataKey('clone:sec')), null, path)
    const lock = path === 'set' ? null : COOLDOWN_MARKER
    assert.equal(await redis.get(toLockKey('clone')), lock, path)
  }

  for (const scope of scopes) {
    const { redis, cache } = memoryHarness<Value>(
      t,
      {
        key: 'clone-opt',
        ttl: 5,
        codec,
        ...(scope === 'cache' && { memory: false as const }),
        fetch: async () => uncloneable,
      },
      {},
      createLruMemory({ clone: true }),
    )
    if (scope === 'cache') await cache.set(undefined, uncloneable)
    else assert.equal(await cache.get(undefined, { memory: false }), uncloneable, scope)
    const data = await redis.getBuffer(toDataKey('clone-opt'))
    assert.deepEqual(data && decodeEnvelope(data, codec).value, { tag: 'new' }, scope)
  }
})

test('Memory clone: a populate clone error propagates on the initial read and on a cold poll', async (t) => {
  for (const path of ['initial read', 'cold poll'] as const) {
    let value: Record<string, unknown> = { fn: () => {} }
    const codec = { encode: () => new Uint8Array([1]), decode: () => value }
    const { redis, cache } = memoryHarness<Record<string, unknown>>(
      t,
      { key: 'read-clone', codec, fetch: async () => assert.fail('must not fetch') },
      { pollInterval: 0.005 },
      createLruMemory({ clone: true }),
    )
    const envelope = encodeEnvelope(
      { value, cachedAtMs: 1000, freshUntilMs: 5000, expiresAtMs: 10_000 },
      codec,
    )
    if (path === 'initial read') {
      await redis.set(toDataKey('read-clone'), envelope)
    } else {
      await redis.set(toLockKey('read-clone'), 'other-owner', 'PX', 5000)
      const mgetBuffer = redis.mgetBuffer.bind(redis)
      redis.mgetBuffer = (async (...keys: string[]) => {
        redis.mgetBuffer = mgetBuffer
        await redis.set(toDataKey('read-clone'), envelope)
        return mgetBuffer(...keys)
      }) as typeof redis.mgetBuffer
    }
    await assert.rejects(cache.get(), { name: 'DataCloneError' }, path)
    value = { ok: true }
    assert.deepEqual(await cache.get(), { ok: true }, path)
  }
})

test('Memory clone: a populate clone error during a stale wait serves the stale snapshot', async (t) => {
  const events: SwrEvent[] = []
  let fetches = 0
  const text: SwrCodec<string> = {
    encode: (value) => Buffer.from(value),
    decode: (bytes) => Buffer.from(bytes).toString(),
  }
  // The replacement decodes to a value structuredClone rejects.
  const codec: SwrCodec<Record<string, unknown>> = {
    encode: () => assert.fail('must not encode'),
    decode: (bytes) => (text.decode(bytes) === 'stale' ? { tag: 'stale' } : { fn: () => {} }),
  }
  const { redis, cache } = memoryHarness<Record<string, unknown>>(
    t,
    {
      key: 'stale-clone',
      codec,
      fetch: async () => ({ fetched: ++fetches }),
    },
    { staleWait: 10, failureStrategy: 'protect-source', onEvent: (e) => events.push(e) },
    createLruMemory({ clone: true }),
  )
  await redis.set(
    toDataKey('stale-clone'),
    encodeEnvelope({ value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 10_000 }, text),
  )
  await redis.set(toLockKey('stale-clone'), 'other-owner', 'PX', 5000)
  const mgetBuffer = redis.mgetBuffer.bind(redis)
  redis.mgetBuffer = (async (...keys: string[]) => {
    redis.mgetBuffer = mgetBuffer
    await redis.set(toDataKey('stale-clone'), stored('fresh', 5000, text))
    return mgetBuffer(...keys)
  }) as typeof redis.mgetBuffer

  assert.deepEqual(await cache.get(), { tag: 'stale' })
  assert.equal(fetches, 0)
  const fallback = events.find((e) => e.type === 'error')
  assert.ok(fallback?.type === 'error' && fallback.reason === 'stale_fallback')
  assert.equal(fallback.stage, 'read')
  assert.equal((fallback.err as Error).name, 'DataCloneError')
})

test('Memory clone: set() stores a clone, so mutating the input afterwards does not reach memory', async (t) => {
  const { redis, cache } = memoryHarness<{ nested: { tags: string[] } }>(
    t,
    { key: 'set-clone', fetch: async () => assert.fail('must not fetch') },
    {},
    createLruMemory({ clone: true }),
  )
  const value = { nested: { tags: ['original'] } }
  await cache.set(undefined, value)
  value.nested.tags.push('after-set')
  const reads = countReads(redis)
  assert.deepEqual(await cache.get(), { nested: { tags: ['original'] } })
  assert.equal(reads.count, 0)
})

test('Memory: a stale Redis read does not take an LRU slot from a fresh entry', async (t) => {
  const { redis, cache } = memoryHarness<string, string>(
    t,
    { key: (key) => key, fetch: async () => assert.fail('must not fetch') },
    {},
    createLruMemory({ maxEntries: 1 }),
  )
  await cache.set('fresh', 'fresh-value')
  await redis.set(toDataKey('stale'), stored('stale-value', 1000))
  await redis.set(toLockKey('stale'), 'other-owner', 'PX', 5000)
  assert.equal(await cache.get('stale'), 'stale-value')
  const reads = countReads(redis)
  assert.equal(await cache.get('fresh'), 'fresh-value')
  assert.equal(reads.count, 0)
})

test('Memory clone: a write settled synchronously by a populate clone getter blocks the populate', async (t) => {
  const { redis, cache } = memoryHarness<{ tag: string }>(
    t,
    {
      key: 'sync-settle',
      codec: {
        encode: (v) => Buffer.from(v.tag),
        decode: (bytes) => {
          const tag = Buffer.from(bytes).toString()
          if (tag !== 'old') return { tag }
          let invalidation: Promise<void> | undefined
          return {
            get tag() {
              invalidation ??= cache.invalidate()
              return 'old'
            },
          }
        },
      },
      fetch: async () => assert.fail('must not fetch'),
    },
    {},
    createLruMemory({ clone: true }),
  )
  const codec = { encode: (v: { tag: string }) => Buffer.from(v.tag), decode: () => ({ tag: '' }) }
  await redis.set(toDataKey('sync-settle'), stored({ tag: 'old' }, 5000, codec))
  const read = gateCommand(redis.getBuffer.bind(redis), 'after execution')
  redis.getBuffer = read.command
  const pending = cache.get()
  await read.entered
  await redis.set(toDataKey('sync-settle'), stored({ tag: 'new' }, 5000, codec))
  // The invalidation's Redis call throws before any await, so it settles inside the getter.
  redis.unlink = (() => {
    throw new RangeError('Maximum call stack size exceeded')
  }) as typeof redis.unlink
  read.release()
  assert.equal((await pending).tag, 'old')
  assert.deepEqual(await cache.get(), { tag: 'new' })
})

test('Memory: a delayed read cannot repopulate after a completed local write', async (t) => {
  const writes = ['set', 'invalidate', 'invalidateKeys', 'opted-out set', 'opted-out invalidate']
  for (const write of writes) {
    const { redis, cache, internals } = memoryHarness(t, {
      key: 'read-guard',
      fetch: async () => 'fetched',
    })
    const writer = write.startsWith('opted-out')
      ? new SwrCacheImpl(internals, { key: 'read-guard', memory: false, fetch: async () => '' })
      : cache
    await redis.set(toDataKey('read-guard'), stored('old'))
    const read = gateCommand(redis.getBuffer.bind(redis), 'after execution')
    redis.getBuffer = read.command
    const pending = cache.get()
    await read.entered
    if (write.endsWith('set')) await writer.set(undefined, 'explicit')
    else if (write.endsWith('invalidate')) await writer.invalidate()
    else await internals.invalidate(['read-guard'], 'availability')
    read.release()
    assert.equal(await pending, 'old', write)
    assert.equal(await cache.get(), write.endsWith('set') ? 'explicit' : 'fetched', write)
  }
})

test('Memory: a delayed MGET poll cannot repopulate after a completed invalidate', async (t) => {
  for (const path of ['cold poll', 'stale poll'] as const) {
    const { redis, cache } = memoryHarness(
      t,
      { key: 'polled', ttl: 1, fetch: async () => 'fresh' },
      { staleWait: 10 },
    )
    if (path === 'stale poll') await redis.set(toDataKey('polled'), stored('stale', 1000))
    await redis.set(toLockKey('polled'), 'owner', 'PX', 5000)
    const mgetBuffer = redis.mgetBuffer.bind(redis)
    const captured = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    redis.mgetBuffer = (async (...keys: string[]) => {
      redis.mgetBuffer = mgetBuffer
      await redis.set(toDataKey('polled'), stored('polled-value'))
      await redis.unlink(toLockKey('polled'))
      const response = await mgetBuffer(...keys)
      captured.resolve()
      await release.promise
      return response
    }) as typeof redis.mgetBuffer
    const pending = cache.get()
    await captured.promise
    await cache.invalidate()
    release.resolve()
    assert.equal(await pending, 'polled-value', path)
    assert.equal(await cache.get(), 'fresh', path)
  }
})

test('Memory: a delayed commit reply cannot reinstall the fetched value after a completed set or invalidate', async (t) => {
  for (const write of ['set', 'invalidate'] as const) {
    let fetches = 0
    const { redis, commands, cache } = memoryHarness(t, {
      key: 'commit-response',
      fetch: async () => `fetched-${++fetches}`,
    })
    const commit = gateCommand(commands.swrCommitAndRelease.bind(redis), 'after execution')
    commands.swrCommitAndRelease = commit.command
    const pending = cache.get()
    await commit.entered
    if (write === 'set') await cache.set(undefined, 'explicit')
    else await cache.invalidate()
    commit.release()
    assert.equal(await pending, 'fetched-1', write)
    assert.equal(await cache.get(), write === 'set' ? 'explicit' : 'fetched-2', write)
  }
})

test('Memory: a set started by a getter during a populate clone wins', async (t) => {
  const {
    redis,
    cache: writer,
    internals,
  } = memoryHarness<{ tag: string }>(
    t,
    { key: 'getter', fetch: async () => assert.fail('must not fetch') },
    {},
    createLruMemory({ clone: true }),
  )
  let write: Promise<void> | undefined
  const reader = new SwrCacheImpl<{ tag: string }>(internals, {
    key: 'getter',
    codec: {
      encode: () => new Uint8Array([1]),
      decode: () => ({
        get tag() {
          write ??= writer.set(undefined, { tag: 'explicit' })
          return 'old'
        },
      }),
    },
    fetch: async () => assert.fail('must not fetch'),
  })
  await redis.set(toDataKey('getter'), stored({ tag: 'old' }))
  assert.equal((await reader.get()).tag, 'old')
  await write
  assert.deepEqual(await writer.get(), { tag: 'explicit' })
})

test('Memory: set() during a slow fetch wins; the fetch reports lock_lost and evicts', async (t) => {
  const events: SwrEvent[] = []
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const { redis, cache } = memoryHarness(
    t,
    {
      key: 'versioned',
      fetch: async () => {
        entered.resolve()
        await release.promise
        return 'slow-fetch'
      },
    },
    { onEvent: (event) => events.push(event) },
  )
  const slow = cache.get()
  await entered.promise
  await cache.set(undefined, 'explicit')
  release.resolve()
  assert.equal(await slow, 'slow-fetch')
  assert.ok(events.some((e) => e.type === 'error' && e.reason === 'lock_lost'))
  const reads = countReads(redis)
  assert.equal(await cache.get(), 'explicit')
  assert.equal(await cache.get(), 'explicit')
  assert.equal(reads.count, 1)
  assert.equal(await redis.get(toLockKey('versioned')), null)
})

test('Memory: a background batch refresh replaces a still-fresh secondary once flushed', async (t) => {
  const { redis, cache, internals } = memoryHarness<string, string>(t, {
    key: (key) => key,
    ttl: 60,
    fetch: async () =>
      new SwrWriteBatch({ primary: 'p-new', secondary: [{ key: 'sec', value: 'sec-new' }] }),
  })
  await redis.set(toDataKey('p'), stored('p-old', 1000))
  await cache.set('sec', 'sec-old')
  assert.equal(await cache.get('p'), 'p-old')
  await internals.drain()
  const reads = countReads(redis)
  assert.equal(await cache.get('sec'), 'sec-new')
  assert.equal(await cache.get('p'), 'p-new')
  assert.equal(reads.count, 0)
})

test('Memory: a cold-poll MGET failure releases read tracking before the direct fallback', async (t) => {
  let trackedDuringFallback = -1
  const { redis, memory, cache } = memoryHarness<string>(
    t,
    {
      key: 'poll-err',
      fetch: async (_ctx, meta) => {
        assert.equal(meta.cacheWrite, 'skip')
        trackedDuringFallback = memory.versionCount
        return 'direct'
      },
    },
    { pollInterval: 0.005 },
  )
  await redis.set(toLockKey('poll-err'), 'other-owner', 'PX', 5000)
  const mgetBuffer = redis.mgetBuffer.bind(redis)
  redis.mgetBuffer = async () => {
    redis.mgetBuffer = mgetBuffer
    throw new Error('poll mgetBuffer down')
  }
  assert.equal(await cache.get(), 'direct')
  assert.equal(trackedDuringFallback, 0)
  assert.equal(memory.entryCount, 0)
})
