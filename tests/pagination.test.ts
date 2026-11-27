import test from 'node:test'
import assert from 'node:assert/strict'
import { createFakeRedis } from './fake-redis.ts'
import { createSwrClient, SwrNoCache, SwrValidationError, type SwrCodec } from '../src/index.ts'
import { createSwrPaginatedCache, type SwrPaginatedPage } from '../src/pagination/index.ts'
import { createRedisKeys } from '../src/keys.ts'
import { COOLDOWN_MARKER } from '../src/lock.ts'
import { DEFAULTS } from '../src/options.ts'
import { useRedisClock } from './helpers/clock.ts'

const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)

const INVALID_NON_NEGATIVE_INTS = [-1, 1.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1]
const INVALID_POSITIVE_INTS = [0, ...INVALID_NON_NEGATIVE_INTS]

test('createSwrPaginatedCache: validates static pageSize and prefetchPages at creation', () => {
  const swr = createSwrClient({ redis: createFakeRedis() })

  for (const pageSize of INVALID_POSITIVE_INTS) {
    assert.throws(
      () =>
        createSwrPaginatedCache(swr, {
          pageSize,
          pageIndex: () => 0,
          key: () => 'k',
          fetch: async () => [],
        }),
      (err: unknown) =>
        err instanceof SwrValidationError &&
        err.message.includes('pageSize must be a positive safe integer'),
    )
  }

  for (const prefetchPages of INVALID_NON_NEGATIVE_INTS) {
    assert.throws(
      () =>
        createSwrPaginatedCache(swr, {
          pageSize: 10,
          prefetchPages,
          pageIndex: () => 0,
          key: () => 'k',
          fetch: async () => [],
        }),
      (err: unknown) =>
        err instanceof SwrValidationError &&
        err.message.includes('prefetchPages must be a non-negative safe integer'),
    )
  }

  assert.throws(
    () =>
      createSwrPaginatedCache(swr, {
        pageSize: Math.floor(Number.MAX_SAFE_INTEGER / 2) + 1,
        prefetchPages: 1,
        pageIndex: () => 0,
        key: () => 'k',
        fetch: async () => [],
      }),
    (err: unknown) =>
      err instanceof SwrValidationError &&
      err.message.includes('pageSize * (1 + prefetchPages) exceeds maximum safe integer'),
  )
})

test('createSwrPaginatedCache: rejects invalid dynamic pageIndex, pageSize, and derived arithmetic overflows before fetch', async () => {
  interface Ctx {
    pageIndex: number
    pageSize: number
  }

  const swr = createSwrClient({ redis: createFakeRedis() })
  let keyCalls = 0
  let fetchCalls = 0

  const cache = createSwrPaginatedCache<{ id: number }, Ctx>(swr, {
    pageIndex: (ctx) => ctx.pageIndex,
    pageSize: (ctx) => ctx.pageSize,
    prefetchPages: 2,
    key: ({ pageIndex: page, pageSize: size }) => {
      keyCalls++
      return `p:${page}:s:${size}`
    },
    fetch: async () => {
      fetchCalls++
      return { docs: [{ id: 1 }], totalDocs: 1 }
    },
  })

  for (const pageIndex of INVALID_NON_NEGATIVE_INTS) {
    const ctx: Ctx = { pageIndex, pageSize: 10 }
    keyCalls = 0
    fetchCalls = 0

    assert.throws(() => cache.key(ctx), SwrValidationError)
    await assert.rejects(() => cache.get(ctx), SwrValidationError)
    assert.equal(keyCalls, 0)
    assert.equal(fetchCalls, 0)
  }

  for (const pageSize of INVALID_POSITIVE_INTS) {
    const ctx: Ctx = { pageIndex: 0, pageSize }
    keyCalls = 0
    fetchCalls = 0

    assert.throws(() => cache.key(ctx), SwrValidationError)
    await assert.rejects(() => cache.get(ctx), SwrValidationError)
    assert.equal(keyCalls, 0)
    assert.equal(fetchCalls, 0)
  }

  // Derived arithmetic overflow boundaries
  fetchCalls = 0
  await assert.rejects(
    () => cache.get({ pageIndex: Math.floor(Number.MAX_SAFE_INTEGER / 2) + 1, pageSize: 2 }),
    (err: unknown) =>
      err instanceof SwrValidationError && err.message.includes('pageIndex * pageSize'),
  )
  await assert.rejects(
    () => cache.get({ pageIndex: 0, pageSize: Math.floor(Number.MAX_SAFE_INTEGER / 2) + 1 }),
    (err: unknown) =>
      err instanceof SwrValidationError && err.message.includes('pageSize * (1 + prefetchPages)'),
  )
  await assert.rejects(
    () => cache.get({ pageIndex: Number.MAX_SAFE_INTEGER, pageSize: 1 }),
    (err: unknown) =>
      err instanceof SwrValidationError && err.message.includes('pageIndex + prefetchPages'),
  )
  assert.equal(fetchCalls, 0)
})

test('createSwrPaginatedCache: pre-warms secondary pages and caps over-returning fetchers at prefetchPages', async () => {
  const redis = createFakeRedis()
  const swr = createSwrClient({ redis })
  let dbQueries = 0

  interface QueryCtx {
    topic: string
    pageIndex: number
  }

  const allArticles = Array.from({ length: 25 }, (_, i) => ({
    title: `Article ${i + 1}`,
  }))

  const keyInputs: unknown[] = []
  const cache = createSwrPaginatedCache(swr, {
    name: 'articles',
    pageSize: 10,
    prefetchPages: 2,
    ttl: 30,
    pageIndex: (ctx: QueryCtx) => ctx.pageIndex,
    key: (input) => {
      keyInputs.push(input)
      const { context: ctx, pageIndex: page, pageSize: size } = input
      return `articles:${ctx.topic}:p${page}:s${size}`
    },
    fetch: async (_ctx, { skip, limit }) => {
      dbQueries++
      return { docs: allArticles.slice(skip, skip + limit), totalDocs: allArticles.length }
    },
  })

  const context = { topic: 'news', pageIndex: 0 }
  const page0 = await cache.get(context)
  assert.equal(dbQueries, 1)
  // The requested page, then each prefetched page, with the caller's context.
  assert.deepEqual(keyInputs, [
    { context, pageIndex: 0, pageSize: 10 },
    { context, pageIndex: 1, pageSize: 10 },
    { context, pageIndex: 2, pageSize: 10 },
  ])
  assert.equal(page0.docs.length, 10)
  assert.equal(page0.totalDocs, 25)

  const page1 = await cache.get({ topic: 'news', pageIndex: 1 })
  assert.equal(dbQueries, 1)
  assert.equal(page1.docs.length, 10)
  assert.equal(page1.docs[0]!.title, 'Article 11')

  const page2 = await cache.get({ topic: 'news', pageIndex: 2 })
  assert.equal(dbQueries, 1)
  assert.equal(page2.docs.length, 5)
  assert.equal(page2.docs[0]!.title, 'Article 21')

  assert.equal(cache.key({ topic: 'news', pageIndex: 99 }), 'articles:news:p99:s10')

  // Over-returning fetcher with prefetchPages: 0 must NOT prewarm page 1
  let zeroPrefetchQueries = 0
  const noPrefetch = createSwrPaginatedCache(swr, {
    pageSize: 2,
    prefetchPages: 0,
    pageIndex: (ctx: { page: number }) => ctx.page,
    key: ({ pageIndex: page, pageSize: size }) => `noprefetch:p${page}:s${size}`,
    fetch: async (_ctx, { skip }) => {
      zeroPrefetchQueries++
      // Deliberately returns 6 docs even though limit=2
      return [skip, skip + 1, skip + 2, skip + 3, skip + 4, skip + 5]
    },
  })
  const p0 = await noPrefetch.get({ page: 0 })
  assert.deepEqual(p0, { docs: [0, 1], pageIndex: 0, pageSize: 2 })
  assert.equal('totalDocs' in p0, false)
  // Page 1 must miss and trigger a second fetch because prefetchPages is 0.
  const p1 = await noPrefetch.get({ page: 1 })
  assert.deepEqual(p1, { docs: [2, 3], pageIndex: 1, pageSize: 2 })
  assert.equal(zeroPrefetchQueries, 2)
  await swr.drain()
})

test('createSwrPaginatedCache: uses the source page size when computing prefetch keys', async () => {
  const swr = createSwrClient({ redis: createFakeRedis() })
  const fetched: number[] = []
  const cache = createSwrPaginatedCache(swr, {
    pageSize: (page) => (page === 1 ? 3 : 2),
    prefetchPages: 2,
    pageIndex: (page: number) => page,
    key: ({ pageIndex: page, pageSize: size }) => `variable:p${page}:s${size}`,
    fetch: async (page, { skip, limit }) => {
      fetched.push(page)
      return Array.from({ length: limit }, (_, i) => skip + i)
    },
  })

  assert.deepEqual((await cache.get(0)).docs, [0, 1])
  assert.deepEqual((await cache.get(2)).docs, [4, 5])
  assert.deepEqual((await cache.get(1)).docs, [3, 4, 5])
  assert.deepEqual(fetched, [0, 1])
})

test('createSwrPaginatedCache: supports SwrNoCache without caching primary or prewarming secondary pages', async () => {
  const redis = createFakeRedis()
  const swr = createSwrClient({ redis, memory: true })
  let calls = 0
  let ttlCalls = 0
  const keyCalls: number[] = []

  const cache = createSwrPaginatedCache(swr, {
    pageSize: 2,
    prefetchPages: 2,
    pageIndex: (ctx: { page: number }) => ctx.page,
    key: ({ pageIndex: page }) => {
      keyCalls.push(page)
      return `nc-page:${page}`
    },
    ttl: () => {
      ttlCalls++
      return 60
    },
    fetch: async (_ctx, { skip }) => {
      calls++
      if (calls === 1) {
        return new SwrNoCache({ docs: [10, 20, 30, 40], totalDocs: 0 })
      }
      return { docs: [skip + 1, skip + 2], totalDocs: 10 }
    },
  })

  const uncached = await cache.get({ page: 0 })
  assert.deepEqual(uncached, { docs: [10, 20], pageIndex: 0, pageSize: 2, totalDocs: 0 })
  assert.equal(ttlCalls, 0)
  // key was called only for primary page 0, never for secondary pages 1 or 2.
  assert.deepEqual(keyCalls, [0])

  const cached = await cache.get({ page: 0 })
  assert.deepEqual(cached, { docs: [1, 2], pageIndex: 0, pageSize: 2, totalDocs: 10 })
  assert.equal(calls, 2)
  assert.equal(ttlCalls, 1)
  await swr.drain()
})

test('createSwrPaginatedCache: uses limit=pageSize on availability fallback (cacheWrite: "skip")', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const observed: Array<{ skip: number; limit: number; cacheWrite: string }> = []

  interface QueryCtx {
    topic: string
    pageIndex: number
  }

  let generation = 0
  let encodeCalls = 0
  const keyPages: number[] = []
  const swr = createSwrClient({
    redis,
    failureStrategy: 'availability',
  })
  const cache = createSwrPaginatedCache(swr, {
    pageSize: 5,
    prefetchPages: 2,
    pageIndex: (ctx: QueryCtx) => ctx.pageIndex,
    key: ({ context: ctx, pageIndex: page, pageSize: size }) => {
      keyPages.push(page)
      return `art:${ctx.topic}:p${page}:s${size}`
    },
    codec: {
      encode: (page: SwrPaginatedPage<{ id: number }>) => {
        encodeCalls++
        return Buffer.from(JSON.stringify(page))
      },
      decode: (bytes) =>
        JSON.parse(Buffer.from(bytes).toString()) as SwrPaginatedPage<{ id: number }>,
    },
    fetch: async (_ctx, range, meta) => {
      generation++
      observed.push({
        skip: range.skip,
        limit: range.limit,
        cacheWrite: meta.cacheWrite,
      })
      // Over-return on skip to verify fallback skips secondary writes.
      const count = meta.cacheWrite === 'skip' ? range.limit * 2 : range.limit
      const docs = Array.from({ length: count }, (_, i) => ({
        id: generation * 100 + range.skip + i,
      }))
      return { docs, totalDocs: 50 }
    },
  })

  const page0Fresh = await cache.get({ topic: 'db', pageIndex: 0 })
  assert.equal(page0Fresh.docs[0]!.id, 100)
  assert.deepEqual(observed[0], { skip: 0, limit: 15, cacheWrite: 'attempt' })

  const origGetBuffer = redis.getBuffer.bind(redis)
  redis.getBuffer = async () => {
    throw new Error('Redis read down')
  }
  keyPages.length = 0
  encodeCalls = 0
  try {
    const fallbackPage = await cache.get({ topic: 'db', pageIndex: 1 })
    assert.equal(fallbackPage.pageIndex, 1)
    assert.equal(fallbackPage.docs.length, 5)
    assert.equal(fallbackPage.docs[0]!.id, 205)
    assert.deepEqual(observed[1], { skip: 5, limit: 5, cacheWrite: 'skip' })
    assert.deepEqual(keyPages, [1], 'fallback must not invoke key callback for secondary pages')
    assert.equal(encodeCalls, 0, 'fallback must not encode primary or secondary pages')
  } finally {
    redis.getBuffer = origGetBuffer
  }
  await swr.drain()
})

test('createSwrPaginatedCache: single page codec round-trips primary and prefetched pages with one origin call', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const pageCodec: SwrCodec<SwrPaginatedPage<number>> = {
    encode: (page) => Buffer.from(`PG:${JSON.stringify(page)}`),
    decode: (bytes) => {
      const text = Buffer.from(bytes).toString()
      assert.ok(text.startsWith('PG:'))
      return JSON.parse(text.slice(3)) as SwrPaginatedPage<number>
    },
  }
  let fetches = 0
  const swr = createSwrClient({ redis, memory: true, ttl: 60 })
  const cache = createSwrPaginatedCache(swr, {
    pageSize: 2,
    prefetchPages: 1,
    pageIndex: (ctx: { page: number }) => ctx.page,
    key: ({ pageIndex: page }) => `coded:p${page}`,
    codec: pageCodec,
    fetch: async (_ctx, { skip, limit }) => {
      fetches++
      return { docs: [10, 20, 30, 40].slice(skip, skip + limit), totalDocs: 4 }
    },
  })

  assert.deepEqual(await cache.get({ page: 0 }, { memory: false }), {
    docs: [10, 20],
    pageIndex: 0,
    pageSize: 2,
    totalDocs: 4,
  })
  assert.deepEqual(await cache.get({ page: 1 }, { memory: false }), {
    docs: [30, 40],
    pageIndex: 1,
    pageSize: 2,
    totalDocs: 4,
  })
  assert.equal(fetches, 1)
  await swr.drain()
})

test('createSwrPaginatedCache: a default (staleWait 0) stale refresh warms secondary pages in the tracked background refresh', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const waitUntilPromises: Promise<unknown>[] = []
  const swr = createSwrClient({
    redis,
    memory: true,
    ttl: 5,
    waitUntil: (p) => waitUntilPromises.push(p),
  })
  let version = 0
  const cache = createSwrPaginatedCache(swr, {
    pageSize: 2,
    prefetchPages: 1,
    pageIndex: (ctx: { page: number }) => ctx.page,
    key: ({ pageIndex: page }) => `bg-stale:p${page}`,
    fetch: async () => {
      version++
      return [version * 10 + 1, version * 10 + 2, version * 10 + 3, version * 10 + 4]
    },
  })
  await cache.set({ page: 0 }, { docs: [0], pageIndex: 0, pageSize: 2 })
  redis.advanceTime(6000)

  assert.deepEqual(await cache.get({ page: 0 }), { docs: [0], pageIndex: 0, pageSize: 2 })
  assert.equal(waitUntilPromises.length, 1)
  await swr.drain()
  assert.deepEqual(await cache.get({ page: 0 }), { docs: [11, 12], pageIndex: 0, pageSize: 2 })
  assert.deepEqual(await cache.get({ page: 1 }), { docs: [13, 14], pageIndex: 1, pageSize: 2 })
  assert.equal(version, 1)
})

test('createSwrPaginatedCache: overwrites fresh and stale secondary pages in Redis and memory', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  let fetches = 0
  let ttlCalls = 0

  const swr = createSwrClient({
    redis,
    memory: true,
    ttl: 10,
    stale: ({ ttl }) => ttl,
  })

  const cache = createSwrPaginatedCache(swr, {
    pageSize: 2,
    prefetchPages: 2,
    pageIndex: (ctx: { page: number }) => ctx.page,
    key: ({ pageIndex: page }) => `overwrite:p${page}`,
    ttl: ({ value }) => {
      ttlCalls++
      return value.docs.length > 0 ? 10 : 1
    },
    fetch: async () => {
      fetches++
      return [11, 12, 13, 14, 15, 16]
    },
  })

  // Seed page 1 (will stay fresh) and page 2 (will become stale, still present in Redis)
  await cache.set({ page: 2 }, { docs: [200, 201], pageIndex: 2, pageSize: 2 })
  redis.advanceTime(12_000) // page 2 is now stale (10s fresh, 20s expires), still in Redis
  await cache.set({ page: 1 }, { docs: [100, 101], pageIndex: 1, pageSize: 2 })
  ttlCalls = 0

  assert.deepEqual(await cache.get({ page: 0 }), { docs: [11, 12], pageIndex: 0, pageSize: 2 })
  assert.equal(ttlCalls, 1, 'dynamic ttl must be evaluated only for the primary page')
  for (const page of [1, 2]) {
    const expected = { docs: [11 + page * 2, 12 + page * 2], pageIndex: page, pageSize: 2 }
    assert.deepEqual(await cache.get({ page }), expected)
    assert.deepEqual(await cache.get({ page }, { memory: false }), expected)
  }
  assert.equal(fetches, 1)
})

test('createSwrPaginatedCache: a secondary clone failure fails the batch before Redis and keeps memory', async () => {
  const redis = createFakeRedis()
  const swr = createSwrClient({ redis, memory: { clone: true } })
  const cache = createSwrPaginatedCache<{ id: number; callback?: () => number }, number>(swr, {
    pageSize: 1,
    prefetchPages: 2,
    pageIndex: (page) => page,
    key: ({ pageIndex: page }) => `clone:p${page}`,
    fetch: async () => [{ id: 1 }, { id: 2, callback: () => 2 }, { id: 3 }],
    codec: {
      encode: (page) => Buffer.from(JSON.stringify(page)),
      decode: (bytes) => JSON.parse(Buffer.from(bytes).toString()),
    },
  })
  await cache.set(1, { docs: [{ id: 99 }], pageIndex: 1, pageSize: 1 })

  await assert.rejects(() => cache.get(0), { name: 'DataCloneError' })
  assert.equal(await redis.get(lockKey('clone:p0')), COOLDOWN_MARKER)
  for (const page of [0, 2]) assert.equal(await redis.getBuffer(dataKey(`clone:p${page}`)), null)
  let reads = 0
  const getBuffer = redis.getBuffer.bind(redis)
  redis.getBuffer = async (key) => {
    reads++
    return getBuffer(key)
  }
  assert.deepEqual(await cache.get(1), { docs: [{ id: 99 }], pageIndex: 1, pageSize: 1 })
  assert.equal(reads, 0)
})

test('createSwrPaginatedCache: a throwing key callback for a secondary page fails the batch before Redis and starts a cooldown', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const swr = createSwrClient({ redis, memory: true })

  const keyError = new Error('secondary key failed')
  const keyFailure = createSwrPaginatedCache(swr, {
    pageSize: 1,
    prefetchPages: 2,
    pageIndex: (page: number) => page,
    key: ({ pageIndex: page }) => {
      if (page === 1) throw keyError
      return `key-failure:p${page}`
    },
    fetch: async () => [1, 2, 3],
  })
  await assert.rejects(
    () => keyFailure.get(0),
    (err) => err === keyError,
  )
  assert.equal(await redis.get(lockKey('key-failure:p0')), COOLDOWN_MARKER)
  assert.equal(await redis.getBuffer(dataKey('key-failure:p0')), null)
  assert.equal(await redis.getBuffer(dataKey('key-failure:p2')), null)
})
