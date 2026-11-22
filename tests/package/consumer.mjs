import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { ObjectId } from 'bson'
import { Redis } from 'ioredis'
import * as api from 'swr-redis'
import * as pagination from 'swr-redis/pagination'
const { createFakeRedis } = await import(process.argv[2])

assert.deepEqual(
  Object.keys(api).sort(),
  [
    'SwrCacheError',
    'SwrNoCache',
    'SwrRedisError',
    'SwrTimeoutError',
    'SwrValidationError',
    'SwrWriteBatch',
    'createSwrClient',
  ].sort(),
)

assert.deepEqual(Object.keys(pagination).sort(), ['createSwrPaginatedCache'].sort())

const realRedis = new Redis({ lazyConnect: true, retryStrategy: () => null })
try {
  await api.createSwrClient({ redis: realRedis }).flush()
} finally {
  realRedis.disconnect()
}

let calls = 0
const events = []
const swr = api.createSwrClient({
  redis: createFakeRedis(),
  memory: true,
  onEvent: (e) => events.push(e),
})
// The MongoDB driver loads BSON through its CommonJS entry point.
const { ObjectId: DriverObjectId } = createRequire(import.meta.url)('bson')
const id = new DriverObjectId()
const documents = swr.cache({
  key: 'document',
  memory: false,
  fetch: async () => assert.fail('expected a Redis hit'),
})
await documents.set(undefined, { _id: id })
const document = await documents.get()
assert.ok(document._id instanceof ObjectId)
assert.ok(document._id.equals(id))
assert.ok(id.equals(document._id))

const ttlInputs = []
const doubleCache = swr.cache({
  name: 'doubles',
  key: (n) => `double:${n}`,
  fetch: async (n, meta) => {
    calls++
    if (n === 0) return new api.SwrNoCache(0)
    assert.equal(meta.cacheWrite, 'attempt')
    return n * 2
  },
  ttl: (input) => {
    ttlInputs.push(input)
    return input.value === 42 ? 60 : 5
  },
})
assert.equal(await doubleCache.get(0), 0)
assert.equal(await doubleCache.get(0), 0)
assert.equal(calls, 2)
assert.equal(await doubleCache.get(21, { memory: false }), 42)
assert.equal(await doubleCache.get(21), 42)
assert.equal(await doubleCache.get(21), 42)
assert.equal(calls, 3)
assert.deepEqual(ttlInputs, [{ context: 21, key: 'double:21', value: 42 }])
assert.ok(events.some((e) => e.type === 'hit' && e.cache === 'doubles' && e.source === 'memory'))

// Exercise root SwrWriteBatch from packed tarball
let batchFetches = 0
const products = swr.cache({
  name: 'products',
  key: (q) => `pkg-products:${q.category}:${q.page}`,
  fetch: async (q) => {
    batchFetches++
    return new api.SwrWriteBatch({
      primary: { items: [`${q.category}:${q.page}`] },
      secondary: [
        {
          key: `pkg-products:${q.category}:${q.page + 1}`,
          value: { items: [`${q.category}:${q.page + 1}`] },
          ttl: 30,
        },
      ],
    })
  },
})
assert.deepEqual(await products.get({ category: 'shoes', page: 0 }), { items: ['shoes:0'] })
assert.deepEqual(await products.get({ category: 'shoes', page: 1 }), { items: ['shoes:1'] })
assert.equal(batchFetches, 1)

// Exercise ./pagination subpath from packed tarball
let pageFetches = 0
const pages = pagination.createSwrPaginatedCache(swr, {
  name: 'pages',
  pageSize: 2,
  prefetchPages: 1,
  pageIndex: (ctx) => ctx.page,
  key: (_ctx, page, size) => `pkg-page:${page}:${size}`,
  fetch: async (_ctx, { skip, limit }) => {
    pageFetches++
    return { docs: ['a', 'b', 'c', 'd'].slice(skip, skip + limit), totalDocs: 4 }
  },
})
assert.deepEqual(await pages.get({ page: 0 }), {
  docs: ['a', 'b'],
  pageIndex: 0,
  pageSize: 2,
  totalDocs: 4,
})
assert.deepEqual(await pages.get({ page: 1 }), {
  docs: ['c', 'd'],
  pageIndex: 1,
  pageSize: 2,
  totalDocs: 4,
})
assert.equal(pageFetches, 1)
await swr.flush()
