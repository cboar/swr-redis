import {
  createSwrClient,
  SwrNoCache,
  SwrWriteBatch,
  type SwrCache,
  type SwrCacheWrite,
  type SwrFetchMeta,
  type SwrCodec,
  type SwrEvent,
  type SwrFetchResult,
  type SwrWriteBatchInput,
} from 'swr-redis'
import { createSwrPaginatedCache, type SwrPaginatedPage } from 'swr-redis/pagination'
import { Redis } from 'ioredis'

const redis = new Redis({ lazyConnect: true })
const swr = createSwrClient({
  redis,
  compression: { level: 1, minBytes: 256 },
  onEvent: (event: SwrEvent) => {
    void event.type
  },
})
const settings = swr.cache({
  name: 'settings',
  key: 'settings:v1',
  fetch: async (_ctx, { cacheWrite }) =>
    cacheWrite === 'skip' ? { ok: true } : new SwrNoCache({ ok: false }),
  ttl: ({ context, key, value }) => {
    const _context: void = context
    const _key: string = key
    return value.ok ? 60 : 5
  },
})
const res: Promise<{ ok: boolean }> = settings.get(undefined, { memory: false })
const setRes: Promise<void> = settings.set(undefined, { ok: true })
const userCache = swr.cache<string | null, string>({
  key: (id) => `user:${id}`,
  fetch: async (id, { cacheWrite }): Promise<SwrFetchResult<string | null>> => {
    if (cacheWrite === 'skip') return id
    if (id === 'skip') return new SwrNoCache(null)
    return id
  },
  ttl: ({ context, value }) => {
    const _id: string = context
    return value === null ? 5 : 60
  },
})
const metaCheck: SwrFetchMeta<string> = {
  key: (id) => id,
  cacheWrite: 'attempt',
}
const user: Promise<string | null> = userCache.get('1', { memory: false })
const plainValueObj = swr.cache({
  key: 'plain-value-obj',
  fetch: async () => ({ value: 42 }),
})
const plainValueRes: Promise<{ value: number }> = plainValueObj.get()
const wrappedValueObj = swr.cache({
  key: 'wrapped-value-obj',
  fetch: async () => new SwrNoCache({ value: 42 }),
})
const wrappedValueRes: Promise<{ value: number }> = wrappedValueObj.get()
const plainBatchLikeObj = swr.cache({
  key: 'plain-batch-like-obj',
  fetch: async () => ({ primary: 42, secondary: [{ key: 'k', value: 1 }] }),
})
const plainBatchLikeRes: Promise<{ primary: number; secondary: { key: string; value: number }[] }> =
  plainBatchLikeObj.get()
const strCodec: SwrCodec<string> = {
  encode: (s) => Buffer.from(s),
  decode: (b) => Buffer.from(b).toString(),
}
const batchWrite: SwrCacheWrite = { key: 'batch:count', value: 1, ttl: 30 }
const labelWrite: SwrCacheWrite = { key: 'batch:label', value: 'heading', codec: strCodec }
const batchInput: SwrWriteBatchInput<string> = {
  primary: 'p0',
  secondary: [batchWrite, labelWrite],
}
const batchCache = swr.cache({
  key: (q: { page: number }) => `batch:${q.page}`,
  fetch: async (q): Promise<SwrFetchResult<string>> =>
    q.page === 0 ? new SwrWriteBatch(batchInput) : `p${q.page}`,
})
const batchRes: Promise<string> = batchCache.get({ page: 0 })
// @ts-expect-error Published declarations keep SwrNoCache nominal against plain { value } objects.
swr.cache<number>({ key: 'explicit-obj', fetch: async () => ({ value: 42 }) })
swr.cache<number>({
  key: 'explicit-batch-obj',
  // @ts-expect-error Published declarations keep SwrWriteBatch nominal against plain { primary, secondary } objects.
  fetch: async () => ({ primary: 42, secondary: [] }),
})
// @ts-expect-error Published declarations preserve context argument types.
userCache.get(21)
// @ts-expect-error Request-level memory only accepts false.
userCache.get('1', { memory: true })
// @ts-expect-error Client-level ttl must remain numeric.
createSwrClient({ redis, ttl: () => 5 })
// @ts-expect-error The clock is not an option.
createSwrClient({ redis, now: () => 0 })
// @ts-expect-error Narrow value codec cannot be used as universal client codec.
createSwrClient({ redis, codec: strCodec })

const paginated: SwrCache<
  SwrPaginatedPage<{ id: string }>,
  { page: number }
> = createSwrPaginatedCache(swr, {
  pageSize: 10,
  prefetchPages: 1,
  pageIndex: (ctx: { page: number }) => ctx.page,
  key: (_ctx, page, size) => `p:${page}:${size}`,
  fetch: async () => ({ docs: [{ id: '1' }], totalDocs: 1 }),
})
const pageRes: Promise<SwrPaginatedPage<{ id: string }>> = paginated.get({ page: 0 })

void metaCheck
void res
void setRes
void user
void plainValueRes
void plainBatchLikeRes
void batchRes
void wrappedValueRes
void pageRes
