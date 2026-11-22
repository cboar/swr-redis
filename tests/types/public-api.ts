// Compile-time API regression tests. This file is checked by tsc, not executed.
import {
  createSwrClient,
  SwrNoCache,
  SwrWriteBatch,
  type SwrCache,
  type SwrCacheWrite,
  type SwrErrorReason,
  type SwrFetchMeta,
  type SwrNonEmptyKeys,
  type SwrSharedOptions,
  type SwrTtlInput,
  type SwrTtlOption,
  type SwrCodec,
  type SwrEvent,
  type SwrFetchResult,
} from '../../src/index.ts'
import {
  createSwrPaginatedCache,
  type SwrPaginatedCacheOptions,
  type SwrPaginatedPage,
  type SwrPaginationRange,
} from '../../src/pagination/index.ts'
import type { Redis } from 'ioredis'

declare const redis: Redis
declare function expectType<T>(value: T): void

const swr = createSwrClient({ redis, prefix: 'app' })

// Static string key -> void context with 0-arg get/invalidate/key.
const settingsCache = swr.cache({
  key: 'settings:v1',
  fetch: async () => ({ theme: 'dark' as const }),
})
expectType<SwrCache<{ theme: 'dark' }, void>>(settingsCache)
expectType<Promise<{ theme: 'dark' }>>(settingsCache.get())
expectType<Promise<{ theme: 'dark' }>>(settingsCache.get(undefined, { memory: false }))
expectType<Promise<void>>(settingsCache.set(undefined, { theme: 'dark' }))
expectType<Promise<void>>(settingsCache.invalidate())
expectType<string>(settingsCache.key())

// Function key -> Context is inferred from key and flows into fetch.
const userCache = swr.cache({
  key: (userId: string) => `user:${userId}`,
  fetch: async (userId, { signal }) => ({ id: userId, aborted: Boolean(signal?.aborted) }),
})
expectType<Promise<{ id: string; aborted: boolean }>>(userCache.get('u_1'))
// @ts-expect-error Missing required string context argument.
userCache.get()
// @ts-expect-error Context argument type must be preserved.
userCache.get(123)
// @ts-expect-error Return type must not widen to any or unknown.
expectType<Promise<number>>(userCache.get('u_1'))

// Context inferred from an annotated fetch rather than the key.
const fetchDeclared = swr.cache({
  key: (params) => `p:${params.page}`,
  fetch: async (params: { page: number }) => params.page,
})
expectType<SwrCache<number, { page: number }>>(fetchDeclared)

// Extracted fetch helper typed with SwrFetchMeta and SwrFetchResult.
async function fetchProfile(
  userId: string,
  meta: SwrFetchMeta<string>,
): Promise<SwrFetchResult<{ id: string }>> {
  if (meta.cacheWrite === 'skip') return new SwrNoCache({ id: userId })
  return { id: meta.key(userId) }
}
expectType<Promise<{ id: string }>>(
  swr.cache({ key: (id: string) => `profile:${id}`, fetch: fetchProfile }).get('u'),
)

// Codecs: client-level codecs are universal; cache codecs must match the value type (encode is contravariant).
const mapCodec: SwrCodec<Map<string, number>> = {
  encode: (value) => Buffer.from(JSON.stringify([...value])),
  decode: (bytes) => new Map(JSON.parse(Buffer.from(bytes).toString())),
}
const stringCodec: SwrCodec<string> = {
  encode: (value) => Buffer.from(value),
  decode: (bytes) => Buffer.from(bytes).toString(),
}
const universalCodec: SwrCodec<unknown> = {
  encode: (value) => Buffer.from(JSON.stringify(value ?? null)),
  decode: (bytes) => JSON.parse(Buffer.from(bytes).toString()),
}
createSwrClient({ redis, codec: universalCodec, compression: { level: 3, minBytes: 128 } })
// @ts-expect-error Narrow value codec cannot be passed as client-wide universal codec.
createSwrClient({ redis, codec: mapCodec })
expectType<Promise<Map<string, number>>>(
  swr.cache({ key: 'map', codec: mapCodec, fetch: async () => new Map([['a', 1]]) }).get(),
)
// @ts-expect-error Codec and result types must match.
swr.cache({ key: 'wrong-codec', codec: mapCodec, fetch: async () => 'value' })
// @ts-expect-error Contravariant encode rejects a narrow string codec for a string | number cache.
swr.cache<string | number>({ key: 'narrow-codec', codec: stringCodec, fetch: async () => 42 })

// Client-only options: onEvent and memory config; caches and requests may only opt out.
createSwrClient({
  redis,
  memory: { maxEntries: 500, maxTtl: 30, clone: true },
  onEvent: (e) => expectType<SwrEvent>(e),
})
// @ts-expect-error onEvent is client-only, not a cache option.
swr.cache({ key: 'k', onEvent: () => {}, fetch: async () => 'ok' })
// @ts-expect-error Cache-level memory: true is not allowed.
swr.cache({ key: 'k', memory: true, fetch: async () => 'ok' })
// @ts-expect-error Request-level memory: true is not allowed.
userCache.get('u_1', { memory: true })
// waitUntil is a client or per-request hook, not a cache option.
createSwrClient({ redis, waitUntil: () => {} })
userCache.get('u_1', { waitUntil: () => {} })
// @ts-expect-error waitUntil is not a cache option.
swr.cache({ key: 'k', waitUntil: () => {}, fetch: async () => 'ok' })

// Dynamic TTL callbacks infer context/key/value regardless of option order and must be synchronous numbers.
const ttlCache = swr.cache({
  key: (id: number) => `item:${id}`,
  ttl: ({ context, key, value }) => {
    expectType<number>(context)
    expectType<string>(key)
    expectType<string | null>(value)
    return value === null ? 5 : 60
  },
  fetch: async (id: number): Promise<string | null> => (id > 0 ? 'ok' : null),
})
expectType<Promise<string | null>>(ttlCache.get(1))
// @ts-expect-error Client-level ttl must remain numeric.
createSwrClient({ redis, ttl: () => 15 })
// @ts-expect-error Dynamic ttl callback cannot return a Promise.
swr.cache({ key: 'k', ttl: async () => 10, fetch: async () => 'a' })
// @ts-expect-error NoInfer prevents the ttl callback from overriding the fetched value type.
swr.cache({ key: 'k', ttl: ({ value }: { value: number }) => value, fetch: async () => 'a' })

// Exported shared option and TTL types describe the options.
const shared: SwrSharedOptions = {
  ttl: 60,
  mode: 'wait-on-stale',
  failureStrategy: 'protect-source',
}
createSwrClient({ redis, ...shared })
const ttlOption: SwrTtlOption<string, number> = (input: SwrTtlInput<string, number>) =>
  input.context + input.value.length
void ttlOption
// @ts-expect-error The clock is not an option; tests mock Date.now instead.
createSwrClient({ redis, now: () => 0 })

// Event unions narrow by type, and error events narrow by reason.
declare const evt: SwrEvent
if (evt.type === 'hit') expectType<'memory' | 'redis'>(evt.source)
if (evt.type === 'error') {
  expectType<'warn' | 'error'>(evt.severity)
  expectType<SwrErrorReason>(evt.reason)
  if (evt.reason === 'redis_error' || evt.reason === 'redis_error_fallback')
    expectType<SwrNonEmptyKeys>(evt.keys)
  if (evt.reason === 'timeout') expectType<number>(evt.err.elapsedMs + evt.durationMs)
  if (evt.reason === 'lock_lost') {
    expectType<number>(evt.durationMs)
    // @ts-expect-error A lost lock carries no error.
    void evt.err
  }
  // @ts-expect-error Only redis_error* events carry keys.
  if (evt.reason === 'fatal') void evt.keys
  // @ts-expect-error The operation field was removed.
  void evt.operation
}

// Wrappers unwrap in inference, and plain look-alike objects stay ordinary values.
const unionCache = swr.cache({
  key: (id: string) => `union:${id}`,
  fetch: async (id) => {
    if (id === 'skip') return new SwrNoCache({ id, ok: false })
    if (id === 'batch') {
      return new SwrWriteBatch({
        primary: { id, ok: true },
        secondary: [
          { key: 'counter', value: 123, ttl: 15 },
          { key: 'label', value: 'heading', codec: stringCodec },
        ],
      })
    }
    return { id, ok: true }
  },
  ttl: ({ value }) => {
    expectType<{ id: string; ok: boolean }>(value)
    return 60
  },
})
expectType<Promise<{ id: string; ok: boolean }>>(unionCache.get('batch'))
expectType<Promise<{ value: number }>>(
  swr.cache({ key: 'plain', fetch: async () => ({ value: 42 }) }).get(),
)
// @ts-expect-error Plain { value } cannot satisfy SwrNoCache<number>.
swr.cache<number>({ key: 'k', fetch: async () => ({ value: 42 }) })
// @ts-expect-error SwrNoCache<number> cannot satisfy a string cache.
swr.cache<string>({ key: 'k', fetch: async () => new SwrNoCache(123) })
swr.cache<string>({
  key: 'k',
  // @ts-expect-error SwrWriteBatch<number> cannot satisfy a string cache.
  fetch: async () => new SwrWriteBatch({ primary: 123 }),
})
// Secondary writes accept any value codec through `encode` contravariance; the pairing with
// `value` is not type-checked. Client-level codecs still reject narrow codecs (see above).
const writes: SwrCacheWrite[] = [
  { key: 'map', value: new Map([['a', 1]]), codec: mapCodec },
  { key: 'label', value: 'heading', codec: stringCodec },
  { key: 'plain', value: 1, ttl: 30 },
]
void writes

// Pagination subpath infers item and context types from pageIndex and fetch.
interface ArticleQuery {
  topic: string
  pageIndex: number
}
const articles = createSwrPaginatedCache(swr, {
  pageSize: 10,
  prefetchPages: 2,
  pageIndex: (ctx: ArticleQuery) => ctx.pageIndex,
  key: (ctx, pageIndex, pageSize) => `articles:${ctx.topic}:${pageIndex}:${pageSize}`,
  fetch: async (ctx, range, meta) => {
    expectType<number>(range.limit)
    expectType<'attempt' | 'skip'>(meta.cacheWrite)
    if (ctx.pageIndex === 0) return new SwrNoCache({ docs: [{ id: '1' }], totalDocs: 1 })
    return { docs: [{ id: '2' }], totalDocs: 2 }
  },
})
expectType<SwrCache<SwrPaginatedPage<{ id: string }>, ArticleQuery>>(articles)
const arrayPages = createSwrPaginatedCache(swr, {
  pageSize: 5,
  pageIndex: (ctx: { page: number }) => ctx.page,
  key: (_ctx, p, s) => `arr:${p}:${s}`,
  ttl: ({ context, value }) => {
    expectType<{ page: number }>(context)
    return value.docs.length === 0 ? 5 : 60
  },
  fetch: async () => [{ id: 1 }],
})
expectType<Promise<SwrPaginatedPage<{ id: number }>>>(arrayPages.get({ page: 0 }))
createSwrPaginatedCache(swr, {
  pageSize: 10,
  pageIndex: (ctx: { page: number }) => ctx.page,
  key: (_ctx, p, s) => `invalid:${p}:${s}`,
  // @ts-expect-error Primitive fetch result does not satisfy SwrPaginationFetchResult.
  fetch: async () => 42,
})

// Pagination options default to a void context.
const voidPages: SwrPaginatedCacheOptions<string> = {
  pageSize: 10,
  pageIndex: () => 0,
  key: (_context, p) => `void:${p}`,
  fetch: async (_context, range: SwrPaginationRange) => [String(range.skip)],
}
expectType<Promise<SwrPaginatedPage<string>>>(createSwrPaginatedCache(swr, voidPages).get())
expectType<Promise<SwrPaginatedPage<string>>>(
  createSwrPaginatedCache(swr, {
    pageSize: 10,
    pageIndex: () => 0,
    key: (_ctx, p) => `inferred-void:${p}`,
    fetch: async () => ['a'],
  }).get(),
)
expectType<Promise<SwrPaginatedPage<string>>>(createSwrPaginatedCache<string>(swr, voidPages).get())
