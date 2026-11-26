import { SwrValidationError } from '../errors.ts'
import { isNonNegativeSafeInteger, isPositiveSafeInteger } from '../options.ts'
import type {
  SwrCache,
  SwrCacheOptions,
  SwrCacheWrite,
  SwrClient,
  SwrFetchMeta,
  SwrTtlOption,
} from '../types.ts'
import { SwrNoCache, SwrWriteBatch } from '../wrappers.ts'

export interface SwrPaginatedPage<TItem> {
  docs: TItem[]
  pageIndex: number
  pageSize: number
  totalDocs?: number
}

export type SwrPaginationData<TItem> = { docs: TItem[]; totalDocs?: number } | TItem[]

export interface SwrPaginationRange {
  skip: number
  limit: number
}

export interface SwrPaginatedCacheOptions<TItem = unknown, Context = void> extends Omit<
  SwrCacheOptions<SwrPaginatedPage<TItem>, Context>,
  'key' | 'fetch'
> {
  /** Use `pageIndex` and `pageSize` (not `context`) so prefetched pages get distinct keys. */
  key: (context: Context, pageIndex: number, pageSize: number) => string
  fetch: (
    context: Context,
    range: SwrPaginationRange,
    meta: SwrFetchMeta<Context>,
  ) => Promise<SwrPaginationData<TItem> | SwrNoCache<SwrPaginationData<TItem>>>
  /** Pure function returning the 0-based page index (runs for key building and inside `fetch`). */
  pageIndex: (context: Context) => number
  /** Positive safe integer, or a pure function (runs for key building and inside `fetch`). */
  pageSize: number | ((context: Context) => number)
  /** Additional following pages to fetch and cache in the same write. Default: `0`. */
  prefetchPages?: number
  /** Resolved once for the requested page and shared by prefetched pages. */
  ttl?: SwrTtlOption<NoInfer<SwrPaginatedPage<TItem>>, NoInfer<Context>>
}

function computePageLimit(pageSize: number, prefetchPages: number) {
  const totalPages = 1 + prefetchPages
  const limit = pageSize * totalPages
  if (!Number.isSafeInteger(totalPages) || !Number.isSafeInteger(limit)) {
    throw new SwrValidationError('pageSize * (1 + prefetchPages) exceeds maximum safe integer')
  }
  return limit
}

function computePaginationRange(
  pageIndex: number,
  pageSize: number,
  prefetchPages: number,
  limitOverride?: number,
) {
  const skip = pageIndex * pageSize
  if (!Number.isSafeInteger(skip)) {
    throw new SwrValidationError('pageIndex * pageSize exceeds maximum safe integer')
  }
  const limit = limitOverride ?? computePageLimit(pageSize, prefetchPages)
  if (!Number.isSafeInteger(pageIndex + prefetchPages)) {
    throw new SwrValidationError('pageIndex + prefetchPages exceeds maximum safe integer')
  }
  return { skip, limit }
}

function extractDocs<TItem>(data: SwrPaginationData<TItem>) {
  if (Array.isArray(data)) return { docs: data }
  return { docs: data.docs, totalDocs: data.totalDocs }
}

function buildPage<TItem>(
  docs: TItem[],
  offset: number,
  pageIndex: number,
  pageSize: number,
  totalDocs: number | undefined,
) {
  return {
    docs: docs.slice(offset, offset + pageSize),
    pageIndex,
    pageSize,
    ...(totalDocs !== undefined ? { totalDocs } : {}),
  }
}

/** Caches offset-paginated queries one page per key, optionally prefetching following pages. */
export function createSwrPaginatedCache<TItem = unknown, Context = void>(
  client: SwrClient,
  options: SwrPaginatedCacheOptions<TItem, Context>,
): SwrCache<SwrPaginatedPage<TItem>, Context> {
  const {
    pageSize: rawPageSize,
    prefetchPages = 0,
    pageIndex: getPageIndex,
    key: keyFn,
    fetch: fetcher,
    ...cacheOptions
  } = options

  if (typeof rawPageSize === 'number' && !isPositiveSafeInteger(rawPageSize)) {
    throw new SwrValidationError('pageSize must be a positive safe integer')
  }

  if (!isNonNegativeSafeInteger(prefetchPages)) {
    throw new SwrValidationError('prefetchPages must be a non-negative safe integer')
  }

  const staticLimit =
    typeof rawPageSize === 'number' ? computePageLimit(rawPageSize, prefetchPages) : undefined

  const resolveParams = (context: Context) => {
    const pageIndex = getPageIndex(context)
    if (!isNonNegativeSafeInteger(pageIndex)) {
      throw new SwrValidationError('pageIndex must be a non-negative safe integer')
    }

    if (typeof rawPageSize === 'number') return { pageIndex, pageSize: rawPageSize }

    const pageSize = rawPageSize(context)
    if (!isPositiveSafeInteger(pageSize)) {
      throw new SwrValidationError('pageSize must be a positive safe integer')
    }

    return { pageIndex, pageSize }
  }

  const key = (context: Context) => {
    const { pageIndex, pageSize } = resolveParams(context)
    return keyFn(context, pageIndex, pageSize)
  }

  return client.cache<SwrPaginatedPage<TItem>, Context>({
    ...cacheOptions,
    key,
    fetch: async (context, meta) => {
      const { pageIndex, pageSize } = resolveParams(context)
      const isSkip = meta.cacheWrite === 'skip'
      const range = computePaginationRange(
        pageIndex,
        pageSize,
        isSkip ? 0 : prefetchPages,
        isSkip ? pageSize : staticLimit,
      )
      const result = await fetcher(context, range, meta)

      if (result instanceof SwrNoCache) {
        const { docs, totalDocs } = extractDocs(result.value)
        return new SwrNoCache(buildPage(docs, 0, pageIndex, pageSize, totalDocs))
      }

      const { docs, totalDocs } = extractDocs(result)
      const primary = buildPage(docs, 0, pageIndex, pageSize, totalDocs)
      if (prefetchPages > 0 && !isSkip && docs.length > pageSize) {
        const secondary: SwrCacheWrite[] = []
        for (let i = 1; i <= prefetchPages; i++) {
          const offset = i * pageSize
          if (offset >= docs.length) break
          const nextPageIndex = pageIndex + i
          secondary.push({
            key: keyFn(context, nextPageIndex, pageSize),
            value: buildPage(docs, offset, nextPageIndex, pageSize, totalDocs),
            codec: cacheOptions.codec,
          })
        }
        return new SwrWriteBatch({ primary, secondary })
      }
      return primary
    },
  })
}
