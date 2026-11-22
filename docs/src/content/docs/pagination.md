---
title: Pagination and prefetching
description: Cache offset-paginated queries and prefetch the following pages in the same Redis write.
---

`createSwrPaginatedCache` from `swr-redis/pagination` caches one page per
key and optionally prefetches following pages in the same write. It returns a
regular `SwrCache<SwrPaginatedPage<TItem>, Context>`. The example builds on
`swr` from [getting started](/swr-redis/).

## Cache paginated queries

```ts
import { createSwrPaginatedCache } from 'swr-redis/pagination'

interface Product {
  id: string
  name: string
  createdAt: Date
}

interface ProductQuery {
  categoryId: string
  pageIndex: number
}

const products = createSwrPaginatedCache(swr, {
  name: 'products',
  pageSize: 20,
  prefetchPages: 2,
  ttl: 120,
  pageIndex: (query: ProductQuery) => query.pageIndex,
  key: (query, page, size) => `products:${query.categoryId}:p${page}:s${size}`,
  fetch: async (query, { skip, limit }, { signal }) => {
    const [docs, totalDocs] = await Promise.all([
      db.findProducts(query.categoryId, { skip, limit, signal }),
      db.countProducts(query.categoryId, { signal }),
    ])
    return { docs, totalDocs }
  },
})

const page0 = await products.get({ categoryId: 'shoes', pageIndex: 0 })
const page1 = await products.get({ categoryId: 'shoes', pageIndex: 1 })
```

Reading page `0` fetches up to 60 items and caches pages `0`, `1`, and `2`, so
reading page `1` is a cache hit.

Each cached page has the shape `{ docs, pageIndex, pageSize, totalDocs? }`.
`fetch` can return `{ docs, totalDocs? }`, a plain `TItem[]`, or either wrapped
in `SwrNoCache`. `totalDocs` is included on cached pages only when `fetch`
returns it.

## Pagination rules

- **Keep `pageIndex` and `pageSize` pure.** Both run once to build the key and
  again inside `fetch`. `pageIndex` must return a non-negative safe integer;
  `pageSize` (a number or `(context) => number`) must be a positive safe
  integer.
- **Build keys from all inputs.** `key(context, page, size)` runs for the
  requested page and every prefetched page. Use the `page` and `size` arguments
  (not `context`) so prefetched pages get distinct keys, and include every
  filter and sort field from `context`.
- **Sort with a unique tiebreaker.** End your `ORDER BY` with a unique column
  such as `id` so tied rows stay in a stable order across queries. Note that
  offset pagination can still shift rows between pages when rows are inserted or
  deleted.
- **Stay within safe integers.** `prefetchPages` defaults to `0`.
  `pageIndex * pageSize`, `pageSize * (1 + prefetchPages)`, and
  `pageIndex + prefetchPages` must be safe integers or `SwrValidationError` is
  thrown.

## Prefetching

When `prefetchPages > 0`, `fetch` receives `skip = pageIndex * pageSize` and
`limit = pageSize * (1 + prefetchPages)`. The first `pageSize` items form the
requested page; remaining items are sliced into up to `prefetchPages` additional
pages and committed via [`SwrWriteBatch`](/swr-redis/fetch-results/).
Trailing empty pages are not written.

- All written pages share `totalDocs`, the resolved TTL (a `ttl` callback runs
  once for the requested page), `codec`, and `compression`.
- During a [fallback fetch](/swr-redis/consistency/#failure-strategies)
  (`cacheWrite: 'skip'`), `fetch` receives `limit = pageSize` and returns only
  the requested page.
- Returning `SwrNoCache` delivers the requested page and caches none of the
  pages.
