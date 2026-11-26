---
title: Configure clients and caches
description: Option defaults and scopes, dynamic TTLs, the in-memory LRU cache, and advanced tuning for locks, codecs, compression, and ioredis timeouts.
---

Set shared defaults on `createSwrClient` and override them per cache on
`swr.cache`. All client and cache methods are bound, so you can destructure
them. Examples build on `redis`, `swr`, and `userCache` from
[getting started](/swr-redis/).

## Options

Durations are in seconds.

| Option | Set on | Description |
| --- | --- | --- |
| `redis` | Client | Required. Your `ioredis` connection. |
| `prefix` | Client | Namespace isolating every Redis key written by this client. Default: `'default'`. |
| `key` | Cache | Required. Static string, or `(context) => string`. |
| `fetch` | Cache | Required. `(context, meta) => Promise<value>`. See [fetch results](/swr-redis/fetch-results/). |
| `ttl` | Client, cache | Fresh lifetime in seconds. A cache can pass a callback. Default: `120`. |
| `staleMultiplier` | Client, cache | Total Redis lifetime is `ttl * staleMultiplier`. Set `1` to disable the stale window. Default: `5`. |
| `staleWait` | Client, cache, `get()` | Seconds a caller holding stale data waits for fresh data (`0` returns stale immediately, `Infinity` waits until settled). Default: `0`. |
| `runtime` | Client | What happens without a `waitUntil` hook: `'server'` (background work allowed) or `'serverless'` (lock winner waits inline). Default: `'server'`. |
| `fetchTimeout` | Client, cache | Seconds a single `fetch` call may run; also bounds how long waiters poll. Default: `30`. |
| `failureCooldown` | Client, cache | Seconds a key rests after a fetch fails or times out (`0` disables). Default: `1`. |
| `failureStrategy` | Client, cache | Behavior on Redis errors when no stale value is available: `'availability'` or `'protect-source'`. Default: `'availability'`. |
| `memory` | Client, cache, `get()` | Process-local LRU cache. Enable on the client with `true` or `{ maxEntries, maxTtl, clone }`; pass `false` on a cache or `get()` to opt out. Default: `false`. |
| `signal` | `get()` | Cancels this call (checked between steps) without aborting a leased refresh (a direct fallback fetch under `'availability'` aborts once its last caller leaves). Pass `AbortSignal.timeout(ms)` for a request deadline. |
| `name` | Cache | Low-cardinality label reported as `event.cache`. |
| `codec` | Client, cache | Serializer: `{ encode, decode }`. Default: BSON. |
| `compression` | Client, cache | zlib settings: `true`, `false`, or `{ level, minBytes }`. Default: `{ level: 1, minBytes: 256 }`. |
| `lockTtl` | Client, cache | Fetch lock TTL, renewed while `fetch` runs; controls crash detection. Default: `3`. |
| `pollInterval` | Client, cache | Base interval between poll checks while waiting, plus up to 30% jitter. Default: `0.08`. |
| `isFatal` | Client, cache | Predicate marking fetch errors that should throw to a waiting caller instead of falling back to stale data. |
| `onEvent` | Client | Telemetry callback. See [observability](/swr-redis/observability/). |
| `waitUntil` | Client, `get()` | Keep-alive hook for refresh promises; enables background work under either `runtime`. A per-call hook overrides the client hook. |

## Set the TTL from the fetched value

A cache's `ttl` can be a synchronous callback receiving `{ context, key, value }`,
for example to cache misses for a shorter time:

```ts
const userById = swr.cache({
  name: 'users',
  key: (id: string) => `user:${id}`,
  fetch: (id) => db.getUser(id), // Promise<User | null>
  ttl: ({ value }) => (value === null ? 5 : 120),
  staleMultiplier: 5, // null: 5s fresh, 25s total. User: 120s fresh, 600s total.
})
```

Place `fetch` before `ttl` so TypeScript infers `value` (or annotate `fetch` /
pass explicit type arguments `swr.cache<User | null, string>()`).

The callback runs when preparing a cache write from `fetch` or `set()`.
Return a positive finite number in seconds. Other clients reading the same
key use the lifetime chosen by the writer.

## Cache values in memory

Enable `memory` on `createSwrClient` to keep fresh entries in a process-local
LRU cache shared by every cache on that client. Opt out per cache or per `get()`
call with `memory: false`.

```ts
const swrWithMemory = createSwrClient({
  redis,
  prefix: 'my-app:prod',
  memory: {
    maxEntries: 2_000,
    maxTtl: 30,
    clone: false,
  },
})

const auditCache = swrWithMemory.cache({
  key: (id: string) => `audit:${id}`,
  memory: false,
  fetch: (id) => db.getAuditRecord(id),
})

const userFromRedis = await userCache.get('u_123', { memory: false })
```

- **Limits.** `maxEntries` defaults to 1,000. Memory serves fresh entries only.
  Set `maxTtl` in seconds to cap their time in memory from the original write.
- **Invalidation.** Each client instance has its own memory cache. An
  invalidation elsewhere can leave a local copy readable until its freshness
  or memory TTL expires.
- **Values.** Treat cached objects as immutable with the default `clone: false`.
  Use `clone: true` for values supported by `structuredClone`. Cloning can
  change types such as `Buffer`, `ObjectId`, and custom classes. Memory serves
  values directly, so your codec should preserve them on a round trip.

## Advanced

### Timing, cooldowns, and polling

`fetchTimeout` bounds each source call (including background refreshes) and also
caps how long waiters poll behind it. Set it above how long your source normally
takes, because late results are discarded and trigger a cooldown.

`failureCooldown` spaces out retries after a fetch fails or times out (about one
attempt per key per cooldown across all processes). Increase it for expensive
sources, or set `0` to disable it.

`lockTtl` controls how quickly a crashed fetcher's lock expires (locks renew
automatically until `fetch` settles or `fetchTimeout` fires). A longer
`pollInterval` reduces Redis reads at the cost of extra wait latency.

### Serialization and compression

The default codec uses your application's `bson` peer dependency. It preserves
`Date`, `Buffer`, arrays, and top-level `null` or `undefined`. To round-trip
BSON types such as `ObjectId`, match the BSON major used by the code that
creates them (BSON 6.10.4 or later in the 6.x line for MongoDB driver 6,
BSON 7.x for driver 7).

Compare IDs with `id.equals(otherId)`. The MongoDB driver and the default
codec can load different `ObjectId` constructors through BSON's CommonJS and
ESM entry points. If your code relies on `instanceof mongodb.ObjectId`,
configure a codec that uses `BSON.serialize` and `BSON.deserialize` from
`mongodb`.

For other types such as `Map` or `bigint`, provide a `SwrCodec<T>` on a cache
or `SwrCodec<unknown>` on the client:

```ts
import type { SwrCodec } from 'swr-redis'

const countsCodec: SwrCodec<Map<string, number>> = {
  encode: (counts) => Buffer.from(JSON.stringify([...counts])),
  decode: (bytes) => new Map(JSON.parse(Buffer.from(bytes).toString())),
}

const featureCounts = swr.cache({
  key: 'feature-counts:v1',
  codec: countsCodec,
  compression: { level: 3, minBytes: 512 },
  fetch: () => db.loadFeatureCounts(),
})
```

Payloads of 256 bytes or more are zlib-compressed at level `1` by default. Pass
`compression: false` or `level: 0` to disable compression, or `{ level, minBytes }`
to tune it (`level` `0`–`9`).

Compression settings can be changed safely without invalidating existing keys.
Changing a `codec` cannot decode old entries, so bump a version segment in your
key. Decode and decompression errors throw under both failure strategies without
triggering a new fetch, unless a caller holding an unexpired stale value can
fall back to it (reported via a `stale_fallback` event).

### Set ioredis command timeouts

Configure `commandTimeout` (in milliseconds) and `maxRetriesPerRequest` on your
`ioredis` connection. Caller `signal` cancellation is checked between Redis
commands rather than interrupting commands in flight, so these settings bound
how long a stalled Redis command can block a call. A timed-out command is
treated as a [Redis failure](/swr-redis/consistency/#failure-strategies).

For caches with large payloads, duplicate the connection with a longer timeout
and create a second client sharing the same `prefix`:

```ts
const fastRedis = new Redis({ host: '127.0.0.1', port: 6379, commandTimeout: 2_000 })
const fastSwr = createSwrClient({ redis: fastRedis, prefix: 'my-app:prod' })

const largePayloadRedis = fastRedis.duplicate({ commandTimeout: 10_000 })
const largePayloadSwr = createSwrClient({
  redis: largePayloadRedis,
  prefix: 'my-app:prod',
})
```

Each client keeps its own memory cache and `flush()` tracks only that client's
background refreshes. Close both connections on shutdown.
