# Design decisions

This document records established design decisions whose rationale or consequences
are easy to miss. Keep entries brief and explain the choice, why it was made, and
what trade-offs it carries.

These decisions can evolve. When a change departs from a recorded decision, call
out the departure explicitly and explain why.

## Timing is best-effort

Timers bound waiting, not CPU work. They fire only when the event loop is free,
so a fetch, codec, or callback that blocks the loop can overrun `fetchTimeout`,
`staleWait`, or the waiting limit, and its result is still accepted. Don't add
elapsed-time re-checks or millisecond-precision guards for slow synchronous
work; only the caller can fix it. Schedules must still never land exactly on an
expiry (renewal runs every 40% of `lockTtl`).

## Redis leases guard commits

Acquire, renew, release, and commit use an ownership token generated before the
first Redis call. A retry with the same token can restore the lease after a lost
reply, and cleanup can release that token even when acquisition was never
acknowledged. The initial Redis TTL is `lockTtl` so the lease expires quickly
after a crash. Renewals extend by the full `lockTtl`, uncapped, until the fetch
settles or `fetchTimeout` fires, so the lease is still held for the cooldown swap
after a timeout.

After a shared fetch fails or hits `fetchTimeout`, the owner token is swapped for
a fixed cooldown marker that expires after `failureCooldown`. A failing source
then sees about one attempt per key per cooldown, however many callers wait. The
swap checks ownership, so a racing `set` or `invalidate` wins and no marker is
written; both also clear an active marker.

One Lua script checks ownership, publishes the prepared entries, and releases the
lease. Explicit writes and invalidations clear the affected keys' leases, so a
fetch holding the old lease can no longer commit under it. A batch uses only the
primary key's lease. Concurrent writers can still race on secondary keys, and
Redis keeps earlier writes if a later command in the script fails.

## Lock winners skip a second read

Waiters read the data and lease together with one `MGET` to get a consistent
snapshot. After acquiring the lease, the winner goes straight to the source to
save a Redis round trip on each refresh. A write between the read and acquisition
can therefore trigger a redundant fetch and be overwritten. The same
gap lets a caller repopulate a key after invalidation. A lagging source can make
that repopulated value out of date for its entire fresh and stale lifetime.

## Callers attach to a shared refresh

A refresh that holds the lease runs as one tracked promise covering the fetch,
the commit or cooldown swap, and the memory update. A caller's `signal` or
`staleWait` only detaches it and never aborts the fetch: aborting would livelock
a key whose callers all give up before the source responds. `fetchTimeout` is
the fetch's only bound, and a result that arrives after it is discarded.

The refresh reports its own failure once, based on whether usable stale data
remains when it settles, so events don't depend on whether a caller is still
attached. Waiters poll up to the waiting limit (`fetchTimeout`) and never call
the source themselves; `failureStrategy` applies to Redis failures only.

## Cancellation is checked between steps

There is no caller deadline option; callers pass `AbortSignal.timeout`. The
signal is checked between Redis commands rather than raced against them, so no
reply arrives after the caller has left and no late lock grant needs cleanup.
An abort may wait for the current reply; ioredis `commandTimeout` bounds a
stalled Redis.

## Stale handling separates preference from capability

`staleWait` is a freshness preference: how long a caller holding usable stale
data waits for fresh data. Whether a lock winner may stop waiting is a runtime
capability: `runtime: 'server'` or a `waitUntil` hook. A serverless winner
without a hook ignores `staleWait` and waits for its refresh, because detached
work may freeze with the function.

## The writer sets freshness and expiry

Each envelope starts with a 25-byte header containing a compression flag byte and
three big-endian float64 timestamps for write time, freshness, and expiry. The
header lets readers reject unusable entries before decoding the payload.
Read timing is best-effort during synchronous decoding, so one clock read suffices.

Readers honor the writer's deadlines even when configured with different TTLs.
Redis expiry is rounded up to whole seconds for key cleanup. The deadlines use
wall-clock time, so clock skew between writers and readers affects how long
entries appear fresh or usable. The format version lives in the Redis key
namespace to keep incompatible envelopes apart.

## Fallbacks avoid uncoordinated source load and writes

An already-read stale snapshot is re-checked against `expiresAtMs` after async
lock acquisition, polling, and inline fetches so an expired snapshot is not
served; once expired, the call falls back to the normal cold-miss or error path
(detached refreshes still settle without rejecting after returning a valid
snapshot). Without a usable snapshot, decode, decompression, and
memory-clone errors throw without fetching so a codec mismatch or corrupt entry
cannot trigger a fetch storm. Direct fallback fetches under `availability` skip
all cache writes because they hold no lease. Concurrent callers in one cache
instance share one direct fetch per key, and the last caller to leave aborts it,
since without a cache write nobody else can use its result.

## Memory cache consistency

The optional per-client LRU serves fresh entries only, leaving stale handling
to Redis coordination. Memory updates settle after the Redis call completes,
so a read during a write can still observe the previous value. Per-key versions
prevent earlier reads from repopulating memory after a local write settles.

A write evicts the local entry if another write settled during its Redis call
or its outcome is uncertain. Replies alone cannot establish which value Redis
kept. Even a commit that reports a lost lease can be uncertain. If its first
reply was lost, Redis may have applied the write before the retry found the
lease gone. Eviction sends the next read to Redis. Once a local write settles,
a memory hit on that client cannot return a value that predates that write in
Redis execution order. Other clients can serve their own fresh copies until
expiry or eviction.
