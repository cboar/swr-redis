# Design decisions

This document records what a reader cannot get from the code, especially
deliberate choices that could look like a bug or oversight to "fix":

- Why a choice was made, and rejected alternatives.
- Non-obvious consequences and trade-offs.
- Guarantees to preserve, and rules for future changes.

Omit:

- Mechanics readable from the source.
- User-facing behavior already in the docs.
- Self-evident or platform-forced choices.

Keep entries brief (the choice, why, and what it costs), with only the minimal
context a non-obvious point needs.

These decisions can evolve. When a change departs from a recorded decision, call
out the departure explicitly and explain why.

## Timing is best-effort

Timers bound waiting, not CPU work. Because they fire only when the event loop is
free, a fetch, codec, or callback that blocks the loop can overrun `fetchTimeout`,
`staleWait`, or the waiting limit and still have its result accepted. Don't add
elapsed-time re-checks or millisecond-precision guards for slow synchronous work;
only the caller can fix it. Schedules must still never land exactly on an expiry
(for example, lock renewal runs every 40% of `lockTtl`).

## Redis leases guard commits

Generating the lease token before the first Redis call lets a retry restore the
lease after a lost reply and lets cleanup release it even when acquisition was
never acknowledged. Renewals extend by the full `lockTtl` without capping at
`fetchTimeout`, so the lease is still held when a timed-out fetch swaps it for
the cooldown marker.

Swapping the lease to a cooldown marker after a failed or timed-out shared fetch
limits a failing source to roughly one attempt per key per `failureCooldown`
across all processes. The swap checks ownership, so a racing `set` or
`invalidate`, which clears the lease, wins without leaving a cooldown behind.

`set` and `invalidate` clear the lease so a fetch already in flight cannot commit
older data over them. Batch writes guard only the primary key's lease, so
concurrent writers can race on secondary keys. Redis also does not roll back
earlier commands if a later write in the commit script fails.

## Lock winners skip a second read

Waiters read the data and lease together with one `MGET` to get a consistent
snapshot. After acquiring the lease, the winner goes straight to the source to
save a Redis round trip on each refresh. A write between the read and acquisition
can therefore trigger a redundant fetch and be overwritten. The same gap lets a
caller repopulate a key after invalidation. A lagging source can make that
repopulated value out of date for its entire fresh and stale lifetime.

## Callers attach to a shared refresh

A caller's `signal` or `staleWait` only detaches it from a leased refresh and
never aborts the fetch: aborting would livelock a key whose callers all give up
before the source responds. `fetchTimeout` is the fetch's only bound, and late
results are discarded.

The refresh reports its own failure once, based on whether usable stale data
remains when it settles, so telemetry does not depend on whether a caller is
still attached. Hitting the waiting limit (`fetchTimeout`) never triggers a
direct fetch; `failureStrategy` applies to Redis failures only.

## Cancellation is checked between steps

There is no caller deadline option; callers pass `AbortSignal.timeout`. The
signal is checked between Redis commands rather than raced against them, so no
reply arrives after the caller has left and no late lock grant needs cleanup.
An abort may wait for the current reply; ioredis `commandTimeout` bounds a
stalled Redis.

## The writer sets freshness and expiry

Envelopes carry a small header with their deadlines so readers can reject
unusable entries before decoding the payload and honor the writer's deadlines
even when processes are configured with different TTLs. Deadlines use wall-clock
time, so clock skew between writers and readers affects how long entries appear
fresh or usable. Any incompatible envelope change must bump the version in the
Redis key namespace (`swr:v1` in `src/keys.ts`) so old and new formats do not
collide.

## Fallbacks avoid uncoordinated source load and writes

An expired stale snapshot is never served: if a snapshot expires before a call
returns it, the call falls back to the cold-miss or error path. Without a usable
snapshot, decode, decompression, and memory-clone errors throw instead of
refetching so a codec mismatch or corrupt entry cannot trigger a fetch storm.

Direct fallback fetches under `availability` skip all cache writes because they
hold no lease. Because nothing is cached, the last attached caller to leave
aborts the shared direct fetch—nobody else can use its result.

## Memory cache consistency

The optional per-client LRU serves fresh entries only, leaving stale handling
to Redis coordination. Once a local write settles, a memory hit on that client
never returns a value older than that write in Redis execution order; other
clients can still serve their own fresh copies until expiry or eviction.

To preserve that guarantee, concurrent or uncertain local writes evict the
memory entry so the next read goes to Redis: client replies alone cannot
establish which value Redis kept. Even a commit that reports a lost lease is
uncertain, because a lost reply followed by a resend can apply the write on the
first attempt before the retry finds the lease gone.
