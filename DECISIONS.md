# Design decisions

This document records established design decisions whose rationale or consequences
are easy to miss. Keep entries brief and explain the choice, why it was made, and
what trade-offs it carries.

These decisions can evolve. When a change departs from a recorded decision, call
out the departure explicitly and explain why.

## Redis leases guard commits

Acquire, renew, release, and commit use an ownership token generated before the
first Redis call. A retry with the same token can restore the lease after a lost
reply, and cleanup can release that token even when acquisition was never
acknowledged. The initial Redis TTL is `lockTtl` so the lease expires quickly
after a crash. Renewals give slow fetches time to finish. Each renewal requests
at most `lockTtl`, capped by the time left in the client's `maxLockTtl` budget.
Redis applies that TTL when it executes the command, so command latency can
push expiry past the budget.

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

## The writer sets freshness and expiry

The envelope header stores the writer's timestamps so readers can reject
unusable entries before decoding the payload. Readers check again afterward
because decoding can cross a deadline.

Readers honor the writer's deadlines even when configured with different TTLs.
Redis expiry is rounded up to whole seconds for key cleanup. The deadlines use
wall-clock time, so clock skew between writers and readers affects how long
entries appear fresh or usable. The format version lives in the Redis key
namespace to keep incompatible envelopes apart.

## Failure policy depends on whether the source has been called

When Redis fails before a fetch, `availability` serves the stale snapshot when
available or fetches directly. That direct fetch skips all cache writes because
it holds no lease. `protect-source` fails the request to avoid sending
uncoordinated traffic to the source. The same choice applies on a cold-miss
timeout. On a stale wait, both strategies return the original snapshot if the
wait times out or the lease clears without a fresh entry. Serving the snapshot
adds no source load, even if it expired during the wait.

Once the fetched value is ready to commit, both strategies return it if the
cache write fails or ownership is lost. The source has already done the work.

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
