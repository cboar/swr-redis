import type { ClientInternals } from './client.ts'
import { decodeEnvelopeValue, encodeEnvelope, readEnvelopeHeader, type Envelope } from './codec.ts'
import { DirectFetches } from './direct.ts'
import { isFatalError, SwrTimeoutError, SwrValidationError } from './errors.ts'
import {
  bindCacheEmitter,
  errorEvent,
  reportRedisError,
  timeoutEvent,
  type Emit,
} from './events.ts'
import { requireNonEmpty } from './keys.ts'
import { isCooldownMarker, RedisLock, type AcquireOutcome, type RedisWriteEntry } from './lock.ts'
import type { MemoryWrite, ReadGuard } from './memory.ts'
import {
  requireTtl,
  resolveCacheOptions,
  resolveStaleWaitMs,
  toRedisTtlMs,
  type ResolvedCacheOptions,
} from './options.ts'
import { awaitWithin, pollDelayMs, sleep, startBoundedFetch, type FetchFailure } from './timing.ts'
import type {
  SwrNonEmptyKeys,
  SwrCache,
  SwrCacheOptions,
  SwrCodec,
  SwrEntryTiming,
  SwrEvent,
  SwrFetchResult,
  SwrGetOptions,
  SwrGetResult,
} from './types.ts'
import { SwrNoCache, SwrWriteBatch } from './wrappers.ts'

interface CacheRequest<Context> {
  readonly context: Context
  readonly key: string
  readonly dataKey: string
  readonly lockKey: string
  readonly signal?: AbortSignal
  readonly waitUntil?: (promise: Promise<unknown>) => void
  readonly useMemory: boolean
  readonly staleWaitMs: number
  // True when work may outlive the call (runtime 'server' or a waitUntil hook).
  readonly canDetach: boolean
}

type Accept = 'fresh' | 'usable'

type LockAttempt =
  | { status: 'acquired'; lock: RedisLock }
  | { status: Exclude<AcquireOutcome, 'acquired'> }
  | { status: 'redis_error'; err: unknown }

type PreparedWrite = RedisWriteEntry & MemoryWrite & { readonly timing: SwrEntryTiming }

type PreparedCommit<T> =
  | { kind: 'no-cache'; value: T }
  | {
      kind: 'write'
      value: T
      entries: readonly [PreparedWrite, ...PreparedWrite[]]
      durationMs: number
    }

type PollSnapshot<T> =
  | { status: 'hit'; envelope: Envelope<T> }
  | { status: 'locked' | 'cooldown' | 'unlocked' }
  | { status: 'redis_error'; err: unknown }

type WaitResult<T> =
  | { outcome: 'resolved' | 'stale_fallback'; envelope: Envelope<T> }
  | { outcome: 'timeout'; elapsedMs: number; stale?: Envelope<T> }
  | { outcome: 'error'; err: unknown }
  | { outcome: 'lock_stolen'; lock: RedisLock }

// Result of a tracked refresh; never rejects and has already reported any failure.
type RefreshOutcome<T> =
  { kind: 'ok'; result: SwrGetResult<T> } | { kind: 'failed'; err: unknown; fallback?: Envelope<T> }

interface PollProgress {
  attempts: number
  readonly startedAt: number
}

type PollOutcome = Extract<SwrEvent, { type: 'poll' }>['outcome']
type RefreshTrigger = Extract<SwrEvent, { type: 'refresh' }>['trigger']
type StaleFallbackStage = Extract<SwrEvent, { reason: 'stale_fallback' }>['stage']

export class SwrCacheImpl<T = unknown, Context = void> implements SwrCache<T, Context> {
  private readonly client: ClientInternals
  private readonly options: ResolvedCacheOptions<T, Context>
  private readonly fetch: SwrCacheOptions<T, Context>['fetch']
  private readonly emit?: Emit
  private readonly directFetches: DirectFetches<T>

  constructor(client: ClientInternals, options: SwrCacheOptions<T, Context>) {
    this.client = client
    this.options = resolveCacheOptions(client.options, options)
    this.fetch = options.fetch
    this.emit = bindCacheEmitter(client.emit, options.name)
    this.directFetches = new DirectFetches((key, failure) =>
      this.reportFailure(key, failure, undefined),
    )
  }

  key(context: Context) {
    return this.options.keyOf(context)
  }

  async get(context: Context, options?: SwrGetOptions) {
    return (await this.getWithMeta(context, options)).value
  }

  async getWithMeta(context: Context, options?: SwrGetOptions): Promise<SwrGetResult<T>> {
    const staleWaitMs =
      options?.staleWait === undefined
        ? this.options.staleWaitMs
        : resolveStaleWaitMs(options.staleWait)
    options?.signal?.throwIfAborted()
    const key = this.options.keyOf(context)
    const useMemory = this.options.memoryEnabled && options?.memory !== false

    if (useMemory) {
      const hit = this.client.memory.get<T>(key, Date.now())
      if (hit) {
        this.emit?.({
          type: 'hit',
          key,
          source: 'memory',
          stale: false,
          startedRevalidation: false,
        })
        return cached(hit, 'memory')
      }
    }

    const { keys, redis } = this.client
    const waitUntil = options?.waitUntil ?? this.client.waitUntil
    const req: CacheRequest<Context> = {
      context,
      key,
      dataKey: keys.data(key),
      lockKey: keys.lock(key),
      signal: options?.signal,
      waitUntil,
      useMemory,
      staleWaitMs,
      canDetach: this.client.options.runtime === 'server' || waitUntil !== undefined,
    }

    const read = await this.guardedRead(req, async (guard) => {
      let buf: Buffer | null
      try {
        buf = await redis.getBuffer(req.dataKey)
      } catch (err) {
        req.signal?.throwIfAborted()
        return { redisError: err }
      }
      req.signal?.throwIfAborted()
      return { entry: this.decodeAndPopulate(buf, 'usable', guard) }
    })
    if ('redisError' in read) return this.onRedisError(req, read.redisError)

    const { entry } = read
    if (!entry) return this.onColdMiss(req)
    if (!entry.fresh) return this.onStaleHit(req, entry.envelope)
    this.emit?.({ type: 'hit', key, source: 'redis', stale: false, startedRevalidation: false })
    return cached(entry.envelope, 'redis')
  }

  async set(context: Context, value: T) {
    const { redis, keys, memory } = this.client
    const key = this.options.keyOf(context)
    const ttl = this.resolvePrimaryTtl(context, key, value)
    const { envelope, buffer, ttlMs } = this.prepareWrite(
      value,
      Date.now(),
      ttl,
      this.options.codec,
      this.options.memoryEnabled,
    )

    const pending = memory.beginWrite([{ key, envelope }])
    try {
      await redis.swrSetAndClearLock(keys.data(key), keys.lock(key), buffer, ttlMs)
    } catch (err) {
      memory.settle(pending, 'evict')
      const redisError = this.reportRedisError(key, err)
      if (redisError) throw redisError
      return
    }
    memory.settle(pending, 'populate')
  }

  async invalidate(context: Context) {
    await this.client.invalidate(
      [this.options.keyOf(context)],
      this.options.failureStrategy,
      this.emit,
    )
  }

  private async guardedRead<R>(
    req: CacheRequest<Context>,
    read: (guard: ReadGuard | undefined) => Promise<R>,
  ) {
    const { memory } = this.client
    const guard = req.useMemory ? memory.beginRead(req.key) : undefined
    try {
      return await read(guard)
    } finally {
      memory.endRead(guard)
    }
  }

  private decodeAndPopulate(buf: Buffer | null, accept: Accept, guard: ReadGuard | undefined) {
    const entry = decodeEntry(buf, accept, this.options.codec)
    if (entry?.fresh) this.client.memory.populate(guard, entry.envelope, entry.now)
    return entry
  }

  private async tryLock(req: CacheRequest<Context>): Promise<LockAttempt> {
    const { signal } = req
    signal?.throwIfAborted()
    const lock = new RedisLock(this.client.redis, req.lockKey)
    let outcome: AcquireOutcome
    try {
      outcome = await lock.acquire(this.options.lockTtlMs)
    } catch (err) {
      // Redis may have granted the lock before the reply was lost.
      const release = this.safeRelease(req, lock)
      if (signal?.aborted) {
        await release
        throw signal.reason
      }
      this.client.track(release, req.waitUntil)
      return { status: 'redis_error', err }
    }
    if (signal?.aborted) {
      if (outcome === 'acquired') await this.safeRelease(req, lock)
      throw signal.reason
    }
    return outcome === 'acquired' ? { status: 'acquired', lock } : { status: outcome }
  }

  private async onStaleHit(
    req: CacheRequest<Context>,
    stale: Envelope<T>,
  ): Promise<SwrGetResult<T>> {
    const { key } = req
    const attempt = await this.tryLock(req)
    const acquired = attempt.status === 'acquired'
    const usable = isUsable(stale)
    this.emit?.({ type: 'hit', key, source: 'redis', stale: true, startedRevalidation: acquired })

    switch (attempt.status) {
      case 'redis_error':
        return usable
          ? this.serveStaleAfterError(key, stale, 'redis', attempt.err)
          : this.onRedisError(req, attempt.err)
      case 'cooldown':
        return usable ? cached(stale, 'redis') : this.poll(req)
      case 'busy':
        if (!usable) return this.poll(req)
        return req.staleWaitMs === 0 ? cached(stale, 'redis') : this.poll(req, stale)
      case 'acquired':
        // An expired snapshot refreshes as a cold miss.
        const snapshot = usable ? stale : undefined
        return this.refreshAsOwner(req, attempt.lock, 'stale_revalidation', snapshot)
    }
  }

  private async onColdMiss(req: CacheRequest<Context>) {
    const attempt = await this.tryLock(req)
    if (attempt.status === 'redis_error') return this.onRedisError(req, attempt.err)

    if (attempt.status !== 'acquired') {
      this.emit?.({ type: 'miss', key: req.key, action: 'wait' })
      return this.poll(req)
    }

    this.emit?.({ type: 'miss', key: req.key, action: 'fetch' })
    return this.refreshAsOwner(req, attempt.lock, 'cold_miss')
  }

  private async poll(req: CacheRequest<Context>, stale?: Envelope<T>): Promise<SwrGetResult<T>> {
    const progress: PollProgress = { attempts: 0, startedAt: Date.now() }
    let result: WaitResult<T>
    try {
      result = await this.waitForEntry(req, stale, progress)
    } catch (err) {
      if (req.signal?.aborted) {
        this.emitPoll(req, 'cancelled', progress)
        throw req.signal.reason
      }
      this.emitPoll(req, 'error', progress)
      // waitForEntry only throws non-abort errors from decodeAndPopulate.
      if (isUsable(stale)) return this.serveStaleAfterError(req.key, stale, 'read', err)
      throw err
    }
    this.emitPoll(
      req,
      result.outcome,
      progress,
      result.outcome === 'timeout' ? result.elapsedMs : undefined,
    )

    switch (result.outcome) {
      case 'resolved':
      case 'stale_fallback':
        return cached(result.envelope, 'redis')
      case 'timeout': {
        // staleWait, or the waiting limit capping it, is an expected outcome: no timeout event.
        if (result.stale) return cached(result.stale, 'redis')
        const err = new SwrTimeoutError(req.key, result.elapsedMs, 'wait')
        this.emit?.(timeoutEvent(err, false))
        throw err
      }
      case 'error':
        return isUsable(stale)
          ? this.serveStaleAfterError(req.key, stale, 'redis', result.err)
          : this.onRedisError(req, result.err)
      case 'lock_stolen':
        return this.refreshAsOwner(req, result.lock, 'lock_stolen')
    }
  }

  private async waitForEntry(
    req: CacheRequest<Context>,
    stale: Envelope<T> | undefined,
    progress: PollProgress,
  ): Promise<WaitResult<T>> {
    const { fetchTimeoutMs, pollIntervalMs } = this.options
    const { signal } = req

    while (true) {
      // fetchTimeout is the waiting limit and caps staleWait. An expired snapshot waits as cold.
      const usable = isUsable(stale) ? stale : undefined
      const limitMs = usable ? Math.min(req.staleWaitMs, fetchTimeoutMs) : fetchTimeoutMs
      const elapsedMs = Date.now() - progress.startedAt
      if (elapsedMs >= limitMs) return { outcome: 'timeout', elapsedMs, stale: usable }

      await sleep(pollDelayMs(pollIntervalMs, limitMs, elapsedMs), signal)

      progress.attempts++
      const accept: Accept = isUsable(stale) ? 'fresh' : 'usable'
      const snapshot = await this.pollSnapshot(req, accept)
      if (snapshot.status === 'redis_error') return { outcome: 'error', err: snapshot.err }
      if (snapshot.status === 'hit') return { outcome: 'resolved', envelope: snapshot.envelope }
      if (snapshot.status === 'locked') continue
      // The lock is free or cooling down, so no refresh is coming for a stale waiter.
      if (isUsable(stale)) return { outcome: 'stale_fallback', envelope: stale }
      // Cold waiters keep polling through a cooldown; one acquires once Redis reports it gone.
      if (snapshot.status === 'cooldown') continue

      const attempt = await this.tryLock(req)
      if (attempt.status === 'redis_error') return { outcome: 'error', err: attempt.err }
      if (attempt.status === 'acquired') return { outcome: 'lock_stolen', lock: attempt.lock }
    }
  }

  private pollSnapshot(req: CacheRequest<Context>, accept: Accept): Promise<PollSnapshot<T>> {
    return this.guardedRead(req, async (guard) => {
      let data: Buffer | null | undefined
      let lock: Buffer | null | undefined
      try {
        ;[data, lock] = await this.client.redis.mgetBuffer(req.dataKey, req.lockKey)
      } catch (err) {
        req.signal?.throwIfAborted()
        return { status: 'redis_error', err }
      }
      req.signal?.throwIfAborted()

      const entry = this.decodeAndPopulate(data ?? null, accept, guard)
      if (entry) return { status: 'hit', envelope: entry.envelope }
      if (!lock) return { status: 'unlocked' }
      return { status: isCooldownMarker(lock) ? 'cooldown' : 'locked' }
    })
  }

  private emitPoll(
    req: CacheRequest<Context>,
    outcome: PollOutcome,
    progress: PollProgress,
    durationMs?: number,
  ) {
    this.emit?.({
      type: 'poll',
      outcome,
      key: req.key,
      attempts: progress.attempts,
      durationMs: durationMs ?? Date.now() - progress.startedAt,
    })
  }

  private async refreshAsOwner(
    req: CacheRequest<Context>,
    lock: RedisLock,
    trigger: RefreshTrigger,
    stale?: Envelope<T>,
  ) {
    // onEvent listeners run after tryLock and may abort the caller.
    if (req.signal?.aborted) {
      await this.safeRelease(req, lock)
      throw req.signal.reason
    }
    const done = this.runRefresh(req, lock, trigger, stale)
    this.client.track(done, req.waitUntil)

    // Leaving via `signal` or `staleWait` detaches the caller; the refresh keeps running.
    // Serverless without a hook ignores staleWait: the winner waits for its refresh.
    if (stale && req.canDetach) {
      // Return without awaiting so a microtask-fast refresh can't beat a 0 ms staleWait.
      if (req.staleWaitMs === 0) return cached(stale, 'redis')
      const outcome = await awaitWithin(done, req.signal, req.staleWaitMs)
      if (outcome) return unwrapRefresh(outcome)
      if (isUsable(stale)) return cached(stale, 'redis')
      // The snapshot expired while waiting, so keep waiting as a cold caller.
    }
    return unwrapRefresh(await awaitWithin(done, req.signal))
  }

  // Never rejects; settles after the commit or cooldown swap and memory update.
  private async runRefresh(
    req: CacheRequest<Context>,
    lock: RedisLock,
    trigger: RefreshTrigger,
    stale: Envelope<T> | undefined,
  ): Promise<RefreshOutcome<T>> {
    const { key } = req
    const startedAt = Date.now()
    const renewal = lock.startRenewal(this.options.lockTtlMs, {
      onRenewError: (err) => this.emit?.(errorEvent('lock_renewal_failure', key, { err })),
      onLockLost: () =>
        this.emit?.(errorEvent('lock_lost', key, { durationMs: Date.now() - startedAt })),
    })
    const outcome = await startBoundedFetch(key, this.options.fetchTimeoutMs, (signal) =>
      this.fetch(req.context, { key: this.options.keyOf, signal, cacheWrite: 'attempt' }),
    ).result
    renewal.stop()

    if (outcome.status !== 'resolved') {
      await this.endFailedRefresh(req, lock, renewal.lost)
      return this.reportFailure(key, outcome, stale)
    }
    let prepared: PreparedCommit<T>
    try {
      prepared = this.prepareCommit(req, outcome.value, startedAt)
    } catch (err) {
      // Cool down here too, so a deterministic post-fetch bug can't hot-loop the source.
      await this.endFailedRefresh(req, lock, renewal.lost)
      return this.reportFailure(key, { status: 'rejected', err }, stale)
    }
    // A lost lock sends no write, so memory is left as it is.
    if (renewal.lost) return { kind: 'ok', result: fetched(prepared.value) }
    if (prepared.kind === 'no-cache') {
      await this.safeRelease(req, lock)
      return { kind: 'ok', result: fetched(prepared.value) }
    }
    return { kind: 'ok', result: await this.commit(req, lock, prepared, trigger) }
  }

  private async endFailedRefresh(req: CacheRequest<Context>, lock: RedisLock, lockLost: boolean) {
    if (lockLost) return
    const { failureCooldownMs } = this.options
    if (failureCooldownMs === 0) return this.safeRelease(req, lock)
    try {
      // A false return means set() or invalidate() cleared the lock first, so no marker is needed.
      await lock.cooldown(failureCooldownMs)
    } catch (err) {
      this.emit?.(errorEvent('lock_release_failure', req.key, { err }))
    }
  }

  // Reports a failed refresh once, whether or not a caller is still attached.
  private reportFailure(
    key: string,
    failure: FetchFailure,
    stale: Envelope<T> | undefined,
  ): RefreshOutcome<T> {
    // Judged when the refresh settles: the snapshot may have expired meanwhile.
    const fallback = isUsable(stale) ? stale : undefined
    if (failure.status === 'timeout') {
      this.emit?.(timeoutEvent(failure.err, fallback !== undefined))
      return { kind: 'failed', err: failure.err, fallback }
    }
    if (!fallback) {
      this.emit?.(errorEvent('fetch_cold_failure', key, { err: failure.err }))
      return { kind: 'failed', err: failure.err }
    }
    const { err, fatal } = this.classifyFailure(failure.err)
    if (fatal) {
      this.emit?.(errorEvent('fatal', key, { err }))
      return { kind: 'failed', err }
    }
    this.emit?.(errorEvent('stale_fallback', key, { stage: 'refresh', err }))
    return { kind: 'failed', err, fallback }
  }

  // A throwing isFatal makes its own error the fatal one, so the refresh still never rejects.
  private classifyFailure(err: unknown) {
    try {
      return { err, fatal: isFatalError(err, this.options.isFatal) }
    } catch (thrown) {
      return { err: thrown, fatal: true }
    }
  }

  private async onRedisError(req: CacheRequest<Context>, err: unknown) {
    const redisError = this.reportRedisError(req.key, err)
    if (redisError) throw redisError
    return this.fetchDirect(req)
  }

  private async fetchDirect(req: CacheRequest<Context>) {
    req.signal?.throwIfAborted()
    const outcome = await this.directFetches.run(req.key, req.signal, () =>
      startBoundedFetch(req.key, this.options.fetchTimeoutMs, (signal) =>
        this.fetchUncached(req, signal),
      ),
    )
    if (outcome.status === 'resolved') return fetched(outcome.value)
    throw outcome.err // reported once per fetch through DirectFetches' onFailed
  }

  private async fetchUncached(req: CacheRequest<Context>, signal: AbortSignal) {
    const result = await this.fetch(req.context, {
      key: this.options.keyOf,
      signal,
      cacheWrite: 'skip',
    })
    if (result instanceof SwrWriteBatch) {
      validateBatch(result, req.key)
      return result.primary
    }
    return result instanceof SwrNoCache ? result.value : result
  }

  private serveStaleAfterError(
    key: string,
    stale: Envelope<T>,
    stage: StaleFallbackStage,
    err: unknown,
  ) {
    this.emit?.(errorEvent('stale_fallback', key, { stage, err }))
    return cached(stale, 'redis')
  }

  private reportRedisError(key: string, err: unknown) {
    return reportRedisError([key], err, this.options.failureStrategy, this.emit)
  }

  private prepareCommit(
    req: CacheRequest<Context>,
    result: SwrFetchResult<T>,
    startedAt: number,
  ): PreparedCommit<T> {
    if (result instanceof SwrNoCache) return { kind: 'no-cache', value: result.value }

    const isBatch = result instanceof SwrWriteBatch
    const value = isBatch ? result.primary : result
    if (isBatch) validateBatch(result, req.key)
    const primaryTtl =
      (isBatch ? result.ttl : undefined) ?? this.resolvePrimaryTtl(req.context, req.key, value)

    const cachedAtMs = Date.now()
    const { useMemory } = req
    const entries: [PreparedWrite, ...PreparedWrite[]] = [
      {
        key: req.key,
        dataKey: req.dataKey,
        ...this.prepareWrite(value, cachedAtMs, primaryTtl, this.options.codec, useMemory),
      },
    ]
    if (isBatch) {
      for (const { key, value: secondaryValue, ttl, codec } of result.secondary) {
        const encoder =
          (codec as Pick<SwrCodec<unknown>, 'encode'> | undefined) ?? this.client.options.codec
        entries.push({
          key,
          dataKey: this.client.keys.data(key),
          ...this.prepareWrite(secondaryValue, cachedAtMs, ttl ?? primaryTtl, encoder, useMemory),
        })
      }
    }

    return { kind: 'write', value, entries, durationMs: Date.now() - startedAt }
  }

  private async commit(
    req: CacheRequest<Context>,
    lock: RedisLock,
    prepared: Extract<PreparedCommit<T>, { kind: 'write' }>,
    trigger: RefreshTrigger,
  ) {
    const { memory } = this.client
    const { value, entries, durationMs } = prepared
    const pending = memory.beginWrite(entries)

    let committed: boolean
    try {
      committed = await lock.commit(entries)
    } catch (err) {
      // The commit may still have run in Redis, so evict memory. Return the value even under
      // protect-source because fetch already succeeded.
      memory.settle(pending, 'evict')
      const [primary, ...secondary] = entries
      const keys: SwrNonEmptyKeys = [primary.key, ...secondary.map((entry) => entry.key)]
      this.emit?.(errorEvent('redis_error_fallback', req.key, { keys, err }))
      await this.safeRelease(req, lock)
      return fetched(value)
    }

    // Evict even when the lock was lost: if a reply was lost, ioredis resends the command after
    // reconnecting, so the first run may have written every key before this run found no lock.
    memory.settle(pending, committed ? 'populate' : 'evict')
    this.emit?.(
      committed
        ? { type: 'refresh', trigger, key: req.key, durationMs }
        : errorEvent('lock_lost', req.key, { durationMs }),
    )
    return fetched(value, committed ? entries[0].timing : undefined)
  }

  private async safeRelease(req: CacheRequest<Context>, lock: RedisLock) {
    try {
      await lock.release()
    } catch (err) {
      this.emit?.(errorEvent('lock_release_failure', req.key, { err }))
    }
  }

  private prepareWrite<V>(
    value: V,
    cachedAtMs: number,
    ttl: number,
    encoder: Pick<SwrCodec<V>, 'encode'>,
    useMemory: boolean,
  ) {
    const { staleMultiplier, compression } = this.options
    const ttlMs = toRedisTtlMs(ttl, staleMultiplier)
    // Freeze once so callers cannot mutate shared deadlines.
    const timing: SwrEntryTiming = Object.freeze({
      cachedAtMs,
      freshUntilMs: cachedAtMs + ttl * 1000,
      expiresAtMs: cachedAtMs + ttl * staleMultiplier * 1000,
    })
    const buffer = encodeEnvelope(value, timing, encoder, compression)
    if (!useMemory) return { timing, buffer, ttlMs }
    // Clone before writing to Redis so uncloneable values fail before any Redis write.
    const memoryValue = this.client.memory.clones ? structuredClone(value) : value
    return { timing, buffer, ttlMs, envelope: { value: memoryValue, timing } }
  }

  private resolvePrimaryTtl(context: Context, key: string, value: T) {
    const { ttl } = this.options
    if (typeof ttl === 'number') return ttl
    return requireTtl(ttl({ context, key, value }), 'ttl callback result')
  }
}

function isUsable<T>(entry: Envelope<T> | undefined): entry is Envelope<T> {
  return entry !== undefined && Date.now() < entry.timing.expiresAtMs
}

function cached<T>(entry: Envelope<T>, source: 'memory' | 'redis'): SwrGetResult<T> {
  return { value: entry.value, source, timing: entry.timing }
}

function unwrapRefresh<T>(outcome: RefreshOutcome<T>) {
  if (outcome.kind === 'ok') return outcome.result
  if (outcome.fallback) return cached(outcome.fallback, 'redis')
  throw outcome.err
}

function fetched<T>(value: T, timing?: SwrEntryTiming): SwrGetResult<T> {
  return timing ? { value, source: 'fetch', timing } : { value, source: 'fetch' }
}

function decodeEntry<T>(buf: Buffer | null, accept: Accept, codec: SwrCodec<T>) {
  if (!buf) return undefined
  const header = readEnvelopeHeader(buf)
  const { timing } = header
  const now = Date.now()
  const deadlineMs = accept === 'fresh' ? timing.freshUntilMs : timing.expiresAtMs
  if (now >= deadlineMs) return undefined

  const value = decodeEnvelopeValue(buf, header, codec)
  return { envelope: { value, timing }, fresh: now < timing.freshUntilMs, now }
}

function validateBatch(batch: SwrWriteBatch<unknown>, primaryKey: string) {
  if (batch.ttl !== undefined) requireTtl(batch.ttl, 'batch ttl')
  const seenKeys = new Set([primaryKey])
  for (const { key, ttl } of batch.secondary) {
    requireNonEmpty(key, 'key')
    if (seenKeys.has(key)) throw new SwrValidationError(`Duplicate cache write key: "${key}"`)
    seenKeys.add(key)
    if (ttl !== undefined) requireTtl(ttl, 'secondary ttl')
  }
}
