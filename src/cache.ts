import { addAbortListener } from 'node:events'
import type { ClientInternals } from './client.ts'
import {
  decodeEnvelopeValue,
  encodeEnvelope,
  readEnvelopeHeader,
  type CacheEnvelope,
} from './codec.ts'
import { isFatalError, SwrTimeoutError, SwrValidationError } from './errors.ts'
import { bindCacheEmitter, errorEvent, reportRedisError, type Emit } from './events.ts'
import { requireNonEmpty } from './keys.ts'
import { RedisLock, type RedisWriteEntry } from './lock.ts'
import type { MemoryWrite, ReadGuard } from './memory.ts'
import {
  POLL_JITTER,
  requireTtl,
  resolveCacheOptions,
  toRedisTtlSeconds,
  type ResolvedCacheOptions,
} from './options.ts'
import type {
  SwrNonEmptyKeys,
  SwrCache,
  SwrCacheOptions,
  SwrCodec,
  SwrEvent,
  SwrFetchResult,
  SwrGetOptions,
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
}

type Accept = 'fresh' | 'usable'

type LockAttempt =
  | { status: 'acquired'; lock: RedisLock }
  | { status: 'busy' }
  | { status: 'redis_error'; err: unknown }

type PreparedWrite = RedisWriteEntry & MemoryWrite

type PreparedFetch<T> =
  | { kind: 'no-cache'; value: T }
  | {
      kind: 'write'
      value: T
      entries: readonly [PreparedWrite, ...PreparedWrite[]]
      durationMs: number
    }

/** A stale value to fall back to while still within its expiry window, boxed because `undefined` is a valid cached value. */
type StaleFallback<T> = { value: T; expiresAtMs: number }

type PollSnapshot<T> =
  | { status: 'hit'; value: T }
  | { status: 'locked' | 'unlocked' }
  | { status: 'redis_error'; err: unknown }

type WaitResult<T> =
  | { outcome: 'resolved' | 'stale_fallback'; value: T }
  | { outcome: 'timeout'; elapsedMs: number }
  | { outcome: 'error'; err: unknown }
  | { outcome: 'lock_stolen'; lock: RedisLock }

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

  constructor(client: ClientInternals, options: SwrCacheOptions<T, Context>) {
    this.client = client
    this.options = resolveCacheOptions(client.options, options)
    this.fetch = options.fetch
    this.emit = bindCacheEmitter(client.emit, options.name)
  }

  key(context: Context): string {
    return this.options.keyOf(context)
  }

  async get(context: Context, options?: SwrGetOptions): Promise<T> {
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
        return hit.value
      }
    }

    const { keys, redis } = this.client
    const req: CacheRequest<Context> = {
      context,
      key,
      dataKey: keys.data(key),
      lockKey: keys.lock(key),
      signal: options?.signal,
      waitUntil: options?.waitUntil,
      useMemory,
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
    if ('redisError' in read) return await this.onRedisError(req, read.redisError)

    const { entry } = read
    if (!entry) return await this.onColdMiss(req)
    if (!entry.fresh) {
      return await this.onStaleHit(req, {
        value: entry.envelope.value,
        expiresAtMs: entry.envelope.expiresAtMs,
      })
    }
    this.emit?.({ type: 'hit', key, source: 'redis', stale: false, startedRevalidation: false })
    return entry.envelope.value
  }

  async set(context: Context, value: T): Promise<void> {
    const { redis, keys, memory } = this.client
    const key = this.options.keyOf(context)
    const ttl = this.resolvePrimaryTtl(context, key, value)
    const { envelope, buffer, ttlSeconds } = this.prepareWrite(
      value,
      Date.now(),
      ttl,
      this.options.codec,
      this.options.memoryEnabled,
    )

    const pending = memory.beginWrite([{ key, envelope }])
    try {
      await redis.swrSetAndClearLock(keys.data(key), keys.lock(key), buffer, ttlSeconds)
    } catch (err) {
      memory.settle(pending, 'evict')
      this.reportRedisError(key, err)
      return
    }
    memory.settle(pending, 'populate')
  }

  async invalidate(context: Context): Promise<void> {
    await this.client.invalidate(
      [this.options.keyOf(context)],
      this.options.failureStrategy,
      this.emit,
    )
  }

  private async guardedRead<R>(
    req: CacheRequest<Context>,
    read: (guard: ReadGuard | undefined) => Promise<R>,
  ): Promise<R> {
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
    let acquired: boolean
    try {
      acquired = await lock.acquire(this.options.lockTtlMs)
    } catch (err) {
      // Redis may have granted the lock before the reply was lost.
      const release = this.safeRelease(req, lock)
      if (signal?.aborted) {
        await release
        throw signal.reason
      }
      return { status: 'redis_error', err }
    }
    if (signal?.aborted) {
      if (acquired) await this.safeRelease(req, lock)
      throw signal.reason
    }
    return acquired ? { status: 'acquired', lock } : { status: 'busy' }
  }

  private async onStaleHit(req: CacheRequest<Context>, stale: StaleFallback<T>): Promise<T> {
    const { mode } = this.options
    const { key } = req
    const attempt = await this.tryLock(req)
    const acquired = attempt.status === 'acquired'
    const usable = isStaleUsable(stale)
    this.emit?.({ type: 'hit', key, source: 'redis', stale: true, startedRevalidation: acquired })

    if (attempt.status === 'redis_error') {
      return usable
        ? this.serveStaleAfterError(key, stale, 'redis', attempt.err)
        : await this.onRedisError(req, attempt.err)
    }
    if (!acquired) {
      if (!usable) return await this.poll(req)
      return mode === 'wait-on-stale' ? await this.poll(req, stale) : stale.value
    }

    if (mode === 'swr' && usable) {
      // The background refresh must outlive the caller's request and its abort signal.
      const background = { ...req, signal: undefined }
      const refresh = this.refresh(background, attempt.lock, 'stale_revalidation', stale)
      this.client.track(refresh, req.waitUntil)
      return stale.value
    }

    return await this.refresh(req, attempt.lock, 'stale_revalidation', usable ? stale : undefined)
  }

  private async onColdMiss(req: CacheRequest<Context>): Promise<T> {
    const attempt = await this.tryLock(req)
    if (attempt.status === 'redis_error') return await this.onRedisError(req, attempt.err)

    if (attempt.status === 'busy') {
      this.emit?.({ type: 'miss', key: req.key, action: 'wait' })
      return await this.poll(req)
    }

    this.emit?.({ type: 'miss', key: req.key, action: 'fetch' })
    return await this.refresh(req, attempt.lock, 'cold_miss')
  }

  private async poll(req: CacheRequest<Context>, stale?: StaleFallback<T>): Promise<T> {
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
      if (stale && isStaleUsable(stale))
        return this.serveStaleAfterError(req.key, stale, 'read', err)
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
        return result.value
      case 'timeout': {
        if (stale && isStaleUsable(stale)) return stale.value
        if (this.options.failureStrategy !== 'protect-source') return await this.fetchDirect(req)
        const durationMs = result.elapsedMs
        const err = new SwrTimeoutError(req.key, durationMs)
        this.emit?.(errorEvent('timeout', req.key, { err, durationMs }))
        throw err
      }
      case 'error':
        return stale && isStaleUsable(stale)
          ? this.serveStaleAfterError(req.key, stale, 'redis', result.err)
          : await this.onRedisError(req, result.err)
      case 'lock_stolen':
        return await this.refresh(req, result.lock, 'lock_stolen')
    }
  }

  private async waitForEntry(
    req: CacheRequest<Context>,
    stale: StaleFallback<T> | undefined,
    progress: PollProgress,
  ): Promise<WaitResult<T>> {
    const { waitTimeoutMs, pollIntervalMs } = this.options
    const { signal } = req

    while (true) {
      signal?.throwIfAborted()
      const elapsedMs = Date.now() - progress.startedAt
      if (elapsedMs >= waitTimeoutMs) return { outcome: 'timeout', elapsedMs }

      await sleep(pollDelayMs(pollIntervalMs, waitTimeoutMs, elapsedMs), signal)
      signal?.throwIfAborted()

      progress.attempts++
      const accept: Accept = stale && isStaleUsable(stale) ? 'fresh' : 'usable'
      const snapshot = await this.pollSnapshot(req, accept)
      if (snapshot.status === 'redis_error') return { outcome: 'error', err: snapshot.err }
      if (snapshot.status === 'hit') return { outcome: 'resolved', value: snapshot.value }
      if (snapshot.status === 'locked') continue
      if (stale && isStaleUsable(stale)) return { outcome: 'stale_fallback', value: stale.value }

      const attempt = await this.tryLock(req)
      if (attempt.status === 'redis_error') return { outcome: 'error', err: attempt.err }
      if (attempt.status === 'acquired') return { outcome: 'lock_stolen', lock: attempt.lock }
    }
  }

  private pollSnapshot(req: CacheRequest<Context>, accept: Accept): Promise<PollSnapshot<T>> {
    return this.guardedRead(req, async (guard): Promise<PollSnapshot<T>> => {
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
      if (entry) return { status: 'hit', value: entry.envelope.value }
      return { status: lock ? 'locked' : 'unlocked' }
    })
  }

  private emitPoll(
    req: CacheRequest<Context>,
    outcome: PollOutcome,
    progress: PollProgress,
    durationMs?: number,
  ): void {
    this.emit?.({
      type: 'poll',
      outcome,
      key: req.key,
      attempts: progress.attempts,
      durationMs: durationMs ?? Date.now() - progress.startedAt,
    })
  }

  private async onRedisError(req: CacheRequest<Context>, err: unknown): Promise<T> {
    this.reportRedisError(req.key, err)
    return await this.fetchDirect(req)
  }

  private serveStaleAfterError(
    key: string,
    stale: StaleFallback<T>,
    stage: StaleFallbackStage,
    err: unknown,
  ): T {
    this.emit?.(errorEvent('stale_fallback', key, { stage, err }))
    return stale.value
  }

  private reportRedisError(key: string, err: unknown): void {
    reportRedisError([key], err, this.options.failureStrategy, this.emit)
  }

  // Returns the stale fallback when allowed; otherwise rethrows.
  private onFetchError(req: CacheRequest<Context>, err: unknown, stale?: StaleFallback<T>): T {
    if (req.signal?.aborted) throw err
    if (!stale) {
      this.emit?.(errorEvent('fetch_cold_failure', req.key, { err }))
      throw err
    }
    if (isFatalError(err, this.options.isFatal)) {
      this.emit?.(errorEvent('fatal', req.key, { err }))
      if (this.options.mode !== 'swr') throw err
      return stale.value
    }
    if (this.options.mode !== 'swr' && !isStaleUsable(stale)) {
      this.emit?.(errorEvent('fetch_cold_failure', req.key, { err }))
      throw err
    }
    return this.serveStaleAfterError(req.key, stale, 'refresh', err)
  }

  private async fetchDirect(req: CacheRequest<Context>): Promise<T> {
    try {
      req.signal?.throwIfAborted()
      const result = await this.fetch(req.context, {
        key: this.options.keyOf,
        ...(req.signal !== undefined ? { signal: req.signal } : {}),
        cacheWrite: 'skip',
      })
      if (result instanceof SwrWriteBatch) {
        validateBatch(result, req.key)
        return result.primary
      }
      return result instanceof SwrNoCache ? result.value : result
    } catch (err) {
      return this.onFetchError(req, err)
    }
  }

  private async refresh(
    req: CacheRequest<Context>,
    lock: RedisLock,
    trigger: RefreshTrigger,
    stale?: StaleFallback<T>,
  ): Promise<T> {
    let prepared: PreparedFetch<T>
    try {
      prepared = await this.fetchLocked(req, lock)
    } catch (err) {
      return this.onFetchError(req, err, stale)
    }
    return await this.commit(req, lock, prepared, trigger)
  }

  private async fetchLocked(
    req: CacheRequest<Context>,
    lock: RedisLock,
  ): Promise<PreparedFetch<T>> {
    const startedAt = Date.now()
    const onRenewError = (err: unknown) =>
      this.emit?.(errorEvent('lock_renewal_failure', req.key, { err }))

    try {
      req.signal?.throwIfAborted()
      const result = await lock.hold(this.options, onRenewError, () =>
        this.fetch(req.context, {
          key: this.options.keyOf,
          ...(req.signal !== undefined ? { signal: req.signal } : {}),
          cacheWrite: 'attempt',
        }),
      )
      const prepared = this.prepareFetch(req, result, startedAt)
      if (prepared.kind === 'no-cache') await this.safeRelease(req, lock)
      return prepared
    } catch (err) {
      await this.safeRelease(req, lock)
      throw err
    }
  }

  private prepareFetch(
    req: CacheRequest<Context>,
    result: SwrFetchResult<T>,
    startedAt: number,
  ): PreparedFetch<T> {
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
    prepared: PreparedFetch<T>,
    trigger: RefreshTrigger,
  ): Promise<T> {
    if (prepared.kind === 'no-cache') return prepared.value

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
      return value
    }

    // Evict even when the lock was lost: if a reply was lost, ioredis resends the command after
    // reconnecting, so the first run may have written every key before this run found no lock.
    memory.settle(pending, committed ? 'populate' : 'evict')
    this.emit?.(
      committed
        ? { type: 'refresh', trigger, key: req.key, durationMs }
        : errorEvent('lock_lost', req.key, { durationMs }),
    )
    return value
  }

  private async safeRelease(req: CacheRequest<Context>, lock: RedisLock): Promise<void> {
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
    const ttlSeconds = toRedisTtlSeconds(ttl, staleMultiplier)
    const envelope: CacheEnvelope<V> = {
      value,
      cachedAtMs,
      freshUntilMs: cachedAtMs + ttl * 1000,
      expiresAtMs: cachedAtMs + ttl * staleMultiplier * 1000,
    }
    const buffer = encodeEnvelope(envelope, encoder, compression)
    if (!useMemory) return { buffer, ttlSeconds }
    if (!this.client.memory.clones) return { buffer, ttlSeconds, envelope }
    // Clone before writing to Redis so uncloneable values fail before any Redis write.
    return { buffer, ttlSeconds, envelope: { ...envelope, value: structuredClone(value) } }
  }

  private resolvePrimaryTtl(context: Context, key: string, value: T): number {
    const { ttl } = this.options
    if (typeof ttl === 'number') return ttl
    return requireTtl(ttl({ context, key, value }), 'ttl callback result')
  }
}

function isStaleUsable(stale: StaleFallback<unknown>): boolean {
  return Date.now() < stale.expiresAtMs
}

function decodeEntry<T>(buf: Buffer | null, accept: Accept, codec: SwrCodec<T>) {
  if (!buf) return undefined
  const header = readEnvelopeHeader(buf)
  const now = Date.now()
  const deadlineMs = accept === 'fresh' ? header.freshUntilMs : header.expiresAtMs
  if (now >= deadlineMs) return undefined

  const value = decodeEnvelopeValue(buf, header, codec)
  const { cachedAtMs, freshUntilMs, expiresAtMs } = header
  return {
    fresh: now < freshUntilMs,
    envelope: { value, cachedAtMs, freshUntilMs, expiresAtMs },
    now,
  }
}

function validateBatch(batch: SwrWriteBatch<unknown>, primaryKey: string): void {
  if (batch.ttl !== undefined) requireTtl(batch.ttl, 'batch ttl')
  const seenKeys = new Set([primaryKey])
  for (const { key, ttl } of batch.secondary) {
    requireNonEmpty(key, 'key')
    if (seenKeys.has(key)) throw new SwrValidationError(`Duplicate cache write key: "${key}"`)
    seenKeys.add(key)
    if (ttl !== undefined) requireTtl(ttl, 'secondary ttl')
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason)
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      listener?.[Symbol.dispose]()
      resolve()
    }, ms)
    // Unlike addEventListener, this still fires if another listener stops propagation.
    const listener = signal
      ? addAbortListener(signal, () => {
          clearTimeout(timer)
          reject(signal.reason)
        })
      : undefined
  })
}

export function pollDelayMs(
  pollIntervalMs: number,
  waitTimeoutMs: number,
  elapsedMs: number,
): number {
  const jitter = Math.floor(Math.random() * (Math.floor(pollIntervalMs * POLL_JITTER) + 1))
  return Math.min(pollIntervalMs + jitter, Math.max(1, waitTimeoutMs - elapsedMs))
}
