import { DEFAULT_CODEC } from './codec.ts'
import { SwrValidationError } from './errors.ts'
import { requireNonEmpty } from './keys.ts'
import type {
  SwrCacheOptions,
  SwrClientOptions,
  SwrCodec,
  SwrCompressionOption,
  SwrFailureStrategy,
  SwrMemoryOptions,
  SwrMode,
  SwrSharedOptions,
  SwrTtlOption,
} from './types.ts'

export const DEFAULTS = {
  prefix: 'default',
  ttl: 120,
  staleMultiplier: 5,
  mode: 'swr',
  failureStrategy: 'availability',
  lockTtl: 3,
  maxLockTtlFloor: 30,
  waitTimeout: 10,
  pollIntervalMs: 80,
  compression: { level: 1, minBytes: 256 },
  memory: { maxEntries: 1_000, maxTtl: Infinity, clone: false },
} as const

// Node clamps larger timer delays to 1ms.
export const MAX_TIMER_MS = 2_147_483_647
export const POLL_JITTER = 0.3

const MAX_REDIS_TTL_SECONDS = Math.floor(Number.MAX_SAFE_INTEGER / 1000)

export interface ResolvedClientOptions {
  readonly prefix: string
  readonly codec: SwrCodec<unknown>
  readonly memory?: ResolvedMemory
  readonly failureStrategy: SwrFailureStrategy
  /** Kept unmerged: each cache resolves them together with its own overrides. */
  readonly shared: SwrSharedOptions
}

export interface ResolvedCacheOptions<T, Context> {
  readonly keyOf: (context: Context) => string
  readonly ttl: SwrTtlOption<T, Context>
  readonly staleMultiplier: number
  readonly mode: SwrMode
  readonly failureStrategy: SwrFailureStrategy
  readonly lockTtlMs: number
  readonly maxLockTtlMs: number
  readonly waitTimeoutMs: number
  readonly pollIntervalMs: number
  readonly compression: ResolvedCompression
  readonly isFatal?: (err: unknown) => boolean
  readonly codec: SwrCodec<T>
  readonly memoryEnabled: boolean
}

export interface ResolvedMemory {
  readonly maxEntries: number
  readonly maxTtlMs: number
  readonly clone: boolean
}

export type ResolvedCompression = Required<Exclude<SwrCompressionOption, boolean>>

export function resolveClientOptions(
  options: Omit<SwrClientOptions, 'redis' | 'onEvent' | 'waitUntil'>,
): ResolvedClientOptions {
  const { prefix = DEFAULTS.prefix, memory, codec = DEFAULT_CODEC, ...shared } = options
  requireNonEmpty(prefix, 'prefix')
  validateFields(shared)
  validateCrossFieldRules(shared)
  return {
    prefix,
    codec,
    memory: resolveMemory(memory),
    failureStrategy: shared.failureStrategy ?? DEFAULTS.failureStrategy,
    shared,
  }
}

export function resolveCacheOptions<T, Context>(
  client: ResolvedClientOptions,
  cache: SwrCacheOptions<T, Context>,
): ResolvedCacheOptions<T, Context> {
  if (cache.name !== undefined) requireNonEmpty(cache.name, 'name')
  validateFields({ ...cache, ttl: numericTtl(cache.ttl) })
  const { shared } = client
  const lockTtl = cache.lockTtl ?? shared.lockTtl ?? DEFAULTS.lockTtl
  const merged = {
    ttl: cache.ttl ?? shared.ttl ?? DEFAULTS.ttl,
    staleMultiplier: cache.staleMultiplier ?? shared.staleMultiplier ?? DEFAULTS.staleMultiplier,
    lockTtl,
    maxLockTtl:
      cache.maxLockTtl ?? shared.maxLockTtl ?? Math.max(DEFAULTS.maxLockTtlFloor, lockTtl),
    waitTimeout: cache.waitTimeout ?? shared.waitTimeout ?? DEFAULTS.waitTimeout,
  }
  validateCrossFieldRules({ ...merged, ttl: numericTtl(merged.ttl) })

  const lockTtlMs = toMs(merged.lockTtl)
  return {
    keyOf: resolveKey(cache.key),
    ttl: merged.ttl,
    staleMultiplier: merged.staleMultiplier,
    mode: cache.mode ?? shared.mode ?? DEFAULTS.mode,
    failureStrategy: cache.failureStrategy ?? client.failureStrategy,
    lockTtlMs,
    maxLockTtlMs: Math.max(lockTtlMs, toMs(merged.maxLockTtl)),
    waitTimeoutMs: toMs(merged.waitTimeout),
    pollIntervalMs: cache.pollIntervalMs ?? shared.pollIntervalMs ?? DEFAULTS.pollIntervalMs,
    compression: resolveCompression(cache.compression ?? shared.compression),
    isFatal: cache.isFatal ?? shared.isFatal,
    codec: cache.codec ?? (client.codec as SwrCodec<T>),
    memoryEnabled: client.memory !== undefined && cache.memory !== false,
  }
}

export function resolveMemory(
  option: boolean | SwrMemoryOptions | undefined,
): ResolvedMemory | undefined {
  if (!option) return undefined
  const { maxEntries, maxTtl, clone } = option === true ? {} : option
  check(maxEntries, isPositiveSafeInteger, 'memory.maxEntries must be a positive safe integer')
  check(maxTtl, isPositiveFinite, 'memory.maxTtl must be a positive finite number')
  return {
    maxEntries: maxEntries ?? DEFAULTS.memory.maxEntries,
    maxTtlMs: toMs(maxTtl ?? DEFAULTS.memory.maxTtl),
    clone: clone ?? DEFAULTS.memory.clone,
  }
}

// A `ttl` callback is validated per result at write time instead.
function numericTtl(ttl: unknown): number | undefined {
  return typeof ttl === 'number' ? ttl : undefined
}

function resolveKey<Context>(key: string | ((context: Context) => string)) {
  if (typeof key === 'function') return (context: Context) => requireNonEmpty(key(context), 'key')
  const staticKey = requireNonEmpty(key, 'key')
  return () => staticKey
}

export function resolveCompression(option: SwrCompressionOption | undefined): ResolvedCompression {
  if (option === false) return { level: 0, minBytes: 0 }
  const { level = DEFAULTS.compression.level, minBytes = DEFAULTS.compression.minBytes } =
    typeof option === 'object' ? option : {}
  return { level, minBytes }
}

function validateFields(layer: SwrSharedOptions): void {
  const { ttl, staleMultiplier, lockTtl, maxLockTtl, waitTimeout, pollIntervalMs, compression } =
    layer
  const { level, minBytes } = typeof compression === 'object' ? compression : {}

  check(ttl, isPositiveFinite, 'ttl must be a positive finite number')
  check(
    staleMultiplier,
    (v) => Number.isFinite(v) && v >= 1,
    'staleMultiplier must be a finite number >= 1',
  )
  check(
    lockTtl,
    (v) => isPositiveFinite(v) && toMs(v) <= MAX_TIMER_MS,
    `lockTtl must be a positive number of at most ${MAX_TIMER_MS / 1000} seconds`,
  )
  check(
    maxLockTtl,
    isPositiveSafeMs,
    'maxLockTtl must be a positive number of seconds that is a safe integer in ms',
  )
  check(
    waitTimeout,
    isPositiveSafeMs,
    'waitTimeout must be a positive number of seconds that is a safe integer in ms',
  )
  check(
    pollIntervalMs,
    (v) => isPositiveSafeInteger(v) && maxPollDelayMs(v) <= MAX_TIMER_MS,
    `pollIntervalMs must be a positive integer, with jitter at most ${MAX_TIMER_MS}`,
  )
  check(
    level,
    (v) => Number.isInteger(v) && v >= 0 && v <= 9,
    'compression.level must be an integer between 0 and 9',
  )
  check(
    minBytes,
    isNonNegativeSafeInteger,
    'compression.minBytes must be a non-negative safe integer',
  )
}

// On the client layer only the fields set there are combined; a cache may still complete them.
function validateCrossFieldRules(options: SwrSharedOptions): void {
  const { ttl, staleMultiplier, lockTtl, maxLockTtl } = options
  if (ttl !== undefined) toRedisTtlSeconds(ttl, staleMultiplier ?? 1)
  if (lockTtl !== undefined && maxLockTtl !== undefined && maxLockTtl < lockTtl) {
    fail('maxLockTtl must be greater than or equal to lockTtl')
  }
}

function check<V>(value: V | undefined, valid: (value: V) => boolean, message: string): void {
  if (value !== undefined && !valid(value)) fail(message)
}

function fail(message: string): never {
  throw new SwrValidationError(message)
}

export function requireTtl(ttl: number, label: string): number {
  if (!isPositiveFinite(ttl)) fail(`${label} must be a positive finite number`)
  return ttl
}

export function toRedisTtlSeconds(ttl: number, staleMultiplier: number): number {
  const seconds = Math.max(1, Math.ceil(ttl * staleMultiplier))
  if (!Number.isSafeInteger(seconds * 1000)) {
    fail(`Resolved Redis ttl exceeds maximum supported duration (${MAX_REDIS_TTL_SECONDS}s)`)
  }
  return seconds
}

/** Positive seconds to whole milliseconds, at least 1. */
function toMs(seconds: number): number {
  return Math.max(1, Math.round(seconds * 1000))
}

function isPositiveSafeMs(seconds: number): boolean {
  return isPositiveFinite(seconds) && Number.isSafeInteger(toMs(seconds))
}

function maxPollDelayMs(pollIntervalMs: number): number {
  return pollIntervalMs + Math.floor(pollIntervalMs * POLL_JITTER)
}

function isPositiveFinite(value: number): boolean {
  return Number.isFinite(value) && value > 0
}

export function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0
}

export function isNonNegativeSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0
}
