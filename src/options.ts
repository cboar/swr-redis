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
  SwrRuntime,
  SwrSharedOptions,
  SwrStaleOption,
  SwrTtlOption,
} from './types.ts'

export const DEFAULTS = {
  prefix: 'default',
  ttl: 120,
  stale: ({ ttl }: { ttl: number }) => ttl * 4,
  runtime: 'server',
  failureStrategy: 'availability',
  fetchTimeout: 30,
  staleWait: 0,
  failureCooldown: 1,
  lockTtl: 3,
  pollInterval: 0.08,
  compression: { level: 1, minBytes: 256 },
  memory: { maxEntries: 1_000, maxTtl: Infinity, clone: false },
} as const

// Node clamps larger timer delays to 1ms.
export const MAX_TIMER_MS = 2_147_483_647
export const POLL_JITTER = 0.3

export interface ResolvedClientOptions {
  readonly prefix: string
  readonly runtime: SwrRuntime
  readonly codec: SwrCodec<unknown>
  readonly memory?: ResolvedMemory
  readonly failureStrategy: SwrFailureStrategy
  // Kept unmerged so each cache resolves overrides together.
  readonly shared: SwrSharedOptions
}

export interface ResolvedCacheOptions<T, Context> {
  readonly keyOf: (context: Context) => string
  readonly ttl: SwrTtlOption<T, Context>
  readonly stale: SwrStaleOption<T, Context>
  readonly failureStrategy: SwrFailureStrategy
  // Also caps how long a caller waits behind another fetch.
  readonly fetchTimeoutMs: number
  // 0 or Infinity allowed.
  readonly staleWaitMs: number
  // 0 disables the cooldown.
  readonly failureCooldownMs: number
  readonly lockTtlMs: number
  readonly pollIntervalMs: number
  readonly compression: ResolvedCompression
  readonly isFatal?: (err: unknown) => boolean
  readonly codec: SwrCodec<T>
  readonly memoryEnabled: boolean
}

export type ResolvedMemory = NonNullable<ReturnType<typeof resolveMemory>>

export type ResolvedCompression = Required<Exclude<SwrCompressionOption, boolean>>

export function resolveClientOptions(
  options: Omit<SwrClientOptions, 'redis' | 'onEvent' | 'waitUntil'>,
): ResolvedClientOptions {
  const {
    prefix = DEFAULTS.prefix,
    runtime = DEFAULTS.runtime,
    memory,
    codec = DEFAULT_CODEC,
    ...shared
  } = options
  requireNonEmpty(prefix, 'prefix')
  validateFields(shared)
  // A cache may still override these fields, so only check what the client sets.
  if (shared.ttl !== undefined) {
    toRedisTtlMs(shared.ttl + (typeof shared.stale === 'number' ? shared.stale : 0))
  }
  return {
    prefix,
    runtime,
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
  validateFields({ ...cache, ttl: numeric(cache.ttl), stale: numeric(cache.stale) })
  const { shared } = client
  const ttl = cache.ttl ?? shared.ttl ?? DEFAULTS.ttl
  const stale = cache.stale ?? shared.stale ?? DEFAULTS.stale
  // A callback stale can only add to a numeric ttl, so a ttl alone may already be too large.
  if (typeof ttl === 'number') toRedisTtlMs(ttl + (typeof stale === 'number' ? stale : 0))

  return {
    keyOf: resolveKey(cache.key),
    ttl,
    stale,
    failureStrategy: cache.failureStrategy ?? client.failureStrategy,
    fetchTimeoutMs: toMs(cache.fetchTimeout ?? shared.fetchTimeout ?? DEFAULTS.fetchTimeout),
    staleWaitMs: toNonNegativeMs(cache.staleWait ?? shared.staleWait ?? DEFAULTS.staleWait),
    failureCooldownMs: toNonNegativeMs(
      cache.failureCooldown ?? shared.failureCooldown ?? DEFAULTS.failureCooldown,
    ),
    lockTtlMs: toMs(cache.lockTtl ?? shared.lockTtl ?? DEFAULTS.lockTtl),
    pollIntervalMs: toMs(cache.pollInterval ?? shared.pollInterval ?? DEFAULTS.pollInterval),
    compression: resolveCompression(cache.compression ?? shared.compression),
    isFatal: cache.isFatal ?? shared.isFatal,
    codec: cache.codec ?? (client.codec as SwrCodec<T>),
    memoryEnabled: client.memory !== undefined && cache.memory !== false,
  }
}

export function resolveStaleWaitMs(staleWait: number) {
  validateStaleWait(staleWait)
  return toNonNegativeMs(staleWait)
}

export function resolveMemory(option: boolean | SwrMemoryOptions | undefined) {
  if (!option) return undefined
  const { maxEntries, maxTtl, clone } = option === true ? {} : option
  check(maxEntries, isPositiveSafeInteger, 'memory.maxEntries must be a positive safe integer')
  if (maxTtl !== undefined) requirePositive(maxTtl, 'memory.maxTtl')
  return {
    maxEntries: maxEntries ?? DEFAULTS.memory.maxEntries,
    maxTtlMs: toMs(maxTtl ?? DEFAULTS.memory.maxTtl),
    clone: clone ?? DEFAULTS.memory.clone,
  }
}

// `ttl` and `stale` callbacks are validated per result at write time instead.
function numeric(option: unknown) {
  return typeof option === 'number' ? option : undefined
}

function resolveKey<Context>(key: string | ((context: Context) => string)) {
  if (typeof key === 'function') return (context: Context) => requireNonEmpty(key(context), 'key')
  const staticKey = requireNonEmpty(key, 'key')
  return () => staticKey
}

export function resolveCompression(option: SwrCompressionOption | undefined) {
  if (option === false) return { level: 0, minBytes: 0 }
  const { level = DEFAULTS.compression.level, minBytes = DEFAULTS.compression.minBytes } =
    typeof option === 'object' ? option : {}
  return { level, minBytes }
}

function validateFields(layer: SwrSharedOptions) {
  const {
    ttl,
    stale,
    fetchTimeout,
    staleWait,
    failureCooldown,
    lockTtl,
    pollInterval,
    compression,
  } = layer
  const { level, minBytes } = typeof compression === 'object' ? compression : {}

  if (ttl !== undefined) requirePositive(ttl, 'ttl')
  if (typeof stale === 'number') requireNonNegative(stale, 'stale')
  check(
    fetchTimeout,
    isTimerSeconds,
    `fetchTimeout must be a positive number of at most ${MAX_TIMER_MS / 1000} seconds`,
  )
  validateStaleWait(staleWait)
  check(
    failureCooldown,
    (v) => v === 0 || isTimerSeconds(v),
    `failureCooldown must be 0 or a positive number of at most ${MAX_TIMER_MS / 1000} seconds`,
  )
  check(
    lockTtl,
    isTimerSeconds,
    `lockTtl must be a positive number of at most ${MAX_TIMER_MS / 1000} seconds`,
  )
  // Each poll sleep is also capped by the waiting limit, so jitter can't overflow the timer.
  check(
    pollInterval,
    isTimerSeconds,
    `pollInterval must be a positive number of at most ${MAX_TIMER_MS / 1000} seconds`,
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

function validateStaleWait(staleWait: number | undefined) {
  check(
    staleWait,
    (v) => v === 0 || v === Infinity || isTimerSeconds(v),
    `staleWait must be 0, Infinity, or a positive number of at most ${MAX_TIMER_MS / 1000} seconds`,
  )
}

function check<V>(value: V | undefined, valid: (value: V) => boolean, message: string) {
  if (value !== undefined && !valid(value)) fail(message)
}

function fail(message: string): never {
  throw new SwrValidationError(message)
}

export function requirePositive(value: number, label: string) {
  if (!isPositiveFinite(value)) fail(`${label} must be a positive finite number`)
  return value
}

export function requireNonNegative(value: number, label: string) {
  if (!Number.isFinite(value) || value < 0) fail(`${label} must be a non-negative finite number`)
  return value
}

// Ceil so Redis never expires a key before its header's expiresAtMs.
export function toRedisTtlMs(lifetime: number) {
  const ms = Math.ceil(lifetime * 1000)
  if (!Number.isSafeInteger(ms)) {
    fail(`Resolved Redis ttl exceeds maximum supported duration (${Number.MAX_SAFE_INTEGER}ms)`)
  }
  return ms
}

function toMs(seconds: number) {
  return Math.max(1, Math.round(seconds * 1000))
}

function toNonNegativeMs(seconds: number) {
  return seconds === 0 || seconds === Infinity ? seconds : toMs(seconds)
}

function isTimerSeconds(seconds: number) {
  return isPositiveFinite(seconds) && toMs(seconds) <= MAX_TIMER_MS
}

function isPositiveFinite(value: number) {
  return Number.isFinite(value) && value > 0
}

export function isPositiveSafeInteger(value: number) {
  return Number.isSafeInteger(value) && value > 0
}

export function isNonNegativeSafeInteger(value: number) {
  return Number.isSafeInteger(value) && value >= 0
}
