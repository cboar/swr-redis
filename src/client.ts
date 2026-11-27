import { SwrCacheImpl } from './cache.ts'
import { createEventEmitter, reportRedisError, type Emit } from './events.ts'
import { createRedisKeys, requireNonEmpty, type RedisKeys } from './keys.ts'
import { registerLockCommands, type SwrRedis } from './lock.ts'
import { disabledMemory, LruMemory, type MemoryStore } from './memory.ts'
import { resolveClientOptions, type ResolvedClientOptions } from './options.ts'
import type {
  SwrCache,
  SwrCacheOptions,
  SwrClient,
  SwrClientOptions,
  SwrFailureStrategy,
  SwrNonEmptyKeys,
} from './types.ts'

export function createSwrClient(options: SwrClientOptions): SwrClient {
  const { redis, onEvent, waitUntil, ...config } = options
  const resolved = resolveClientOptions(config)
  const internals = createClientInternals({
    redis: registerLockCommands(redis),
    keys: createRedisKeys(resolved.prefix),
    memory: resolved.memory ? new LruMemory(resolved.memory) : disabledMemory,
    options: resolved,
    emit: createEventEmitter(onEvent),
    waitUntil,
  })

  return {
    cache<T = unknown, Context = void>(
      cacheOptions: SwrCacheOptions<T, Context>,
    ): SwrCache<T, Context> {
      const impl = new SwrCacheImpl<T, Context>(internals, cacheOptions)
      return {
        get: impl.get.bind(impl),
        getWithMeta: impl.getWithMeta.bind(impl),
        set: impl.set.bind(impl),
        invalidate: impl.invalidate.bind(impl),
        key: impl.key.bind(impl),
      }
    },

    async invalidateKeys(keys) {
      const [first, ...rest] = keys
      if (first === undefined) return
      for (const key of keys) requireNonEmpty(key, 'key')
      await internals.invalidate([first, ...rest], resolved.failureStrategy)
    },

    drain: () => internals.drain(),
  }
}

interface ClientInternalsInit {
  readonly redis: SwrRedis
  readonly keys: RedisKeys
  readonly memory: MemoryStore
  readonly options: ResolvedClientOptions
  readonly emit?: Emit
  readonly waitUntil?: (promise: Promise<unknown>) => void
}

export interface ClientInternals extends ClientInternalsInit {
  track(promise: Promise<unknown>, waitUntil?: (promise: Promise<unknown>) => void): void
  drain(): Promise<void>
  invalidate(keys: SwrNonEmptyKeys, strategy: SwrFailureStrategy, emit?: Emit): Promise<void>
}

export function createClientInternals(init: ClientInternalsInit): ClientInternals {
  const { redis, keys: redisKeys, memory } = init
  const inflight = new Set<Promise<unknown>>()

  return {
    ...init,

    track(promise, waitUntil = init.waitUntil) {
      inflight.add(promise)
      const cleanup = () => inflight.delete(promise)
      void promise.then(cleanup, cleanup)
      waitUntil?.(promise)
    },

    async drain() {
      while (inflight.size > 0) {
        await Promise.allSettled([...inflight])
      }
    },

    async invalidate(keys, strategy, emit = init.emit) {
      const pending = memory.beginWrite(keys.map((key) => ({ key })))
      try {
        await redis.unlink(keys.flatMap((k) => [redisKeys.data(k), redisKeys.lock(k)]))
      } catch (err) {
        memory.settle(pending, 'evict')
        const redisError = reportRedisError(keys, err, strategy, emit)
        if (redisError) throw redisError
        return
      }
      memory.settle(pending, 'evict')
    },
  }
}
