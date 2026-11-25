import assert from 'node:assert/strict'
import { SwrCacheImpl } from '../../src/cache.ts'
import { createEventEmitter } from '../../src/events.ts'
import { createRedisKeys } from '../../src/keys.ts'
import { registerLockCommands } from '../../src/lock.ts'
import { LruMemory } from '../../src/memory.ts'
import { DEFAULTS, resolveClientOptions, resolveMemory } from '../../src/options.ts'
import { createClientInternals } from '../../src/client.ts'
import type {
  SwrCacheOptions,
  SwrEvent,
  SwrMemoryOptions,
  SwrRuntime,
  SwrSharedOptions,
} from '../../src/types.ts'
import type { TestContext } from 'node:test'
import { createFakeRedis } from '../fake-redis.ts'
import { useRedisClock } from './clock.ts'

export function createLruMemory(options: true | SwrMemoryOptions = true): LruMemory {
  return new LruMemory(resolveMemory(options)!)
}

export interface MemoryHarnessClientOptions extends SwrSharedOptions {
  onEvent?: (event: SwrEvent) => void
  runtime?: SwrRuntime
}

// Uses the real client wiring and fails the test if any key versions leak.
export function memoryHarness<T, Context = void>(
  t: TestContext,
  options: SwrCacheOptions<T, Context>,
  clientOptions: MemoryHarnessClientOptions = {},
  memory = createLruMemory(),
) {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const commands = registerLockCommands(redis)
  const { onEvent, ...sharedOptions } = clientOptions
  const internals = createClientInternals({
    redis: commands,
    keys: createRedisKeys(DEFAULTS.prefix),
    memory,
    options: resolveClientOptions({ pollIntervalMs: 1, ...sharedOptions, memory: true }),
    emit: createEventEmitter(onEvent),
  })
  const cache = new SwrCacheImpl<T, Context>(internals, options)
  t.after(async () => {
    await internals.flush()
    assert.equal(memory.versionCount, 0, 'leaked memory key versions')
  })

  return { redis, memory, commands, cache, internals }
}
