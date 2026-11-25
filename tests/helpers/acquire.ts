import type { Redis } from 'ioredis'
import { registerLockCommands } from '../../src/lock.ts'

export type Acquire = (key: string, token: string, ttlMs: number) => Promise<boolean>

export function interceptAcquire(
  redis: Redis,
  intercept: (acquire: Acquire, key: string, token: string, ttlMs: number) => Promise<boolean>,
): void {
  const commands = registerLockCommands(redis)
  const run = commands.swrAcquireLock.bind(redis)
  const acquire: Acquire = async (...args) => (await run(...args)) === 'acquired'
  commands.swrAcquireLock = async (...args) =>
    (await intercept(acquire, ...args)) ? 'acquired' : 'busy'
}

export function failAcquire(redis: Redis, error = new Error('acquire failed')): void {
  interceptAcquire(redis, async () => {
    throw error
  })
}

/** Hold a lock as another owner. */
export async function holdLock(
  redis: Redis,
  lockKey: string,
  ttlMs: number,
  owner = 'owner',
): Promise<void> {
  await redis.set(lockKey, owner, 'PX', ttlMs, 'NX')
}
