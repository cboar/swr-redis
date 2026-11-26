import type { Redis } from 'ioredis'
import { registerLockCommands, type AcquireOutcome } from '../../src/lock.ts'

export type Acquire = (key: string, token: Buffer, ttlMs: number) => Promise<AcquireOutcome>

export function interceptAcquire(
  redis: Redis,
  intercept: (
    acquire: Acquire,
    key: string,
    token: Buffer,
    ttlMs: number,
  ) => Promise<AcquireOutcome>,
): void {
  const commands = registerLockCommands(redis)
  const acquire: Acquire = commands.swrAcquireLock.bind(redis)
  commands.swrAcquireLock = (...args) => intercept(acquire, ...args)
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
