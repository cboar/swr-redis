import { randomUUID } from 'node:crypto'
import type { Redis } from 'ioredis'

// Held in the lock key after a failed fetch. Owner tokens are UUIDs, so it can't collide.
export const COOLDOWN_MARKER = 'CD'
const COOLDOWN_MARKER_BUFFER = Buffer.from(COOLDOWN_MARKER)

export type AcquireOutcome = 'acquired' | 'busy' | 'cooldown'

export const ACQUIRE_LOCK_SCRIPT = `
local owner = redis.call("GET", KEYS[1])
if owner == "${COOLDOWN_MARKER}" then
  return "cooldown"
end
if owner and owner ~= ARGV[1] then
  return "busy"
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return "acquired"
`

export const EXTEND_LOCK_SCRIPT = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return "0"
end
local ttl = tonumber(ARGV[2])
if redis.call("PTTL", KEYS[1]) >= ttl then
  return "1"
end
return tostring(redis.call("PEXPIRE", KEYS[1], ttl))
`

export const COOLDOWN_LOCK_SCRIPT = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return "0"
end
redis.call("SET", KEYS[1], "${COOLDOWN_MARKER}", "PX", ARGV[2])
return "1"
`

export const COMMIT_AND_RELEASE_SCRIPT = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return "0"
end
for i = 2, #KEYS do
  redis.call("SET", KEYS[i], ARGV[2 * i - 2], "EX", ARGV[2 * i - 1])
end
redis.call("UNLINK", KEYS[1])
return "1"
`

export const RELEASE_LOCK_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return tostring(redis.call("UNLINK", KEYS[1]))
end
return "0"
`

export const SET_AND_CLEAR_LOCK_SCRIPT = `
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
redis.call("UNLINK", KEYS[2])
return "1"
`

export function isCooldownMarker(value: Buffer): boolean {
  return value.equals(COOLDOWN_MARKER_BUFFER)
}

export interface LockRenewal {
  // True once a renewal sees the lock gone while the fetch is still active.
  readonly lost: boolean
  stop(): void
}

export interface RedisWriteEntry {
  readonly dataKey: string
  readonly buffer: Buffer
  readonly ttlSeconds: number
}

type LuaFlag = '0' | '1'

export interface SwrRedisCommands {
  swrAcquireLock: (lockKey: string, token: string, lockTtlMs: number) => Promise<AcquireOutcome>
  swrCooldownLock: (lockKey: string, token: string, cooldownMs: number) => Promise<LuaFlag>
  swrExtendLock: (lockKey: string, token: string, lockTtlMs: number) => Promise<LuaFlag>
  swrCommitAndRelease: (
    numberOfKeys: number,
    args: readonly (string | Buffer | number)[],
  ) => Promise<LuaFlag>
  swrReleaseLock: (lockKey: string, token: string) => Promise<LuaFlag>
  swrSetAndClearLock: (
    dataKey: string,
    lockKey: string,
    buffer: Buffer,
    ttlSeconds: number,
  ) => Promise<LuaFlag>
}

export type SwrRedis = Redis & SwrRedisCommands

const LOCK_SCRIPTS = {
  swrAcquireLock: { numberOfKeys: 1, lua: ACQUIRE_LOCK_SCRIPT },
  swrCooldownLock: { numberOfKeys: 1, lua: COOLDOWN_LOCK_SCRIPT },
  swrExtendLock: { numberOfKeys: 1, lua: EXTEND_LOCK_SCRIPT },
  swrCommitAndRelease: { lua: COMMIT_AND_RELEASE_SCRIPT },
  swrReleaseLock: { numberOfKeys: 1, lua: RELEASE_LOCK_SCRIPT },
  swrSetAndClearLock: { numberOfKeys: 2, lua: SET_AND_CLEAR_LOCK_SCRIPT },
} satisfies Record<keyof SwrRedisCommands, Parameters<Redis['defineCommand']>[1]>

const registeredClients = new WeakSet<Redis>()

export function registerLockCommands(redis: Redis): SwrRedis {
  if (!registeredClients.has(redis)) {
    for (const [name, definition] of Object.entries(LOCK_SCRIPTS)) {
      redis.defineCommand(name, definition)
    }
    registeredClients.add(redis)
  }
  return redis as SwrRedis
}

export class RedisLock {
  readonly key: string
  readonly token = randomUUID()
  private readonly redis: SwrRedis

  constructor(redis: SwrRedis, key: string) {
    this.redis = redis
    this.key = key
  }

  async acquire(ttlMs: number): Promise<AcquireOutcome> {
    return this.redis.swrAcquireLock(this.key, this.token, ttlMs)
  }

  // Renews the full lockTtlMs until stop(); renewal errors retry on the next tick.
  startRenewal(
    lockTtlMs: number,
    callbacks: { onRenewError(err: unknown): void; onLockLost(): void },
  ): LockRenewal {
    let state: 'active' | 'stopped' | 'lost' = 'active'
    let renewing = false
    const timer = setInterval(
      () => {
        if (renewing) return
        renewing = true
        void this.redis.swrExtendLock(this.key, this.token, lockTtlMs).then(
          (extended) => {
            renewing = false
            // A reply after stop() may follow this fetch's own commit or cooldown swap.
            if (extended === '1' || state !== 'active') return
            state = 'lost'
            clearInterval(timer)
            callbacks.onLockLost()
          },
          (err: unknown) => {
            renewing = false
            if (state === 'active') callbacks.onRenewError(err)
          },
        )
      },
      // At 40%, a promptly failed renewal can retry with about 20% of the lease left.
      Math.max(1, Math.floor(lockTtlMs * 0.4)),
    )
    timer.unref()

    return {
      get lost() {
        return state === 'lost'
      },
      stop() {
        if (state === 'active') state = 'stopped'
        clearInterval(timer)
      },
    }
  }

  // Swaps this owner's token for the cooldown marker; returns false if the lock is gone.
  async cooldown(cooldownMs: number): Promise<boolean> {
    return (await this.redis.swrCooldownLock(this.key, this.token, cooldownMs)) === '1'
  }

  async commit(entries: readonly RedisWriteEntry[]): Promise<boolean> {
    const args: (string | Buffer | number)[] = [this.key]
    for (const entry of entries) args.push(entry.dataKey)
    args.push(this.token)
    for (const entry of entries) args.push(entry.buffer, entry.ttlSeconds)
    return (await this.redis.swrCommitAndRelease(1 + entries.length, args)) === '1'
  }

  async release(): Promise<boolean> {
    return (await this.redis.swrReleaseLock(this.key, this.token)) === '1'
  }
}
