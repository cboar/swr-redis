import type { Redis } from 'ioredis'
import {
  ACQUIRE_LOCK_SCRIPT,
  COMMIT_AND_RELEASE_SCRIPT,
  COOLDOWN_LOCK_SCRIPT,
  COOLDOWN_MARKER,
  EXTEND_LOCK_SCRIPT,
  RELEASE_LOCK_SCRIPT,
  SET_AND_CLEAR_LOCK_SCRIPT,
} from '../src/lock.ts'

// Captured before any test mocks Date.now with this fake's own clock (see useRedisClock).
const realNow = Date.now

interface KeyEntry {
  value: Buffer
  expiresAt: number | null
}

export class FakeRedis {
  private readonly store = new Map<string, KeyEntry>()
  private currentTime: number | null = null
  // Count public commands only; script-internal operations are not commands.
  public evalCallCount = 0
  public setCallCount = 0

  constructor(initialTime?: number) {
    if (initialTime !== undefined) this.currentTime = initialTime
  }

  public getTime(): number {
    return this.currentTime !== null ? this.currentTime : realNow()
  }

  public setTime(time: number): void {
    this.currentTime = time
  }

  public advanceTime(ms: number): void {
    if (this.currentTime === null) this.currentTime = realNow()
    this.currentTime += ms
  }

  public clear(): void {
    this.store.clear()
  }

  private cleanExpired(key: string): KeyEntry | null {
    const entry = this.store.get(key)
    if (!entry) return null

    if (entry.expiresAt !== null && this.getTime() >= entry.expiresAt) {
      this.store.delete(key)
      return null
    }

    return entry
  }

  async get(key: string): Promise<string | null> {
    const entry = this.cleanExpired(key)
    if (!entry) return null
    return entry.value.toString('utf-8')
  }

  async getBuffer(key: string): Promise<Buffer | null> {
    const entry = this.cleanExpired(key)
    if (!entry) return null
    return entry.value
  }

  async mgetBuffer(...keys: string[]): Promise<(Buffer | null)[]> {
    return keys.map((key) => {
      const entry = this.cleanExpired(key)
      return entry ? entry.value : null
    })
  }

  private storeValue(key: string, value: string | Buffer, ...args: any[]): 'OK' | null {
    const buf = Buffer.isBuffer(value) ? value : Buffer.from(String(value))
    let expiresAt: number | null = null
    let nx = false

    for (let i = 0; i < args.length; i++) {
      const arg = String(args[i]).toUpperCase()
      if (arg === 'EX' && args[i + 1] !== undefined) {
        expiresAt = this.getTime() + Number(args[i + 1]) * 1000
        i++
      } else if (arg === 'PX' && args[i + 1] !== undefined) {
        expiresAt = this.getTime() + Number(args[i + 1])
        i++
      } else if (arg === 'NX') {
        nx = true
      }
    }

    if (nx && this.cleanExpired(key) !== null) {
      return null
    }

    this.store.set(key, { value: buf, expiresAt })
    return 'OK'
  }

  async set(key: string, value: string | Buffer, ...args: any[]): Promise<'OK' | null> {
    this.setCallCount++
    return this.storeValue(key, value, ...args)
  }

  private removeKeys(...keys: string[]): number {
    let count = 0
    for (const key of keys) {
      if (this.store.delete(key)) count++
    }
    return count
  }

  async unlink(...keys: (string | readonly string[])[]): Promise<number> {
    return this.removeKeys(...keys.flat())
  }

  async pttl(key: string): Promise<number> {
    const entry = this.cleanExpired(key)
    return !entry ? -2 : entry.expiresAt === null ? -1 : entry.expiresAt - this.getTime()
  }

  private expireKey(key: string, ms: number): number {
    const entry = this.cleanExpired(key)
    if (!entry) return 0
    entry.expiresAt = this.getTime() + Number(ms)
    return 1
  }

  async pexpire(key: string, ms: number): Promise<number> {
    return this.expireKey(key, ms)
  }

  defineCommand(
    name: string,
    definition: { lua: string; numberOfKeys?: number; readOnly?: boolean },
  ): void {
    if (definition.numberOfKeys !== undefined) {
      const numberOfKeys = definition.numberOfKeys
      ;(this as any)[name] = (...args: any[]) => this.eval(definition.lua, numberOfKeys, ...args)
    } else {
      ;(this as any)[name] = (numberOfKeys: number, ...args: any[]) =>
        this.eval(definition.lua, numberOfKeys, ...args)
    }
  }

  async eval(script: string, numkeys: number, ...rawArgs: any[]): Promise<any> {
    this.evalCallCount++
    const flatArgs = rawArgs.flat()
    const keys = flatArgs.slice(0, numkeys).map(String)
    const argv = flatArgs.slice(numkeys)

    switch (script) {
      case ACQUIRE_LOCK_SCRIPT: {
        const lockKey = keys[0]!
        const token = String(argv[0])
        const owner = this.cleanExpired(lockKey)?.value.toString('utf-8')
        if (owner === COOLDOWN_MARKER) return 'cooldown'
        if (owner !== undefined && owner !== token) return 'busy'
        this.storeValue(lockKey, token, 'PX', argv[1])
        return 'acquired'
      }

      case COOLDOWN_LOCK_SCRIPT: {
        const lockKey = keys[0]!
        const current = this.cleanExpired(lockKey)
        if (!current || current.value.toString('utf-8') !== String(argv[0])) return '0'
        this.storeValue(lockKey, COOLDOWN_MARKER, 'PX', argv[1])
        return '1'
      }

      case EXTEND_LOCK_SCRIPT: {
        const lockKey = keys[0]!
        const token = String(argv[0])
        const lockTtlMs = Number(argv[1])

        const current = this.cleanExpired(lockKey)
        if (!current || current.value.toString('utf-8') !== token) return '0'
        const currentPttl = current.expiresAt === null ? -1 : current.expiresAt - this.getTime()
        if (currentPttl >= lockTtlMs) return '1'
        return String(this.expireKey(lockKey, lockTtlMs))
      }

      case COMMIT_AND_RELEASE_SCRIPT: {
        const lockKey = keys[0]!
        const lockToken = String(argv[0])

        const current = this.cleanExpired(lockKey)
        if (!current || current.value.toString('utf-8') !== lockToken) return '0'

        for (let i = 1; i < keys.length; i++) {
          const dataKey = keys[i]!
          const value = argv[i * 2 - 1]
          const ttlMs = argv[i * 2]
          if (value === undefined || ttlMs === undefined) {
            throw new Error("ERR wrong number of arguments for 'set' command")
          }
          this.storeValue(dataKey, value, 'PX', Number(ttlMs))
        }
        this.removeKeys(lockKey)
        return '1'
      }

      case SET_AND_CLEAR_LOCK_SCRIPT: {
        const dataKey = keys[0]!
        const lockKey = keys[1]!
        const buf = argv[0]
        this.storeValue(dataKey, buf, 'PX', Number(argv[1]))
        this.removeKeys(lockKey)
        return '1'
      }

      case RELEASE_LOCK_SCRIPT: {
        const lockKey = keys[0]!
        const lockToken = String(argv[0])

        const current = this.cleanExpired(lockKey)
        if (!current || current.value.toString('utf-8') !== lockToken) return '0'
        this.removeKeys(lockKey)
        return '1'
      }

      default:
        throw new Error('Unknown Lua script in FakeRedis')
    }
  }
}

export function createFakeRedis(initialTime?: number): FakeRedis & Redis {
  return new FakeRedis(initialTime) as unknown as FakeRedis & Redis
}
