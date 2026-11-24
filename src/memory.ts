import type { Envelope } from './codec.ts'
import type { ResolvedMemory } from './options.ts'

export interface ReadGuard {
  key: string
  version: number
}

/** A write to settle into memory; without `envelope` the key is only evicted. */
export interface MemoryWrite {
  key: string
  envelope?: Envelope<unknown>
}

export type PendingWrites = ReadonlyArray<MemoryWrite & { version: number }>

export type SettleOutcome = 'populate' | 'evict'

export interface MemoryStore {
  readonly clones: boolean
  get<T>(key: string, now: number): Envelope<T> | undefined
  beginRead(key: string): ReadGuard
  endRead(guard: ReadGuard | undefined): void
  populate(guard: ReadGuard | undefined, envelope: Envelope<unknown>, now: number): void
  beginWrite(writes: readonly MemoryWrite[]): PendingWrites
  settle(pending: PendingWrites, outcome: SettleOutcome): void
}

export const disabledMemory: MemoryStore = {
  clones: false,
  get: () => undefined,
  beginRead: (key) => ({ key, version: 0 }),
  endRead: () => {},
  populate: () => {},
  beginWrite: () => [],
  settle: () => {},
}

export class LruMemory implements MemoryStore {
  readonly clones: boolean
  private readonly maxEntries: number
  private readonly maxTtlMs: number
  private readonly entries = new Map<string, Envelope<unknown>>()
  private readonly versions = new KeyVersions()

  constructor(options: ResolvedMemory) {
    this.maxEntries = options.maxEntries
    this.maxTtlMs = options.maxTtlMs
    this.clones = options.clone
  }

  get entryCount(): number {
    return this.entries.size
  }

  get versionCount(): number {
    return this.versions.size
  }

  get<T>(key: string, now: number): Envelope<T> | undefined {
    const entry = this.entries.get(key)
    if (!entry) return undefined

    this.entries.delete(key)
    const { freshUntilMs, cachedAtMs } = entry.timing
    if (now >= freshUntilMs || now - cachedAtMs >= this.maxTtlMs) return undefined

    this.entries.set(key, entry) // most recently used
    return (
      this.clones ? { value: structuredClone(entry.value), timing: entry.timing } : entry
    ) as Envelope<T>
  }

  beginRead(key: string): ReadGuard {
    return { key, version: this.versions.acquire(key) }
  }

  endRead(guard: ReadGuard | undefined): void {
    if (guard) this.versions.release(guard.key)
  }

  populate(guard: ReadGuard | undefined, envelope: Envelope<unknown>, now: number): void {
    if (!guard || now - envelope.timing.cachedAtMs >= this.maxTtlMs) return
    if (this.versions.current(guard.key) !== guard.version) return
    if (!this.clones) {
      this.store(guard.key, envelope)
      return
    }
    const value = structuredClone(envelope.value)
    // structuredClone runs property getters. A getter that calls set() or invalidate() can finish
    // that write synchronously (its Redis call throws), so check the version again.
    if (this.versions.current(guard.key) !== guard.version) return
    this.store(guard.key, { value, timing: envelope.timing })
  }

  beginWrite(writes: readonly MemoryWrite[]): PendingWrites {
    return writes.map(({ key, envelope }) => ({
      key,
      envelope,
      version: this.versions.acquire(key),
    }))
  }

  settle(pending: PendingWrites, outcome: SettleOutcome): void {
    for (const { key, envelope, version } of pending) {
      const uncontested = this.versions.endWrite(key, version)
      // If another write to the key finished meanwhile, which one Redis applied last is unknown.
      if (outcome === 'populate' && envelope && uncontested) this.store(key, envelope)
      else this.entries.delete(key)
    }
  }

  private store(key: string, envelope: Envelope<unknown>): void {
    if (this.entries.has(key)) {
      this.entries.delete(key)
    } else if (this.entries.size >= this.maxEntries) {
      this.entries.delete(this.entries.keys().next().value!)
    }
    this.entries.set(key, envelope)
  }
}

/** Counters bumped by every finished write; a key is tracked only while an operation on it runs. */
class KeyVersions {
  private readonly versions = new Map<string, { version: number; refs: number }>()

  get size(): number {
    return this.versions.size
  }

  acquire(key: string): number {
    let v = this.versions.get(key)
    if (!v) {
      v = { version: 0, refs: 0 }
      this.versions.set(key, v)
    }
    v.refs++
    return v.version
  }

  current(key: string): number {
    return this.versions.get(key)!.version
  }

  /** Finishes a write; returns false if another write finished since `version` was taken. */
  endWrite(key: string, version: number): boolean {
    const v = this.versions.get(key)!
    const uncontested = v.version === version
    v.version++
    this.release(key, v)
    return uncontested
  }

  release(key: string, v = this.versions.get(key)!): void {
    if (--v.refs === 0) this.versions.delete(key)
  }
}
