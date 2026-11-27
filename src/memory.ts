import type { Envelope } from './codec.ts'
import type { ResolvedMemory } from './options.ts'

export interface ReadGuard {
  key: string
  version: number
}

// Without envelope, settling only evicts the key.
export interface MemoryWrite {
  key: string
  envelope?: Envelope<unknown>
}

export type PendingWrites = ReadonlyArray<MemoryWrite & { version: number }>

type SettleOutcome = 'populate' | 'evict'

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

  get entryCount() {
    return this.entries.size
  }

  get versionCount() {
    return this.versions.size
  }

  get<T>(key: string, now: number) {
    const entry = this.entries.get(key)
    if (!entry) return undefined

    this.entries.delete(key)
    const { freshUntilMs, cachedAtMs } = entry.timing
    if (now >= freshUntilMs || now - cachedAtMs >= this.maxTtlMs) return undefined

    this.entries.set(key, entry)
    return (
      this.clones ? { value: structuredClone(entry.value), timing: entry.timing } : entry
    ) as Envelope<T>
  }

  beginRead(key: string) {
    return { key, version: this.versions.acquire(key) }
  }

  endRead(guard: ReadGuard | undefined) {
    if (guard) this.versions.release(guard.key)
  }

  populate(guard: ReadGuard | undefined, envelope: Envelope<unknown>, now: number) {
    if (!guard || now - envelope.timing.cachedAtMs >= this.maxTtlMs) return
    if (this.versions.current(guard.key) !== guard.version) return
    if (!this.clones) {
      this.store(guard.key, envelope)
      return
    }
    const value = structuredClone(envelope.value)
    // structuredClone runs getters; a re-entrant set() or invalidate() that throws synchronously
    // still settles its version, so re-check before storing.
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

  settle(pending: PendingWrites, outcome: SettleOutcome) {
    for (const { key, envelope, version } of pending) {
      const uncontested = this.versions.endWrite(key, version)
      // If another write to the key finished meanwhile, which one Redis applied last is unknown.
      if (outcome === 'populate' && envelope && uncontested) this.store(key, envelope)
      else this.entries.delete(key)
    }
  }

  private store(key: string, envelope: Envelope<unknown>) {
    if (this.entries.has(key)) {
      this.entries.delete(key)
    } else if (this.entries.size >= this.maxEntries) {
      this.entries.delete(this.entries.keys().next().value!)
    }
    this.entries.set(key, envelope)
  }
}

// Per-key write counters; entries exist only while an operation on the key is active.
class KeyVersions {
  private readonly versions = new Map<string, { version: number; refs: number }>()

  get size() {
    return this.versions.size
  }

  acquire(key: string) {
    let v = this.versions.get(key)
    if (!v) {
      v = { version: 0, refs: 0 }
      this.versions.set(key, v)
    }
    v.refs++
    return v.version
  }

  current(key: string) {
    return this.versions.get(key)!.version
  }

  endWrite(key: string, version: number) {
    const v = this.versions.get(key)!
    const uncontested = v.version === version
    v.version++
    this.release(key, v)
    return uncontested
  }

  release(key: string, v = this.versions.get(key)!) {
    if (--v.refs === 0) this.versions.delete(key)
  }
}
