import { awaitWithin, type BoundedFetch, type FetchFailure } from './timing.ts'

interface Entry<T> {
  readonly fetch: BoundedFetch<T>
  callers: number
}

// Coalesces one in-flight direct fetch per key across concurrent callers.
export class DirectFetches<T> {
  private readonly inFlight = new Map<string, Entry<T>>()
  private readonly onFailed: (key: string, failure: FetchFailure) => void

  // onFailed runs once per failed fetch, except when aborted because all callers left.
  constructor(onFailed: (key: string, failure: FetchFailure) => void) {
    this.onFailed = onFailed
  }

  // Joins an active fetch or starts one; rejects only when signal aborts first.
  async run(key: string, signal: AbortSignal | undefined, start: () => BoundedFetch<T>) {
    const entry = this.inFlight.get(key) ?? this.open(key, start)
    entry.callers++
    try {
      return await awaitWithin(entry.fetch.result, signal)
    } catch (err) {
      if (--entry.callers === 0) {
        // Remove first so an abort listener that calls get() starts a fresh fetch.
        this.remove(key, entry)
        entry.fetch.abort()
      }
      throw err
    }
  }

  private open(key: string, start: () => BoundedFetch<T>) {
    const entry: Entry<T> = { fetch: start(), callers: 0 }
    this.inFlight.set(key, entry)
    void entry.fetch.result.then((outcome) => {
      if (entry.callers === 0) return
      this.remove(key, entry)
      if (outcome.status !== 'resolved') this.onFailed(key, outcome)
    })
    return entry
  }

  // A fetch that settles late removes only its own entry, never a replacement.
  private remove(key: string, entry: Entry<T>) {
    if (this.inFlight.get(key) === entry) this.inFlight.delete(key)
  }
}
