import { addAbortListener } from 'node:events'
import { SwrTimeoutError } from './errors.ts'
import { POLL_JITTER } from './options.ts'

export type FetchFailure =
  | { readonly status: 'rejected'; readonly err: unknown }
  | { readonly status: 'timeout'; readonly err: SwrTimeoutError }

export type BoundedFetchResult<R> =
  { readonly status: 'resolved'; readonly value: R } | FetchFailure

export interface BoundedFetch<R> {
  // Never rejects; stays pending after abort() if the fetch ignores its signal.
  readonly result: Promise<BoundedFetchResult<R>>
  abort(): void
}

export function startBoundedFetch<R>(
  key: string,
  fetchTimeoutMs: number,
  run: (signal: AbortSignal) => Promise<R>,
): BoundedFetch<R> {
  const startedAt = Date.now()
  const controller = new AbortController()
  const { promise, resolve } = Promise.withResolvers<BoundedFetchResult<R>>()

  const timer = setTimeout(() => {
    const err = new SwrTimeoutError(key, Date.now() - startedAt, 'fetch')
    // Settle before aborting so a cooperative abort rejection can't replace the timeout.
    resolve({ status: 'timeout', err })
    controller.abort(err)
  }, fetchTimeoutMs)
  // Bounds waiting for the fetch; it must not keep the process alive on its own.
  timer.unref()

  // Deferred so the caller's bookkeeping (tracking, the direct-fetch entry) exists before user code.
  void Promise.resolve()
    .then(() => run(controller.signal))
    .then(
      (value) => {
        clearTimeout(timer)
        resolve({ status: 'resolved', value })
      },
      (err: unknown) => {
        clearTimeout(timer)
        resolve({ status: 'rejected', err })
      },
    )

  return {
    result: promise,
    abort() {
      clearTimeout(timer)
      controller.abort()
    },
  }
}

// Resolves with promise, or undefined after ms; rejects with signal.reason if aborted first.
export function awaitWithin<R>(promise: Promise<R>, signal: AbortSignal | undefined): Promise<R>
export function awaitWithin<R>(
  promise: Promise<R>,
  signal: AbortSignal | undefined,
  ms: number,
): Promise<R | undefined>
export function awaitWithin<R>(
  promise: Promise<R>,
  signal: AbortSignal | undefined,
  ms = Infinity,
): Promise<R | undefined> {
  if (signal?.aborted) return Promise.reject(signal.reason)
  if (!signal && ms === Infinity) return promise
  return new Promise((resolve, reject) => {
    const finish = () => {
      clearTimeout(timer)
      listener?.[Symbol.dispose]()
    }
    const timer =
      ms === Infinity
        ? undefined
        : setTimeout(() => {
            finish()
            resolve(undefined)
          }, ms)
    // Unlike addEventListener, this still fires if another listener stops propagation.
    const listener =
      signal &&
      addAbortListener(signal, () => {
        finish()
        reject(signal.reason)
      })
    promise.then(
      (value) => {
        finish()
        resolve(value)
      },
      (err: unknown) => {
        finish()
        reject(err)
      },
    )
  })
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason)
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      listener?.[Symbol.dispose]()
      resolve()
    }, ms)
    const listener = signal
      ? addAbortListener(signal, () => {
          clearTimeout(timer)
          reject(signal.reason)
        })
      : undefined
  })
}

export function pollDelayMs(pollIntervalMs: number, limitMs: number, elapsedMs: number) {
  const jitter = Math.floor(Math.random() * (Math.floor(pollIntervalMs * POLL_JITTER) + 1))
  return Math.min(pollIntervalMs + jitter, Math.max(1, limitMs - elapsedMs))
}
