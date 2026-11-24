import { SwrRedisError } from './errors.ts'
import type {
  SwrErrorEvent,
  SwrErrorReason,
  SwrEvent,
  SwrFailureStrategy,
  SwrNonEmptyKeys,
} from './types.ts'

export type Emit = (event: SwrEvent) => void

export function createEventEmitter(onEvent?: (event: SwrEvent) => void): Emit | undefined {
  if (!onEvent) return undefined
  return (event) => {
    try {
      const result: unknown = onEvent(event)
      if (typeof (result as PromiseLike<unknown> | null)?.then === 'function') {
        void Promise.resolve(result).catch(() => {})
      }
    } catch {}
  }
}

export function bindCacheEmitter(
  emit: Emit | undefined,
  cacheName: string | undefined,
): Emit | undefined {
  if (!emit || cacheName === undefined) return emit
  return (event) => emit({ ...event, cache: cacheName })
}

type ErrorEventFor<R, E = SwrErrorEvent> = E extends { reason: infer X }
  ? R extends X
    ? E
    : never
  : never
type ErrorFields<R extends SwrErrorReason> = Omit<
  ErrorEventFor<R>,
  'type' | 'cache' | 'key' | 'reason' | 'severity'
>

const ERROR_SEVERITY: { [R in SwrErrorReason]: ErrorEventFor<R>['severity'] } = {
  redis_error: 'error',
  redis_error_fallback: 'warn',
  fetch_cold_failure: 'error',
  fatal: 'error',
  timeout: 'error',
  stale_fallback: 'warn',
  lock_release_failure: 'warn',
  lock_renewal_failure: 'warn',
  lock_lost: 'warn',
}

export function errorEvent<R extends SwrErrorReason>(
  reason: R,
  key: string,
  fields: ErrorFields<R>,
): ErrorEventFor<R> {
  const severity = ERROR_SEVERITY[reason]
  return { type: 'error', reason, severity, key, ...fields } as ErrorEventFor<R>
}

export function reportRedisError(
  keys: SwrNonEmptyKeys,
  err: unknown,
  strategy: SwrFailureStrategy,
  emit: Emit | undefined,
): void {
  if (strategy === 'protect-source') {
    emit?.(errorEvent('redis_error', keys[0], { keys, err }))
    throw new SwrRedisError(keys, err)
  }
  emit?.(errorEvent('redis_error_fallback', keys[0], { keys, err }))
}
