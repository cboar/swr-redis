import type { TestContext } from 'node:test'
import type { FakeRedis } from '../fake-redis.ts'

const clocks = new WeakMap<TestContext, { redis: FakeRedis }>()

/**
 * Drive the client clock (`Date.now`) from FakeRedis for the rest of the test; restored
 * automatically. Calling it again in the same test only switches the FakeRedis, because a second
 * `Date.now` mock would be restored to the first mock instead of the real clock.
 */
export function useRedisClock(t: TestContext, redis: FakeRedis): void {
  const clock = clocks.get(t)
  if (clock) {
    clock.redis = redis
    return
  }
  const state = { redis }
  clocks.set(t, state)
  t.mock.method(Date, 'now', () => state.redis.getTime())
}
