// Randomized model check of the memory cache guarantee (DECISIONS.md "Memory cache consistency").
// Two processes share one FakeRedis. A seeded scheduler picks, one step at a time, which command
// executes, which reply is delivered, which fetch resolves, and when timers or the clock advance.
// Reproduce a failure with MEMORY_MODEL_SEED=<seed>.
import test, { type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { AsyncLocalStorage } from 'node:async_hooks'
import { SwrCacheImpl } from '../src/cache.ts'
import { createClientInternals, type ClientInternals } from '../src/client.ts'
import type { Envelope } from '../src/codec.ts'
import { createEventEmitter } from '../src/events.ts'
import { createRedisKeys } from '../src/keys.ts'
import { registerLockCommands } from '../src/lock.ts'
import type { LruMemory, PendingWrites, ReadGuard } from '../src/memory.ts'
import { DEFAULTS, resolveClientOptions } from '../src/options.ts'
import { SwrWriteBatch } from '../src/wrappers.ts'
import { createFakeRedis } from './fake-redis.ts'
import { decodeEnvelope, encodeEnvelope } from './helpers/envelope.ts'
import { createLruMemory } from './helpers/memory-harness.ts'

// Stale handling: the winner detaches at once, waits up to staleWait, or refreshes inline. A
// waitUntil hook lets a serverless winner detach, so only the inline config runs without one.
const STALE_HANDLING = {
  detached: { runtime: 'server', staleWait: 0 },
  staleWait: { runtime: 'server', staleWait: 2 },
  inline: { runtime: 'serverless', staleWait: 0 },
} as const
type StaleHandling = keyof typeof STALE_HANDLING
const ORDERS = ['fifo', 'reordered'] as const
const FAULTS = ['lost-replies', 'resends-and-set-failures'] as const
const RUNS = Number(process.env.MEMORY_MODEL_RUNS ?? 200)
const OPS = 10
const MAX_STEPS = 20_000
const KEYS = ['k1', 'k2'] as const
const START_MS = 10_000

type Key = (typeof KEYS)[number]
type Order = (typeof ORDERS)[number]
type Faults = (typeof FAULTS)[number]
type Fault = 'none' | 'lost-before' | 'lost-after' | 'resend'
type Reply = { ok: true; value: unknown } | { ok: false; err: unknown }

interface OpContext {
  proc: Proc
  writes: Partial<Record<Key, number>>
}

interface Command {
  label: string
  name: string
  args: unknown[]
  run: () => Promise<unknown>
  ctx?: OpContext
  fault: Fault
  executions: number
  executed: boolean
  reply?: Reply
  resolve: (value: unknown) => void
  reject: (err: unknown) => void
}

interface Proc {
  name: string
  memory: LruMemory
  entries: Map<string, Envelope<unknown>>
  internals: ClientInternals
  queue: Command[]
  constraints: Record<Key, number>
}

interface Stats {
  memoryHits: number
  staleHits: number
  readPopulations: number
  writePopulations: number
}

const redisKeys = createRedisKeys(DEFAULTS.prefix)
const logicalKey = new Map<string, Key>(KEYS.map((k) => [redisKeys.data(k), k]))
const als = new AsyncLocalStorage<OpContext>()

function splitmix32(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x9e3779b9) | 0
    let z = state
    z = Math.imul(z ^ (z >>> 16), 0x85ebca6b)
    z = Math.imul(z ^ (z >>> 13), 0xc2b2ae35)
    return ((z ^ (z >>> 16)) >>> 0) / 2 ** 32
  }
}

const LOSSY = ['getBuffer', 'mgetBuffer', 'swrAcquireLock', 'unlink', 'swrCommitAndRelease']
const RESENT = ['swrAcquireLock', 'swrCommitAndRelease', 'swrSetAndClearLock', 'unlink']

function faultFor(faults: Faults, name: string, r: number): Fault {
  const writes = faults === 'resends-and-set-failures'
  if (LOSSY.includes(name) || (writes && name === 'swrSetAndClearLock')) {
    if (r < 0.06) return 'lost-before'
    if (r < 0.16) return 'lost-after'
  }
  return writes && RESENT.includes(name) && r >= 0.84 ? 'resend' : 'none'
}

async function runOnce(
  t: TestContext,
  seed: number,
  stale: StaleHandling,
  order: Order,
  faults: Faults,
  stats: Stats,
): Promise<string | undefined> {
  const rand = splitmix32(seed)
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!
  const fake = createFakeRedis(START_MS)
  const dateNow = t.mock.method(Date, 'now', () => fake.getTime())
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] })

  const log: string[] = []
  const seq: Record<Key, number> = { k1: 0, k2: 0 }
  // Per key: the sequence number of each value's latest Redis write, used to detect memory serving
  // a value older than a settled write.
  const seqOfValue: Record<Key, Map<unknown, number>> = { k1: new Map(), k2: new Map() }
  const recordWrite = (key: Key, value: unknown, ctx: OpContext | undefined) => {
    seq[key]++
    if (value !== null) seqOfValue[key].set(value, seq[key])
    if (ctx) ctx.writes[key] = seq[key]
    log.push(`  write ${key}=${String(value)} @${seq[key]}${ctx ? ` by ${ctx.proc.name}` : ''}`)
  }

  // Count a write when Redis runs the command, not when the reply arrives, even if nothing changes
  // (e.g. unlinking a missing key while memory still holds it).
  const recordExecution = (cmd: Command, result: unknown) => {
    if (cmd.name === 'swrSetAndClearLock') {
      const [dataKey, , buffer] = cmd.args as [string, string, Buffer]
      recordWrite(logicalKey.get(dataKey)!, decodeEnvelope(buffer).value, cmd.ctx)
    } else if (cmd.name === 'unlink') {
      for (const k of cmd.args.flat() as string[]) {
        const key = logicalKey.get(k)
        if (key) recordWrite(key, null, cmd.ctx)
      }
    } else if (cmd.name === 'swrCommitAndRelease' && result === '1') {
      const [numberOfKeys, args] = cmd.args as [number, unknown[]]
      for (let i = 1; i < numberOfKeys; i++) {
        const buffer = args[numberOfKeys + 2 * i - 1] as Buffer
        recordWrite(logicalKey.get(args[i] as string)!, decodeEnvelope(buffer).value, cmd.ctx)
      }
    }
  }

  const running = new Set<Promise<unknown>>()
  const runningTrack = (promise: Promise<unknown>) => {
    const tracked = promise.then(
      () => {},
      () => {},
    )
    running.add(tracked)
    void tracked.then(() => running.delete(tracked))
  }
  const applyConstraints = (ctx: OpContext) => {
    for (const [key, at] of Object.entries(ctx.writes) as [Key, number][]) {
      ctx.proc.constraints[key] = Math.max(ctx.proc.constraints[key], at)
    }
  }

  let unique = 0
  const fetchGates: (() => void)[] = []
  const gate = () => new Promise<void>((resolve) => fetchGates.push(resolve))
  const failures: string[] = []

  const procs = ['A', 'B'].map((name) => {
    const queue: Command[] = []
    let commands = 0
    const enqueue = (cmdName: string, args: unknown[], run: () => Promise<unknown>) =>
      new Promise((resolve, reject) => {
        queue.push({
          label: `${name}.${cmdName}#${++commands}`,
          name: cmdName,
          args,
          run,
          ctx: als.getStore(),
          fault: faultFor(faults, cmdName, rand()),
          executions: 0,
          executed: false,
          resolve,
          reject,
        })
      })
    const conn: any = {
      defineCommand: (cmdName: string, definition: { lua: string; numberOfKeys?: number }) => {
        fake.defineCommand(cmdName, definition)
        const run = (fake as any)[cmdName]
        conn[cmdName] = (...args: unknown[]) => enqueue(cmdName, args, () => run(...args))
      },
    }
    for (const cmdName of ['getBuffer', 'mgetBuffer', 'unlink'] as const) {
      conn[cmdName] = (...args: unknown[]) =>
        enqueue(cmdName, args, () => (fake[cmdName] as Function).apply(fake, args))
    }

    const memory = createLruMemory({ maxEntries: 10 })
    const entries = (memory as any).entries as Proc['entries']
    const populate = memory.populate.bind(memory)
    memory.populate = (guard: ReadGuard | undefined, envelope, now) => {
      const before = guard && entries.get(guard.key)
      populate(guard, envelope, now)
      if (guard && entries.has(guard.key) && entries.get(guard.key) !== before) {
        stats.readPopulations++
      }
    }
    const settle = memory.settle.bind(memory)
    memory.settle = (pending: PendingWrites, outcome) => {
      const before = pending.map(({ key }) => entries.get(key))
      settle(pending, outcome)
      pending.forEach(({ key }, i) => {
        if (entries.has(key) && entries.get(key) !== before[i]) stats.writePopulations++
      })
    }

    const proc = { name, memory, entries, queue, constraints: { k1: 0, k2: 0 } } as Proc
    proc.internals = createClientInternals({
      redis: registerLockCommands(conn),
      keys: redisKeys,
      memory,
      options: resolveClientOptions({
        ttl: 1,
        lockTtl: 1,
        // Long enough that the schedule's mock-timer ticks never time out a fetch or a waiter.
        fetchTimeout: 3600,
        pollIntervalMs: 1,
        ...STALE_HANDLING[stale],
        memory: true,
      }),
      emit: createEventEmitter((event) => {
        if (event.type === 'hit' && event.source === 'memory') stats.memoryHits++
        if (event.type === 'hit' && event.stale) stats.staleHits++
        log.push(
          `  ${name} event ${event.type}${'reason' in event ? `:${event.reason}` : ''}${'source' in event ? `:${event.source}${event.stale ? ':stale' : ''}` : ''}`,
        )
      }),
      // Background refresh writes count once the background promise settles.
      waitUntil:
        stale === 'inline'
          ? undefined
          : (promise) => {
              const ctx = als.getStore()
              runningTrack(
                promise.then(
                  () => ctx && applyConstraints(ctx),
                  (err: unknown) =>
                    failures.push(`${name} background refresh rejected: ${String(err)}`),
                ),
              )
            },
    })
    const fetchValue = async (label: string) => {
      await gate()
      return `${label}#${++unique}`
    }
    return Object.assign(proc, {
      shared: new SwrCacheImpl<string, string>(proc.internals, {
        key: (k) => k,
        fetch: (k) => fetchValue(`${name}-${k}-f`),
      }),
      optOut: new SwrCacheImpl<string, string>(proc.internals, {
        key: (k) => k,
        memory: false,
        fetch: (k) => fetchValue(`${name}-${k}-of`),
      }),
      batch: new SwrCacheImpl<string>(proc.internals, {
        key: () => 'k1',
        fetch: async () => {
          await gate()
          const n = ++unique
          return new SwrWriteBatch({
            primary: `${name}-k1-b#${n}`,
            secondary: [{ key: 'k2', value: `${name}-k2-b#${n}` }],
          })
        },
      }),
    })
  })

  for (const key of KEYS) {
    const r = rand()
    if (r < 0.5) {
      const value = `seed-${key}`
      const cachedAtMs = START_MS - (r < 0.35 ? 1500 : 200)
      const envelope = {
        value,
        cachedAtMs,
        freshUntilMs: cachedAtMs + 1000,
        expiresAtMs: cachedAtMs + 5000,
      }
      await fake.set(redisKeys.data(key), encodeEnvelope(envelope), 'EX', 5)
      recordWrite(key, value, undefined)
    }
    if (rand() < 0.25) {
      await fake.set(redisKeys.lock(key), 'external', 'PX', 600 + Math.floor(rand() * 1000))
    }
  }

  const kinds = [
    'get',
    'get',
    'get',
    'getNoMem',
    'getOptOut',
    'set',
    'setOptOut',
    'invalidate',
    'invalidateKeys',
    'batch',
  ] as const
  // Ops start together, or spread out so that later reads follow completed writes.
  const spread = pick([1, 0.3, 0.05])
  const trace: string[] = []
  const pendingOps = Array.from({ length: OPS }, () => {
    const proc = pick(procs)
    const key = pick(KEYS)
    const kind = pick(kinds)
    return () => {
      const label = `${proc.name}.${kind}(${key})`
      trace.push(label)
      log.push(`START ${label}`)
      const ctx: OpContext = { proc, writes: {} }
      const promise = als.run(ctx, (): Promise<unknown> => {
        switch (kind) {
          case 'get':
            return proc.shared.get(key)
          case 'getNoMem':
            return proc.shared.get(key, { memory: false })
          case 'getOptOut':
            return proc.optOut.get(key)
          case 'set':
            return proc.shared.set(key, `${proc.name}-${key}-s#${++unique}`)
          case 'setOptOut':
            return proc.optOut.set(key, `${proc.name}-${key}-os#${++unique}`)
          case 'invalidate':
            return proc.shared.invalidate(key)
          case 'invalidateKeys':
            return proc.internals.invalidate(rand() < 0.5 ? [key] : ['k1', 'k2'], 'availability')
          case 'batch':
            return proc.batch.get()
        }
      })
      runningTrack(
        promise.then(
          () => {
            log.push(`DONE ${label} writes=${JSON.stringify(ctx.writes)}`)
            applyConstraints(ctx)
          },
          (err: unknown) => failures.push(`${label} rejected: ${String(err)}`),
        ),
      )
    }
  })

  const check = (): string | undefined => {
    if (failures.length) return failures[0]
    const now = fake.getTime()
    for (const proc of procs) {
      for (const key of KEYS) {
        const entry = proc.entries.get(key)
        if (!entry) continue
        const at = seqOfValue[key].get(entry.value)
        if (at === undefined) return `${proc.name} memory[${key}]=${entry.value} was never in Redis`
        if (now < entry.timing.freshUntilMs && at < proc.constraints[key]) {
          return `${proc.name} memory[${key}]=${entry.value} (@${at}) is older than its completed write @${proc.constraints[key]}`
        }
      }
    }
  }

  const execute = (cmd: Command) => {
    cmd.executed = true
    if (cmd.fault === 'lost-before') {
      log.push(`exec ${cmd.label} (lost before execution)`)
      cmd.reply = { ok: false, err: new Error('reply lost before execution') }
      return
    }
    cmd.executions++
    log.push(`exec ${cmd.label}${cmd.fault === 'none' ? '' : ` (${cmd.fault})`}`)
    void cmd.run().then(
      (value) => {
        recordExecution(cmd, value)
        // The reply is lost and the command is resent; the caller sees the second reply.
        if (cmd.fault === 'resend' && cmd.executions === 1) cmd.executed = false
        else if (cmd.fault === 'lost-after') cmd.reply = { ok: false, err: new Error('reply lost') }
        else cmd.reply = { ok: true, value }
      },
      (err: unknown) => (cmd.reply = { ok: false, err }),
    )
  }

  const describe = (problem: string) =>
    `${problem}\n  seed=${seed} spread=${spread} stale=${stale} order=${order} faults=${faults}\n  ops: ${trace.join(', ')}\n  trace:\n    ${log.slice(-300).join('\n    ')}`

  try {
    for (let step = 0; ; step++) {
      await new Promise((resolve) => setImmediate(resolve))
      const violation = check()
      if (violation) return describe(violation)

      const done =
        pendingOps.length === 0 &&
        running.size === 0 &&
        fetchGates.length === 0 &&
        procs.every((p) => p.queue.length === 0)
      if (done) break
      if (step >= MAX_STEPS) return describe('schedule did not finish')

      const actions: (() => void)[] = []
      if (pendingOps.length && (running.size === 0 || rand() < spread)) {
        actions.push(() => pendingOps.shift()!())
      }
      for (const { queue } of procs) {
        const unexecuted = queue.filter((c) => !c.executed)
        const next =
          order === 'reordered' ? unexecuted[Math.floor(rand() * unexecuted.length)] : unexecuted[0]
        if (next) actions.push(() => execute(next))
        const ready =
          order === 'reordered'
            ? queue.filter((c) => c.reply)
            : queue.slice(0, 1).filter((c) => c.reply)
        const reply = ready[Math.floor(rand() * ready.length)]
        if (reply) {
          actions.push(() => {
            queue.splice(queue.indexOf(reply), 1)
            log.push(
              `reply ${reply.label} -> ${reply.reply!.ok ? JSON.stringify(reply.reply!.value)?.slice(0, 24) : 'ERR'}`,
            )
            if (reply.reply!.ok) reply.resolve(reply.reply!.value)
            else reply.reject(reply.reply!.err)
          })
        }
      }
      if (fetchGates.length) {
        actions.push(() => {
          log.push('fetch resolves')
          fetchGates.splice(Math.floor(rand() * fetchGates.length), 1)[0]!()
        })
      }
      if (running.size) actions.push(() => t.mock.timers.tick(rand() < 0.8 ? 1 : 500))
      if (rand() < 0.03) {
        actions.push(() => {
          log.push('clock +400ms')
          fake.advanceTime(400)
        })
      }
      pick(actions)()
    }

    for (const proc of procs) {
      await proc.internals.flush()
      if (proc.memory.versionCount !== 0) {
        return describe(`${proc.name} leaked ${proc.memory.versionCount} key versions`)
      }
    }
  } finally {
    t.mock.timers.reset()
    dateNow.mock.restore()
  }
}

const only = process.env.MEMORY_MODEL_SEED ? Number(process.env.MEMORY_MODEL_SEED) : undefined

let configIndex = 0
for (const faults of FAULTS) {
  for (const order of ORDERS) {
    for (const stale of Object.keys(STALE_HANDLING) as StaleHandling[]) {
      const firstSeed = ++configIndex * 1000
      test(`Memory model: ${stale} refresh, ${order}, ${faults}`, async (t) => {
        const stats: Stats = {
          memoryHits: 0,
          staleHits: 0,
          readPopulations: 0,
          writePopulations: 0,
        }
        for (let seed = firstSeed; seed < firstSeed + RUNS; seed++) {
          if (only !== undefined && seed !== only) continue
          const failure = await runOnce(t, seed, stale, order, faults, stats)
          if (failure) assert.fail(failure)
        }
        if (only !== undefined) return
        for (const [name, count] of Object.entries(stats)) {
          assert.ok(count > 0, `no ${name} across ${RUNS} runs: ${JSON.stringify(stats)}`)
        }
      })
    }
  }
}
