import test from 'node:test'
import assert from 'node:assert/strict'
import { ObjectId } from 'bson'
import { createSwrClient, SwrWriteBatch, type SwrCodec, type SwrEvent } from '../src/index.ts'
import { ENVELOPE_HEADER_SIZE, readEnvelopeHeader } from '../src/codec.ts'
import { createRedisKeys } from '../src/keys.ts'
import { COOLDOWN_MARKER } from '../src/lock.ts'
import { DEFAULTS } from '../src/options.ts'
import { createFakeRedis } from './fake-redis.ts'
import { decodeEnvelope, encodeEnvelope } from './helpers/envelope.ts'
import { useRedisClock } from './helpers/clock.ts'

const mapCodec: SwrCodec<Map<string, number>> = {
  encode: (value) => Buffer.from(JSON.stringify([...value])),
  decode: (bytes) => new Map(JSON.parse(Buffer.from(bytes).toString())),
}

test('Default BSON codec preserves documented data types through Redis with memory bypassed', async () => {
  let calls = 0
  const value = {
    id: new ObjectId(),
    date: new Date('2026-01-01T00:00:00Z'),
    data: Buffer.from('hello'),
    nested: [null, true, 1.25, 'text', { count: 2 }],
  }
  const swr = createSwrClient({ redis: createFakeRedis(), memory: false })
  const cache = swr.cache({
    key: 'data',
    fetch: async () => {
      calls++
      return value
    },
  })
  assert.deepEqual(await cache.get(), value)
  const hit = await cache.get()
  assert.deepEqual(hit, value)
  assert.ok(Buffer.isBuffer(hit.data))
  assert.ok(hit.id instanceof ObjectId)
  assert.ok(hit.date instanceof Date)
  assert.equal(calls, 1)

  for (const empty of [null, undefined]) {
    let emptyCalls = 0
    const emptyCache = swr.cache({
      key: `empty:${String(empty)}`,
      fetch: async () => {
        emptyCalls++
        return empty
      },
    })
    assert.equal(await emptyCache.get(), empty)
    assert.equal(await emptyCache.get(), empty)
    assert.equal(emptyCalls, 1)
  }
})

test('25-byte envelope header: small payloads stay raw, large payloads compress, and reader decodes both seamlessly', () => {
  const smallEnv = { value: 'hi', cachedAtMs: 1000, freshUntilMs: 6000, expiresAtMs: 26000 }
  const smallBuf = encodeEnvelope(smallEnv)
  assert.ok(smallBuf.byteLength > ENVELOPE_HEADER_SIZE)
  assert.equal(smallBuf[0], 0x00)
  assert.deepEqual(readEnvelopeHeader(smallBuf), {
    deflated: false,
    timing: {
      cachedAtMs: 1000,
      freshUntilMs: 6000,
      expiresAtMs: 26000,
    },
  })
  assert.deepEqual(decodeEnvelope<string>(smallBuf), smallEnv)

  const largePayload = 'a'.repeat(1000)
  const compressedEnv = {
    value: largePayload,
    cachedAtMs: 2000,
    freshUntilMs: 7000,
    expiresAtMs: 27000,
  }
  const compressedBuf = encodeEnvelope(compressedEnv)
  assert.equal(compressedBuf[0], 0x01)
  assert.equal(readEnvelopeHeader(compressedBuf).deflated, true)
  assert.ok(compressedBuf.byteLength < 200)
  assert.deepEqual(decodeEnvelope<string>(compressedBuf), compressedEnv)

  // Disabling compression writes raw payloads even when large
  const rawLargeEnv = {
    value: largePayload,
    cachedAtMs: 3000,
    freshUntilMs: 8000,
    expiresAtMs: 28000,
  }
  const rawLargeBuf = encodeEnvelope(rawLargeEnv, undefined, false)
  assert.equal(readEnvelopeHeader(rawLargeBuf).deflated, false)
  assert.ok(rawLargeBuf.byteLength > 1000)
  assert.deepEqual(decodeEnvelope<string>(rawLargeBuf), rawLargeEnv)

  // Custom minBytes threshold and level: 0
  const levelZeroEnv = {
    value: largePayload,
    cachedAtMs: 4000,
    freshUntilMs: 9000,
    expiresAtMs: 29000,
  }
  const levelZero = encodeEnvelope(levelZeroEnv, undefined, { level: 0 })
  assert.equal(readEnvelopeHeader(levelZero).deflated, false)
  assert.deepEqual(decodeEnvelope<string>(levelZero), levelZeroEnv)

  const lowThresholdEnv = {
    value: 'medium-payload-repeating-repeating-repeating',
    cachedAtMs: 5000,
    freshUntilMs: 10000,
    expiresAtMs: 30000,
  }
  const lowThreshold = encodeEnvelope(lowThresholdEnv, undefined, { level: 6, minBytes: 10 })
  assert.equal(readEnvelopeHeader(lowThreshold).deflated, true)
  assert.deepEqual(decodeEnvelope<string>(lowThreshold), lowThresholdEnv)
})

test('readEnvelopeHeader rejects a truncated slice instead of reading the backing buffer', () => {
  const backing = Buffer.alloc(64, 0x42)
  assert.throws(
    () => readEnvelopeHeader(backing.subarray(8, 8 + ENVELOPE_HEADER_SIZE - 1)),
    RangeError,
  )
})

test('Custom codec handles empty payloads and nonzero-offset Uint8Array views cleanly', () => {
  const emptyCodec: SwrCodec<string> = {
    encode: () => new Uint8Array(0),
    decode: (bytes) => {
      assert.equal(bytes.byteLength, 0)
      return 'empty-ok'
    },
  }
  const emptyEnv = { value: 'empty-ok', cachedAtMs: 10, freshUntilMs: 20, expiresAtMs: 30 }
  const emptyBuf = encodeEnvelope(emptyEnv, emptyCodec)
  assert.equal(emptyBuf.byteLength, ENVELOPE_HEADER_SIZE)
  assert.equal(readEnvelopeHeader(emptyBuf).deflated, false)
  assert.deepEqual(decodeEnvelope(emptyBuf, emptyCodec), emptyEnv)

  const backing = new Uint8Array([0xaa, 0xbb, 0x68, 0x69, 0xcc])
  const viewCodec: SwrCodec<string> = {
    encode: () => backing.subarray(2, 4), // "hi" with byteOffset=2
    decode: (bytes) => Buffer.from(bytes).toString('utf8'),
  }
  const viewEnv = { value: 'hi', cachedAtMs: 100, freshUntilMs: 200, expiresAtMs: 300 }
  const viewBuf = encodeEnvelope(viewEnv, viewCodec, false)
  assert.deepEqual(decodeEnvelope(viewBuf, viewCodec), viewEnv)
  // Also decode from a nonzero-offset view of the outer envelope buffer.
  const padded = Buffer.concat([Buffer.from([1, 2, 3]), viewBuf, Buffer.from([4, 5])])
  assert.deepEqual(decodeEnvelope(padded.subarray(3, 3 + viewBuf.byteLength), viewCodec), viewEnv)
})

test('Codecs: a cache codec overrides the client codec, which overrides BSON', async () => {
  const jsonCodec: SwrCodec<unknown> = {
    encode: (v) => Buffer.from(JSON.stringify(v ?? null)),
    decode: (b) => JSON.parse(Buffer.from(b).toString()),
  }
  const redis = createFakeRedis()
  const { data: dataKey } = createRedisKeys(DEFAULTS.prefix)
  const swr = createSwrClient({ redis, codec: jsonCodec, compression: false })
  const defaultCache = swr.cache<{ text: string }>({
    key: 'json',
    fetch: async () => ({ text: 'hello' }),
  })
  let mapCalls = 0
  const mapCache = swr.cache({
    key: (id: string) => `counts:${id}`,
    codec: mapCodec,
    fetch: async (id) => {
      mapCalls++
      return new Map([[id, 42]])
    },
  })

  assert.deepEqual(await defaultCache.get(), { text: 'hello' })
  assert.deepEqual(await defaultCache.get(), { text: 'hello' })
  assert.equal(
    (await redis.getBuffer(dataKey('json')))!.subarray(ENVELOPE_HEADER_SIZE).toString(),
    '{"text":"hello"}',
  )

  assert.deepEqual(await mapCache.get('a'), new Map([['a', 42]]))
  const hit = await mapCache.get('a')
  assert.ok(hit instanceof Map)
  assert.equal(mapCalls, 1)
  await mapCache.set('a', new Map([['a', 7]]))
  assert.deepEqual(await mapCache.get('a'), new Map([['a', 7]]))
})

test('Custom codec encoding failure starts a cooldown without populating Redis or memory', async (t) => {
  const redis = createFakeRedis(1000)
  useRedisClock(t, redis)
  const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)
  const failure = new Error('Cannot encode')
  const swr = createSwrClient({ redis, memory: true })
  const reader = swr.cache({
    key: 'primary',
    fetch: async () => assert.fail('reader must not fetch'),
  })
  const cache = swr.cache({
    key: 'primary',
    codec: {
      encode: () => {
        throw failure
      },
      decode: () => 'ok',
    },
    fetch: async () => 'ok',
  })
  await assert.rejects(cache.get(), (error) => error === failure)
  assert.equal(await redis.get(lockKey('primary')), COOLDOWN_MARKER)
  assert.equal(await redis.getBuffer(dataKey('primary')), null)
  // Memory holds nothing, so a reader sees a value written straight to Redis.
  await redis.set(
    dataKey('primary'),
    encodeEnvelope({ value: 'redis', cachedAtMs: 1000, freshUntilMs: 5000, expiresAtMs: 10_000 }),
  )
  assert.equal(await reader.get(), 'redis')
})

test('Envelope decoding surfaces payload and inflate errors', () => {
  // Envelopes are trusted: only the payload decoders can fail.
  assert.throws(() => decodeEnvelope(Buffer.alloc(ENVELOPE_HEADER_SIZE, 0x00)))
  assert.throws(() =>
    decodeEnvelope(Buffer.concat([Buffer.alloc(ENVELOPE_HEADER_SIZE, 0x01), Buffer.from([0xff])])),
  )
})

test('Header-first expiry check skips payload decoding for expired reads and stale poll ticks', async (t) => {
  const redis = createFakeRedis(5000)
  useRedisClock(t, redis)
  const { data: dataKey, lock: lockKey } = createRedisKeys(DEFAULTS.prefix)
  let decodes = 0
  const trackedCodec: SwrCodec<string> = {
    encode: (v) => Buffer.from(v),
    decode: (b) => {
      decodes++
      return Buffer.from(b).toString()
    },
  }

  // 1. Expired entry on initial read must NOT invoke codec.decode
  await redis.set(
    dataKey('expired'),
    encodeEnvelope(
      { value: 'old', cachedAtMs: 1000, freshUntilMs: 2000, expiresAtMs: 5000 },
      trackedCodec,
    ),
  )
  const swr = createSwrClient({ redis, staleWait: 10, pollInterval: 0.001 })
  const expiredCache = swr.cache({
    key: 'expired',
    codec: trackedCodec,
    fetch: async () => 'refreshed',
  })
  assert.equal(await expiredCache.get(), 'refreshed')
  assert.equal(decodes, 0)

  // 2. Stale poll ticks seeing still-stale data must NOT invoke codec.decode
  await redis.set(
    dataKey('stale-poll'),
    encodeEnvelope(
      { value: 'stale', cachedAtMs: 1000, freshUntilMs: 4000, expiresAtMs: 10_000 },
      trackedCodec,
    ),
  )
  await redis.set(lockKey('stale-poll'), 'other-owner', 'PX', 5000)
  const mgetBuffer = redis.mgetBuffer.bind(redis)
  let pollTicks = 0
  redis.mgetBuffer = (async (...keys: string[]) => {
    pollTicks++
    if (pollTicks === 3) {
      await redis.set(
        dataKey('stale-poll'),
        encodeEnvelope(
          { value: 'fresh', cachedAtMs: 5000, freshUntilMs: 9000, expiresAtMs: 20_000 },
          trackedCodec,
        ),
      )
      await redis.unlink(lockKey('stale-poll'))
    }
    return mgetBuffer(...keys)
  }) as typeof redis.mgetBuffer

  const staleCache = swr.cache({
    key: 'stale-poll',
    codec: trackedCodec,
    fetch: async () => assert.fail('waiter must not fetch'),
  })
  assert.equal(await staleCache.get(), 'fresh')
  assert.equal(pollTicks, 3)
  // 1 decode for initial stale value + 1 decode for final fresh value; 2 skipped stale poll ticks.
  assert.equal(decodes, 2)
})

test('Synchronous decoder crossing deadlines keeps the pre-decode read admission', async (t) => {
  const redis = createFakeRedis(1999)
  useRedisClock(t, redis)
  const { data: dataKey } = createRedisKeys(DEFAULTS.prefix)
  const slowCodec: SwrCodec<string> = {
    encode: (v) => Buffer.from(v),
    decode: (b) => {
      redis.advanceTime(5)
      return Buffer.from(b).toString()
    },
  }
  await redis.set(
    dataKey('slow-fresh'),
    encodeEnvelope(
      { value: 'fresh-ok', cachedAtMs: 1000, freshUntilMs: 2000, expiresAtMs: 2002 },
      slowCodec,
    ),
  )
  const cache = createSwrClient({ redis }).cache({
    key: 'slow-fresh',
    codec: slowCodec,
    fetch: async () => assert.fail('must not fetch when header was fresh before decode'),
  })
  assert.equal(await cache.get(), 'fresh-ok')
})

for (const [path, fault, expectedEvents] of [
  ['initial read', 'decode', []],
  ['cold poll', 'decode', ['miss:wait', 'poll:error']],
  ['stale poll', 'decode', ['hit', 'poll:error', 'error']],
  ['stale poll', 'decompression', ['hit', 'poll:error', 'error']],
  ['stale poll', 'abort in decoder', ['hit', 'poll:cancelled']],
] as const) {
  test(`${path} handles ${fault} without fetching`, { timeout: 2000 }, async (t) => {
    const redis = createFakeRedis(1000)
    useRedisClock(t, redis)
    const { data, lock } = createRedisKeys(DEFAULTS.prefix)
    const key = 'corrupt'
    const failure = new Error('Cannot decode')
    const cancellation = new Error('caller cancelled')
    const controller = new AbortController()
    const codec: SwrCodec<string> = {
      encode: (value) => Buffer.from(value),
      decode: (bytes) => {
        const value = Buffer.from(bytes).toString()
        if (value !== 'unreadable') return value
        if (fault === 'abort in decoder') controller.abort(cancellation)
        throw failure
      },
    }
    const broken = encodeEnvelope(
      { value: 'unreadable', cachedAtMs: 1000, freshUntilMs: 2000, expiresAtMs: 5000 },
      codec,
    )
    if (fault === 'decompression') broken[0] = 1
    const matchesFailure = (error: unknown) =>
      fault === 'decompression'
        ? error instanceof Error && 'code' in error && error.code === 'Z_DATA_ERROR'
        : error === failure

    if (path === 'initial read') {
      await redis.set(data(key), broken)
    } else {
      await redis.set(lock(key), 'other-owner', 'PX', 5000)
      if (path === 'stale poll') {
        await redis.set(
          data(key),
          encodeEnvelope(
            { value: 'stale', cachedAtMs: 0, freshUntilMs: 500, expiresAtMs: 5000 },
            codec,
          ),
        )
      }
      const mgetBuffer = redis.mgetBuffer.bind(redis)
      redis.mgetBuffer = (async (...keys: string[]) => {
        await redis.set(data(key), broken)
        return mgetBuffer(...keys)
      }) as typeof redis.mgetBuffer
    }

    let fetches = 0
    const events: SwrEvent[] = []
    const cache = createSwrClient({
      redis,
      memory: true,
      staleWait: 10,
      pollInterval: 0.001,
      onEvent: (event) => events.push(event),
    }).cache({ key, codec, fetch: async () => `fetched-${++fetches}` })

    const pending = cache.get(undefined, { signal: controller.signal })
    if (fault === 'abort in decoder') {
      await assert.rejects(pending, (err) => err === cancellation)
    } else if (path === 'stale poll') {
      assert.equal(await pending, 'stale')
      const fallback = events.at(-1)
      assert.ok(fallback?.type === 'error' && fallback.reason === 'stale_fallback')
      assert.equal(fallback.severity, 'warn')
      assert.equal(fallback.key, key)
      assert.equal(fallback.stage, 'read')
      assert.ok(matchesFailure(fallback.err))
    } else {
      await assert.rejects(pending, matchesFailure)
    }

    assert.deepEqual(
      events.map((event) =>
        event.type === 'poll'
          ? `poll:${event.outcome}`
          : event.type === 'miss'
            ? `miss:${event.action}`
            : event.type,
      ),
      expectedEvents,
    )
    // A subsequent initial read must still see the unreadable value in Redis.
    await assert.rejects(cache.get(), matchesFailure)
    assert.equal(fetches, 0)
    assert.deepEqual(await redis.getBuffer(data(key)), broken)
  })
}

test('Class-based codecs keep their this binding on every encode and decode path', async () => {
  class PrefixCodec<T> implements SwrCodec<T> {
    readonly prefix: string
    constructor(prefix: string) {
      this.prefix = prefix
    }
    encode(value: T): Uint8Array {
      return Buffer.from(this.prefix + JSON.stringify(value))
    }
    decode(bytes: Uint8Array): T {
      return JSON.parse(Buffer.from(bytes).toString().slice(this.prefix.length))
    }
  }
  const redis = createFakeRedis()
  const swr = createSwrClient({ redis, codec: new PrefixCodec<unknown>('client:') })
  const labels = swr.cache({
    key: (id: string) => `label:${id}`,
    codec: new PrefixCodec<string>('label:'),
    fetch: async (): Promise<string> => assert.fail('secondary write must be read back'),
  })
  const counts = swr.cache({
    key: (id: string) => `count:${id}`,
    fetch: async (): Promise<number> => assert.fail('secondary write must be read back'),
  })
  const primary = swr.cache({
    key: 'primary',
    codec: new PrefixCodec<{ ok: boolean }>('primary:'),
    fetch: async () =>
      new SwrWriteBatch({
        primary: { ok: true },
        secondary: [
          { key: labels.key('a'), value: 'heading', codec: new PrefixCodec<string>('label:') },
          { key: counts.key('a'), value: 3 },
        ],
      }),
  })

  assert.deepEqual(await primary.get(), { ok: true })
  assert.deepEqual(await primary.get(), { ok: true })
  assert.equal(await labels.get('a'), 'heading')
  assert.equal(await counts.get('a'), 3)
  await labels.set('b', 'explicit')
  assert.equal(await labels.get('b'), 'explicit')
})
