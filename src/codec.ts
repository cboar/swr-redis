import zlib from 'node:zlib'
import { BSON } from 'bson'
import type { ResolvedCompression } from './options.ts'
import type { SwrCodec, SwrEntryTiming } from './types.ts'

export interface Envelope<T> {
  readonly value: T
  readonly timing: SwrEntryTiming
}

export type EnvelopeHeader = NonNullable<ReturnType<typeof readEnvelopeHeader>>

// Layout: u8 version, u8 flags, u16 header length, u8 compression, 3 reserved bytes, then
// big-endian float64 freshUntilMs, expiresAtMs, cachedAtMs. Deadline offsets never move.
export const ENVELOPE_HEADER_SIZE = 32
const FORMAT_VERSION = 1
// Flag bits 0-3 must be understood; none are defined yet.
const MUST_UNDERSTAND_FLAGS = 0x0f
const COMPRESSION_NONE = 0
const COMPRESSION_DEFLATE = 1

export const DEFAULT_CODEC: SwrCodec<unknown> = {
  encode: (value) => BSON.serialize({ v: value }),
  decode: (bytes) => BSON.deserialize(bytes, { promoteBuffers: true }).v,
}

// Returns undefined for a format this reader doesn't understand.
export function readEnvelopeHeader(buffer: Uint8Array) {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const headerLength = view.getUint16(2)
  const compression = view.getUint8(4)
  if (
    view.getUint8(0) !== FORMAT_VERSION ||
    (view.getUint8(1) & MUST_UNDERSTAND_FLAGS) !== 0 ||
    headerLength < ENVELOPE_HEADER_SIZE ||
    (compression !== COMPRESSION_NONE && compression !== COMPRESSION_DEFLATE)
  ) {
    return undefined
  }
  return {
    headerLength,
    deflated: compression === COMPRESSION_DEFLATE,
    // Freeze once so callers cannot mutate shared deadlines.
    timing: Object.freeze({
      cachedAtMs: view.getFloat64(24),
      freshUntilMs: view.getFloat64(8),
      expiresAtMs: view.getFloat64(16),
    }),
  }
}

export function encodeEnvelope<T>(
  value: T,
  timing: SwrEntryTiming,
  codec: Pick<SwrCodec<T>, 'encode'>,
  compression: ResolvedCompression,
) {
  const payload = codec.encode(value)
  const deflated = compression.level > 0 && payload.byteLength >= compression.minBytes
  const body = deflated ? zlib.deflateSync(payload, { level: compression.level }) : payload

  const out = Buffer.allocUnsafe(ENVELOPE_HEADER_SIZE + body.byteLength)
  out[0] = FORMAT_VERSION
  out[1] = 0
  out.writeUInt16BE(ENVELOPE_HEADER_SIZE, 2)
  out[4] = deflated ? COMPRESSION_DEFLATE : COMPRESSION_NONE
  out.fill(0, 5, 8)
  out.writeDoubleBE(timing.freshUntilMs, 8)
  out.writeDoubleBE(timing.expiresAtMs, 16)
  out.writeDoubleBE(timing.cachedAtMs, 24)
  out.set(body, ENVELOPE_HEADER_SIZE)
  return out
}

export function decodeEnvelopeValue<T>(
  buffer: Uint8Array,
  header: EnvelopeHeader,
  codec: Pick<SwrCodec<T>, 'decode'>,
) {
  const payload = buffer.subarray(header.headerLength)
  return codec.decode(header.deflated ? zlib.inflateSync(payload) : payload)
}
