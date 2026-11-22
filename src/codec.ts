import zlib from 'node:zlib'
import { BSON } from 'bson'
import type { ResolvedCompression } from './options.ts'
import type { SwrCodec } from './types.ts'

export interface CacheEnvelope<T> {
  value: T
  cachedAtMs: number
  freshUntilMs: number
  expiresAtMs: number
}

export interface EnvelopeHeader {
  deflated: boolean
  cachedAtMs: number
  freshUntilMs: number
  expiresAtMs: number
}

// Envelope layout in DECISIONS.md.
export const ENVELOPE_HEADER_SIZE = 25
const FLAG_DEFLATED = 0x01

export const DEFAULT_CODEC: SwrCodec<unknown> = {
  encode: (value) => BSON.serialize({ v: value }),
  decode: (bytes) => BSON.deserialize(bytes, { promoteBuffers: true }).v,
}

export function readEnvelopeHeader(buffer: Uint8Array): EnvelopeHeader {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  return {
    deflated: (view.getUint8(0) & FLAG_DEFLATED) === FLAG_DEFLATED,
    cachedAtMs: view.getFloat64(1),
    freshUntilMs: view.getFloat64(9),
    expiresAtMs: view.getFloat64(17),
  }
}

export function encodeEnvelope<T>(
  envelope: CacheEnvelope<T>,
  codec: Pick<SwrCodec<T>, 'encode'>,
  compression: ResolvedCompression,
): Buffer {
  const payload = codec.encode(envelope.value)
  const deflated = compression.level > 0 && payload.byteLength >= compression.minBytes
  const body = deflated ? zlib.deflateSync(payload, { level: compression.level }) : payload

  const out = Buffer.allocUnsafe(ENVELOPE_HEADER_SIZE + body.byteLength)
  out[0] = deflated ? FLAG_DEFLATED : 0
  out.writeDoubleBE(envelope.cachedAtMs, 1)
  out.writeDoubleBE(envelope.freshUntilMs, 9)
  out.writeDoubleBE(envelope.expiresAtMs, 17)
  out.set(body, ENVELOPE_HEADER_SIZE)
  return out
}

export function decodeEnvelopeValue<T>(
  buffer: Uint8Array,
  header: EnvelopeHeader,
  codec: Pick<SwrCodec<T>, 'decode'>,
): T {
  const payload = buffer.subarray(ENVELOPE_HEADER_SIZE)
  return codec.decode(header.deflated ? zlib.inflateSync(payload) : payload)
}
