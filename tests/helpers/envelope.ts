import {
  DEFAULT_CODEC,
  decodeEnvelopeValue,
  encodeEnvelope as encode,
  readEnvelopeHeader,
  type CacheEnvelope,
} from '../../src/codec.ts'
import { resolveCompression } from '../../src/options.ts'
import type { SwrCodec, SwrCompressionOption } from '../../src/types.ts'

export function encodeEnvelope<T>(
  envelope: CacheEnvelope<T>,
  codec: SwrCodec<T> = DEFAULT_CODEC as SwrCodec<T>,
  compression?: SwrCompressionOption,
): Buffer {
  return encode(envelope, codec, resolveCompression(compression))
}

export function decodeEnvelope<T>(
  buffer: Uint8Array,
  codec: SwrCodec<T> = DEFAULT_CODEC as SwrCodec<T>,
): CacheEnvelope<T> {
  const header = readEnvelopeHeader(buffer)
  const { cachedAtMs, freshUntilMs, expiresAtMs } = header
  return {
    value: decodeEnvelopeValue(buffer, header, codec),
    cachedAtMs,
    freshUntilMs,
    expiresAtMs,
  }
}
