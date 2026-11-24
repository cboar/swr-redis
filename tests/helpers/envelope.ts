import {
  DEFAULT_CODEC,
  decodeEnvelopeValue,
  encodeEnvelope as encode,
  readEnvelopeHeader,
} from '../../src/codec.ts'
import { resolveCompression } from '../../src/options.ts'
import type { SwrCodec, SwrCompressionOption, SwrEntryTiming } from '../../src/types.ts'

type EnvelopeFields<T> = SwrEntryTiming & { value: T }

export function encodeEnvelope<T>(
  envelope: EnvelopeFields<T>,
  codec: SwrCodec<T> = DEFAULT_CODEC as SwrCodec<T>,
  compression?: SwrCompressionOption,
): Buffer {
  return encode(envelope.value, envelope, codec, resolveCompression(compression))
}

export function decodeEnvelope<T>(
  buffer: Uint8Array,
  codec: SwrCodec<T> = DEFAULT_CODEC as SwrCodec<T>,
): EnvelopeFields<T> {
  const header = readEnvelopeHeader(buffer)
  return {
    value: decodeEnvelopeValue(buffer, header, codec),
    ...header.timing,
  }
}
