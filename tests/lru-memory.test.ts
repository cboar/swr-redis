import test from 'node:test'
import assert from 'node:assert/strict'
import type { LruMemory } from '../src/memory.ts'
import { createLruMemory } from './helpers/memory-harness.ts'
import type { Envelope } from '../src/codec.ts'

const envelope = <T>(value: T): Envelope<T> => ({
  value,
  timing: { cachedAtMs: 1000, freshUntilMs: 11_000, expiresAtMs: 31_000 },
})

function write(memory: LruMemory, key: string, stored: Envelope<unknown>): void {
  memory.settle(memory.beginWrite([{ key, envelope: stored }]), 'populate')
}

function populate(memory: LruMemory, key: string, stored: Envelope<unknown>, now = 1000) {
  const read = memory.beginRead(key)
  memory.populate(read, stored, now)
  memory.endRead(read)
}

test('LruMemory: evicts the least recently used entry at capacity; reads and writes refresh recency', () => {
  const memory = createLruMemory({ maxEntries: 2 })
  write(memory, 'a', envelope(1))
  write(memory, 'b', envelope(2))
  assert.equal(memory.get('a', 1000)?.value, 1)
  write(memory, 'c', envelope(3))
  assert.equal(memory.get('b', 1000), undefined)
  write(memory, 'a', envelope(4))
  populate(memory, 'd', envelope(5))
  assert.equal(memory.get('c', 1000), undefined)
  assert.deepEqual(
    ['a', 'd'].map((key) => memory.get(key, 1000)?.value),
    [4, 5],
  )
  assert.equal(memory.entryCount, 2)
})

test('LruMemory: lifetime ends at stored freshness or the non-sliding maxTtl from cachedAtMs', () => {
  // [case, freshUntilMs, read-populate time (undefined: stored by a write), last time memory hits
  // (undefined: never stored)]
  const cases: [string, number, number | undefined, number | undefined][] = [
    ['stored freshness', 1500, undefined, 1499],
    ['memory cap', 6000, undefined, 1999],
    ['populate keeps the writer cachedAtMs', 6000, 1800, 1999],
    ['populate at the memory cap', 6000, 2000, undefined],
  ]
  for (const [name, freshUntilMs, populateAt, lastHit] of cases) {
    const memory = createLruMemory({ maxTtl: 1 })
    const stored = { value: name, timing: { cachedAtMs: 1000, freshUntilMs, expiresAtMs: 31_000 } }
    if (populateAt === undefined) write(memory, 'key', stored)
    else populate(memory, 'key', stored, populateAt)
    if (lastHit === undefined) {
      assert.equal(memory.entryCount, 0, name)
      continue
    }
    assert.deepEqual(memory.get('key', lastHit - 300), stored, name)
    assert.deepEqual(memory.get('key', lastHit), stored, name)
    assert.equal(memory.get('key', lastHit + 1), undefined, name)
    assert.equal(memory.entryCount, 0, name)
  }
})

test('LruMemory: clone controls whether populated and returned values are isolated', () => {
  for (const clone of [false, true]) {
    const memory = createLruMemory({ clone })
    const value = { tags: ['original'] }
    populate(memory, 'key', envelope(value))
    value.tags.push('after-populate')
    const hit = memory.get<typeof value>('key', 1000)!.value
    hit.tags.push('after-read')
    const again = memory.get<typeof value>('key', 1000)!.value
    assert.deepEqual([hit === value, again === hit], [!clone, !clone])
    assert.deepEqual(
      again.tags,
      clone ? ['original'] : ['original', 'after-populate', 'after-read'],
    )
  }
})

test('LruMemory settle: an uncontested write populates, an evicting write removes the key', () => {
  const memory = createLruMemory()
  write(memory, 'key', envelope('written'))
  assert.equal(memory.get('key', 1000)?.value, 'written')
  memory.settle(memory.beginWrite([{ key: 'key', envelope: envelope('unknown') }]), 'evict')
  assert.equal(memory.get('key', 1000), undefined)
  write(memory, 'key', envelope('again'))
  memory.settle(memory.beginWrite([{ key: 'key' }]), 'populate')
  assert.equal(memory.get('key', 1000), undefined)
  assert.equal(memory.versionCount, 0)
})

test('LruMemory settle: of two overlapping writes, the first to settle populates and the second evicts', () => {
  const memory = createLruMemory()
  write(memory, 'key', envelope('old'))
  const first = memory.beginWrite([{ key: 'key', envelope: envelope('first') }])
  const second = memory.beginWrite([{ key: 'key', envelope: envelope('second') }])
  memory.settle(second, 'populate')
  assert.equal(memory.get('key', 1000)?.value, 'second')
  memory.settle(first, 'populate')
  assert.equal(memory.get('key', 1000), undefined)
  assert.equal(memory.versionCount, 0)
})

test('LruMemory settle: a read begun before a settled write cannot populate; a later read can', () => {
  const memory = createLruMemory()
  const older = memory.beginRead('key')
  memory.settle(memory.beginWrite([{ key: 'key' }]), 'evict')
  memory.populate(older, envelope('old-read'), 1000)
  assert.equal(memory.get('key', 1000), undefined)
  const newer = memory.beginRead('key')
  memory.populate(newer, envelope('new-read'), 1000)
  assert.equal(memory.get('key', 1000)?.value, 'new-read')
  memory.endRead(older)
  memory.endRead(newer)
  assert.equal(memory.versionCount, 0)
})

test('LruMemory: an ineligible populate does not clone, so application getters do not run', () => {
  const memory = createLruMemory({ clone: true })
  let getterCalls = 0
  const value = {
    get tag() {
      getterCalls++
      return 'read'
    },
  }
  const read = memory.beginRead('key')
  memory.settle(memory.beginWrite([{ key: 'key' }]), 'evict')
  memory.populate(read, envelope(value), 1000)
  memory.endRead(read)
  assert.deepEqual([getterCalls, memory.entryCount], [0, 0])
  populate(memory, 'key', envelope(value))
  assert.deepEqual([getterCalls, memory.entryCount], [1, 1])
})
