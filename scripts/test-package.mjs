import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import {
  copyFileSync,
  mkdtempSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const root = dirname(dirname(fileURLToPath(import.meta.url)))
const temporary = mkdtempSync(join(tmpdir(), 'swr-package-'))

const peerTargets = [
  {
    name: 'v5',
    ioredisPackage: 'ioredis-v5',
    bsonPackage: 'bson-v6',
    bsonMajor: 6,
    assertVersion: (version) => assert.equal(version, '5.11.1'),
  },
  {
    name: 'v6',
    ioredisPackage: 'ioredis',
    bsonPackage: 'bson',
    bsonMajor: 7,
    assertVersion: (version) =>
      assert.ok(version.startsWith('6.'), `Expected ioredis 6.x, got ${version}`),
  },
]

try {
  // Exercise prepack as well as the actual tarball, outside the source checkout.
  execFileSync('npm', ['pack', '--pack-destination', temporary], { cwd: root, stdio: 'pipe' })
  const tarball = readdirSync(temporary).find((name) => name.endsWith('.tgz'))
  assert.ok(tarball)
  const tarballPath = join(temporary, tarball)
  const testedVersions = []

  for (const target of peerTargets) {
    const runDir = join(temporary, `consumer-${target.name}`)
    const installed = join(runDir, 'node_modules', 'swr-redis')
    mkdirSync(installed, { recursive: true })
    execFileSync('tar', ['-xzf', tarballPath, '--strip-components=1', '-C', installed])

    for (const [dependency, source] of [
      ['bson', target.bsonPackage],
      ['ioredis', target.ioredisPackage],
      ['@types', '@types'],
    ]) {
      symlinkSync(
        join(root, 'node_modules', source),
        join(runDir, 'node_modules', dependency),
        'junction',
      )
    }
    const ioredisManifest = JSON.parse(
      readFileSync(join(runDir, 'node_modules', 'ioredis', 'package.json'), 'utf8'),
    )
    target.assertVersion(ioredisManifest.version)
    const bsonManifest = JSON.parse(
      readFileSync(join(runDir, 'node_modules', 'bson', 'package.json'), 'utf8'),
    )
    assert.equal(Number(bsonManifest.version.split('.')[0]), target.bsonMajor)
    testedVersions.push(`ioredis ${ioredisManifest.version} + bson ${bsonManifest.version}`)

    const manifest = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'))
    assert.equal(manifest.name, 'swr-redis')
    assert.equal(manifest.main, './dist/index.js')
    assert.equal(manifest.types, './dist/index.d.ts')
    assert.equal(manifest.peerDependencies?.ioredis, '^5.11.1 || ^6.0.0')
    assert.equal(manifest.peerDependencies?.bson, '^6.10.4 || ^7.0.0')
    assert.equal(manifest.dependencies?.bson, undefined)
    assert.equal(manifest.sideEffects, false)
    assert.ok(!readdirSync(installed).includes('src'))

    writeFileSync(join(runDir, 'package.json'), JSON.stringify({ type: 'module' }))
    copyFileSync(join(root, 'tests/package/consumer.mjs'), join(runDir, 'consumer.mjs'))
    execFileSync(
      process.execPath,
      ['consumer.mjs', pathToFileURL(join(root, 'tests/fake-redis.ts')).href],
      { cwd: runDir, stdio: 'inherit' },
    )
    copyFileSync(join(root, 'tests/package/consumer.ts'), join(runDir, 'consumer.ts'))
    for (const [moduleKind, resolution] of [
      ['nodenext', 'nodenext'],
      ['esnext', 'bundler'],
    ]) {
      execFileSync(
        process.execPath,
        [
          join(root, 'node_modules/typescript/bin/tsc'),
          '--noEmit',
          '--strict',
          '--exactOptionalPropertyTypes',
          '--skipLibCheck',
          'false',
          '--target',
          'es2024',
          '--module',
          moduleKind,
          '--moduleResolution',
          resolution,
          'consumer.ts',
        ],
        { cwd: runDir, stdio: 'inherit' },
      )
    }
  }

  console.log(
    `Packed JavaScript, public exports, and consumer declarations passed (${testedVersions.join(', ')}).`,
  )
} finally {
  rmSync(temporary, { recursive: true, force: true })
}
