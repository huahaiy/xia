import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  chmod, mkdir, mkdtemp, rm, writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const script = new URL('../script/release-canary.mjs', import.meta.url);
const version = 'v1.2.3';
const targets = [
  { name: 'macos-arm64', binary: 'xia', executable: true },
  { name: 'linux-amd64', binary: 'xia', executable: true },
  { name: 'linux-arm64', binary: 'xia', executable: true },
  { name: 'windows-amd64', binary: 'xia.exe', executable: false },
];

function hash(value) {
  return createHash('sha256').update(value).digest('hex');
}

function run(assetRoot, extractedRoot) {
  return spawnSync(
    process.execPath,
    [script.pathname, 'verify', assetRoot, extractedRoot, version],
    { encoding: 'utf8' },
  );
}

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'xia-release-canary-test-'));
  const assetRoot = join(root, 'assets');
  const extractedRoot = join(root, 'extracted');
  await Promise.all([mkdir(assetRoot), mkdir(extractedRoot)]);

  for (const { name: target, binary, executable } of targets) {
    const stem = `xia-${version}-${target}`;
    const archiveName = `${stem}.zip`;
    const archive = Buffer.from(`zip fixture for ${target}`);
    const binaryContent = Buffer.from(`native binary for ${target}`);
    const componentRef = `pkg:maven/xia/xia@${version}?type=jar`;
    const dependencyRef = 'pkg:maven/example/runtime@1.0.0?type=jar';
    const sbom = Buffer.from(`${JSON.stringify({
      bomFormat: 'CycloneDX',
      specVersion: '1.6',
      metadata: {
        component: {
          type: 'application',
          name: 'xia',
          version,
          'bom-ref': componentRef,
          hashes: [{ alg: 'SHA-256', content: hash(binaryContent) }],
          properties: [
            { name: 'xia:release-target', value: target },
            { name: 'xia:release-binary', value: binary },
          ],
        },
      },
      components: [{
        type: 'library', name: 'runtime', version: '1.0.0', 'bom-ref': dependencyRef,
      }],
      dependencies: [{ ref: componentRef, dependsOn: [dependencyRef] }],
    }, null, 2)}\n`);
    const packageRoot = join(extractedRoot, target, stem);
    await mkdir(packageRoot, { recursive: true });
    await Promise.all([
      writeFile(join(assetRoot, archiveName), archive),
      writeFile(join(assetRoot, `${archiveName}.sha256`), `${hash(archive)}  ${archiveName}\n`),
      writeFile(join(assetRoot, `${stem}.cdx.json`), sbom),
      writeFile(
        join(assetRoot, `${stem}.provenance.sigstore.json`),
        '{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json"}\n',
      ),
      writeFile(join(packageRoot, binary), binaryContent),
      writeFile(join(packageRoot, 'LICENSE'), 'license fixture'),
      writeFile(join(packageRoot, 'README.md'), 'readme fixture'),
      writeFile(join(packageRoot, 'SBOM.cdx.json'), sbom),
    ]);
    if (executable) await chmod(join(packageRoot, binary), 0o755);
  }
  return { root, assetRoot, extractedRoot };
}

test('accepts the exact four-target release candidate', async (t) => {
  const { root, assetRoot, extractedRoot } = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));

  const result = run(assetRoot, extractedRoot);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Release candidate metadata passed/);
});

test('rejects an archive that does not match its checksum sidecar', async (t) => {
  const { root, assetRoot, extractedRoot } = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(assetRoot, `xia-${version}-linux-amd64.zip`), 'tampered archive');

  const result = run(assetRoot, extractedRoot);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /does not match its SHA-256 sidecar/);
});

test('rejects a package whose native binary differs from its SBOM', async (t) => {
  const { root, assetRoot, extractedRoot } = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(
    join(extractedRoot, 'linux-amd64', `xia-${version}-linux-amd64`, 'xia'),
    'tampered native binary',
  );

  const result = run(assetRoot, extractedRoot);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /native binary does not match its SBOM SHA-256/);
});

test('rejects extra release assets', async (t) => {
  const { root, assetRoot, extractedRoot } = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(assetRoot, 'unexpected.txt'), 'unexpected');

  const result = run(assetRoot, extractedRoot);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /unexpected file set/);
});

test('rejects extra files inside a package', async (t) => {
  const { root, assetRoot, extractedRoot } = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(
    join(extractedRoot, 'macos-arm64', `xia-${version}-macos-arm64`, 'unexpected.txt'),
    'unexpected',
  );

  const result = run(assetRoot, extractedRoot);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /package has an unexpected file set/);
});
