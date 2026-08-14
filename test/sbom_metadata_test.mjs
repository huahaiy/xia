import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const script = new URL('../script/sbom-metadata.mjs', import.meta.url);

function run(...args) {
  return spawnSync(process.execPath, [script.pathname, ...args], { encoding: 'utf8' });
}

test('sets the generated POM release version without changing dependencies', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'xia-sbom-test-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const pom = join(root, 'pom.xml');
  await writeFile(pom, `<project>
    <artifactId>xia</artifactId><packaging>jar</packaging><version>0.1.0-SNAPSHOT</version>
    <dependencies><dependency><groupId>example</groupId><artifactId>runtime</artifactId>
      <version>0.1.0-SNAPSHOT</version></dependency></dependencies>
  </project>`);

  const result = run('prepare-pom', pom, 'v1.2.3');
  assert.equal(result.status, 0, result.stderr);
  const updated = await readFile(pom, 'utf8');
  assert.match(updated, /<packaging>jar<\/packaging><version>v1\.2\.3<\/version>/);
  assert.match(updated, /<artifactId>runtime<\/artifactId>\s*<version>0\.1\.0-SNAPSHOT<\/version>/);
});

test('finalizes a target SBOM with reproducible native metadata', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'xia-sbom-test-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const pom = join(root, 'pom.xml');
  const binary = join(root, 'xia');
  const sbomPath = join(root, 'xia.cdx.json');
  const dependencyRef = 'pkg:maven/example/runtime@1.2.3?type=jar';
  const rootRef = 'pkg:maven/xia/xia@v1.2.3?type=jar';
  const additionalRefs = Array.from({ length: 9 }, (_, index) =>
    `pkg:maven/example/runtime-${index}@1.0.${index}?type=jar`);

  await writeFile(pom, `
    <project>
      <artifactId>xia</artifactId><packaging>jar</packaging><version>v1.2.3</version>
      <dependencies><dependency>
        <groupId>example</groupId><artifactId>runtime</artifactId><version>1.2.3</version>
      </dependency>${Array.from({ length: 9 }, (_, index) => `<dependency>
        <groupId>example</groupId><artifactId>runtime-${index}</artifactId><version>1.0.${index}</version>
      </dependency>`).join('')}</dependencies>
    </project>`);
  await writeFile(binary, 'native-binary-fixture');
  await writeFile(sbomPath, JSON.stringify({
    bomFormat: 'CycloneDX',
    specVersion: '1.6',
    metadata: { timestamp: '2099-01-01T00:00:00Z', component: {
      type: 'application', name: 'xia', version: 'v1.2.3', 'bom-ref': rootRef
    } },
    components: [
      { group: 'example', name: 'runtime', version: '1.2.3', 'bom-ref': dependencyRef },
      ...Array.from({ length: 9 }, (_, index) => ({
        group: 'example', name: `runtime-${index}`, version: `1.0.${index}`,
        'bom-ref': additionalRefs[index]
      }))
    ],
    dependencies: [{ ref: rootRef, dependsOn: [dependencyRef, ...additionalRefs] }]
  }));

  const result = run('finalize', sbomPath, pom, 'v1.2.3', 'linux-amd64', binary, '1700000000');
  assert.equal(result.status, 0, result.stderr);

  const sbom = JSON.parse(await readFile(sbomPath, 'utf8'));
  const component = sbom.metadata.component;
  assert.equal(sbom.metadata.timestamp, '2023-11-14T22:13:20Z');
  assert.equal(component.hashes[0].content,
    createHash('sha256').update('native-binary-fixture').digest('hex'));
  assert.deepEqual(component.properties, [
    { name: 'xia:release-target', value: 'linux-amd64' },
    { name: 'xia:release-binary', value: 'xia' }
  ]);
});

test('rejects an SBOM missing a direct production dependency', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'xia-sbom-test-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const pom = join(root, 'pom.xml');
  const binary = join(root, 'xia');
  const sbomPath = join(root, 'xia.cdx.json');
  const dependencies = Array.from({ length: 10 }, (_, index) => `<dependency>
    <groupId>example</groupId><artifactId>runtime-${index}</artifactId><version>1.0.${index}</version>
  </dependency>`).join('');

  await writeFile(pom, `<project><dependencies>${dependencies}</dependencies></project>`);
  await writeFile(binary, 'native-binary-fixture');
  await writeFile(sbomPath, JSON.stringify({
    bomFormat: 'CycloneDX', specVersion: '1.6',
    metadata: { component: {
      type: 'application', name: 'xia', version: 'v1.2.3', 'bom-ref': 'xia'
    } },
    components: [], dependencies: [{ ref: 'xia', dependsOn: [] }]
  }));

  const result = run('finalize', sbomPath, pom, 'v1.2.3', 'linux-amd64', binary, '1700000000');
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /missing direct dependency/);
});
