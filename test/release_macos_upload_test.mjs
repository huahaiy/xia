import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const script = new URL('../script/upload-macos-release', import.meta.url);

function run(...args) {
  return spawnSync('bash', [script.pathname, ...args], { encoding: 'utf8' });
}

test('macOS release uploader documents build and prebuilt-binary modes', () => {
  const result = run('--help');
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /upload the Xia macOS ARM64 release assets/);
  assert.match(result.stdout, /--binary <path>/);
  assert.match(result.stdout, /--clobber/);
});

test('macOS release uploader requires one filename-safe tag', () => {
  const missing = run();
  assert.equal(missing.status, 2);
  assert.match(missing.stderr, /Usage:/);

  const unsafe = run('release/candidate');
  assert.equal(unsafe.status, 1);
  assert.match(unsafe.stderr, /tag cannot be used safely/);
});
