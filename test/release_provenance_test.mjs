import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const workflowPath = new URL('../.github/workflows/release.binaries.yml', import.meta.url);
const canaryScriptPath = new URL('../script/release-canary', import.meta.url);
const [workflow, canaryScript] = await Promise.all([
  readFile(workflowPath, 'utf8'),
  readFile(canaryScriptPath, 'utf8'),
]);

test('release workflow grants signing permissions only to native builds', () => {
  assert.match(workflow, /^permissions:\n  contents: read$/m);
  assert.match(
    workflow,
    /  build:\n[\s\S]*?    permissions:\n      contents: read\n      id-token: write\n      attestations: write/,
  );
  assert.match(
    workflow,
    /  canary:\n[\s\S]*?    permissions:\n      contents: read\n      attestations: read/,
  );
  assert.match(
    workflow,
    /  publish:\n[\s\S]*?    permissions:\n      contents: write/,
  );
});

test('release workflow attests and publishes every public release artifact', () => {
  const attestStart = workflow.indexOf('      - name: Generate release provenance');
  const uploadStart = workflow.indexOf('      - name: Upload workflow artifact');
  const canaryStart = workflow.indexOf('  canary:\n');
  const publishJobStart = workflow.indexOf('  publish:\n');
  const publishStart = workflow.indexOf('      - name: Upload assets to GitHub release');

  assert.ok(attestStart > 0, 'missing release provenance step');
  assert.ok(uploadStart > attestStart, 'provenance must precede workflow upload');
  assert.ok(canaryStart > uploadStart, 'release canary must follow attestation');
  assert.ok(publishJobStart > canaryStart, 'release publication must follow the canary');
  assert.ok(publishStart > publishJobStart, 'missing release publication step');

  const attestation = workflow.slice(attestStart, uploadStart);
  assert.match(
    attestation,
    /actions\/attest@59d89421af93a897026c735860bf21b6eb4f7b26 # v4\.1\.0/,
  );
  assert.match(attestation, /\$\{\{ steps\.names\.outputs\.archive \}\}/);
  assert.match(attestation, /\$\{\{ steps\.names\.outputs\.archive \}\}\.sha256/);
  assert.match(attestation, /\.cdx\.json/);
  assert.match(attestation, /\.provenance\.sigstore\.json/);
  assert.match(attestation, /- name: Verify release provenance/);
  assert.equal(
    (attestation.match(/gh attestation verify/g) ?? []).length,
    3,
    'archive, checksum, and SBOM must all be verified',
  );
  assert.match(attestation, /--bundle "\$PROVENANCE_BUNDLE"/);
  assert.match(attestation, /--signer-workflow "\$SIGNER_WORKFLOW"/);

  const upload = workflow.slice(uploadStart, canaryStart);
  assert.match(upload, /\.provenance\.sigstore\.json/);
  assert.match(workflow.slice(publishStart), /release-assets\/\*\.sigstore\.json/);
});

test('publication consumes only the candidate that passed the packaged canary', () => {
  const canaryStart = workflow.indexOf('  canary:\n');
  const publishStart = workflow.indexOf('  publish:\n');
  assert.ok(canaryStart > 0, 'missing release canary job');
  assert.ok(publishStart > canaryStart, 'publish job must follow canary job');

  const canary = workflow.slice(canaryStart, publishStart);
  assert.match(canary, /needs: build/);
  assert.match(canary, /bash script\/release-canary/);
  assert.match(canary, /name: release-candidate-\$\{\{ github\.run_id \}\}/);
  assert.match(canary, /overwrite: true/);
  assert.ok(
    canary.indexOf('Run packaged release canary')
      < canary.indexOf('Preserve validated release candidate'),
    'candidate bytes must be preserved only after validation succeeds',
  );

  const publish = workflow.slice(publishStart);
  assert.match(publish, /needs: canary/);
  assert.match(publish, /name: release-candidate-\$\{\{ github\.run_id \}\}/);
  assert.doesNotMatch(publish, /pattern: release-\*/);
});

test('release canary validates every package before running the packaged Linux binary', () => {
  assert.match(
    canaryScript,
    /TARGETS=\(macos-arm64 linux-amd64 linux-arm64 windows-amd64\)/,
  );
  assert.match(canaryScript, /unzip -tq "\$archive"/);
  assert.match(canaryScript, /archive contains an unexpected or unsafe entry/);
  assert.match(
    canaryScript,
    /for subject in "\$stem\.zip" "\$stem\.zip\.sha256" "\$stem\.cdx\.json"/,
  );
  assert.match(canaryScript, /gh attestation verify/);
  assert.match(
    canaryScript,
    /script\/native-smoke" \\\n  "\$CANARY_ROOT\/extracted\/linux-amd64\/xia-\$VERSION-linux-amd64\/xia"/,
  );
});
