import { createHash } from 'node:crypto';
import { lstat, readFile, readdir } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';

const supportedTargets = [
  { name: 'macos-arm64', binary: 'xia', executable: true },
  { name: 'linux-amd64', binary: 'xia', executable: true },
  { name: 'linux-arm64', binary: 'xia', executable: true },
  { name: 'windows-amd64', binary: 'xia.exe', executable: false },
];

function fail(message) {
  throw new Error(message);
}

function selectedTargets() {
  const requested = process.env.XIA_RELEASE_TARGETS?.trim();
  if (!requested) return supportedTargets;

  const names = requested.split(/\s+/);
  if (new Set(names).size !== names.length) {
    fail('XIA_RELEASE_TARGETS must not contain duplicate targets.');
  }
  return names.map((name) => {
    const target = supportedTargets.find((candidate) => candidate.name === name);
    if (!target) fail(`Unsupported release target: ${name}.`);
    return target;
  });
}

const targets = selectedTargets();

function sorted(values) {
  return [...values].sort((left, right) => left.localeCompare(right, 'en'));
}

function assertExactNames(actual, expected, label) {
  const actualSorted = sorted(actual);
  const expectedSorted = sorted(expected);
  if (actualSorted.length !== expectedSorted.length
      || actualSorted.some((value, index) => value !== expectedSorted[index])) {
    fail(`${label} has an unexpected file set. Expected ${expectedSorted.join(', ')}; `
      + `found ${actualSorted.join(', ')}.`);
  }
}

async function assertRegularFile(path, label) {
  let status;
  try {
    status = await lstat(path);
  } catch (error) {
    fail(`${label} is missing: ${error.message}`);
  }
  if (!status.isFile() || status.isSymbolicLink()) {
    fail(`${label} must be a regular file.`);
  }
  return status;
}

async function assertDirectory(path, label) {
  let status;
  try {
    status = await lstat(path);
  } catch (error) {
    fail(`${label} is missing: ${error.message}`);
  }
  if (!status.isDirectory() || status.isSymbolicLink()) {
    fail(`${label} must be a directory.`);
  }
}

async function sha256(path) {
  return createHash('sha256').update(await readFile(path)).digest('hex');
}

function rootProperty(component, name) {
  return component.properties?.find((property) => property.name === name)?.value;
}

async function verifySbom(sbomPath, embeddedPath, binaryPath, version, target) {
  const [standalone, embedded, binaryHash] = await Promise.all([
    readFile(sbomPath),
    readFile(embeddedPath),
    sha256(binaryPath),
  ]);
  if (!standalone.equals(embedded)) {
    fail(`${target} embedded SBOM differs from the attested standalone SBOM.`);
  }

  let sbom;
  try {
    sbom = JSON.parse(standalone.toString('utf8'));
  } catch (error) {
    fail(`${target} SBOM is not valid JSON: ${error.message}`);
  }
  if (sbom.bomFormat !== 'CycloneDX' || sbom.specVersion !== '1.6') {
    fail(`${target} SBOM must be CycloneDX 1.6 JSON.`);
  }
  const component = sbom.metadata?.component;
  if (component?.type !== 'application'
      || component.name !== 'xia'
      || component.version !== version) {
    fail(`${target} SBOM root component does not identify Xia ${version}.`);
  }
  if (rootProperty(component, 'xia:release-target') !== target) {
    fail(`${target} SBOM has the wrong release target.`);
  }
  if (rootProperty(component, 'xia:release-binary') !== basename(binaryPath)) {
    fail(`${target} SBOM has the wrong release binary name.`);
  }
  const declaredHash = component.hashes
    ?.find((hash) => hash.alg === 'SHA-256')?.content?.toLowerCase();
  if (declaredHash !== binaryHash) {
    fail(`${target} native binary does not match its SBOM SHA-256.`);
  }
  if (!Array.isArray(sbom.components) || sbom.components.length === 0
      || !Array.isArray(sbom.dependencies) || sbom.dependencies.length === 0) {
    fail(`${target} SBOM is missing its dependency graph.`);
  }
}

function validateVersion(version) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._+-]*$/.test(version)) {
    fail(`Invalid release version for an asset filename: ${version}`);
  }
}

async function verifyAssets(assetDirectory, version) {
  validateVersion(version);
  const assetRoot = resolve(assetDirectory);
  await assertDirectory(assetRoot, 'Release asset directory');

  const expectedAssets = targets.flatMap(({ name }) => {
    const stem = `xia-${version}-${name}`;
    return [
      `${stem}.zip`,
      `${stem}.zip.sha256`,
      `${stem}.cdx.json`,
      `${stem}.provenance.sigstore.json`,
    ];
  });
  const assetEntries = await readdir(assetRoot, { withFileTypes: true });
  assertExactNames(assetEntries.map((entry) => entry.name), expectedAssets, 'Release candidate');
  for (const entry of assetEntries) {
    if (!entry.isFile() || entry.isSymbolicLink()) {
      fail(`Release asset must be a regular file: ${entry.name}`);
    }
  }

  for (const { name: target } of targets) {
    const stem = `xia-${version}-${target}`;
    const archiveName = `${stem}.zip`;
    const archivePath = join(assetRoot, archiveName);
    const checksumPath = `${archivePath}.sha256`;
    const sbomPath = join(assetRoot, `${stem}.cdx.json`);
    const provenancePath = join(assetRoot, `${stem}.provenance.sigstore.json`);
    await Promise.all([
      assertRegularFile(archivePath, `${target} archive`),
      assertRegularFile(checksumPath, `${target} checksum`),
      assertRegularFile(sbomPath, `${target} SBOM`),
      assertRegularFile(provenancePath, `${target} provenance bundle`),
    ]);

    const checksumText = await readFile(checksumPath, 'utf8');
    const checksumMatch = checksumText.match(/^([0-9a-fA-F]{64})  ([^\r\n]+)\r?\n?$/);
    if (!checksumMatch || checksumMatch[2] !== archiveName) {
      fail(`${target} checksum sidecar must contain one SHA-256 for ${archiveName}.`);
    }
    if (checksumMatch[1].toLowerCase() !== await sha256(archivePath)) {
      fail(`${target} archive does not match its SHA-256 sidecar.`);
    }

    let provenance;
    try {
      provenance = JSON.parse(await readFile(provenancePath, 'utf8'));
    } catch (error) {
      fail(`${target} provenance bundle is not valid JSON: ${error.message}`);
    }
    if (!provenance || typeof provenance !== 'object' || Array.isArray(provenance)) {
      fail(`${target} provenance bundle must be a JSON object.`);
    }
  }
}

async function verifyPackages(assetDirectory, extractedDirectory, version) {
  validateVersion(version);
  const assetRoot = resolve(assetDirectory);
  const extractedRoot = resolve(extractedDirectory);
  await assertDirectory(assetRoot, 'Release asset directory');
  await assertDirectory(extractedRoot, 'Extracted archive directory');

  for (const { name: target, binary, executable } of targets) {
    const stem = `xia-${version}-${target}`;
    const sbomPath = join(assetRoot, `${stem}.cdx.json`);
    const targetRoot = join(extractedRoot, target);
    await assertDirectory(targetRoot, `${target} extraction directory`);
    const targetEntries = await readdir(targetRoot, { withFileTypes: true });
    assertExactNames(targetEntries.map((entry) => entry.name), [stem], `${target} archive root`);
    const packageRoot = join(targetRoot, stem);
    await assertDirectory(packageRoot, `${target} package root`);
    const packageEntries = await readdir(packageRoot, { withFileTypes: true });
    const packageFiles = [binary, 'LICENSE', 'README.md', 'SBOM.cdx.json'];
    assertExactNames(packageEntries.map((entry) => entry.name), packageFiles, `${target} package`);
    for (const entry of packageEntries) {
      if (!entry.isFile() || entry.isSymbolicLink()) {
        fail(`${target} package entry must be a regular file: ${entry.name}`);
      }
    }

    const binaryPath = join(packageRoot, binary);
    const binaryStatus = await assertRegularFile(binaryPath, `${target} native binary`);
    if (executable && (binaryStatus.mode & 0o111) === 0) {
      fail(`${target} native binary is not executable after extraction.`);
    }
    await verifySbom(
      sbomPath,
      join(packageRoot, 'SBOM.cdx.json'),
      binaryPath,
      version,
      target,
    );
  }
}

async function verifyCandidate(assetDirectory, extractedDirectory, version) {
  await verifyAssets(assetDirectory, version);
  await verifyPackages(assetDirectory, extractedDirectory, version);
}

const [command, ...args] = process.argv.slice(2);
try {
  if (command === 'verify-assets' && args.length === 2) {
    await verifyAssets(...args);
    console.log(`Release assets passed for Xia ${args[1]}.`);
  } else if (command === 'verify-packages' && args.length === 3) {
    await verifyPackages(...args);
    console.log(`Extracted packages passed for Xia ${args[2]}.`);
  } else if (command === 'verify' && args.length === 3) {
    await verifyCandidate(...args);
    console.log(`Release candidate metadata passed for Xia ${args[2]}.`);
  } else {
    fail('Usage: release-canary.mjs verify-assets <asset-directory> <version> | '
      + 'verify-packages <asset-directory> <extracted-directory> <version> | '
      + 'verify <asset-directory> <extracted-directory> <version>');
  }
} catch (error) {
  console.error(`release canary failed: ${error.message}`);
  process.exitCode = 1;
}
