import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { basename } from 'node:path';

function fail(message) {
  throw new Error(message);
}

function directDependencies(pomXml) {
  const dependencySections = [...pomXml.matchAll(/<dependencies>([\s\S]*?)<\/dependencies>/g)]
    .map((match) => match[1]);
  const section = dependencySections.find((candidate) => candidate.includes('<dependency>'));
  if (!section) fail('Generated POM has no dependency section.');

  return [...section.matchAll(/<dependency>([\s\S]*?)<\/dependency>/g)].map((match) => {
    const xml = match[1];
    const field = (name) => {
      const value = xml.match(new RegExp(`<${name}>([^<]+)</${name}>`))?.[1];
      if (!value) fail(`Generated POM dependency is missing ${name}.`);
      return value;
    };
    return { group: field('groupId'), name: field('artifactId'), version: field('version') };
  });
}

async function preparePom(pomPath, version) {
  const xml = await readFile(pomPath, 'utf8');
  const projectVersion = /(<artifactId>xia<\/artifactId>\s*<packaging>jar<\/packaging>\s*<version>)([^<]+)(<\/version>)/;
  if (!projectVersion.test(xml)) fail('Could not locate Xia project version in generated POM.');
  const escapedVersion = version
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
  await writeFile(pomPath, xml.replace(projectVersion, `$1${escapedVersion}$3`));
}

async function finalize(sbomPath, pomPath, version, target, binaryPath, sourceDateEpoch) {
  const [sbomText, pomXml, binary] = await Promise.all([
    readFile(sbomPath, 'utf8'),
    readFile(pomPath, 'utf8'),
    readFile(binaryPath)
  ]);
  const sbom = JSON.parse(sbomText);
  if (sbom.bomFormat !== 'CycloneDX' || sbom.specVersion !== '1.6') {
    fail('SBOM must be CycloneDX 1.6 JSON.');
  }
  if (sbom.serialNumber) fail('Release SBOM must not contain a random serial number.');
  if (!/^\d+$/.test(sourceDateEpoch)) fail('SOURCE_DATE_EPOCH must be an integer.');
  const timestamp = new Date(Number(sourceDateEpoch) * 1000);
  if (Number.isNaN(timestamp.valueOf())) fail('SOURCE_DATE_EPOCH is outside the supported range.');
  sbom.metadata.timestamp = timestamp.toISOString().replace('.000Z', 'Z');

  const root = sbom.metadata?.component;
  if (!root || root.name !== 'xia' || root.type !== 'application' || root.version !== version) {
    fail('SBOM root component does not identify the requested Xia release.');
  }

  const binaryHash = createHash('sha256').update(binary).digest('hex');
  root.hashes = [
    ...(root.hashes ?? []).filter((hash) => hash.alg !== 'SHA-256'),
    { alg: 'SHA-256', content: binaryHash }
  ];
  root.properties = [
    ...(root.properties ?? []).filter((property) => !property.name.startsWith('xia:release-')),
    { name: 'xia:release-target', value: target },
    { name: 'xia:release-binary', value: basename(binaryPath) }
  ];

  const componentsByCoordinate = new Map((sbom.components ?? []).map((component) => [
    `${component.group ?? ''}:${component.name}:${component.version}`, component
  ]));
  const direct = directDependencies(pomXml);
  if (direct.length === 0) fail('Generated POM has no direct production dependencies.');
  const rootDependency = (sbom.dependencies ?? []).find((dependency) => dependency.ref === root['bom-ref']);
  if (!rootDependency) fail('SBOM dependency graph is missing the Xia root component.');
  const directEdges = new Set(rootDependency.dependsOn ?? []);
  for (const component of direct) {
    const ref = `${component.group}:${component.name}:${component.version}`;
    const sbomComponent = componentsByCoordinate.get(ref);
    if (!sbomComponent) fail(`SBOM is missing direct dependency ${ref}.`);
    if (!directEdges.has(sbomComponent['bom-ref'])) {
      fail(`SBOM root graph is missing direct dependency ${ref}.`);
    }
  }

  await writeFile(sbomPath, `${JSON.stringify(sbom, null, 2)}\n`);

  const written = JSON.parse(await readFile(sbomPath, 'utf8'));
  const writtenRoot = written.metadata.component;
  const writtenHash = writtenRoot.hashes.find((hash) => hash.alg === 'SHA-256')?.content;
  const writtenTarget = writtenRoot.properties.find((property) => property.name === 'xia:release-target')?.value;
  if (writtenHash !== binaryHash || writtenTarget !== target) {
    fail('Final SBOM did not retain its binary hash and release target.');
  }
}

const [command, ...args] = process.argv.slice(2);
if (command === 'prepare-pom' && args.length === 2) {
  await preparePom(args[0], args[1]);
} else if (command === 'finalize' && args.length === 6) {
  await finalize(...args);
} else {
  fail('Usage: sbom-metadata.mjs prepare-pom <pom> <version> | finalize <sbom> <pom> <version> <target> <binary> <source-date-epoch>');
}
