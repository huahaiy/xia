param(
  [Parameter(Mandatory = $true)][string]$Version,
  [Parameter(Mandatory = $true)][string]$Target,
  [Parameter(Mandatory = $true)][string]$NativeBinary,
  [Parameter(Mandatory = $true)][string]$OutputPath
)

$ErrorActionPreference = "Stop"

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$workDir = if ($env:XIA_SBOM_WORK_DIR) {
  $env:XIA_SBOM_WORK_DIR
} else {
  Join-Path $repoRoot "target/sbom/work-$Target"
}
if (-not [System.IO.Path]::IsPathRooted($workDir)) {
  $workDir = Join-Path $repoRoot $workDir
}
if (-not [System.IO.Path]::IsPathRooted($NativeBinary)) {
  $NativeBinary = Join-Path $repoRoot $NativeBinary
}
if (-not [System.IO.Path]::IsPathRooted($OutputPath)) {
  $OutputPath = Join-Path $repoRoot $OutputPath
}
$workDir = [System.IO.Path]::GetFullPath($workDir)
$NativeBinary = [System.IO.Path]::GetFullPath($NativeBinary)
$OutputPath = [System.IO.Path]::GetFullPath($OutputPath)

if (-not $OutputPath.EndsWith(".json", [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "SBOM output path must end in .json: $OutputPath"
}
if (-not (Test-Path -LiteralPath $NativeBinary -PathType Leaf)) {
  throw "Native binary does not exist: $NativeBinary"
}

$null = New-Item -ItemType Directory -Path $workDir -Force
$null = New-Item -ItemType Directory -Path ([System.IO.Path]::GetDirectoryName($OutputPath)) -Force
$pomPath = Join-Path $workDir "pom.xml"
$pomRelative = [System.IO.Path]::GetRelativePath($repoRoot, $pomPath)
if ($pomRelative -eq ".." -or $pomRelative.StartsWith("..$([System.IO.Path]::DirectorySeparatorChar)")) {
  throw "SBOM work directory must be inside the Xia checkout: $workDir"
}

Push-Location $repoRoot
try {
  & lein with-profile "-user,-dev,+release" pom $pomRelative
  if ($LASTEXITCODE -ne 0) {
    throw "Leiningen failed to generate the release POM."
  }
} finally {
  Pop-Location
}

& node (Join-Path $repoRoot "script/sbom-metadata.mjs") prepare-pom $pomPath $Version
if ($LASTEXITCODE -ne 0) {
  throw "Failed to set the release version in the generated POM."
}

$outputDirectory = [System.IO.Path]::GetDirectoryName($OutputPath)
$outputName = [System.IO.Path]::GetFileNameWithoutExtension($OutputPath)
$mavenCommand = if ($env:XIA_MAVEN) { $env:XIA_MAVEN } else { "mvn.cmd" }
$mavenArguments = @(
  "-B",
  "-f", $pomPath,
  "org.cyclonedx:cyclonedx-maven-plugin:2.9.2:makeBom",
  "-DprojectType=application",
  "-DschemaVersion=1.6",
  "-DincludeBomSerialNumber=false",
  "-DincludeTestScope=false",
  "-DoutputFormat=json",
  "-DoutputName=$outputName",
  "-DoutputDirectory=$outputDirectory",
  "-Dcyclonedx.skipAttach=true"
)
& $mavenCommand @mavenArguments
if ($LASTEXITCODE -ne 0) {
  throw "CycloneDX Maven plugin failed to generate the SBOM."
}

$sourceDateEpoch = if ($env:SOURCE_DATE_EPOCH) {
  $env:SOURCE_DATE_EPOCH
} else {
  (& git -C $repoRoot log -1 --format=%ct).Trim()
}
if ($LASTEXITCODE -ne 0) {
  throw "Failed to resolve the source commit timestamp."
}
& node (Join-Path $repoRoot "script/sbom-metadata.mjs") finalize `
  $OutputPath $pomPath $Version $Target $NativeBinary $sourceDateEpoch
if ($LASTEXITCODE -ne 0) {
  throw "Generated SBOM failed Xia's metadata checks."
}
Write-Host "Generated and verified $OutputPath"
