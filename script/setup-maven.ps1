param(
  [string]$ToolsDir = "target/build-tools"
)

$ErrorActionPreference = "Stop"

$mavenVersion = "3.9.16"
$archiveSha256 = "5af3b743dd8b876b5c45da33b676251e5f1687712644abb4ee519ca56e1d89ce"
$archiveName = "apache-maven-$mavenVersion-bin.zip"
$archiveUrl = "https://archive.apache.org/dist/maven/maven-3/$mavenVersion/binaries/$archiveName"
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if (-not [System.IO.Path]::IsPathRooted($ToolsDir)) {
  $ToolsDir = Join-Path $repoRoot $ToolsDir
}
$ToolsDir = [System.IO.Path]::GetFullPath($ToolsDir)
$mavenHome = Join-Path $ToolsDir "apache-maven-$mavenVersion"
$mavenCommand = Join-Path $mavenHome "bin/mvn.cmd"

if (-not (Test-Path -LiteralPath $mavenCommand -PathType Leaf)) {
  $null = New-Item -ItemType Directory -Path $ToolsDir -Force
  if (Test-Path -LiteralPath $mavenHome) {
    throw "Incomplete Maven installation already exists at $mavenHome. Remove that directory and retry."
  }
  $tempRoot = Join-Path $ToolsDir (".maven-setup-" + [Guid]::NewGuid().ToString("N"))
  $archivePath = Join-Path $tempRoot $archiveName
  $extractRoot = Join-Path $tempRoot "extract"
  $null = New-Item -ItemType Directory -Path $extractRoot -Force

  try {
    Invoke-WebRequest -Uri $archiveUrl -OutFile $archivePath | Out-Null
    $actualHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actualHash -ne $archiveSha256) {
      throw "Apache Maven archive checksum verification failed (expected $archiveSha256, actual $actualHash)."
    }

    Expand-Archive -LiteralPath $archivePath -DestinationPath $extractRoot -Force
    $extractedHome = Join-Path $extractRoot "apache-maven-$mavenVersion"
    if (-not (Test-Path -LiteralPath (Join-Path $extractedHome "bin/mvn.cmd") -PathType Leaf)) {
      throw "Verified Apache Maven archive did not contain bin/mvn.cmd."
    }
    Move-Item -LiteralPath $extractedHome -Destination $mavenHome
  } finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
  }
}

Write-Output $mavenHome
