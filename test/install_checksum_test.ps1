$ErrorActionPreference = "Stop"

$env:XIA_INSTALL_SOURCE_ONLY = "1"
. (Join-Path $PSScriptRoot "../script/install.ps1")

function Assert-ChecksumRejected {
  param(
    [string]$ArchivePath,
    [string]$ChecksumPath,
    [string]$ArchiveName,
    [string]$FailureMessage
  )

  $rejected = $false
  try {
    Test-ArchiveChecksum `
      -ArchivePath $ArchivePath `
      -ChecksumPath $ChecksumPath `
      -ArchiveName $ArchiveName
  } catch {
    $rejected = $true
  }

  if (-not $rejected) {
    throw $FailureMessage
  }
}

$testRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("xia-install-test-" + [Guid]::NewGuid().ToString("N"))
$null = New-Item -ItemType Directory -Path $testRoot -Force

try {
  $archiveName = "xia-test.zip"
  $archivePath = Join-Path $testRoot $archiveName
  $checksumPath = "$archivePath.sha256"
  [System.IO.File]::WriteAllText($archivePath, "xia installer checksum fixture")
  $actualHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()

  [System.IO.File]::WriteAllText($checksumPath, "$actualHash  $archiveName`n")
  Test-ArchiveChecksum -ArchivePath $archivePath -ChecksumPath $checksumPath -ArchiveName $archiveName

  [System.IO.File]::WriteAllText($checksumPath, "$($actualHash.ToUpperInvariant())  $archiveName`n")
  Test-ArchiveChecksum -ArchivePath $archivePath -ChecksumPath $checksumPath -ArchiveName $archiveName

  [System.IO.File]::WriteAllText($checksumPath, "not-a-sha256`n")
  Assert-ChecksumRejected `
    -ArchivePath $archivePath `
    -ChecksumPath $checksumPath `
    -ArchiveName $archiveName `
    -FailureMessage "Malformed checksum was accepted."

  [System.IO.File]::WriteAllText($checksumPath, "$("0" * 64)  $archiveName`n")
  Assert-ChecksumRejected `
    -ArchivePath $archivePath `
    -ChecksumPath $checksumPath `
    -ArchiveName $archiveName `
    -FailureMessage "Mismatched checksum was accepted."

  Remove-Item -LiteralPath $checksumPath
  Assert-ChecksumRejected `
    -ArchivePath $archivePath `
    -ChecksumPath $checksumPath `
    -ArchiveName $archiveName `
    -FailureMessage "Missing checksum was accepted."

  Write-Host "PowerShell installer checksum tests passed."
} finally {
  Remove-Item Env:XIA_INSTALL_SOURCE_ONLY -ErrorAction SilentlyContinue
  Remove-Item -Recurse -Force $testRoot -ErrorAction SilentlyContinue
}
