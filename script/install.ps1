[CmdletBinding()]
param(
  [string]$Version = $(if ($env:XIA_VERSION) { $env:XIA_VERSION } else { "latest" }),
  [string]$InstallDir = $(if ($env:XIA_INSTALL_DIR) {
      $env:XIA_INSTALL_DIR
    } elseif ($env:LOCALAPPDATA) {
      Join-Path $env:LOCALAPPDATA "Programs\Xia\bin"
    } else {
      Join-Path $HOME "AppData\Local\Programs\Xia\bin"
    }),
  [string]$Repo = $(if ($env:XIA_GITHUB_REPO) { $env:XIA_GITHUB_REPO } else { "huahaiy/xia" }),
  [bool]$AddToPath = $true
)

$ErrorActionPreference = "Stop"

function Get-XiaTarget {
  $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
  switch ($arch.ToString()) {
    "X64" { return "windows-amd64" }
    default { throw "Unsupported Windows architecture: $arch" }
  }
}

function Resolve-XiaVersion {
  param([string]$RequestedVersion, [string]$RepoName)

  if ($RequestedVersion -ne "latest") {
    return $RequestedVersion
  }

  $release = Invoke-RestMethod `
    -Headers @{
      "Accept" = "application/vnd.github+json"
      "User-Agent" = "xia-installer"
    } `
    -Uri "https://api.github.com/repos/$RepoName/releases/latest"

  if (-not $release.tag_name) {
    throw "Failed to resolve the latest Xia release tag from GitHub."
  }

  return [string]$release.tag_name
}

function Download-File {
  param(
    [string]$Url,
    [string]$Destination
  )

  Invoke-WebRequest `
    -Headers @{ "User-Agent" = "xia-installer" } `
    -Uri $Url `
    -OutFile $Destination | Out-Null
}

function Test-ArchiveChecksum {
  param(
    [string]$ArchivePath,
    [string]$ChecksumPath,
    [string]$ArchiveName
  )

  if (-not (Test-Path -LiteralPath $ChecksumPath -PathType Leaf)) {
    throw "Required checksum file is missing for $ArchiveName"
  }

  $checksumText = (Get-Content -Raw -LiteralPath $ChecksumPath).Trim()
  $expectedHash = ($checksumText -split '\s+')[0]
  if (-not [regex]::IsMatch($expectedHash, '\A[0-9A-Fa-f]{64}\z')) {
    throw "Invalid SHA-256 checksum file for $ArchiveName"
  }

  $expectedHash = $expectedHash.ToLowerInvariant()
  $actualHash = (Get-FileHash -LiteralPath $ArchivePath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($expectedHash -ne $actualHash) {
    throw "Checksum verification failed for $ArchiveName (expected $expectedHash, actual $actualHash)"
  }
}

function Ensure-PathContainsInstallDir {
  param([string]$Dir)

  $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
  $segments = @()
  if (-not [string]::IsNullOrWhiteSpace($userPath)) {
    $segments = $userPath -split ';' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
  }

  if ($segments -notcontains $Dir) {
    $newPath = if ([string]::IsNullOrWhiteSpace($userPath)) { $Dir } else { "$userPath;$Dir" }
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
  }

  $currentSegments = $env:Path -split ';' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
  if ($currentSegments -notcontains $Dir) {
    $env:Path = if ([string]::IsNullOrWhiteSpace($env:Path)) { $Dir } else { "$env:Path;$Dir" }
  }
}

# Tests dot-source the verification functions without entering this block.
if ($env:XIA_INSTALL_SOURCE_ONLY -ne "1") {
  $target = Get-XiaTarget
  $resolvedVersion = Resolve-XiaVersion -RequestedVersion $Version -RepoName $Repo
  $archive = "xia-$resolvedVersion-$target.zip"
  $archiveUrl = "https://github.com/$Repo/releases/download/$resolvedVersion/$archive"
  $checksumUrl = "$archiveUrl.sha256"

  $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("xia-install-" + [Guid]::NewGuid().ToString("N"))
  $null = New-Item -ItemType Directory -Path $tempRoot -Force

  try {
    $archivePath = Join-Path $tempRoot $archive
    $checksumPath = "$archivePath.sha256"
    $extractDir = Join-Path $tempRoot "extract"

    Write-Host "Downloading Xia $resolvedVersion for $target..."
    Download-File -Url $archiveUrl -Destination $archivePath | Out-Null

    try {
      Download-File -Url $checksumUrl -Destination $checksumPath | Out-Null
    } catch {
      throw "Required checksum asset could not be downloaded for $archive from ${checksumUrl}: $($_.Exception.Message)"
    }
    Test-ArchiveChecksum -ArchivePath $archivePath -ChecksumPath $checksumPath -ArchiveName $archive
    Write-Host "Verified archive checksum."

    Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force

    $binaryPath = Join-Path $extractDir "xia-$resolvedVersion-$target\xia.exe"
    if (-not (Test-Path $binaryPath)) {
      $binaryPath = Get-ChildItem -Path $extractDir -Filter "xia.exe" -Recurse -File |
        Select-Object -ExpandProperty FullName -First 1
    }

    if (-not $binaryPath) {
      throw "Failed to locate the Xia binary in the downloaded archive."
    }

    $null = New-Item -ItemType Directory -Path $InstallDir -Force
    Copy-Item $binaryPath (Join-Path $InstallDir "xia.exe") -Force

    if ($AddToPath) {
      Ensure-PathContainsInstallDir -Dir $InstallDir
    }

    Write-Host "Installed Xia to $(Join-Path $InstallDir 'xia.exe')"
    & (Join-Path $InstallDir "xia.exe") --help > $null
    Write-Host "Verified binary startup."

    if ($AddToPath) {
      Write-Host "Xia is ready. Run: xia"
    } else {
      Write-Host "Xia is installed. Add $InstallDir to PATH to run 'xia' directly."
    }
  } finally {
    Remove-Item -Recurse -Force $tempRoot -ErrorAction SilentlyContinue
  }
}
