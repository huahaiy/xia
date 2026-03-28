#!/bin/sh

set -eu

REPO="${XIA_GITHUB_REPO:-huahaiy/xia}"
VERSION="${XIA_VERSION:-latest}"
INSTALL_DIR="${XIA_INSTALL_DIR:-$HOME/.local/bin}"

usage() {
  cat <<'EOF'
Install Xia from GitHub release artifacts.

Usage:
  install.sh [--version <tag>] [--install-dir <dir>] [--repo <owner/repo>]

Examples:
  curl -fsSL https://raw.githubusercontent.com/huahaiy/xia/main/script/install.sh | sh
  curl -fsSL https://raw.githubusercontent.com/huahaiy/xia/main/script/install.sh | sh -s -- --version v0.1.0

Environment overrides:
  XIA_VERSION
  XIA_INSTALL_DIR
  XIA_GITHUB_REPO
EOF
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

fetch_text() {
  url="$1"
  if have_cmd curl; then
    curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "User-Agent: xia-installer" \
      "$url"
  elif have_cmd wget; then
    wget -qO- "$url"
  else
    printf >&2 'Missing required command: curl or wget\n'
    exit 1
  fi
}

download_file() {
  url="$1"
  dest="$2"
  if have_cmd curl; then
    curl -fsSL \
      -H "User-Agent: xia-installer" \
      "$url" \
      -o "$dest"
  elif have_cmd wget; then
    wget -qO "$dest" "$url"
  else
    printf >&2 'Missing required command: curl or wget\n'
    exit 1
  fi
}

download_optional_file() {
  url="$1"
  dest="$2"
  if have_cmd curl; then
    curl -fsSL \
      -H "User-Agent: xia-installer" \
      "$url" \
      -o "$dest" >/dev/null 2>&1
  elif have_cmd wget; then
    wget -qO "$dest" "$url" >/dev/null 2>&1
  else
    return 1
  fi
}

extract_zip() {
  archive="$1"
  dest="$2"
  if have_cmd unzip; then
    unzip -q "$archive" -d "$dest"
  elif have_cmd bsdtar; then
    bsdtar -xf "$archive" -C "$dest"
  elif have_cmd ditto; then
    ditto -x -k "$archive" "$dest"
  else
    printf >&2 'Missing required command to extract zip: unzip, bsdtar, or ditto\n'
    exit 1
  fi
}

sha256_file() {
  file="$1"
  if have_cmd sha256sum; then
    sha256sum "$file" | awk '{print $1}'
  elif have_cmd shasum; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    printf '%s' ""
  fi
}

resolve_target() {
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os" in
    Darwin)
      case "$arch" in
        arm64|aarch64) printf '%s' "macos-arm64" ;;
        x86_64|amd64)
          printf >&2 'Unsupported platform: macOS x86_64 is not published yet.\n'
          exit 1
          ;;
        *)
          printf >&2 'Unsupported macOS architecture: %s\n' "$arch"
          exit 1
          ;;
      esac
      ;;
    Linux)
      case "$arch" in
        x86_64|amd64) printf '%s' "linux-amd64" ;;
        arm64|aarch64) printf '%s' "linux-arm64" ;;
        *)
          printf >&2 'Unsupported Linux architecture: %s\n' "$arch"
          exit 1
          ;;
      esac
      ;;
    *)
      printf >&2 'Unsupported operating system: %s\n' "$os"
      exit 1
      ;;
  esac
}

resolve_version() {
  if [ "$VERSION" != "latest" ]; then
    printf '%s' "$VERSION"
    return 0
  fi
  release_json="$(fetch_text "https://api.github.com/repos/$REPO/releases/latest" | tr -d '\n')"
  release_tag="$(printf '%s' "$release_json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  if [ -z "$release_tag" ]; then
    printf >&2 'Failed to resolve the latest Xia release tag from GitHub.\n'
    exit 1
  fi
  printf '%s' "$release_tag"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --install-dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    --repo)
      REPO="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf >&2 'Unknown option: %s\n\n' "$1"
      usage >&2
      exit 1
      ;;
  esac
done

TARGET="$(resolve_target)"
RESOLVED_VERSION="$(resolve_version)"
ARCHIVE="xia-${RESOLVED_VERSION}-${TARGET}.zip"
ARCHIVE_URL="https://github.com/${REPO}/releases/download/${RESOLVED_VERSION}/${ARCHIVE}"
CHECKSUM_URL="${ARCHIVE_URL}.sha256"

TMPDIR_XIA="$(mktemp -d 2>/dev/null || mktemp -d -t xia-install)"
cleanup() {
  rm -rf "$TMPDIR_XIA"
}
trap cleanup EXIT INT TERM

ARCHIVE_PATH="${TMPDIR_XIA}/${ARCHIVE}"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"
EXTRACT_DIR="${TMPDIR_XIA}/extract"

printf 'Downloading Xia %s for %s...\n' "$RESOLVED_VERSION" "$TARGET"
download_file "$ARCHIVE_URL" "$ARCHIVE_PATH"

if download_optional_file "$CHECKSUM_URL" "$CHECKSUM_PATH"; then
  expected_hash="$(awk 'NR==1 {print $1}' "$CHECKSUM_PATH")"
  actual_hash="$(sha256_file "$ARCHIVE_PATH")"
  if [ -z "$actual_hash" ]; then
    printf 'Checksum file downloaded, but no SHA-256 tool is available. Skipping verification.\n'
  elif [ "$expected_hash" != "$actual_hash" ]; then
    printf >&2 'Checksum verification failed for %s\n' "$ARCHIVE"
    printf >&2 'Expected: %s\n' "$expected_hash"
    printf >&2 'Actual:   %s\n' "$actual_hash"
    exit 1
  else
    printf 'Verified archive checksum.\n'
  fi
else
  printf 'Checksum asset not found for %s. Skipping verification.\n' "$ARCHIVE"
fi

mkdir -p "$EXTRACT_DIR"
extract_zip "$ARCHIVE_PATH" "$EXTRACT_DIR"

BINARY_PATH="${EXTRACT_DIR}/xia-${RESOLVED_VERSION}-${TARGET}/xia"
if [ ! -f "$BINARY_PATH" ]; then
  BINARY_PATH="$(find "$EXTRACT_DIR" -type f -name xia 2>/dev/null | head -n 1 || true)"
fi
if [ ! -f "$BINARY_PATH" ]; then
  printf >&2 'Failed to locate the Xia binary in the downloaded archive.\n'
  exit 1
fi

mkdir -p "$INSTALL_DIR"
cp "$BINARY_PATH" "${INSTALL_DIR}/xia"
chmod 755 "${INSTALL_DIR}/xia"

printf 'Installed Xia to %s/xia\n' "$INSTALL_DIR"
if "${INSTALL_DIR}/xia" --help >/dev/null 2>&1; then
  printf 'Verified binary startup.\n'
fi

case ":$PATH:" in
  *":$INSTALL_DIR:"*)
    printf 'Xia is ready. Run: xia\n'
    ;;
  *)
    printf 'Xia is installed, but %s is not on PATH.\n' "$INSTALL_DIR"
    printf 'Add this line to your shell profile:\n'
    printf '  export PATH="%s:$PATH"\n' "$INSTALL_DIR"
    ;;
esac
