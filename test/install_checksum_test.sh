#!/bin/sh

set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
XIA_INSTALL_SOURCE_ONLY=1
export XIA_INSTALL_SOURCE_ONLY
. "$repo_root/script/install.sh"

test_root="$(mktemp -d 2>/dev/null || mktemp -d -t xia-install-test)"
cleanup() {
  rm -rf "$test_root"
}
trap cleanup EXIT INT TERM

archive_path="$test_root/xia-test.zip"
checksum_path="$archive_path.sha256"
archive_name="xia-test.zip"
printf '%s' 'xia installer checksum fixture' > "$archive_path"
actual_hash="$(sha256_file "$archive_path")"

printf '%s  %s\n' "$actual_hash" "$archive_name" > "$checksum_path"
verify_archive_checksum "$archive_path" "$checksum_path" "$archive_name"

uppercase_hash="$(printf '%s' "$actual_hash" | tr '[:lower:]' '[:upper:]')"
printf '%s  %s\n' "$uppercase_hash" "$archive_name" > "$checksum_path"
verify_archive_checksum "$archive_path" "$checksum_path" "$archive_name"

printf '%s\n' 'not-a-sha256' > "$checksum_path"
if verify_archive_checksum "$archive_path" "$checksum_path" "$archive_name" 2>/dev/null; then
  printf >&2 'Malformed checksum was accepted.\n'
  exit 1
fi

printf '%064d  %s\n' 0 "$archive_name" > "$checksum_path"
if verify_archive_checksum "$archive_path" "$checksum_path" "$archive_name" 2>/dev/null; then
  printf >&2 'Mismatched checksum was accepted.\n'
  exit 1
fi

rm -f "$checksum_path"
if verify_archive_checksum "$archive_path" "$checksum_path" "$archive_name" 2>/dev/null; then
  printf >&2 'Missing checksum was accepted.\n'
  exit 1
fi

(
  have_cmd() {
    return 1
  }
  if sha256_file "$archive_path" >/dev/null 2>&1; then
    printf >&2 'Missing SHA-256 implementation was accepted.\n'
    exit 1
  fi
)

printf 'Unix installer checksum tests passed.\n'
