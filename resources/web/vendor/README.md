# Vendored browser libraries

These files are checked into Xia so the privileged local UI never downloads
executable code at runtime.

- `marked-12.0.2.min.js`: Marked 12.0.2, MIT license, downloaded from the
  package's `marked.min.js` distribution artifact. SHA-256:
  `15fabce5b65898b32b03f5ed25e9f891a729ad4c0d6d877110a7744aa847a894`.
- `dompurify-3.1.7.min.js`: DOMPurify 3.1.7, dual Apache-2.0/MPL-2.0 license,
  downloaded from the package's `dist/purify.min.js` artifact. SHA-256:
  `6407576993a5aa1303eaf9fefb95e5cfc1c0c80645bd3717db671727e6b55b91`.

The upstream version and license notices are retained in each minified file.
When updating either library, update the versioned filename, `index.html`, and
the static asset allowlist together.
