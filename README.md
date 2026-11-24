# tramp-minio: MinIO object storage in Emacs TRAMP

*Author:* Alex Murray <murray.alex@gmail.com><br>
*Version:* 0.1.0 (experimental)<br>
*License:* GPL-3.0-or-later

`tramp-minio.el` adds a `minio` TRAMP method for browsing and editing
objects in MinIO and other S3-compatible object storage.  It drives an
existing MinIO client (`mc`) alias configured outside Emacs, so no
credentials live in Emacs, and no FUSE mount or remote shell is required.

> **NOTE:** This package is experimental.  This initial release is
> safety-oriented, not a verified stable release.  Reads are the default;
> writes require an explicit opt-in and have non-atomic semantics.  Read
> [Safety model](#safety-model) before enabling them.

## Features

- Browse buckets and prefixes with Dired, completion, `find-file`, and
  ordinary TRAMP file operations.
- Visit and revert objects while checking that the object did not change
  during the download.
- Copy objects to local storage, or between local and remote locations,
  through private local staging files.
- Keep reads read-only by default; enable uploads and bucket creation
  only when needed.
- Classify recognised `mc` failures without exposing raw client output,
  which may contain credentials.
- Cache listings and alias completion briefly, with explicit cache
  invalidation.

## Requirements

- Emacs 29.1 or later.
- The MinIO client (`mc`) available in `exec-path`.
- A POSIX `/bin/sh` for streaming transfers.
- An `mc` alias configured outside Emacs.  The alias endpoint must be a
  bare HTTP(S) URL; aliases with a path, credentials, query, or fragment
  are ignored.
- Read access to the bucket for listing and visiting.  Write access is
  optional and disabled by default.

## Installation

Install the file with `M-x package-install-file`, or put
`tramp-minio.el` on your `load-path`, then require it:

```elisp
(require 'tramp-minio)
```

Configure the alias with `mc` itself, outside Emacs:

```sh
mc alias set minio https://minio.example.net ACCESS_KEY SECRET_KEY
mc ls minio
```

Do not put access keys or secret keys in your Emacs configuration.

## Usage

The TRAMP filename syntax is:

```text
/minio:ALIAS:/BUCKET/KEY
```

`ALIAS` is the name of an `mc` alias, and everything after the second
colon is the bucket and object key path.  User names, ports, and
multi-hop filenames are not supported.

```elisp
(dired "/minio:minio:/")                  ; list buckets
(dired "/minio:minio:/bucket/")           ; list objects and prefixes
(find-file "/minio:minio:/bucket/file.txt")
```

At the host prompt, <kbd>TAB</kbd> completes the allowed aliases.  Dired
at the root lists buckets; Dired inside a bucket lists objects and common
prefixes.  Directories in Dired are virtual prefixes, not real S3
objects.

Saving a visited object, copying to a remote destination, and creating a
bucket are write operations.  They are disabled by default; see
[Safety model](#safety-model).

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `tramp-minio-program` | `"mc"` | MinIO client executable.  An absolute path is recommended. |
| `tramp-minio-arguments` | `nil` | Trusted global arguments passed to every `mc` invocation.  Use absolute configuration paths. |
| `tramp-minio-allowed-aliases` | `nil` | Alias names to allow.  `nil` allows every configured HTTP(S) alias. |
| `tramp-minio-read-only` | `t` | Disable all remote mutations when non-nil. |
| `tramp-minio-allow-overwrite` | `nil` | Accept non-atomic uploads, including overwrites that may replace object metadata. |
| `tramp-minio-metadata-timeout` | `30` | Timeout in seconds for metadata commands. |
| `tramp-minio-transfer-timeout` | `nil` | Timeout in seconds for transfers, or `nil` for no limit. |
| `tramp-minio-stat-arguments` | `'("--no-list")` | Arguments for `mc stat`.  Set to `nil` for older clients without `--no-list`. |
| `tramp-minio-cache-ttl` | `3` | Listing cache lifetime in seconds.  Zero disables listing caching. |
| `tramp-minio-alias-cache-ttl` | `30` | Alias completion cache lifetime in seconds. |
| `tramp-minio-debug` | `nil` | Log dispatched operation names.  Object names are not logged. |

A minimal read-only configuration:

```elisp
(require 'tramp-minio)
(setq tramp-minio-program "/usr/local/bin/mc"
      tramp-minio-allowed-aliases '("minio"))
```

Experimental write configuration:

```elisp
(setq tramp-minio-read-only nil
      tramp-minio-allow-overwrite t)
```

Read the safety notes below before enabling writes.  Use a separate `mc`
configuration and an account restricted to a disposable bucket for
testing.

## Safety model

### Read-only by default

`tramp-minio-read-only` is `t` by default.  Listing, completion,
visiting, reverting, and copying objects out to local storage work in
this mode.  Saving a buffer, copying to a remote destination, and
creating a bucket signal an error.

### Writes are non-atomic

Uploads use `mc pipe`.  The client cannot conditionally create or
replace an object, so `tramp-minio-allow-overwrite` must be non-nil even
for a new key: a concurrent writer can create the key after this package
checks that it is absent.  A timed-out upload may still have succeeded.

Overwriting an object can replace its metadata, including content type,
tags, encryption, and storage class.  Remote-to-remote copies stage the
bytes locally and do not preserve metadata.  Do not enable writes for
important objects without backups or server-side versioning.

Revert the buffer after a successful upload before editing it again.  A
later stat cannot prove that the object contains the bytes just uploaded,
so the package does not claim that it does.  Backups and auto-save are
inhibited on visited buffers, and there is no remote locking.

### Deliberately unsupported operations

Rename, deletion, recursive deletion, recursive copying, symbolic and
hard links, permission and ACL changes, timestamp changes, remote
processes or shells, remote Lisp loading, file notifications, append,
exclusive creation, partial visiting, and non-MinIO remote hops are
unsupported.  Rename and deletion were removed because `mc` cannot
provide the required conditional guarantees; use `mc` directly for those
tasks.

### Identity, paths, and permissions

- File attributes and permissions are synthetic.  IAM policy is
  authoritative.
- File identity is a synthetic, alias-scoped value, not a globally
  unique S3 identity.  Different aliases are not deduplicated.
- Empty virtual directories are not representable.
- Listings containing both an object and a prefix with the same name are
  rejected as ambiguous.
- Names containing newlines or repeated slashes are unsupported.  `.`
  and `..` have their ordinary filesystem navigation meanings, not
  literal S3 key meanings.
- Some unusual filenames and quoted filenames may not work.

### Client configuration is not a sandbox

Alias information is validated against HTTP(S) endpoints, and only the
alias name and URL are retained for completion.  Each invocation uses a
private working directory so that a missing alias cannot silently access
files in the user's ordinary working directory.  Local output is checked
and rejected.  The alias configuration is not transactionally pinned,
however; do not reconfigure aliases during an operation.  This package
is not a security sandbox.  Use a separate restricted `mc` configuration
for sensitive deployments.

## Client behaviour and caching

- The executable is checked with `mc --version` before use.
- `tramp-minio-arguments` are trusted global arguments, not a place for
  per-operation options.  Use absolute paths.
- Metadata commands time out after `tramp-minio-metadata-timeout`
  seconds.  Transfers are unlimited by default unless
  `tramp-minio-transfer-timeout` is set.  Emacs waits synchronously, but
  subprocess waits are interruptible.  Cleanup happens outside the
  subprocess timeout.
- Transfers stream through private local staging files.  Directory
  listings and objects opened for editing still consume memory in Emacs.
- Listings are cached for `tramp-minio-cache-ttl` seconds and alias
  completion for `tramp-minio-alias-cache-ttl` seconds.
  `M-x tramp-minio-clear-cache` clears both caches, and refreshing Dired
  clears the listing cache.  Alias validation and object modification
  checks are always fresh.  The listing cache is bounded and flushed
  after mutation attempts.
- Raw `mc` error output is withheld because it may contain secrets.
  Access-denied and missing-object failures are classified when the
  client supplies structured codes; other failures are reported
  generically.

## Troubleshooting

### `MinIO executable not found`

Set `tramp-minio-program` to an absolute path, or add the directory
containing `mc` to `exec-path`.

### `Unknown or disallowed mc alias`

Check that the alias exists and points at a bare HTTP(S) endpoint:

```sh
mc alias list
```

If `tramp-minio-allowed-aliases` is non-nil, the alias must also appear
in that list.

### `Invalid MinIO stat schema` or unsupported `--no-list`

Older `mc` releases may not support `mc stat --no-list`.  Restore the
ordinary stat command:

```elisp
(setq tramp-minio-stat-arguments nil)
```

Ordinary stat may require listing permissions that `--no-list` avoids.

### `MinIO is read-only`

Writes are disabled.  Enable them only after reading
[Safety model](#safety-model):

```elisp
(setq tramp-minio-read-only nil
      tramp-minio-allow-overwrite t)
```

### `Revert MinIO buffer before another save`

The buffer was uploaded or otherwise needs a fresh object identity.
Revert it, then edit and save again.

### `MinIO object changed during download; retry`

The object changed while it was being read.  Retry the download.

### `Unsupported MinIO path` or `Repeated slashes in MinIO keys are unsupported`

Use a normal absolute key path without a user name, port, multi-hop
syntax, newline, or repeated slash.  Do not use this backend to address
S3 keys with non-canonical components.

### `Unexpected local output from mc; operation rejected`

The client ran in its private working directory but still produced
unexpected local files.  Check the alias and `mc` configuration.

### Access denied or TLS failures

Validate the alias outside Emacs with `mc ls`.  Raw client diagnostics
are not shown, so check the endpoint, credentials, IAM policy, and
certificate trust with `mc` directly.

### `Operation not supported by MinIO object storage`

The requested TRAMP operation is intentionally unsupported.  Use `mc`
directly for rename, deletion, permission changes, and other operations
listed in [Safety model](#safety-model).

### Debug logging

Set `tramp-minio-debug` to log dispatched operation names.  Object names
are not logged.

## Testing

Byte-compile with warnings treated as errors:

```sh
emacs -Q --batch -L . \
  --eval '(setq byte-compile-error-on-warn t)' \
  -f batch-byte-compile tramp-minio.el
```

Run the fixture test suite:

```sh
TRAMP_MINIO_FAKE_MC="$PWD/tests/fake-mc" \
  emacs -Q --batch -L . -l tests/tramp-minio-tests.el \
  -f ert-run-tests-batch-and-exit
```

The fake executable is a synthetic protocol fixture, not recorded server
output.  An optional live read-only smoke test can read an existing
object without writing:

```sh
TRAMP_MINIO_TEST_OBJECT=/minio:minio:/bucket/file.txt \
  emacs -Q --batch -L . -l tests/tramp-minio-tests.el \
  -f ert-run-tests-batch-and-exit
```

CI runs byte compilation and the fixture suite on Emacs 29.4, 30.2, and
snapshot.  The fixture suite covers Dired, file visitation, saving and
reverting existing and new objects, error classification, caching, and
interrupted transfers.  Emacs 31.1 also passed the fixture suite and a
live read-only smoke test against MinIO Play using
`mc RELEASE.2025-08-13T08-35-41Z`.  With writes enabled in a disposable
bucket, that check also created and overwrote a synthetic object, and
both revert and an independent `mc` download confirmed the bytes.

Other Emacs and TRAMP versions, compression, large objects, and
restricted IAM policies still need runtime verification.  The `mc` JSON
response schemas and structured error codes also need verification
against the full range of client releases.

## License

GPL-3.0-or-later.  See [COPYING](COPYING).

This file is not part of GNU Emacs.  Bug reports and patches are
welcome; please keep the read-only default and the safety checks intact.
