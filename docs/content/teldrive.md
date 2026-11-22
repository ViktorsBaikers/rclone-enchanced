---
title: "TelDrive"
description: "Rclone docs for TelDrive"
versionIntroduced: "v1.69"
---

# TelDrive

TelDrive is a self-hosted file storage solution that uses Telegram
channels as the underlying storage backend. Files are split into chunks,
optionally encrypted, and uploaded as messages in a designated Telegram
channel or group. The TelDrive API server indexes and manages the file
metadata, providing a file-system-like interface over Telegram's storage.

Paths are specified as `remote:path`.

To use TelDrive, you need:

- A running TelDrive server (self-hosted)
- An API key for authentication

## Filesystem behavior

TelDrive uses case-sensitive names, matching the normal behavior of Linux
filesystems. For example, `Photo.jpg` and `photo.jpg` are distinct files in
the same directory.

Directory listings are delivered page by page. Failed multipart uploads can reuse
compatible open TelDrive upload sessions. Destination, exact-case name, size,
MIME type, modification time, encryption mode, conflict policy, and any expected
hash select a candidate session; they do not prove source-byte equality.

Before reusing a stored part, rclone reads the current bytes and compares their
length and checksum with the stored proof. Equal bytes avoid another upload;
different bytes fail before completion. Missing proof and legacy unaligned
sessions cannot be safely resumed. Unknown-size uploads must prove actual EOF
and visit every stored part before completion.

Upgrade this backend and the TelDrive service together. Sessions must advertise
`requiresCompletionManifest: true`; there is no fallback to older servers.
Completion sends the exact plaintext size and current BLAKE3 root. Disabling
`hash_enabled` changes advertised hashing, not the required upload proof.
Servers that omit required part checksums cause uploads to fail closed.

For a true local source, completion also requires an independent fresh read
matching the uploaded bytes and the initial source metadata. A detected change
fails before completion, so Move cannot treat that attempt as a successful copy.
This is not an atomic filesystem snapshot or compare-and-unlink guarantee:
another writer can still change the source after final proof and before Move
deletes it. Do not modify source files while moving them.

An HTTP 409 response with the decoded code `upload_parent_unavailable` is
fatal, including when the service cannot prove a legacy session's destination.
That failed upload does not authorize source deletion or publication over the
old destination. Other conflict codes retain their existing error policy.
Interrupted error responses retain their transport cause for normal retry
handling; undecodable responses are not treated as decoded parent failures.

This decoded-error protection alone does not establish command-wide retry
identity or guard discovery paths that can skip uploading an unchanged file.
Those lifecycle boundaries require separate command-selection, discovery, and
actual CLI verification; backend upload tests do not prove them.

### Upload admission and retry lifetime

The lifecycle candidate binds one original source and primary requested target
before transforms, destination overrides, comparison substitution or stream
wrapping. Ordinary Copy and all three CompareOrCopyDest callers (single-file,
sync pair checker and source-only callback) carry that same intent. Neither an
inverse transform nor a comparison filename reconstructs user intent.

Logical identity and transport identity are separate. The original filesystem,
root and source path select the attempt; the requested primary target remains
unchanged when its effective spelling changes. A server copy still sends the
actual comparison FileID. Manual uploads still freeze the actual body size,
modtime, MIME type, geometry, encryption and destination request. Logical
unknown size does not replace the measured buffered or spooled body size.

One actual retrying CLI command retains this selection across low-level and
outer retries, including fresh filesystem and object values. Its private key,
frozen request and selected session are not user-supplied settings or authority.

After selection, rclone retrieves that exact session in every state. A lost
create acknowledgement repeats the same frozen request and key. Missing,
expired, terminal or undecodable outcomes do not select another session or
re-resolve the parent. A compatible session found during initial resume is
selected before its exact authorized lookup and before reading parts. A legacy
session with an unprovable destination is unavailable, including a genuinely
old root upload; an automatic retry cannot turn it into new root intent.

Completed upload replay still reads current source bytes, verifies stored part
proof and EOF, and submits the original completion manifest with current result
authorization. An ordinary same-key copy replay sends its frozen comparison
FileID, payload and key; the service returns the original operation or currently
authorized result without another remote copy. Changed logical source,
comparison transport or frozen wire request conflicts rather than allocating a
replacement. Carrier-bearing copy success also verifies full current original
source/result bytes and the current result identity, not just metadata.

Copy preserves original intent through server/manual fallback, Update,
Put/PutUnchecked/PutStream, chunk writers and Rcat. A positively unadmitted
ErrorCantCopy keeps its normal manual fallback. Once dispatch has admitted work,
an unresolved or failed outcome stops fallback, including range and stream
paths. A marked comparison failure cannot enqueue an upload, rename, ordered
source deletion, FixCase or backup operation. Checking accounting retains the error.
Ordinary unmarked comparison failures retain their existing useful fallback;
NoCheckDest, CompareDest and CopyDest are not disabled or reinterpreted.

Destination-role NewObject, paged ListP and recursive ListR reconcile both
requested and effective targets before equality can skip work. Source reads
remain source reads. Enumeration respects the selected source root and filters;
an excluded or unrelated sibling is not blocked merely for sharing a directory.
Missing, denied, live, expired, mismatched or undecodable proof fails closed.
A copy has no read-only exact-outcome route: discovery refuses an unknown copy
outcome instead of sending an allocation-capable POST as a lookup. Restored
size/modtime equality is never current byte proof or source-deletion permission.

Directory creation and backend Move/DirMove refuse affected same-scope
admissions before path or directory-cache preparation, destination deletion or
move RPC. This includes a fresh-Fs directory shortcut after an earlier
ErrorDirExists forced per-file Copy. Case-change temporary hops and backup
moves keep the original primary carrier, not a temporary source identity.
Unresolved create/copy and metadata-only completed results cannot authorize
subtree movement. Unrelated subtrees, ordinary unadmitted moves and genuinely
new explicit invocations remain useful. Restore never revives a losing attempt.

The candidate VFS cache journals each explicit dirty generation, request key,
frozen create body and selected upload before POST or part transfer. It replaces
private metadata through a synced same-directory temporary file and rename.
Retry, reopen, queue expiry, Reset and process restart retain that journal.
Reset still skips dirty files. Only an actual new write/truncation/create or a
changed-target user rename can rotate it, after the old writer is canceled.
Failed persistence or a stale writer cannot acknowledge the generation as clean.
Successful cache writes persist their present ranges, sparse-zero ranges and logical size
before returning, including later writes in the same unfrozen generation. A metadata-save
failure returns a write error and keeps the item dirty; subsequent mutation first repairs
that persistence failure.

Legacy dirty entries without a trustworthy journal, invalid admission metadata
and unknown legacy destinations remain dirty and unavailable; their bytes are
retained. Do not delete the cache to retry, or use an older binary that ignores
the journal as rollback. Forward recovery requires admission-aware code.
This provides process-restart continuity, not power-loss or Windows execution proof.
The optional journal does not become a requirement for unrelated backends.

VFS writeback reaches direct operations.Copy; it does not gain comparison
flags or a copy-DTO journal. Dirty-generation evidence outlives command and job
identities. Keep both dirty bytes and metadata during forward recovery.

An actual RC job owns its ephemeral admissions, independent of caller `_group`.
Copied contexts preserve the original private job lookup, but HTTP and
cancellation use the current context. Admission checks the actual job's live state before
scope lookup and after finish-listener registration/readiness. A newer job finishing cannot
exclude an older live job. Finish listeners prune exact scopes; completed or expired actual
jobs cannot acquire replacement ownership. A new submitted job is new intent, except that
recovered VFS evidence always wins. Marker-only jobless calls remain independent.
No RC job restart persistence is added.

Raw buffered/spooled Rcat owns one transient scope around its existing inner
Retry, after a rewindable buffer or spool exists. Actual rcat uses
cmd.Run(false) and Background, not CLI outer retries. Every retrying Put
inherits the same scope, key and session; independent calls are distinct, and
return/cancellation releases the scope and its indexes. Nonbuffered PutStream
remains one-shot, and ordinary transient/manual stream transfers remain useful.
No crash replay is promised after stdin or another raw stream disappears.

An admission-aware Background Copy without a durable, job, command or inherited
transient owner also owns one bounded transient lifetime for that call. Its
post-failure checks run before release. Scope precedence is durable dirty
generation, actual RC job, effective CLI command, inherited transient scope,
then this one-call lifetime. An RC marker or caller `_group` alone is not a job.

Channel allocation compensation retains orphaned or uncertain created channels
instead of risking committed or foreign bytes. This does not disable legitimate
explicit administrative Delete; current selection, references, reservations and
physical deletion intents still govern that separate operation.

This coordinated candidate has partial isolated runtime proof, not deployment approval.
Missing genuine legacy provenance and open proof obligations still block acceptance.
Source-authored fixtures alone cannot prove generated HTTP/SQL, actual CLI retries,
durable cache restart, Windows execution or live recovery.
There is no new setting, HTTP payload or public recovery API here.
Keep write-back at 24h; no retry lifetime changes that policy.

The provenance-free Go `fs.HashSetter`, local `Object.SetHash`, and wrapper
`OverrideRemote.SetHash` APIs have been removed. External Go callers using these
APIs must migrate; transfer digests are no longer injected into source caches.

The file tree root is BLAKE3 over ordered 16 MiB block digests. For an empty
file, the root is BLAKE3 of the empty digest sequence.

Deleting a file or directory moves it to TelDrive trash. Use
`rclone cleanup remote:` to permanently purge all top-level trashed entries
under the configured remote root. Individual entries can be managed by ID:

```console
rclone backend trash-list remote:
rclone backend restore remote: FILE_ID
rclone backend purge remote: FILE_ID
```

## Configuration

Here is an example of making a remote for TelDrive.

First run:

```console
rclone config
```

This will guide you through an interactive setup process:

```text
No remotes found, make a new one?
n) New remote
d) Delete remote
q) Quit config
n/d/q> n

Enter name for new remote.
name> remote

Option Storage.
Type of storage to configure.
Choose a number from below, or type in your own value.
XX / TelDrive
   \ (teldrive)
Storage> XX

Option api_host.
URL of the TelDrive API server.
Enter a value.
api_host> https://teldrive.example.com

Option api_key.
API key for authentication.
Enter a value.
api_key> YOUR_API_KEY

Configuration complete.
Options:
- type: teldrive
- api_host: https://teldrive.example.com
- api_key: XXXXXXXXXXX
Keep this "remote" remote?
y) Yes this is OK (default)
e) Edit this remote
d) Delete this remote
y/e/d> y
```

<!-- autogenerated options start - DO NOT EDIT - instead edit fs.RegInfo in backend/teldrive/teldrive.go and run make backenddocs to verify --> <!-- markdownlint-disable-line line-length -->
### Standard options

Here are the Standard options specific to teldrive (Tel Drive).

#### --teldrive-api-key

API key for authentication with the TelDrive server.

This is the API token used to authenticate requests. Obtain it from
your TelDrive dashboard or server admin.


Properties:

- Config:      api_key
- Env Var:     RCLONE_TELDRIVE_API_KEY
- Type:        string
- Required:    false

#### --teldrive-api-host

URL of the TelDrive API server.

The base URL for the TelDrive API endpoint (e.g. https://teldrive.example.com).
This is required for all operations.


Properties:

- Config:      api_host
- Env Var:     RCLONE_TELDRIVE_API_HOST
- Type:        string
- Required:    false

#### --teldrive-chunk-size

Upload chunk size.

Files larger than this will be uploaded in multiple chunks. The chunk size
is automatically aligned to the nearest 16 MiB multiple (the BLAKE3 tree
hash block size) for optimal hashing performance.

Larger chunk sizes reduce the number of API calls but use more memory
during uploads (one chunk is buffered per concurrent upload stream).
Minimum: 64 MiB, Maximum: 2000 MiB.


Properties:

- Config:      chunk_size
- Env Var:     RCLONE_TELDRIVE_CHUNK_SIZE
- Type:        SizeSuffix
- Default:     512Mi

#### --teldrive-link-password

Password to set on created public links.

When set, all public links created via the link command will require
this password to access. If not set, links will be publicly accessible.


Properties:

- Config:      link_password
- Env Var:     RCLONE_TELDRIVE_LINK_PASSWORD
- Type:        string
- Required:    false

#### --teldrive-page-size

Number of items to return per page when listing files.

Controls the page size for directory listing API calls. Larger values
reduce the number of API requests needed for directories with many files,
but may increase response time and memory usage for each request.


Properties:

- Config:      page_size
- Env Var:     RCLONE_TELDRIVE_PAGE_SIZE
- Type:        int
- Default:     200

#### --teldrive-upload-host

Optional alternative URL for upload API calls.

If set, upload chunk requests will be sent to this host instead of
the main api_host. Useful for load-balancing or directing upload
traffic to a different server or CDN endpoint.


Properties:

- Config:      upload_host
- Env Var:     RCLONE_TELDRIVE_UPLOAD_HOST
- Type:        string
- Required:    false

#### --teldrive-encrypt-files

Enable native TelDrive encryption for stored files.

When enabled, files will be encrypted at rest using TelDrive's
built-in encryption before being sent to Telegram. The encryption
keys are managed by the TelDrive server.


Properties:

- Config:      encrypt_files
- Env Var:     RCLONE_TELDRIVE_ENCRYPT_FILES
- Type:        bool
- Default:     false

#### --teldrive-hash-enabled

Advertise BLAKE3 tree hashes for file integrity verification.

Files are split into 16 MiB blocks, each hashed with BLAKE3, then
the block hashes are combined into a final tree hash. This allows
rclone to verify upload integrity and detect modifications.

Disabling this option does not disable current-byte resume comparisons,
verified part acknowledgements, or the required completion manifest.
Uploads require a server supporting the completion manifest protocol.


Properties:

- Config:      hash_enabled
- Env Var:     RCLONE_TELDRIVE_HASH_ENABLED
- Type:        bool
- Default:     true

### Advanced options

Here are the Advanced options specific to teldrive (Tel Drive).

#### --teldrive-upload-concurrency

Number of chunks to upload in parallel per file.

Higher concurrency can speed up large file uploads at the cost of
more memory usage and potential rate-limiting. Each concurrent upload
buffers one chunk in memory.


Properties:

- Config:      upload_concurrency
- Env Var:     RCLONE_TELDRIVE_UPLOAD_CONCURRENCY
- Type:        int
- Default:     4

#### --teldrive-channel-id

Telegram channel ID to store uploaded chunks in.

When set, uploads go to this channel instead of the channel currently
selected on the TelDrive server, without changing the selection for
other clients. The channel must already be registered with your
TelDrive account. Uploads fail rather than rolling over to another
channel when this channel is full or unavailable.

Leave at 0 to use the server's selected channel. Requires a TelDrive
server that supports the channelId upload parameter.


Properties:

- Config:      channel_id
- Env Var:     RCLONE_TELDRIVE_CHANNEL_ID
- Type:        int
- Default:     0

#### --teldrive-encoding

The encoding for the backend.

See the [encoding section in the overview](/overview/#encoding) for more info.

Properties:

- Config:      encoding
- Env Var:     RCLONE_TELDRIVE_ENCODING
- Type:        Encoding
- Default:     Slash,BackSlash,Del,Ctl,LeftSpace,RightSpace,InvalidUtf8,Dot

#### --teldrive-description

Description of the remote.

Properties:

- Config:      description
- Env Var:     RCLONE_TELDRIVE_DESCRIPTION
- Type:        string
- Required:    false

## Backend commands

Here are the commands specific to the teldrive backend.

Run them with:

```console
rclone backend COMMAND remote:
```

The help below will explain what arguments each command takes.

See the [backend](/commands/rclone_backend/) command for more
info on how to pass options and arguments.

These can be run on a running backend using the rc command
[backend/command](/rc/#backend-command).

### trash-list

List top-level trashed files and directories.

```console
rclone backend trash-list remote: [options] [<arguments>+]
```

Lists top-level trashed entries reachable from the configured remote root.

The result is returned as TelDrive file metadata including each entry ID.

### restore

Restore a trashed file or directory by ID.

```console
rclone backend restore remote: [options] [<arguments>+]
```

Restores one trashed file or directory.

Usage:

    rclone backend restore teldrive: FILE_ID

### purge

Permanently purge a trashed file or directory by ID.

```console
rclone backend purge remote: [options] [<arguments>+]
```

Schedules permanent deletion of one trashed file or directory and its subtree.

Usage:

    rclone backend purge teldrive: FILE_ID

<!-- autogenerated options stop -->

## Offline quarantine of held cache state

A guarded write that conflicts with an authoritative change is held: its bytes stay readable from the mount, it never uploads, and the mount refuses `unlink`, `rename` and `rmdir` on it. Normal resolution is to fix the destination and let the guarded generation publish. When a held generation cannot be resolved that way, an operator may separate the whole dedicated cache root while the client is stopped.

Quarantine is explicit, offline and one-shot. It runs only outside a live client, and it never deletes: it copies the held bytes and a witness manifest into a quarantine directory, verifies every copy against that manifest, then moves the cache data and metadata trees together in a single rename on the same filesystem.

Preconditions, all checked before anything is written:

- the cache root is a dedicated private directory owned by you, mode `0700`, holding exactly `vfs` and `vfsMeta`;
- no client holds the cache root (a running client takes a shared lock on it, so quarantine refuses);
- every held item has a complete byte proof, and the publication outcome is declared and matches the journal;
- the quarantine target does not exist and lives on the same filesystem as the cache root.

If any precondition fails, quarantine changes nothing and reports why. There is no discard path: after quarantine, the separated state is retained for inspection and any destruction is a separate, manual decision outside rclone.

```text
quarantine target = <same filesystem as the dedicated cache root>
layout:
  manifest.json          per-item path, generation, reason, admission key,
                         session id, size, sha256, proven ranges,
                         publication outcome, receipt if any
  held/<remote path>     verified copy of each held generation
  state/                 the original cache root, moved in one rename
```

## Limitations

Invalid UTF-8 bytes will be replaced as they can't be used in JSON
strings. Filenames containing certain special characters may be encoded
as configured by the `--teldrive-encoding` option.
