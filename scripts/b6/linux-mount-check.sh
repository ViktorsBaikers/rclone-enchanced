#!/usr/bin/env bash
# Prove the native Linux rclone mount against a disposable TelDrive.
#
# Isolation contract: the mountpoint, VFS cache, rclone config, API key, and
# server containers all live under the private b6 state root. The production
# mount is never inspected, replaced, or unmounted, and --vfs-write-back 24h
# is kept unchanged (immediate test sending uses `rclone move`).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
# shellcheck source=../../../teldrive-enchanced/scripts/b6/lib.sh
source "$ROOT_DIR/teldrive-enchanced/scripts/b6/lib.sh"

export LC_ALL=C
umask 077

ALL_SCENES=(empty unicode case rename)
SNAPSHOT=""
RECEIPT=""
IMAGE=""
OUT=""
KEEP=false
SERVER_URL=""
API_KEY_FILE=""
SCENES=()

LIMIT_SECONDS=180
LIMIT_BYTES=8589934592
START=$SECONDS
BYTES_READ=0
BYTES_WRITTEN=0
LIMIT_STATE="none"
REASON=""
RECEIPT_WRITTEN=false
RUNTIME=""
WORK=""
ROWS=""
CACHE=""
MOUNT=""
MOUNT_PID=""
RCLONE_BIN=""
CONF=""
BASE_URL=""
API_SECRET=""
PG_NAME=""
TD_NAME=""
NETWORK=""
RUN_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

usage() {
  cat >&2 <<'EOF'
usage: linux-mount-check.sh [options]
Runs a native rclone mount matrix against a disposable TelDrive and writes a
private per-scene receipt. Fails closed when the plan 07-05 receipt does not
bind the supplied snapshot and image.
  --snapshot DIR        sealed snapshot (default: receipt snapshot_path)
  --receipt PATH        plan 07-05 receipt
                        (default: $B6_PRIVATE_DIR/linux-docker-receipt.json)
  --image TAG           sealed TelDrive image (default: receipt tag)
  --out DIR             receipt directory (default: <receipt dir>/mount)
  --scene NAME          repeatable; any of: empty unicode case rename
  --server-url URL      existing disposable server (skips container start)
  --api-key-file PATH   API key for --server-url
  --keep                keep the private work root
EOF
  exit 2
}

log() { printf 'b6-mount: %s\n' "$*" >&2; }

fail() {
  REASON="$1"
  printf 'b6-mount: %s\n' "$1" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) [[ $# -ge 2 ]] || usage; SNAPSHOT="$2"; shift 2 ;;
    --receipt) [[ $# -ge 2 ]] || usage; RECEIPT="$2"; shift 2 ;;
    --image) [[ $# -ge 2 ]] || usage; IMAGE="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --scene) [[ $# -ge 2 ]] || usage; SCENES+=("$2"); shift 2 ;;
    --server-url) [[ $# -ge 2 ]] || usage; SERVER_URL="$2"; shift 2 ;;
    --api-key-file) [[ $# -ge 2 ]] || usage; API_KEY_FILE="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

if [[ ${#SCENES[@]} -eq 0 ]]; then
  SCENES=("${ALL_SCENES[@]}")
fi
for scene in "${SCENES[@]}"; do
  found=false
  for known in "${ALL_SCENES[@]}"; do [[ "$scene" == "$known" ]] && found=true; done
  [[ "$found" == true ]] || usage
done

# A production variable in the caller environment could silently redirect this
# run. Refuse before creating anything.
if inherited="$(env | cut -d= -f1 | grep -E '^TELDRIVE_' || true)" && [[ -n "$inherited" ]]; then
  printf 'b6-mount: refusing to run with TelDrive variables present:\n%s\n' "$inherited" >&2
  exit 1
fi

PRIV="$(b6_private_root)"
# --receipt names the plan 07-05 input receipt; --out is this plan's output
# directory and always receives receipt.json.
[[ -n "$RECEIPT" ]] || RECEIPT="$PRIV/linux-docker-receipt.json"
[[ -n "$OUT" ]] || OUT="$(dirname -- "$RECEIPT")/mount"
install -d -m 0700 "$PRIV" "$OUT"
RECEIPT_OUT="$OUT/receipt.json"
[[ "$RECEIPT_OUT" != "$RECEIPT" ]] || fail "--out would overwrite the plan 07-05 receipt"

# --- Fail-closed precondition: the plan 07-05 receipt binds this run --------
[[ -s "$RECEIPT" ]] || fail "plan 07-05 receipt missing"
RECEIPT_OUTCOME="$(jq -er '.outcome' "$RECEIPT")" || fail "receipt has no outcome"
[[ "$RECEIPT_OUTCOME" == PASS ]] || fail "plan 07-05 outcome is $RECEIPT_OUTCOME, not PASS"
SNAP_FROM_RECEIPT="$(jq -er '.snapshot_path' "$RECEIPT")" || fail "receipt has no snapshot_path"
[[ -n "$SNAPSHOT" ]] || SNAPSHOT="$SNAP_FROM_RECEIPT"
SNAP="$(cd -- "$SNAPSHOT" 2>/dev/null && pwd)" || fail "snapshot directory not found: $SNAPSHOT"
SNAP_RECEIPT_ABS="$(cd -- "$SNAP_FROM_RECEIPT" 2>/dev/null && pwd)"
[[ "$SNAP" == "$SNAP_RECEIPT_ABS" ]] || fail "snapshot argument does not equal receipt snapshot_path"
[[ -f "$SNAP/snapshot.sha256" ]] || fail "snapshot manifest missing: snapshot.sha256"
[[ -n "$IMAGE" ]] || IMAGE="$(jq -er '.server_image_tag' "$RECEIPT")" || fail "receipt has no server_image_tag"
RECEIPT_IMAGE_TAG="$(jq -er '.server_image_tag' "$RECEIPT")" || fail "receipt has no server_image_tag"
[[ "$IMAGE" == "$RECEIPT_IMAGE_TAG" ]] || fail "image argument does not equal receipt server_image_tag"
SNAP_SEAL_SHA="$(sha256sum "$SNAP/snapshot.sha256" | cut -d' ' -f1)"
RECEIPT_SNAP_SHA="$(jq -er '.snapshot_sha256' "$RECEIPT")" || fail "receipt has no snapshot_sha256"
[[ "$SNAP_SEAL_SHA" == "$RECEIPT_SNAP_SHA" ]] ||
  fail "snapshot seal digest does not match the receipt"
SNAP_DIGEST="$(cat "$SNAP/snapshot.sha256")"
[[ "$SNAP_DIGEST" =~ ^[0-9a-f]{64}$ ]] || fail "snapshot digest is not a sha256"
RECEIPT_IMAGE_DIGEST="$(jq -er '.server_image_digest' "$RECEIPT")" || fail "receipt has no server_image_digest"

if [[ -z "$SERVER_URL" ]]; then
  RUNTIME="$(b6_runtime)"
  IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null)" ||
    fail "image not found: $IMAGE"
  [[ "$IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "image ID is not a sha256 digest"
  [[ "$IMAGE_ID" == "$RECEIPT_IMAGE_DIGEST" ]] ||
    fail "image digest does not match the receipt"
else
  IMAGE_ID="$RECEIPT_IMAGE_DIGEST"
  [[ -n "$API_KEY_FILE" && -s "$API_KEY_FILE" ]] || fail "--server-url requires --api-key-file"
fi

# --- Missing facilities are a recorded NOT-RUN, never a silent skip ---------
if [[ ! -e /dev/fuse ]]; then
  jq -n --arg snapshot_path "$SNAP" --arg snapshot_sha256 "$SNAP_SEAL_SHA" \
    --arg snapshot_digest "$SNAP_DIGEST" --arg image "$IMAGE" --arg image_id "$IMAGE_ID" \
    --arg reason "missing /dev/fuse" \
    '{plan:"07-06", outcome:"NOT-RUN", reason:$reason, facility:"fuse",
      snapshot_path:$snapshot_path, snapshot_sha256:$snapshot_sha256,
      snapshot_digest:$snapshot_digest, server_image_tag:$image,
      server_image_digest:$image_id, scenes:[], impact_limits:{seconds:180,bytes:8589934592},
      limit_state:"none", production_touched:false}' > "$RECEIPT_OUT"
  chmod 600 "$RECEIPT_OUT"
  log "NOT-RUN: missing /dev/fuse"
  exit 2
fi

if ! command -v fusermount3 >/dev/null 2>&1; then
  jq -n --arg snapshot_path "$SNAP" --arg snapshot_sha256 "$SNAP_SEAL_SHA" \
    --arg snapshot_digest "$SNAP_DIGEST" --arg image "$IMAGE" --arg image_id "$IMAGE_ID" \
    --arg reason "missing fusermount3" \
    '{plan:"07-06", outcome:"NOT-RUN", reason:$reason, facility:"fusermount3",
      snapshot_path:$snapshot_path, snapshot_sha256:$snapshot_sha256,
      snapshot_digest:$snapshot_digest, server_image_tag:$image,
      server_image_digest:$image_id, scenes:[], impact_limits:{seconds:180,bytes:8589934592},
      limit_state:"none", production_touched:false}' > "$RECEIPT_OUT"
  chmod 600 "$RECEIPT_OUT"
  log "NOT-RUN: missing fusermount3"
  exit 2
fi

# --- rclone artifact built from the same sealed snapshot -------------------
BUILD_WORK="$(dirname -- "$SNAP")/work-$(basename -- "$SNAP")"
RCLONE_BIN="$BUILD_WORK/out1/rclone"
BUILD_RECEIPT="$BUILD_WORK/receipt.json"
if [[ ! -x "$RCLONE_BIN" ]]; then
  log "building rclone from the sealed snapshot"
  "$ROOT_DIR/teldrive-enchanced/scripts/b6/build-repro.sh" --snapshot "$SNAP" --native rclone ||
    fail "reproducible rclone build failed"
fi
[[ -x "$RCLONE_BIN" ]] || fail "rclone artifact not found at $RCLONE_BIN"
[[ -s "$BUILD_RECEIPT" ]] || fail "build receipt not found at $BUILD_RECEIPT"
RCLONE_ARTIFACT_SHA="$(sha256sum "$RCLONE_BIN" | cut -d' ' -f1)"
RECEIPT_ARTIFACT_SHA="$(jq -er '.artifacts["rclone-enchanced"].artifact_sha256_run1' "$BUILD_RECEIPT")" ||
  fail "build receipt has no rclone artifact digest"
[[ "$RCLONE_ARTIFACT_SHA" == "$RECEIPT_ARTIFACT_SHA" ]] ||
  fail "rclone artifact digest does not match the build receipt"
GO_VERSION="$(jq -er '.go_version' "$BUILD_RECEIPT")" || GO_VERSION="$(go version | awk '{print $3}')"
RCLONE_VERSION="$("$RCLONE_BIN" version | head -n 1)"
RCLONE_UPSTREAM="$(jq -r '.metadata.upstream_base // empty' "$BUILD_RECEIPT")"
"$RCLONE_BIN" version | grep -qx -- "- build/snapshot: $SNAP_DIGEST" ||
  fail "rclone artifact does not report the sealed snapshot digest"

# --- helpers ---------------------------------------------------------------
hash_of() { sha256sum -- "$1" | cut -d' ' -f1; }

add_read() { BYTES_READ=$((BYTES_READ + $1)); SCENE_BYTES_READ=$((SCENE_BYTES_READ + $1)); }
add_write() { BYTES_WRITTEN=$((BYTES_WRITTEN + $1)); SCENE_BYTES_WRITTEN=$((SCENE_BYTES_WRITTEN + $1)); }
# rclone move reads the mount file once and uploads the same bytes once.
account_transfer() { add_read "$1"; add_write "$1"; }

limit_check() {
  if (( SECONDS - START >= LIMIT_SECONDS )); then
    LIMIT_STATE=seconds
    return 1
  fi
  if (( BYTES_READ + BYTES_WRITTEN > LIMIT_BYTES )); then
    LIMIT_STATE=bytes
    return 1
  fi
  return 0
}

limit_abort() {
  REASON="impact limit reached: $LIMIT_STATE"
  log "$REASON (read=$BYTES_READ written=$BYTES_WRITTEN elapsed=$((SECONDS - START))s)"
  write_receipt FAIL "$REASON"
  exit 3
}

rc() { "$RCLONE_BIN" --config "$CONF" --cache-dir "$CACHE/rclone" "$@"; }

unmount_private() {
  [[ -n "$MOUNT" ]] || return 0
  if mountpoint -q "$MOUNT" 2>/dev/null; then
    timeout 10 fusermount3 -u "$MOUNT" 2>/dev/null ||
      timeout 10 umount -l "$MOUNT" 2>/dev/null || true
  fi
  if [[ -n "$MOUNT_PID" ]] && kill -0 "$MOUNT_PID" 2>/dev/null; then
    kill "$MOUNT_PID" 2>/dev/null || true
    for _ in $(seq 1 40); do
      kill -0 "$MOUNT_PID" 2>/dev/null || break
      sleep 0.25
    done
    kill -9 "$MOUNT_PID" 2>/dev/null || true
  fi
  [[ -n "$MOUNT_PID" ]] && wait "$MOUNT_PID" 2>/dev/null || true
}

add_row() {
  jq -n \
    --arg scene "$SCENE_NAME" \
    --arg operation "$SCENE_OPERATION" \
    --arg outcome "$SCENE_OUTCOME" \
    --arg reason "$SCENE_REASON" \
    --argjson bytes_read "$SCENE_BYTES_READ" \
    --argjson bytes_written "$SCENE_BYTES_WRITTEN" \
    --argjson elapsed "$SCENE_ELAPSED" \
    --arg sha256 "$SCENE_SHA256" \
    --arg expected_sha256 "$SCENE_EXPECTED_SHA256" \
    --arg files "$SCENE_FILES" \
    --arg limit_state "$LIMIT_STATE" \
    --arg snapshot_path "$SNAP" \
    --arg snapshot_sha256 "$SNAP_SEAL_SHA" \
    --arg rclone_artifact_sha256 "$RCLONE_ARTIFACT_SHA" \
    --arg server_image_digest "$IMAGE_ID" \
    --arg go_version "$GO_VERSION" \
    --arg rclone_version "$RCLONE_VERSION" \
    '{scene:$scene, operation:$operation, outcome:$outcome, reason:$reason,
      bytes_read:$bytes_read, bytes_written:$bytes_written, elapsed_seconds:$elapsed,
      sha256:$sha256, expected_sha256:$expected_sha256, files:$files,
      limit_state:$limit_state, snapshot_path:$snapshot_path,
      snapshot_sha256:$snapshot_sha256, rclone_artifact_sha256:$rclone_artifact_sha256,
      server_image_digest:$server_image_digest, go_version:$go_version,
      rclone_version:$rclone_version}' >> "$ROWS"
}

write_receipt() {
  local outcome="$1" reason="$2" scenes_json elapsed total finished
  scenes_json="$(jq -s '.' "$ROWS" 2>/dev/null || printf '[]')"
  elapsed=$((SECONDS - START))
  total=$((BYTES_READ + BYTES_WRITTEN))
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local tmp="$RECEIPT_OUT.tmp.$$"
  jq -n \
    --arg outcome "$outcome" --arg reason "$reason" \
    --arg snapshot_path "$SNAP" --arg snapshot_sha256 "$SNAP_SEAL_SHA" \
    --arg snapshot_digest "$SNAP_DIGEST" \
    --arg rclone_path "$RCLONE_BIN" --arg rclone_sha "$RCLONE_ARTIFACT_SHA" \
    --arg rclone_version "$RCLONE_VERSION" --arg rclone_upstream "$RCLONE_UPSTREAM" \
    --arg image_tag "$IMAGE" --arg image_id "$IMAGE_ID" --arg go_version "$GO_VERSION" \
    --arg mountpoint "$MOUNT" --arg cache "$CACHE" --arg limit_state "$LIMIT_STATE" \
    --arg base_url "$BASE_URL" --arg started "$RUN_STARTED_AT" --arg finished "$finished" \
    --argjson scenes "$scenes_json" --argjson read "$BYTES_READ" \
    --argjson written "$BYTES_WRITTEN" --argjson total "$total" \
    --argjson elapsed "$elapsed" \
    --argjson commands "$(jq -n --args '$ARGS.positional' -- \
      "rclone mount td: <private-mountpoint> --vfs-cache-mode writes --cache-dir <private-cache> --vfs-write-back 24h" \
      "rclone move <private-mountpoint>/<file> td:verify/<scene>" \
      "rclone copy td:verify/<scene> <private-download>")" \
    '{plan:"07-06", outcome:$outcome, reason:$reason,
      snapshot_path:$snapshot_path, snapshot_sha256:$snapshot_sha256,
      snapshot_digest:$snapshot_digest,
      rclone_artifact_path:$rclone_path, rclone_artifact_sha256:$rclone_sha,
      rclone_version:$rclone_version, rclone_upstream_base:$rclone_upstream,
      server_image_tag:$image_tag, server_image_digest:$image_id,
      go_version:$go_version,
      mount:{mountpoint:$mountpoint, vfs_cache_dir:$cache,
        vfs_cache_mode:"writes", vfs_write_back:"24h", allow_other:false},
      production_touched:false,
      impact_limits:{seconds:180, bytes:8589934592},
      limit_state:$limit_state,
      elapsed_seconds:$elapsed,
      bytes:{read:$read, written:$written, total:$total,
        method:"logical size-based: mount writes, move reads, uploads, mount reads, downloads"},
      server:{base_url:$base_url, disposable:true},
      scenes:$scenes, commands:$commands,
      started_at:$started, finished_at:$finished}' > "$tmp"
  chmod 600 "$tmp"
  mv -f "$tmp" "$RECEIPT_OUT"
  RECEIPT_WRITTEN=true
}

on_exit() {
  local status=$?
  unmount_private
  if [[ "$RECEIPT_WRITTEN" != true && -n "${SNAP:-}" && -n "${RECEIPT_OUT:-}" ]]; then
    write_receipt FAIL "${REASON:-unexpected exit $status}" || true
  fi
  local keep="$KEEP"
  [[ "$status" -ne 0 ]] && keep=true
  if [[ -n "$RUNTIME" && -n "$WORK" ]]; then
    if [[ "$keep" == true && "$status" -ne 0 ]]; then
      printf 'b6-mount: private work root kept at %s (not published)\n' "$WORK" >&2
    fi
    b6_cleanup "$RUNTIME" "$keep" "$WORK"
  elif [[ -n "$WORK" && "$keep" != true ]]; then
    rm -rf -- "$WORK"
  fi
  [[ -n "$ROWS" ]] && rm -f "$ROWS" 2>/dev/null || true
}
trap on_exit EXIT
trap 'exit 130' INT TERM

# --- disposable server -----------------------------------------------------
if [[ -z "$SERVER_URL" ]]; then
  WORK="$PRIV/b6-$(b6_run_id)-mount-work"
  mkdir -m 700 "$WORK"
  ROWS="$WORK/scenes.jsonl"
  : > "$ROWS"
  mkdir -m 700 "$WORK/telegram-root" "$WORK/src" "$WORK/dl"

  DB_NAME="teldrive_b6"
  DB_USER="b6"
  SCHEMA="b6_mount"
  PG_NAME="$(b6_name postgres)"
  TD_NAME="$(b6_name teldrive)"
  NETWORK="$(b6_name net)"

  "$RUNTIME" network create "$NETWORK" >/dev/null
  b6_track_network "$NETWORK"

  PG_PASSWORD="$(b6_hex32)"
  b6_write_env_file "$WORK/postgres.env" \
    "POSTGRES_USER=$DB_USER" \
    "POSTGRES_PASSWORD=$PG_PASSWORD" \
    "POSTGRES_DB=$DB_NAME"
  POSTGRES_IMAGE="$(jq -r '.postgres_image // empty' "$RECEIPT")"
  [[ -n "$POSTGRES_IMAGE" ]] ||
    POSTGRES_IMAGE="postgres:18-alpine@sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015"
  b6_start_postgres "$RUNTIME" "$PG_NAME" "$POSTGRES_IMAGE" "$NETWORK" "$WORK/postgres.env"
  limit_check || limit_abort
  b6_wait_postgres "$RUNTIME" "$PG_NAME" "$DB_USER" "$DB_NAME" 120 ||
    fail "disposable PostgreSQL did not become ready"

  SIGNING_KEY="$(b6_hex32)"
  DATA_KEY="$(b6_data_key)"
  DB_URL="postgres://$DB_USER:$PG_PASSWORD@postgres:5432/$DB_NAME?sslmode=disable"
  b6_write_env_file "$WORK/teldrive.env" \
    "TELDRIVE_DATABASE_URL=$DB_URL" \
    "TELDRIVE_DATABASE_SCHEMA=$SCHEMA" \
    "TELDRIVE_SECURITY_SIGNING_KEY=$SIGNING_KEY" \
    "TELDRIVE_SECURITY_DATA_KEY=$DATA_KEY" \
    "TELDRIVE_TELEGRAM_BACKEND=filesystem" \
    "TELDRIVE_TELEGRAM_LOCAL_ROOT=/data/local-telegram" \
    "TELDRIVE_HTTP_ADDRESS=0.0.0.0:8080"
  b6_start_teldrive "$RUNTIME" "$TD_NAME" "$IMAGE" "$NETWORK" \
    "$WORK/teldrive.env" "$WORK/telegram-root"
  HTTP_PORT="$(b6_published_port "$RUNTIME" "$TD_NAME" 8080)"
  [[ "$HTTP_PORT" =~ ^[0-9]+$ ]] || fail "could not determine the disposable TelDrive port"
  BASE_URL="http://127.0.0.1:$HTTP_PORT"
  b6_wait_health "$BASE_URL/health/live" 120 || fail "disposable TelDrive did not start"
  b6_assert_env_allowlist "$RUNTIME" "$TD_NAME" \
    TELDRIVE_DATABASE_URL TELDRIVE_DATABASE_SCHEMA \
    TELDRIVE_SECURITY_SIGNING_KEY TELDRIVE_SECURITY_DATA_KEY \
    TELDRIVE_TELEGRAM_BACKEND TELDRIVE_TELEGRAM_LOCAL_ROOT \
    TELDRIVE_HTTP_ADDRESS || fail "resolved container environment contains a non-allowlisted variable"

  API_SECRET="$(b6_api_secret)"
  b6_bootstrap_api_key "$RUNTIME" "$PG_NAME" "$DB_USER" "$DB_NAME" "$SCHEMA" "$API_SECRET" \
    "b6-linux-mount" || fail "API key bootstrap failed"
else
  ROWS="$(mktemp "${TMPDIR:-/tmp}/b6-mount-scenes.XXXXXX")"
  : > "$ROWS"
  WORK="$(mktemp -d "${TMPDIR:-/tmp}/b6-mount-work.XXXXXX")"
  chmod 700 "$WORK"
  mkdir -m 700 "$WORK/src" "$WORK/dl"
  BASE_URL="$SERVER_URL"
  API_SECRET="$(head -c 4096 "$API_KEY_FILE" | tr -d '\n')"
fi
BASE_URL="${BASE_URL%/}"

CONF="$WORK/rclone.conf"
( umask 077
  {
    printf '[td]\n'
    printf 'type = teldrive\n'
    printf 'api_host = %s\n' "$BASE_URL"
    printf 'api_key = %s\n' "$API_SECRET"
  } > "$CONF" )
chmod 600 "$CONF"

CACHE="$WORK/vfs-cache"
MOUNT="$WORK/mnt"
mkdir -m 700 "$CACHE" "$MOUNT"

# --- private mount ---------------------------------------------------------
MOUNT_PID_FILE="$WORK/mount.pid"
"$RCLONE_BIN" mount td: "$MOUNT" --config "$CONF" --cache-dir "$CACHE" \
  --vfs-cache-mode writes \
  --vfs-write-back 24h --dir-cache-time 1s --attr-timeout 1s \
  --log-file "$WORK/mount.log" --log-level INFO >>"$WORK/mount.err" 2>&1 &
MOUNT_PID=$!
printf '%s\n' "$MOUNT_PID" > "$MOUNT_PID_FILE"
chmod 600 "$MOUNT_PID_FILE"

mounted=false
for _ in $(seq 1 240); do
  if mountpoint -q "$MOUNT" 2>/dev/null; then mounted=true; break; fi
  kill -0 "$MOUNT_PID" 2>/dev/null || fail "rclone mount exited before becoming ready"
  limit_check || limit_abort
  sleep 0.25
done
[[ "$mounted" == true ]] || fail "rclone mount did not become ready"

# --- scenes ----------------------------------------------------------------
scene_empty() {
  SCENE_OPERATION="create zero-byte file, rclone move, independent copy"
  local src="$WORK/src/empty.bin" dl="$WORK/dl/empty"
  : > "$src"
  : > "$MOUNT/empty.bin" || { SCENE_REASON="could not create empty file"; return 1; }
  [[ ! -s "$MOUNT/empty.bin" ]] || { SCENE_REASON="empty file is not zero bytes"; return 1; }
  limit_check || { SCENE_REASON="impact limit: $LIMIT_STATE"; return 1; }
  rc move "$MOUNT/empty.bin" "td:verify/empty/" >/dev/null 2>&1 ||
    { SCENE_REASON="rclone move failed"; return 1; }
  mkdir -p "$dl"
  rc copy "td:verify/empty" "$dl" >/dev/null 2>&1 ||
    { SCENE_REASON="independent copy failed"; return 1; }
  [[ -f "$dl/empty.bin" ]] || { SCENE_REASON="downloaded empty.bin missing"; return 1; }
  add_read "$(stat -c %s "$dl/empty.bin")"
  SCENE_SHA256="$(hash_of "$dl/empty.bin")"
  SCENE_EXPECTED_SHA256="$(hash_of "$src")"
  SCENE_FILES="empty.bin"
  [[ "$SCENE_SHA256" == "$SCENE_EXPECTED_SHA256" ]] ||
    { SCENE_REASON="SHA-256 differs ($SCENE_SHA256 != $SCENE_EXPECTED_SHA256)"; return 1; }
  return 0
}

scene_unicode() {
  SCENE_OPERATION="write, list, rclone move, independent copy"
  local name='тест-😀.bin' src="$WORK/src/unicode.bin" dl="$WORK/dl/unicode" found=false
  printf 'unicode-content-тест-😀\n' > "$src"
  cp "$src" "$MOUNT/$name" || { SCENE_REASON="could not write unicode name"; return 1; }
  add_write "$(stat -c %s "$src")"
  while IFS= read -r entry; do
    [[ "$entry" == "$name" ]] && found=true
  done < <(find "$MOUNT" -maxdepth 1 -mindepth 1 -printf '%f\n')
  [[ "$found" == true ]] || { SCENE_REASON="mount listing lacks the exact unicode name"; return 1; }
  limit_check || { SCENE_REASON="impact limit: $LIMIT_STATE"; return 1; }
  account_transfer "$(stat -c %s "$src")"
  rc move "$MOUNT/$name" "td:verify/unicode/" >/dev/null 2>&1 ||
    { SCENE_REASON="rclone move failed"; return 1; }
  mkdir -p "$dl"
  rc copy "td:verify/unicode" "$dl" >/dev/null 2>&1 ||
    { SCENE_REASON="independent copy failed"; return 1; }
  [[ -f "$dl/$name" ]] || { SCENE_REASON="downloaded unicode name differs"; return 1; }
  add_read "$(stat -c %s "$dl/$name")"
  SCENE_SHA256="$(hash_of "$dl/$name")"
  SCENE_EXPECTED_SHA256="$(hash_of "$src")"
  SCENE_FILES="$name"
  [[ "$SCENE_SHA256" == "$SCENE_EXPECTED_SHA256" ]] ||
    { SCENE_REASON="SHA-256 differs ($SCENE_SHA256 != $SCENE_EXPECTED_SHA256)"; return 1; }
  return 0
}

scene_case() {
  SCENE_OPERATION="write Case.txt and case.txt, list, rclone move, independent copy"
  local up="$WORK/src/Case.txt" low="$WORK/src/case.txt" dl="$WORK/dl/case" has_up=false has_low=false
  printf 'upper-case-content\n' > "$up"
  printf 'lower-case-content\n' > "$low"
  cp "$up" "$MOUNT/Case.txt" || { SCENE_REASON="could not write Case.txt"; return 1; }
  cp "$low" "$MOUNT/case.txt" || { SCENE_REASON="could not write case.txt"; return 1; }
  add_write "$(( $(stat -c %s "$up") + $(stat -c %s "$low") ))"
  while IFS= read -r entry; do
    [[ "$entry" == "Case.txt" ]] && has_up=true
    [[ "$entry" == "case.txt" ]] && has_low=true
  done < <(find "$MOUNT" -maxdepth 1 -mindepth 1 -printf '%f\n')
  [[ "$has_up" == true && "$has_low" == true ]] ||
    { SCENE_REASON="mount listing does not show both case-distinct names"; return 1; }
  limit_check || { SCENE_REASON="impact limit: $LIMIT_STATE"; return 1; }
  account_transfer "$(( $(stat -c %s "$up") + $(stat -c %s "$low") ))"
  rc move "$MOUNT/Case.txt" "td:verify/case/" >/dev/null 2>&1 ||
    { SCENE_REASON="rclone move Case.txt failed"; return 1; }
  rc move "$MOUNT/case.txt" "td:verify/case/" >/dev/null 2>&1 ||
    { SCENE_REASON="rclone move case.txt failed"; return 1; }
  mkdir -p "$dl"
  rc copy "td:verify/case" "$dl" >/dev/null 2>&1 ||
    { SCENE_REASON="independent copy failed"; return 1; }
  [[ -f "$dl/Case.txt" && -f "$dl/case.txt" ]] ||
    { SCENE_REASON="downloaded names are not case-distinct"; return 1; }
  add_read "$(( $(stat -c %s "$dl/Case.txt") + $(stat -c %s "$dl/case.txt") ))"
  local hu hl eu el
  hu="$(hash_of "$dl/Case.txt")"; hl="$(hash_of "$dl/case.txt")"
  eu="$(hash_of "$up")"; el="$(hash_of "$low")"
  SCENE_EXPECTED_SHA256="Case.txt=$eu case.txt=$el"
  SCENE_SHA256="Case.txt=$hu case.txt=$hl"
  SCENE_FILES="Case.txt,case.txt"
  [[ "$hu" == "$eu" && "$hl" == "$el" ]] ||
    { SCENE_REASON="case-distinct SHA-256 differ"; return 1; }
  return 0
}

scene_rename() {
  SCENE_OPERATION="create folder and file, rename both through the mount, rclone move, independent copy"
  local src="$WORK/src/rename.txt" dl="$WORK/dl/rename" got
  printf 'rename-content-preserved\n' > "$src"
  mkdir "$MOUNT/dir-a" || { SCENE_REASON="could not create dir-a"; return 1; }
  cp "$src" "$MOUNT/dir-a/file.txt" || { SCENE_REASON="could not create dir-a/file.txt"; return 1; }
  add_write "$(stat -c %s "$src")"
  mv "$MOUNT/dir-a" "$MOUNT/dir-b" || { SCENE_REASON="directory rename failed"; return 1; }
  mv "$MOUNT/dir-b/file.txt" "$MOUNT/dir-b/renamed.txt" ||
    { SCENE_REASON="file rename failed"; return 1; }
  [[ -f "$MOUNT/dir-b/renamed.txt" && ! -e "$MOUNT/dir-a" ]] ||
    { SCENE_REASON="renamed paths are not visible through the mount"; return 1; }
  limit_check || { SCENE_REASON="impact limit: $LIMIT_STATE"; return 1; }
  account_transfer "$(stat -c %s "$src")"
  rc move "$MOUNT/dir-b" "td:verify/rename" >/dev/null 2>&1 ||
    { SCENE_REASON="rclone move failed"; return 1; }
  mkdir -p "$dl"
  rc copy "td:verify/rename" "$dl" >/dev/null 2>&1 ||
    { SCENE_REASON="independent copy failed"; return 1; }
  got="$(find "$dl" -type f -name renamed.txt | head -n 1)"
  [[ -n "$got" ]] || { SCENE_REASON="downloaded renamed.txt missing"; return 1; }
  add_read "$(stat -c %s "$got")"
  SCENE_SHA256="$(hash_of "$got")"
  SCENE_EXPECTED_SHA256="$(hash_of "$src")"
  SCENE_FILES="dir-b/renamed.txt"
  [[ "$SCENE_SHA256" == "$SCENE_EXPECTED_SHA256" ]] ||
    { SCENE_REASON="SHA-256 differs ($SCENE_SHA256 != $SCENE_EXPECTED_SHA256)"; return 1; }
  return 0
}

run_scene() {
  local name="$1"
  SCENE_NAME="$name"
  SCENE_START=$SECONDS
  SCENE_BYTES_READ=0
  SCENE_BYTES_WRITTEN=0
  SCENE_SHA256=""
  SCENE_EXPECTED_SHA256=""
  SCENE_FILES=""
  SCENE_REASON=""
  SCENE_ELAPSED=0
  SCENE_OUTCOME=PASS
  limit_check || limit_abort
  if ! "scene_$name"; then
    SCENE_OUTCOME=FAIL
    [[ -n "$SCENE_REASON" ]] || SCENE_REASON="scene assertion failed"
    SCENE_ELAPSED=$((SECONDS - SCENE_START))
    add_row
    REASON="scene $name failed: $SCENE_REASON"
    write_receipt FAIL "$REASON"
    log "$REASON"
    exit 1
  fi
  SCENE_ELAPSED=$((SECONDS - SCENE_START))
  add_row
  log "$name PASS bytes_read=$SCENE_BYTES_READ bytes_written=$SCENE_BYTES_WRITTEN elapsed=${SCENE_ELAPSED}s"
}

for scene in "${SCENES[@]}"; do
  run_scene "$scene"
done

unmount_private
limit_check || limit_abort
write_receipt PASS ""
log "PASS scenes=${#SCENES[@]} bytes_read=$BYTES_READ bytes_written=$BYTES_WRITTEN elapsed=$((SECONDS - START))s"
printf 'LINUX-MOUNT PASS snapshot_sha256=%s rclone_artifact_sha256=%s server_image_digest=%s scenes=%s receipt=%s\n' \
  "$SNAP_SEAL_SHA" "$RCLONE_ARTIFACT_SHA" "$IMAGE_ID" "${#SCENES[@]}" "$RECEIPT_OUT"
