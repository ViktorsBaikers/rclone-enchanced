#!/usr/bin/env bash
# upstream-gate.sh checks that the fork history stays attributable and that the
# contracts an upstream change can break still pass.
#
# Path groups are closed. A commit may touch backend/teldrive and docs, or
# shared rclone paths and docs, but never backend/teldrive and shared paths
# together. Merge commits are integration points and are skipped. Two commits
# published before the grouping policy (D-12/D-13 forbid rewriting them) are
# recorded as legacy exceptions; every later mixed commit fails.
#
# Runtime junk (vfs/vfs, vfs/vfsMeta) is excluded from grouping. A commit made
# only of runtime junk fails.
#
# Usage:
#   ./scripts/b6/upstream-gate.sh --classify-only [--base <ref>]
#   ./scripts/b6/upstream-gate.sh --full [--base <ref>] [--windows-evidence PATH]
#   ./scripts/b6/upstream-gate.sh --full --accept-windows-not-run --reason TEXT
#   ./scripts/b6/upstream-gate.sh --self-test
#
# --full fails closed unless Windows execution is proven by a successful
# b6-windows run on this commit. Provide that proof with --windows-evidence
# (flat JSON: conclusion, run_id, head_sha; or key=value lines: conclusion=,
# run_id=, commit=), or record an explicit acceptance with
# --accept-windows-not-run --reason "<non-empty>".
#
# --full writes a private receipt to ${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}
# unless B6_GATE_RECEIPT names one.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SELF="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
DEFAULT_REPO=$(cd "$SCRIPT_DIR/../.." && pwd)
REPO=${UPSTREAM_GATE_REPO:-$DEFAULT_REPO}
TELDRIVE_REPO=${TELDRIVE_REPO:-$(cd "$REPO/../teldrive-enchanced" 2>/dev/null && pwd || true)}

BASE=v1.76.0
MODE=classify
TMP_ROOT=
RECEIPT=
WINDOWS_EVIDENCE=
ACCEPT_WINDOWS_NOT_RUN=false
WINDOWS_REASON=

# Published before the grouping policy. These two commits mix backend and
# shared paths; rewriting them is prohibited by D-12/D-13.
LEGACY_MIXED_COMMITS=(
  d972a5037a85bd66c5092e0b3bb6d2a7182b8adc
  7c71e41a6a2362aa699ad9434e1c6be83bb1589f
)

usage() {
  printf '%s\n' \
    "usage: $0 [--classify-only|--full|--self-test] [--base <ref>]" \
    "                    [--windows-evidence PATH]" \
    "                    [--accept-windows-not-run --reason TEXT]" \
    "" \
    "  --classify-only  check commit path groups (default)" \
    "  --full           classify and run the contract gate" \
    "  --self-test      prove the classifier on synthetic histories" \
    "  --base <ref>     upstream base commit or tag (default: v1.76.0)" \
    "  --windows-evidence PATH" \
    "                   sanitized Windows CI proof; --full fails closed without it" \
    "                   flat JSON: {\"conclusion\":\"success\",\"run_id\":\"...\",\"head_sha\":\"...\"}" \
    "                   or key=value lines: conclusion=success, run_id=..., commit=..." \
    "                   a commit/head_sha must match HEAD when present" \
    "  --accept-windows-not-run" \
    "                   record Windows execution as NOT-RUN (accepted) instead" \
    "  --reason TEXT    non-empty justification required by --accept-windows-not-run"
}

cleanup() {
  if [[ -n $TMP_ROOT && -d $TMP_ROOT ]]; then
    rm -rf "$TMP_ROOT"
  fi
}
trap cleanup EXIT INT TERM

while (($#)); do
  case $1 in
    --base)
      [[ $# -ge 2 ]] || { echo "--base needs a value" >&2; exit 2; }
      BASE=$2
      shift 2
      ;;
    --base=*)
      BASE=${1#*=}
      shift
      ;;
    --classify-only)
      MODE=classify
      shift
      ;;
    --full)
      MODE=full
      shift
      ;;
    --self-test)
      MODE=self-test
      shift
      ;;
    --windows-evidence)
      [[ $# -ge 2 ]] || { echo "--windows-evidence needs a value" >&2; exit 2; }
      WINDOWS_EVIDENCE=$2
      shift 2
      ;;
    --accept-windows-not-run)
      ACCEPT_WINDOWS_NOT_RUN=true
      shift
      ;;
    --reason)
      [[ $# -ge 2 ]] || { echo "--reason needs a value" >&2; exit 2; }
      WINDOWS_REASON=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ $ACCEPT_WINDOWS_NOT_RUN == true ]]; then
  if [[ -z ${WINDOWS_REASON//[[:space:]]/} ]]; then
    echo "--accept-windows-not-run requires a non-empty --reason" >&2
    exit 2
  fi
elif [[ -n $WINDOWS_REASON ]]; then
  echo "--reason requires --accept-windows-not-run" >&2
  exit 2
fi

if ! git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
  echo "not a Git repository: $REPO" >&2
  exit 2
fi

say() {
  printf '%s\n' "$*"
  if [[ -n $RECEIPT ]]; then
    printf '%s\n' "$*" >>"$RECEIPT"
  fi
}

classify_path() {
  local path=$1

  case $path in
    vfs/vfs/*|vfs/vfsMeta/*)
      echo runtime-junk
      return
      ;;
  esac

  case $path in
    backend/teldrive/*|backend/all/*)
      echo backend
      return
      ;;
  esac

  case $path in
    fs/chunkedreader/*|fs/operations/*|lib/pool/*|fs/rc/*|vfs/*|fs/cache/*|fs/sync/*|fs/march/*|lib/dircache/*|backend/local/*|cmd/*|fstest/*)
      echo shared
      return
      ;;
    fs/writeguard.go|fs/features.go|fs/types.go|fs/override.go|fs/config*.go|fs/version.go|fs/versiontag.go|fs/versionsuffix.go|go.mod|go.sum)
      echo shared
      return
      ;;
  esac

  case $path in
    docs/*|*.md|MANUAL.*|rclone.1|.github/*|Dockerfile|Makefile|bin/*|scripts/*|VERSION|LICENSE|.gitignore|.dockerignore|.golangci.yml|*_test.go)
      echo docs-tests
      return
      ;;
  esac

  echo unknown
}

is_legacy_mixed() {
  local commit=$1 legacy
  for legacy in "${LEGACY_MIXED_COMMITS[@]}"; do
    if [[ $commit == "$legacy" ]]; then
      return 0
    fi
  done
  return 1
}

classify_range() {
  local base=$1
  local commit full subject parents path group
  local backend shared docs junk unknown
  local failures=0 checked=0 merges=0 legacy=0

  if ! git -C "$REPO" rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
    echo "base is not a commit: $base" >&2
    return 2
  fi
  if ! git -C "$REPO" merge-base --is-ancestor "$base" HEAD; then
    echo "base $base is not an ancestor of HEAD" >&2
    return 2
  fi

  say "classify: repo=$REPO base=$base head=$(git -C "$REPO" rev-parse HEAD)"
  say "groups: backend | shared | docs-tests | runtime-junk(excluded)"

  while IFS= read -r commit; do
    [[ -n $commit ]] || continue
    full=$(git -C "$REPO" rev-parse "$commit")
    subject=$(git -C "$REPO" log -1 --format=%s "$commit")
    parents=$(git -C "$REPO" rev-list --parents -n 1 "$commit" | wc -w)
    if ((parents > 2)); then
      say "SKIP merge $commit $subject"
      merges=$((merges + 1))
      continue
    fi

    checked=$((checked + 1))
    backend=0
    shared=0
    docs=0
    junk=0
    unknown=0
    unknown_paths=

    while IFS= read -r path; do
      [[ -n $path ]] || continue
      group=$(classify_path "$path")
      case $group in
        backend) backend=$((backend + 1)) ;;
        shared) shared=$((shared + 1)) ;;
        docs-tests) docs=$((docs + 1)) ;;
        runtime-junk) junk=$((junk + 1)) ;;
        *)
          unknown=$((unknown + 1))
          unknown_paths+=" $path"
          ;;
      esac
    done < <(git -C "$REPO" show --name-only --format= --no-renames "$commit")

    if ((unknown > 0)); then
      say "FAIL unknown $commit $subject ->$unknown_paths"
      failures=$((failures + 1))
      continue
    fi
    if ((backend > 0 && shared > 0)); then
      if is_legacy_mixed "$full"; then
        say "LEGACY $commit backend+shared $subject"
        legacy=$((legacy + 1))
      else
        say "FAIL mixed $commit $subject (backend and shared paths in one commit)"
        failures=$((failures + 1))
      fi
      continue
    fi
    if ((junk > 0 && backend == 0 && shared == 0 && docs == 0)); then
      say "FAIL junk-only $commit $subject (runtime junk must not be committed)"
      failures=$((failures + 1))
      continue
    fi

    if ((backend > 0)); then
      group=backend
    elif ((shared > 0)); then
      group=shared
    else
      group=docs-tests
    fi
    say "PASS $commit $group $subject"
  done < <(git -C "$REPO" rev-list --reverse "$base..HEAD")

  say "classify summary: checked=$checked merges=$merges legacy=$legacy failures=$failures"
  if ((failures > 0)); then
    return 1
  fi
  return 0
}

new_fixture() {
  local dir
  dir=$(mktemp -d "$TMP_ROOT/fixture.XXXXXX")
  git -C "$dir" init -q -b main
  git -C "$dir" config user.name "B6 Gate Self Test"
  git -C "$dir" config user.email "b6-gate-self-test@localhost.invalid"
  printf '%s\n' "fixture" >"$dir/README.md"
  git -C "$dir" add README.md
  git -C "$dir" commit -q -m "fixture base"
  git -C "$dir" tag fixture-base
  echo "$dir"
}

fixture_commit() {
  local dir=$1 message=$2
  shift 2
  local path
  for path in "$@"; do
    mkdir -p "$dir/$(dirname "$path")"
    printf '%s\n' "$message" >>"$dir/$path"
    git -C "$dir" add "$path"
  done
  git -C "$dir" commit -q -m "$message"
}

expect_gate() {
  local dir=$1 expected=$2 name=$3
  local out status
  set +e
  out=$(UPSTREAM_GATE_REPO="$dir" "$SELF" --classify-only --base fixture-base 2>&1)
  status=$?
  set -e
  if [[ $expected == pass && $status -eq 0 ]]; then
    say "SELF-TEST PASS $name"
    return 0
  fi
  if [[ $expected == fail && $status -ne 0 ]]; then
    say "SELF-TEST PASS $name (rejected as expected)"
    return 0
  fi
  say "SELF-TEST FAIL $name (expected $expected, exit $status)"
  printf '%s\n' "$out" | sed 's/^/  /'
  return 1
}

expect_windows_gate() {
  local name=$1 expected=$2 want=${3:-} out status ok=true
  set +e
  out=$(windows_gate 2>&1)
  status=$?
  set -e
  if [[ $expected == pass ]]; then
    [[ $status -eq 0 ]] || ok=false
  else
    [[ $status -ne 0 ]] || ok=false
  fi
  if [[ -n $want && $out != *"$want"* ]]; then
    ok=false
  fi
  if [[ $ok == true ]]; then
    say "SELF-TEST PASS $name"
    return 0
  fi
  say "SELF-TEST FAIL $name (expected $expected, exit $status)"
  printf '%s\n' "$out" | sed 's/^/  /'
  return 1
}

self_test() {
  local failures=0 dir head_sha status

  TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/b6-upstream-gate.XXXXXX")

  dir=$(new_fixture)
  fixture_commit "$dir" "backend only" backend/teldrive/fixture.go
  expect_gate "$dir" pass "backend-only" || failures=$((failures + 1))

  dir=$(new_fixture)
  fixture_commit "$dir" "shared only" fs/operations/fixture.go
  expect_gate "$dir" pass "shared-only" || failures=$((failures + 1))

  dir=$(new_fixture)
  fixture_commit "$dir" "docs only" docs/content/fixture.md
  expect_gate "$dir" pass "docs-only" || failures=$((failures + 1))

  dir=$(new_fixture)
  fixture_commit "$dir" "mixed backend and shared" backend/teldrive/fixture.go fs/operations/fixture.go
  expect_gate "$dir" fail "mixed-backend-shared" || failures=$((failures + 1))

  dir=$(new_fixture)
  fixture_commit "$dir" "runtime junk only" vfs/vfs/127.0.0.1:1/fixture
  expect_gate "$dir" fail "runtime-junk-only" || failures=$((failures + 1))

  dir=$(new_fixture)
  fixture_commit "$dir" "backend with junk" backend/teldrive/fixture.go vfs/vfsMeta/local/fixture
  expect_gate "$dir" pass "backend-with-junk" || failures=$((failures + 1))

  dir=$(new_fixture)
  fixture_commit "$dir" "unknown path" unknown/fixture.go
  expect_gate "$dir" fail "unknown-path" || failures=$((failures + 1))

  head_sha=$(git -C "$REPO" rev-parse HEAD)

  WINDOWS_EVIDENCE=
  ACCEPT_WINDOWS_NOT_RUN=false
  WINDOWS_REASON=
  expect_windows_gate "windows-missing-evidence" fail || failures=$((failures + 1))

  printf '{"conclusion":"failure","run_id":"100","head_sha":"%s"}\n' "$head_sha" >"$TMP_ROOT/windows.json"
  WINDOWS_EVIDENCE="$TMP_ROOT/windows.json"
  expect_windows_gate "windows-failed-conclusion" fail || failures=$((failures + 1))

  printf '{"conclusion":"success","run_id":"101","head_sha":"%s"}\n' "$head_sha" >"$TMP_ROOT/windows.json"
  expect_windows_gate "windows-success" pass || failures=$((failures + 1))

  printf 'conclusion=failure\nrun_id=102\n' >"$TMP_ROOT/windows.txt"
  WINDOWS_EVIDENCE="$TMP_ROOT/windows.txt"
  expect_windows_gate "windows-text-failed-conclusion" fail || failures=$((failures + 1))

  printf 'conclusion=success\nrun_id=103\ncommit=%s\n' "$head_sha" >"$TMP_ROOT/windows.txt"
  expect_windows_gate "windows-text-success" pass || failures=$((failures + 1))

  printf '{"conclusion":"success","run_id":"104","head_sha":"%040d"}\n' 0 >"$TMP_ROOT/windows.json"
  WINDOWS_EVIDENCE="$TMP_ROOT/windows.json"
  expect_windows_gate "windows-wrong-commit" fail || failures=$((failures + 1))

  WINDOWS_EVIDENCE=
  ACCEPT_WINDOWS_NOT_RUN=true
  WINDOWS_REASON="no Windows runner in this environment"
  expect_windows_gate "windows-accepted-not-run" pass "NOT-RUN (accepted)" || failures=$((failures + 1))

  set +e
  "$SELF" --accept-windows-not-run --reason "" --classify-only >/dev/null 2>&1
  status=$?
  set -e
  if ((status != 0)); then
    say "SELF-TEST PASS windows-empty-reason (rejected as expected)"
  else
    say "SELF-TEST FAIL windows-empty-reason (empty reason accepted)"
    failures=$((failures + 1))
  fi

  ACCEPT_WINDOWS_NOT_RUN=false
  WINDOWS_REASON=

  if ((failures > 0)); then
    say "self-test summary: failures=$failures"
    return 1
  fi
  say "self-test summary: 7 classifier fixtures and 7 windows-execution cases passed"
  return 0
}

make_receipt() {
  local dir
  if [[ -n ${B6_GATE_RECEIPT:-} ]]; then
    RECEIPT=$B6_GATE_RECEIPT
    mkdir -p "$(dirname "$RECEIPT")"
  else
    dir=${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}
    mkdir -p "$dir"
    chmod 700 "$dir"
    RECEIPT="$dir/upstream-gate-$(date -u +%Y%m%dT%H%M%SZ).log"
  fi
  : >"$RECEIPT"
  chmod 600 "$RECEIPT"
  say "receipt: $RECEIPT"
}

run_step() {
  local name=$1 command=$2
  shift 2
  local started status
  started=$(date +%s)
  say "COMMAND[$name]: $command"
  set +e
  "$@" 2>&1 | tee -a "$RECEIPT"
  status=${PIPESTATUS[0]}
  set -e
  if ((status == 0)); then
    say "RESULT[$name]: PASS ($(( $(date +%s) - started ))s)"
    return 0
  fi
  say "RESULT[$name]: FAIL (exit $status, $(( $(date +%s) - started ))s)"
  return "$status"
}

step_range() {
  (cd "$REPO" && go test -count=1 ./backend/teldrive/...)
}

step_resume_hash() {
  (cd "$REPO" && go test -count=1 \
    -run 'TestB1|TestUpload|TestUnknownSizeResume|TestBlockHashes|TestTreeHash' \
    ./backend/teldrive ./backend/teldrive/tdhash)
}

step_sse() {
  (cd "$REPO" && go test -count=1 -run 'TestB3SSE' ./backend/teldrive)
}

step_shared_suites() {
  (cd "$REPO" && go test -count=1 \
    ./fs/chunkedreader/... ./fs/operations/... ./lib/pool/... ./fs/rc/... ./vfs/...)
}

step_server_tree_hash() {
  [[ -n $TELDRIVE_REPO && -d $TELDRIVE_REPO ]] || {
    echo "teldrive-enchanced not found next to $REPO" >&2
    return 1
  }
  (cd "$TELDRIVE_REPO" && go test -count=1 ./internal/treehash/...)
}

step_windows_build() {
  (cd "$REPO" && env GOOS=windows GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=local \
    go build -trimpath -buildvcs=false -o "$TMP_ROOT/rclone.exe" .)
}

step_windows_vet() {
  (cd "$REPO" && env GOOS=windows GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=local \
    go vet ./backend/teldrive/... ./fs/chunkedreader/... ./fs/operations/... \
    ./lib/pool/... ./fs/rc/...)
}

step_windows_test_compile() {
  local pkg count=0
  while IFS= read -r pkg; do
    [[ -n $pkg ]] || continue
    say "  compile: $pkg"
    if ! (cd "$REPO" && env GOOS=windows GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=local \
      go test -c -o "$TMP_ROOT/test-$count.exe" "$pkg" >>"$RECEIPT" 2>&1); then
      return 1
    fi
    count=$((count + 1))
  done < <(cd "$REPO" && go list ./backend/teldrive/... ./backend/teldrive/tdhash/... \
    ./fs/chunkedreader/... ./fs/operations/... ./fs/rc/jobs ./fs/rc/rcserver \
    ./lib/pool/... ./lib/dircache/... ./vfs/... ./backend/local/...)
  say "  compiled packages: $count"
}

# Read one field from a sanitized evidence file. Flat JSON uses quotes; txt uses
# key=value lines and an optional commit= key. Unknown keys yield an empty value.
windows_evidence_field() {
  local file=$1 key=$2
  if [[ "$(head -c 1 "$file")" == "{" ]]; then
    tr '\n' ' ' <"$file" |
      sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" |
      head -n 1
    return
  fi
  sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*//p" "$file" |
    head -n 1 | tr -d '\r'
}

# Windows execution proof is a successful b6-windows CI run on this commit.
# Missing or failed evidence fails the gate unless the operator recorded an
# explicit acceptance with a non-empty reason.
windows_gate() {
  if [[ $ACCEPT_WINDOWS_NOT_RUN == true ]]; then
    say "RESULT[windows-execution]: NOT-RUN (accepted)"
    say "windows-execution acceptance: $WINDOWS_REASON"
    return 0
  fi
  if [[ -z $WINDOWS_EVIDENCE ]]; then
    say "FAIL windows-execution: no evidence (pass --windows-evidence PATH or --accept-windows-not-run --reason TEXT)"
    return 1
  fi
  if [[ ! -f $WINDOWS_EVIDENCE ]]; then
    say "FAIL windows-execution: evidence file not found: $WINDOWS_EVIDENCE"
    return 1
  fi

  local conclusion run_id commit head
  conclusion=$(windows_evidence_field "$WINDOWS_EVIDENCE" conclusion)
  run_id=$(windows_evidence_field "$WINDOWS_EVIDENCE" run_id)
  commit=$(windows_evidence_field "$WINDOWS_EVIDENCE" head_sha)
  [[ -n $commit ]] || commit=$(windows_evidence_field "$WINDOWS_EVIDENCE" commit)
  head=$(git -C "$REPO" rev-parse HEAD)
  [[ -n $run_id ]] || run_id=unknown

  if [[ -n $commit && $commit != "$head" ]]; then
    say "FAIL windows-execution: evidence commit $commit does not match HEAD $head"
    return 1
  fi
  say "RESULT[windows-execution]: ${conclusion:-missing} run=$run_id commit=${commit:-unbound}"
  if [[ $conclusion != success ]]; then
    say "FAIL windows-execution: conclusion '${conclusion:-missing}' is not success"
    return 1
  fi
  return 0
}

run_full() {
  local failures=0
  mkdir -p "${TMPDIR:-/tmp}"
  TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/b6-upstream-gate.XXXXXX")
  make_receipt

  say "full gate: base=$BASE head=$(git -C "$REPO" rev-parse HEAD)"
  say "fork commits in range: $(git -C "$REPO" rev-list --count "$BASE..HEAD")"

  classify_range "$BASE" || failures=$((failures + 1))

  run_step range "./backend/teldrive/..." step_range || failures=$((failures + 1))
  run_step resume-hash "go test -run 'TestB1|TestUpload|TestUnknownSizeResume|...' ./backend/teldrive ./backend/teldrive/tdhash" step_resume_hash || failures=$((failures + 1))
  run_step SSE "go test -run 'TestB3SSE' ./backend/teldrive" step_sse || failures=$((failures + 1))
  run_step shared-suites "./fs/chunkedreader/... ./fs/operations/... ./lib/pool/... ./fs/rc/... ./vfs/..." step_shared_suites || failures=$((failures + 1))
  run_step server-tree-hash "teldrive-enchanced: go test ./internal/treehash/..." step_server_tree_hash || failures=$((failures + 1))
  run_step windows-build "GOOS=windows GOARCH=amd64 go build -trimpath -buildvcs=false -o rclone.exe ." step_windows_build || failures=$((failures + 1))
  run_step windows-vet "GOOS=windows GOARCH=amd64 go vet <contract packages>" step_windows_vet || failures=$((failures + 1))
  run_step windows-test-compile "GOOS=windows go test -c for the b6-windows package set" step_windows_test_compile || failures=$((failures + 1))

  say "COMMAND[windows-execution]: .github/workflows/b6-windows.yml on windows-latest"
  windows_gate || failures=$((failures + 1))

  if ((failures > 0)); then
    say "full summary: FAIL failures=$failures receipt=$RECEIPT"
    return 1
  fi
  say "full summary: PASS receipt=$RECEIPT"
  return 0
}

case $MODE in
  self-test)
    self_test
    ;;
  classify)
    classify_range "$BASE"
    ;;
  full)
    run_full
    ;;
esac
