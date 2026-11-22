# rclone Submodule Guide

Repository-wide guidance lives in the [root AGENTS.md](../AGENTS.md). Read it first. This file keeps only details specific to `rclone-enchanced/`.

## AI-Assisted Contributions

Rclone welcomes AI-assisted contributions, but the human submitter must understand every proposed line and must compile and test it against real rclone code. Read the "AI-assisted contributions" section of [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

## Cross-Backend Tests

Tests in `fs/operations`, `fs/sync`, `cmd/bisync`, `cmd/gitannex`, and `vfs` run against all backends; the definitive list is `fstest/test_all/config.yaml`. They must tolerate backends with limited functionality. Skip unsupported paths with `Fs.Features()` flags instead of assuming every backend implements every operation.

## Architecture

- `rclone.go` imports `backend/all` and `cmd/all`; both registries use `init()` and blank imports. Each backend calls `fs.Register()` with an `fs.RegInfo`.
- Core contracts live in `fs/`: `fs.Fs` and `fs.Object` in `fs/types.go`, optional capabilities in `fs/features.go`, registration metadata in `fs/registry.go`.
- Each backend's core implementation is one package. Keep the main `Fs`/`Object` implementation in one file, API types in `api/types.go`, and standardized integration tests via `fstests.Run()` in `fstest/fstests`. Do not split the core into generic `fs.go` and `object.go` files; auxiliary feature files and helper subpackages such as `tdhash/` are allowed.
- Register HTTP backends through `backend/all/all.go`. Use `lib/rest` and `fs/fshttp` for HTTP, `lib/dircache` for ID-based remotes, `lib/oauthutil` for OAuth, and `lib/pacer` for rate limiting.
- Top-level commands are packages under `cmd/`, registered in `cmd/all/all.go`, and use cobra through `cmd.Main()`.
- Key subsystems: `fs/operations` (copy/move/delete), `fs/sync` and `fs/march` (sync and walking), `fs/filter`, `fs/accounting`, `fs/config`, `vfs` (mount/serve), `librclone` (C interface), and `fstest` (test framework and generic backend suite).

## Comment Style

- Every exported declaration gets a godoc comment starting with its name as a present-tense statement.
- Document the contract: preconditions, returned sentinel errors, and idempotency caveats.
- Inline comments explain why, not what: API quirks, workarounds, ordering constraints.
- Comments describe current behavior only. Do not narrate the change or compare with previous behavior; that belongs in the commit message.
- Use `FIXME` and `TODO` for known shortcomings.

## Tooling And Docs

- Linting uses golangci-lint v2 with `.golangci.yml`; enabled linters are errcheck, govet, ineffassign, staticcheck, unused, gocritic, misspell, revive, and unconvert, plus the `goimports` formatter.
- Backend option docs come from `Help:` fields in the Go Options structs. Command docs live in command source, such as `cmd/ls/ls.go`.
- Do not commit regenerated backend or command docs from `make backenddocs` or `make commanddocs`.
- Website docs are Markdown in `docs/content/`, built with Hugo; `make serve` previews them.
