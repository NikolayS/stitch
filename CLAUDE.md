# CLAUDE.md -- sqlever

## Project

sqlever -- Sqitch-compatible PostgreSQL migration tool with static analysis, expand/contract support, batched DML, and AI-powered explanations. The repo is internally named "stitch" but the product and npm package are "sqlever."

## Architecture

TypeScript + Bun. Single compiled binary via `bun build --compile`. Uses `pg` (node-postgres) for tracking operations and `libpg-query` (libpg_query WASM) for SQL parsing. Migration scripts execute via shelling out to `psql` (DD12) -- this guarantees full psql metacommand compatibility (`\i`, `\ir`, `\set`, `\copy`, `\if`/`\endif`).

### Source layout

```
src/
  cli.ts                    # Entry point, command routing
  commands/                 # One file per CLI command (deploy, revert, add, etc.)
  plan/                     # sqitch.plan parser, writer, topological sort
  db/                       # pg client wrapper, sqitch.* registry operations, URI parsing
  analysis/                 # Static analysis engine
    rules/                  # SA001-SA021, one file per rule
    suppression.ts          # Inline suppression (-- sqlever:disable)
    reporter.ts             # Output formatting (text/json/github/gitlab)
  includes/snapshot.ts      # Git-correlated \i/\ir resolution
  expand-contract/          # Expand/contract pattern generator + tracker
  batch/                    # PGQ-style 3-partition queue, worker, progress tracking
  tui/deploy.ts             # Live TTY deploy dashboard
  ai/                       # LLM-powered explain + review
  config/                   # sqitch.conf (INI) + sqlever.toml parsing
  output.ts                 # Shared output formatting
  lock-guard.ts             # Auto-prepend SET lock_timeout before risky DDL
  psql.ts                   # psql subprocess execution
  signals.ts                # Graceful shutdown handling
```

### Key design decisions (from spec/SPEC.md)

- **DD1 -- TypeScript + Bun, not Rust.** Single binary, fast iteration, easier contributor onboarding.
- **DD2 -- PostgreSQL first.** Depth over breadth. No multi-DB abstraction.
- **DD3 -- Sqitch tracking schema compatibility.** We read/write `sqitch.*` tables. sqlever-specific features use a separate `sqlever.*` schema.
- **DD4 -- SQL parser.** `libpg-query` for AST-level analysis. Fall back to regex only when necessary, with a clear comment.
- **DD5 -- Plan file is source of truth.** Never modified without explicit command.
- **DD8 -- All state in Postgres.** No local state files, no `.sqlever/` directory.
- **DD9 -- PGQ + SKIP LOCKED for batched DML.** 3-partition rotating queue table for bloat-free cleanup. `SELECT ... FOR UPDATE SKIP LOCKED` for concurrent worker dequeue.
- **DD12 -- psql for script execution.** Shell out to psql for migration scripts. `pg` for tracking/introspection only.
- **DD13 -- PgBouncer detection.** Session-level advisory locks for deploy coordination. Direct PG connections required for deploy/revert/batch.

## Development workflow

### Prerequisites

- [Bun](https://bun.sh) 1.1+
- Docker (for integration tests -- provides PostgreSQL)
- psql (for running migrations at runtime)

### Install dependencies

```bash
bun install
```

### Run tests

```bash
bun test                       # all tests
bun test tests/unit/           # unit tests only (fast, no DB needed)
bun test tests/integration/    # integration tests (requires PostgreSQL)
bun test tests/compat/         # Sqitch oracle compatibility tests
```

For integration tests, start PostgreSQL first:

```bash
docker compose up -d           # starts PG 17 on port 5417
bun test tests/integration/
docker compose down -v
```

### Type-check

```bash
bun x tsc --noEmit
```

### Build

```bash
bun run build                  # produces dist/sqlever
```

### CI

CI runs on every push and PR (`.github/workflows/ci.yml`):
- Unit tests
- Integration tests against PG 14, 15, 16, 17, 18 (matrix)
- Sqitch compatibility tests (oracle comparison via `sqitch/sqitch` Docker image)
- Build + smoke test on Ubuntu and macOS

## Code conventions

### TypeScript patterns

- **Strict mode.** `tsconfig.json` enables `strict`, `noUnusedLocals`, `noUnusedParameters`, `noUncheckedIndexedAccess`, `noFallthroughCasesInSwitch`.
- **ESM only.** `"type": "module"` in package.json, ESNext target and module resolution.
- **No classes unless necessary.** Prefer plain functions and interfaces.
- **Match existing style.** When editing a file, follow its conventions even if you would do it differently.
- **Surgical changes only.** Touch only what you must. Do not "improve" adjacent code, formatting, or comments unless that is the task.

### SQL style

- **Lowercase SQL keywords** -- `select`, `create table`, not `SELECT`, `CREATE TABLE`.
- **`snake_case`** for all identifiers.
- **`int8 generated always as identity`** for primary keys (not `serial`).
- **`timestamptz`** over `timestamp`, **`text`** over `varchar`**.
- Root keywords on their own line; arguments indented. `AND`/`OR` at the beginning of the line.
- Always use `AS` for aliases; use meaningful alias names (not single letters).

### Shell scripts

- Header: `#!/usr/bin/env bash` + `set -Eeuo pipefail` + `IFS=$'\n\t'`.
- 2-space indent. 80-char line limit.
- Quote all variable expansions. `[[ ]]` over `[ ]`. `$(command)` over backticks.

## Testing requirements

### Real tests, not mocks

- **Unit tests** are pure functions, no I/O. Located in `tests/unit/`.
- **Integration tests** use a real PostgreSQL instance (Docker). Located in `tests/integration/`. Each test gets a fresh database. No mocking the DB layer.
- **Compatibility tests** run Sqitch and sqlever side-by-side against identical databases and diff the tracking table state. Located in `tests/compat/`.

### What to test

- Happy path (minimum, not sufficient alone).
- Negative cases (invalid inputs, malformed data, empty/null).
- Boundary values.
- Edge cases (concurrency, state transitions).
- Error paths (exceptions, cleanup, propagation).
- Assertions must verify specific values, not just existence.

### TDD workflow

Write failing test first, confirm it fails, write minimal code to pass, refactor. For bug fixes, first write a test that reproduces the bug.

## Git and PR workflow

### Commits

- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `perf:`, `ci:`, `build:`.
- Scope encouraged: `feat(analysis): add SA022 rule`.
- Subject under 50 characters, body lines under 72.
- Present tense ("add feature" not "added feature").
- **Never amend** -- create a new commit.
- **Never force-push** unless explicitly confirmed.

### PRs

- Keep PRs focused. One logical change per PR.
- CI must be green before merge.
- All features open source under Apache 2.0.

## Writing and communication rules

These rules are derived from the team's shared guidelines at https://gitlab.com/postgres-ai/rules/-/tree/main/rules -- consult the originals for full detail.

### Professional communication

- **No emojis** in code, commits, docs, or reports.
- **Objective, neutral tone.** No emotional language, no subjective criticism.
- **Platform neutrality.** Never criticize other tools or platforms. Frame constraints as opportunities. Focus on what can be done.
- **Em dash spacing:** always use spaces around em dashes (`word -- word`, not `word--word`). Use `--` (double hyphen) in plain text contexts.

### Terminology

- **Postgres** (not PostgreSQL, unless required for technical precision).
- **PostgresAI** (no space, no dot).
- **DBLab** / **DBLab Engine** (not DLE, not Database Lab).
- **Sentence-style capitalization** for titles and headings -- only capitalize the first word and proper nouns.

### Units and timestamps

- **Binary units** in docs/reports: GiB, MiB, KiB (not GB, MB, KB). Exception: Postgres config values use PG's own format (`shared_buffers = '32GB'`).
- **Timestamps:** dynamic UI uses relative timestamps with ISO 8601 tooltip; static content uses `YYYY-MM-DD HH:mm:ss UTC`.

### AI coding guidelines

- **Think before coding.** State assumptions explicitly. Surface tradeoffs. Ask when uncertain.
- **Simplicity first.** No speculative features, no unnecessary abstractions.
- **Surgical changes.** Every changed line traces to the task.
- **Goal-driven execution.** Define success criteria, verify each step.
- **Fix root causes, not symptoms.** One fix at a time, validate, undo if it did not work.

## Spec reference

The full product specification is at `spec/SPEC.md`. Key sections:

- **Section 4 (Requirements):** R1 -- Sqitch CLI compatibility, R2 -- plan file format, R3 -- tracking schema, R4 -- static analysis on deploy, R5 -- machine-readable output, R6 -- exit codes.
- **Section 5 (Features):** 22 analysis rules (SA001-SA021 + SA002b), snapshot includes, expand/contract, batched DML, lock timeout guard, TUI deploy dashboard, AI explain/review.
- **Section 6 (Design decisions):** DD1-DD14 covering technology choices, compatibility strategy, and operational constraints.
- **Section 8 (Testing strategy):** Test pyramid, Sqitch oracle testing, PG version matrix (14-18).

## Security

- Never put API keys, tokens, or secrets in code, issues, or PR comments.
- No hidden network calls (DD10). No telemetry, no update checks. LLM calls only when `sqlever explain` or `sqlever review` is explicitly invoked.
