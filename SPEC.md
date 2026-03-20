# stitch — Product Specification

**Version:** 0.2 (draft)
**Status:** Pre-development
**License:** Apache 2.0

---

## Table of contents

1. [Inspiration and prior art](#1-inspiration-and-prior-art)
2. [Problems we're solving](#2-problems-were-solving)
3. [Goals and non-goals](#3-goals-and-non-goals)
4. [Requirements](#4-requirements)
5. [Feature ideas](#5-feature-ideas)
6. [Design decisions](#6-design-decisions)
7. [Architecture](#7-architecture)
8. [Testing strategy](#8-testing-strategy)
9. [Implementation plan](#9-implementation-plan)

---

## 1. Inspiration and prior art

### Sqitch — the foundation

https://sqitch.org — the best migration tool that exists today. It gets the fundamentals right:

- Dependency-aware changes (not just sequential numbers)
- Database-native: tracks state in the database itself, not migration files
- No ORM coupling: plain SQL, works with anything
- Proper deploy/revert/verify trinity

What it gets wrong: written in Perl (distribution nightmare, no contributors), zero awareness of what a migration *does* to a running system, no primitives for zero-downtime or large-table surgery.

### pgroll

https://github.com/xataio/pgroll — expand/contract pattern for PostgreSQL. The right idea (dual-schema transition so app and DB deploys decouple) but wrong implementation (full table recreation, heavy). Teaches us what to do and what not to do.

### migrationpilot

https://github.com/mickelsamuel/migrationpilot — static analysis of migration SQL. Catches dangerous patterns. Thin implementation but the right instinct. We'll reimplement and extend.

### GitLab migration helpers

https://gitlab.com/gitlab-org/gitlab/-/tree/master/lib/gitlab/database/migration_helpers

Battle-tested at massive scale. `BatchedMigration` framework: throttled background DML, pause/resume, per-batch transactions, state tracked in Postgres. The gold standard for large-table data migrations. We extract the concepts, drop the Rails dependency.

### SkyTools / PGQ

https://github.com/pgq/pgq — 3-partition rotating queue table, entirely inside Postgres. Proven architecture for durable queuing without external systems. Inspiration for our batched DML queue.

### Flyway / Liquibase

Sequential-numbered files, XML/YAML config, JVM runtime. Wrong philosophy. We take nothing.

---

## 2. Problems we're solving

### Problem 1: Dangerous migrations reach production undetected

`ALTER TABLE orders ADD COLUMN processed_at timestamptz NOT NULL DEFAULT now()` — on PostgreSQL < 11 this rewrites the entire table. On a 500GB orders table at 3am, that's an outage. No existing migration tool catches this before deploy.

### Problem 2: Sqitch is Perl

Installing Sqitch means managing a Perl runtime. `cpan` in 2026. Binary distribution is painful. Contributing to it is painful. It has accumulated technical debt that will never be paid.

### Problem 3: No tooling for zero-downtime schema changes

Renaming a column, changing a type, adding NOT NULL to an existing column — these all require coordinating application and database deploys. No migration tool has first-class primitives for the expand/contract pattern. Teams either skip the pattern (causing downtime) or implement it manually every time (error-prone).

### Problem 4: Large-table data migrations cause incidents

Backfilling a new column across 100M rows in a single transaction locks the table. The standard advice ("batch it") has no tooling. Engineers write one-off scripts, forget to throttle, forget to handle failures, forget to track progress.

### Problem 5: `\i` includes are not version-aware

Sqitch supports `\i file.sql` to include shared SQL. But if `shared/functions.sql` changes after a migration is written, replaying that migration on a fresh database uses the *current* version of the file — not the version that existed when the migration was created. Silent correctness bug, especially painful in CI.

### Problem 6: Migration tooling is invisible to AI agents

In the agentic development era, engineers increasingly have AI assistants writing and reviewing code. Migration tools have no affordances for this: no machine-readable output, no structured risk assessment, no integration with code review, no explanation of what SQL actually does.

---

## 3. Goals and non-goals

### Goals

- Drop-in CLI replacement for Sqitch — alias `sqitch` → `stitch` and nothing breaks
- PostgreSQL-only — depth over breadth, know the target platform deeply
- Single compiled binary — `bun build --compile`, no runtime deps, <50ms startup
- Static analysis as a first-class citizen — dangerous patterns caught before deploy, not after
- All advanced features are opt-in — v1.0 is safe to adopt without understanding expand/contract or batching
- AI-native — structured output, machine-readable risk reports, CI integration

### Non-goals

- MySQL, SQLite, Oracle, CockroachDB support — explicitly out of scope
- ORM integration (ActiveRecord, Django ORM, Alembic, Prisma) — out of scope
- GUI or web dashboard — CLI only, composable with other tools
- Cloud-hosted service — out of scope for now
- Replacing application-level migration frameworks for teams already happy with them

---

## 4. Requirements

### R1 — Sqitch CLI compatibility (mandatory)

All Sqitch commands must be supported with identical flags and semantics:

| Command | Description |
|---------|-------------|
| `stitch init [project]` | Initialize project, create `sqitch.conf` and `sqitch.plan` |
| `stitch add <name> [-n note] [-r dep] [-c change]` | Add new change |
| `stitch deploy [target] [--to change] [--mode [all\|change\|tag]]` | Deploy changes |
| `stitch revert [target] [--to change] [-y]` | Revert changes |
| `stitch verify [target] [--from change] [--to change]` | Run verify scripts |
| `stitch status [target]` | Show deployment status |
| `stitch log [target]` | Show deployment history |
| `stitch tag [name]` | Tag current deployment state |
| `stitch engine add\|alter\|remove\|show\|list` | Manage database engines |
| `stitch target add\|alter\|remove\|show\|list` | Manage deploy targets |
| `stitch config` | Read/write configuration |
| `stitch help [command]` | Show help |

Flags that must be supported: `--db-uri`, `--db-client`, `--plan-file`, `--top-dir`, `--registry`, `--quiet`, `--verbose`.

### R2 — Plan file format compatibility

`sqitch.plan` format must be parsed and written without modification. Existing Sqitch projects must be adoptable with zero file changes.

### R3 — Tracking schema compatibility

Sqitch tracking tables (`sqitch.changes`, `sqitch.events`, `sqitch.tags`, `sqitch.projects`) must be used as-is. Teams currently using Sqitch must be able to switch to stitch mid-project without re-deploying all migrations.

### R4 — Static analysis on deploy

`stitch deploy` must run static analysis before executing any SQL. On `error`-severity findings, deploy must be blocked (unless `--force` is passed). On `warn`, deploy proceeds with output.

### R5 — Machine-readable output

All commands must support `--format json` for structured output. Risk reports must be JSON-serializable.

### R6 — Exit codes

Standard exit codes: 0 = success, 1 = deploy failed, 2 = analysis blocked deploy, 3 = verification failed, 127 = database unreachable.

---

## 5. Feature ideas

### 5.1 Static analysis (v1.1)

Analyze migration SQL before deploy and flag dangerous patterns.

**Severity levels:**
- `error` — blocks deploy
- `warn` — prints warning, deploy proceeds
- `info` — informational

**Rules:**

| Rule ID | Severity | Trigger | Why dangerous |
|---------|----------|---------|---------------|
| `SA001` | error | `ADD COLUMN ... NOT NULL` without default | Takes `AccessExclusiveLock`, blocks all reads+writes |
| `SA002` | warn | `ADD COLUMN ... DEFAULT <volatile>` on PG < 11 | Full table rewrite |
| `SA003` | error | `ALTER COLUMN ... TYPE` (non-trivial cast) | Full table rewrite + `AccessExclusiveLock` |
| `SA004` | warn | `CREATE INDEX` without `CONCURRENT` | Blocks writes for duration |
| `SA005` | warn | `DROP INDEX` without `CONCURRENT` | Takes `AccessExclusiveLock` |
| `SA006` | warn | `DROP COLUMN` | Data loss, irreversible |
| `SA007` | error | `DROP TABLE` (non-revert context) | Data loss |
| `SA008` | warn | `TRUNCATE` | Data loss |
| `SA009` | warn | `ADD FOREIGN KEY` with no index on referencing column | Seq scan on FK check |
| `SA010` | error | `UPDATE` or `DELETE` without `WHERE` | Full table DML |
| `SA011` | warn | `UPDATE` or `DELETE` on large table (estimated rows > threshold) | Long lock, bloat |
| `SA012` | info | `ALTER SEQUENCE RESTART` | May break application assumptions |
| `SA013` | warn | `SET lock_timeout` missing before risky DDL | Runaway lock wait |
| `SA014` | warn | `VACUUM FULL` | Full table lock, avoid in migrations |
| `SA015` | error | `ALTER TABLE ... RENAME` without expand/contract | Breaks running application |

Configuration via `stitch.toml`:
```toml
[analysis]
error_on_warn = false
max_affected_rows = 10_000
skip = []
pg_version = 17               # affects which rules apply
```

### 5.2 Snapshot includes (v1.2)

`\i` / `\ir` includes resolved from the git commit where the migration was added — not HEAD.

```
stitch deploy               # uses historically-correct included files
stitch deploy --no-snapshot # falls back to HEAD (opt-out)
```

### 5.3 TUI — interactive deployment dashboard (v1.3)

When stdout is a TTY, stitch shows a live TUI during deploy:

```
stitch deploy
┌─ Deploying to production ──────────────────────────────┐
│  [✓] 001_create_users          12ms                    │
│  [✓] 002_create_orders         8ms                     │
│  [→] 003_add_order_status      running...              │
│  [ ] 004_create_indexes        pending                 │
│                                                        │
│  Analysis: 0 errors, 1 warning (SA004: missing CONCURRENT) │
│  Progress: 2/4 changes  ████████░░░░  50%             │
└────────────────────────────────────────────────────────┘
```

Plain output when piped (`--no-tui` or non-TTY).

### 5.4 Zero-downtime migrations — expand/contract (v2.0)

First-class support for the expand/contract pattern.

**Expand phase** (backward-compatible):
- Add new column alongside old
- Install trigger: writes to old column → synced to new column and vice versa
- Deploy application code that reads/writes both

**Contract phase** (after full app rollout):
- Verify all rows backfilled (stitch checks before proceeding)
- Drop sync trigger
- Drop old column or constraint

**CLI:**
```bash
stitch add rename_users_name --expand    # generates expand migration pair
stitch deploy --phase expand
stitch deploy --phase contract
stitch status                            # shows expand/contract state
```

Inspired by pgroll — but surgical (no full table recreation).

### 5.5 Batched background DML (v2.1)

Queue-based large-table data migrations, entirely inside Postgres.

**Queue architecture:**
- 3-partition rotating table (PGQ-inspired from SkyTools)
- No external dependencies (no Redis, no Kafka)
- All state visible in Postgres — `pg_stat_activity`, standard monitoring

**Job lifecycle:**
```
pending → running → done
               ↓
             failed → (retry) → done
                             → dead (max retries exceeded)
```

**CLI:**
```bash
stitch batch add backfill_user_tier --table users --batch-size 500 --sleep 100ms
stitch batch list
stitch batch status backfill_user_tier
stitch batch pause backfill_user_tier
stitch batch resume backfill_user_tier
stitch batch cancel backfill_user_tier
```

**Features:**
- Configurable batch size, sleep interval, lock timeout per batch
- Progress: rows done / rows remaining / ETA
- Per-batch transaction — each batch commits independently
- Inspired by GitLab `BatchedMigration`: throttling, pause/resume, retry, state tracking in Postgres

### 5.6 CI integration (v1.1+)

```bash
# GitHub Actions
stitch analyze --format github-annotations  # native GH annotation format
stitch analyze --format json | jq .         # structured for any CI

# GitLab CI
stitch analyze --format gitlab-codequality  # native GL code quality report

# General
stitch analyze --exit-code                  # non-zero if any errors found
```

**Example GitHub Actions step:**
```yaml
- name: Analyze migrations
  run: stitch analyze --format github-annotations
```

Annotations appear inline in PR diff — dangerous migration SQL highlighted at the line.

### 5.7 AI integration (v1.2+)

**`stitch explain <migration>`** — plain-English summary of what a migration does and its risk profile, via LLM.

**`stitch review`** — structured risk report suitable for posting as a PR comment (Markdown output). Designed to be called by AI coding agents reviewing PRs.

**`stitch suggest-revert <migration>`** — LLM-assisted revert script generation when no revert was written.

**`stitch chat`** — interactive mode: ask questions about the migration history, planned changes, risk.

### 5.8 DBLab integration (v3.0)

Test deploy + revert against a full-size production clone before touching prod.

```bash
stitch deploy --dblab-url https://dblab.example.com --dblab-token $TOKEN
# stitch provisions a clone, runs deploy+verify+revert, reports result
# No prod changes until clone test passes
```

The PostgresAI native advantage — no other migration tool can offer this.

### 5.9 Lock timeout guard (v1.1)

Automatically prepend `SET lock_timeout = '5s'` before any DDL that could take a long lock, unless the migration already sets it. Configurable. Can be disabled.

### 5.10 Dry-run mode

```bash
stitch deploy --dry-run   # prints what would be deployed, runs analysis, exits
```

### 5.11 Migration diff

```bash
stitch diff               # show schema diff between deployed and plan
stitch diff --from tag_a --to tag_b
```

---

## 6. Design decisions

### DD1 — TypeScript + Bun, not Rust

Rust would give a faster binary but TypeScript + Bun gives:
- `bun build --compile` produces a single static binary, fast enough (<50ms startup)
- Easier onboarding for contributors (more TypeScript developers than Rust)
- Faster iteration — spec is still evolving
- `pg` (node-postgres) is a mature, battle-tested driver

Revisit if performance becomes an issue.

### DD2 — PostgreSQL-only

Depth beats breadth. Sqitch's multi-DB support is one reason it can't do PG-specific things (CONCURRENT indexes, advisory locks, partition introspection, `pg_stat_activity`). We know our target. Every feature can assume PG-native primitives.

### DD3 — Sqitch tracking schema compatibility

We use the existing Sqitch tables (`sqitch.changes`, etc.) rather than our own schema. Reason: zero migration cost for existing Sqitch users. A team can `alias sqitch=stitch` and evaluate us before committing. This is the adoption path.

### DD4 — SQL parser

We use a PostgreSQL-aware SQL parser for static analysis, not regex.

Options evaluated:
- `pgsql-parser` (npm) — JS wrapper around the actual PG parser. Exact fidelity, same AST as Postgres. Use this.
- `pg-query-parser` — older, same approach
- Hand-rolled regex — too fragile for production rules

Decision: **`pgsql-parser`** (or equivalent). If AST is unavailable for some construct, fall back to regex with a clear comment.

### DD5 — Plan file is source of truth

stitch never modifies `sqitch.plan` without an explicit command. The plan file is append-only during `add`, never rewritten during deploy/revert.

### DD6 — No magic sequencing

Like Sqitch, stitch uses explicit dependency declarations (`-r dep1 -r dep2`), not sequential numbers. Sequential numbers create false ordering assumptions and merge conflicts. Dependencies are explicit.

### DD7 — Expand/contract is opt-in

The expand/contract pattern requires application-side changes. stitch never automatically applies it. It provides the primitives and tracks state. Engineers choose when to use it.

### DD8 — All state in Postgres

No lock files, no local state files, no `.stitch/` directory with runtime state. Everything that matters (what's deployed, batch job state, expand/contract phase) lives in the database. This makes stitch safe to run from multiple machines (CI + developer laptop) without coordination.

### DD9 — 3-partition queue, not SKIP LOCKED

For batched DML, we use a PGQ-style 3-partition rotating table rather than `SELECT ... FOR UPDATE SKIP LOCKED`. Reason: partition rotation provides automatic cleanup, explicit job state tracking, and visibility into queue depth without scanning all rows.

### DD10 — No hidden network calls

stitch never calls external services without explicit configuration. No telemetry, no update checks, no LLM calls unless `stitch explain`/`stitch review` is explicitly invoked.

---

## 7. Architecture

```
stitch/
├── src/
│   ├── cli.ts                  # Entry point, command routing
│   ├── commands/
│   │   ├── init.ts
│   │   ├── add.ts
│   │   ├── deploy.ts
│   │   ├── revert.ts
│   │   ├── verify.ts
│   │   ├── status.ts
│   │   ├── log.ts
│   │   ├── tag.ts
│   │   ├── analyze.ts          # Static analysis (standalone)
│   │   ├── explain.ts          # AI explain
│   │   ├── batch.ts            # Batched DML commands
│   │   └── diff.ts
│   ├── plan/
│   │   ├── parser.ts           # sqitch.plan parser
│   │   ├── writer.ts           # sqitch.plan writer
│   │   └── types.ts            # Change, Tag, Dependency types
│   ├── db/
│   │   ├── client.ts           # pg connection wrapper
│   │   ├── registry.ts         # sqitch.* table operations
│   │   └── introspect.ts       # Schema introspection for analysis
│   ├── analysis/
│   │   ├── index.ts            # Analyzer entry point
│   │   ├── parser.ts           # SQL AST parsing (pgsql-parser)
│   │   ├── rules/
│   │   │   ├── SA001.ts        # One file per rule
│   │   │   ├── SA002.ts
│   │   │   └── ...
│   │   └── reporter.ts         # Output formatting (text/json/github/gitlab)
│   ├── includes/
│   │   └── snapshot.ts         # git-aware \i / \ir resolution
│   ├── expand-contract/
│   │   ├── generator.ts        # Generate expand/contract migration pairs
│   │   └── tracker.ts          # Track phase state in Postgres
│   ├── batch/
│   │   ├── queue.ts            # 3-partition queue schema + operations
│   │   ├── worker.ts           # Batch execution loop
│   │   └── progress.ts         # Progress tracking + ETA
│   ├── tui/
│   │   └── deploy.ts           # Interactive deploy dashboard
│   ├── ai/
│   │   ├── explain.ts          # Migration explainer
│   │   └── review.ts           # PR review comment generator
│   ├── config.ts               # stitch.toml + sqitch.conf parsing
│   └── output.ts               # Shared output formatting
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── SPEC.md
├── README.md
├── package.json
├── tsconfig.json
└── stitch.toml.example
```

### Data flow — deploy

```
stitch deploy
  → parse sqitch.conf + stitch.toml
  → connect to database
  → read sqitch.* tracking tables
  → compute pending changes (topological sort by dependency)
  → for each pending change:
      → resolve \i includes (snapshot or HEAD)
      → run static analysis
      → if error: abort (unless --force)
      → if warn: print, continue
      → BEGIN
      → execute deploy script
      → update sqitch.changes, sqitch.events
      → COMMIT
  → print summary
```

---

## 8. Testing strategy

### Unit tests

- Plan parser: round-trip parse/write for all valid sqitch.plan formats
- Each analysis rule: SQL strings that should trigger / should not trigger
- Config parser: all valid sqitch.conf + stitch.toml combinations
- Snapshot includes: mock git, verify correct commit resolution

### Integration tests

Real Postgres (Docker). Test matrix: PG 14, 15, 16, 17, 18.

For each PG version:
- `init` → creates correct directory structure and conf files
- `add` → creates correct migration files, updates plan
- `deploy` → executes SQL, updates tracking tables correctly
- `revert` → reverts correctly, updates tracking tables
- `verify` → runs verify script, passes/fails correctly
- `status` → accurate pending/deployed counts
- `log` → correct history
- `deploy --dry-run` → no DB changes
- Dependency ordering: deploy respects declared deps
- Partial deploy: `--to <change>` stops at correct point
- Failed deploy: tracking tables left in correct state, revert possible

### Analysis rule tests

For each rule SA001–SA015:
- SQL that must trigger the rule at correct severity
- SQL that must NOT trigger (false positive prevention)
- SQL that's safe on PG 17 but dangerous on PG 14 (version-aware rules)

### Compatibility tests

Run against a real Sqitch project:
- Existing `sqitch.plan` parses without modification
- Existing Sqitch tracking tables are read correctly
- `stitch status` matches what `sqitch status` would show
- `stitch deploy` continues from where `sqitch deploy` left off

### CI matrix

```yaml
# .github/workflows/ci.yml
strategy:
  matrix:
    pg: [14, 15, 16, 17, 18]
    os: [ubuntu-latest, macos-latest]
```

---

## 9. Implementation plan

### Phase 0 — foundation (Sprint 1, ~1 week)

Core infrastructure. Nothing user-visible yet.

- [ ] Repo structure: `src/`, `tests/`, `fixtures/`
- [ ] `tsconfig.json`, `package.json` with bun
- [ ] `bun build --compile` producing single binary
- [ ] CI: GitHub Actions running `bun test` on push
- [ ] Docker Compose for local PG test matrix (PG 14–18)
- [ ] `src/config.ts`: parse `sqitch.conf` and `stitch.toml`
- [ ] `src/db/client.ts`: pg connection wrapper, URI parsing, error handling
- [ ] `src/output.ts`: shared print/error/json output helpers
- [ ] `src/cli.ts`: command router with `--help`, `--version`, `--format`

### Phase 1 — Sqitch parity (Sprint 2–4, ~3 weeks)

After this phase: drop-in replacement for all Sqitch commands.

**Sprint 2 — plan + tracking**
- [ ] `src/plan/parser.ts`: full sqitch.plan format parser
- [ ] `src/plan/writer.ts`: sqitch.plan writer (append-only for `add`)
- [ ] `src/plan/types.ts`: Change, Tag, Dependency, Project types
- [ ] `src/db/registry.ts`: read/write `sqitch.changes`, `sqitch.events`, `sqitch.tags`, `sqitch.projects`
- [ ] `stitch init`: creates sqitch.conf, sqitch.plan, deploy/revert/verify dirs
- [ ] `stitch add`: creates migration files, appends to plan
- [ ] Tests: plan round-trip, init, add

**Sprint 3 — deploy + revert**
- [ ] `src/commands/deploy.ts`: topological sort, execute deploy scripts, update tracking
- [ ] `src/commands/revert.ts`: execute revert scripts in reverse order, update tracking
- [ ] `--to <change>` flag for both
- [ ] `--dry-run` flag
- [ ] `--mode [all|change|tag]` flag
- [ ] Partial deploy / revert with dependency validation
- [ ] Tests: deploy/revert, partial, dry-run, failed deploy recovery

**Sprint 4 — verify + status + log + remaining commands**
- [ ] `stitch verify`: run verify scripts, report pass/fail per change
- [ ] `stitch status`: pending count, deployed count, last deployed, target info
- [ ] `stitch log`: deployment history with timestamps and committers
- [ ] `stitch tag`: create tag at current deployment state
- [ ] `stitch engine`, `stitch target`, `stitch config`: manage configuration
- [ ] Compatibility test: adopt existing Sqitch project, verify parity
- [ ] Tests: verify, status, log, tag

### Phase 2 — static analysis (Sprint 5–6, ~2 weeks)

**Sprint 5 — analysis engine**
- [ ] Integrate `pgsql-parser` (npm): parse SQL to AST
- [ ] `src/analysis/index.ts`: analyzer entry point, rule registry
- [ ] `src/analysis/reporter.ts`: text / json / github-annotations / gitlab-codequality output
- [ ] `stitch analyze <file>`: standalone analysis command
- [ ] Analysis integrated into `stitch deploy` (pre-deploy, blocks on error)
- [ ] `--force` flag to bypass analysis errors
- [ ] `--format github-annotations` for CI
- [ ] Rules SA001–SA007 (lock-related + drop)

**Sprint 6 — remaining rules + version awareness**
- [ ] Rules SA008–SA015 (DML safety + sequence + lock timeout)
- [ ] PG version awareness in rules (SA002 PG < 11, etc.)
- [ ] `stitch.toml` `[analysis]` config: skip, error_on_warn, max_affected_rows, pg_version
- [ ] Tests: all rules trigger/no-trigger, version-aware behavior
- [ ] Exit code 2 when analysis blocks deploy

### Phase 3 — snapshot includes (Sprint 7, ~1 week)

- [ ] `src/includes/snapshot.ts`: resolve `\i`/`\ir` from git history
- [ ] Parse migration SQL for `\i` / `\ir` directives before deploy
- [ ] `git show <commit>:path` for each include, using migration's commit from plan
- [ ] Fallback to HEAD when no git repo or file has no history
- [ ] `--no-snapshot` flag to disable
- [ ] Tests: snapshot resolution, fallback, no-git fallback

### Phase 4 — TUI + CI polish (Sprint 8, ~1 week)

- [ ] `src/tui/deploy.ts`: live deploy dashboard (TTY detection, plain fallback)
- [ ] `--format json` on all commands: structured output
- [ ] `stitch diff`: schema diff between deployed state and plan
- [ ] Lock timeout guard: auto-prepend `SET lock_timeout` before risky DDL (configurable)
- [ ] Man page generation
- [ ] Homebrew formula

### Phase 5 — expand/contract (Sprint 9–10, ~2 weeks)

- [ ] `src/expand-contract/generator.ts`: generate expand + contract migration pair from column rename/type change
- [ ] `src/expand-contract/tracker.ts`: track phase state in Postgres (new table in `stitch.*` schema)
- [ ] `stitch add --expand`: generate linked pair
- [ ] `stitch deploy --phase expand|contract`
- [ ] Trigger generation for old↔new column sync
- [ ] View shim for backward compat during transition
- [ ] Backfill verification before contract phase
- [ ] Tests: full expand/contract cycle, rollback, trigger correctness

### Phase 6 — batched background DML (Sprint 11–13, ~3 weeks)

- [ ] `src/batch/queue.ts`: 3-partition rotating queue schema (PGQ-inspired), DDL migration, partition rotation logic
- [ ] `src/batch/worker.ts`: batch execution loop, lock_timeout per batch, sleep, retry
- [ ] `src/batch/progress.ts`: row counting, ETA, `pg_stat_activity` integration
- [ ] `stitch batch add`: register job, create queue entry
- [ ] `stitch batch list`: show all jobs and state
- [ ] `stitch batch status <job>`: progress, ETA, recent errors
- [ ] `stitch batch pause|resume|cancel <job>`
- [ ] Tests: full job lifecycle, pause/resume, retry on failure, max retries → dead

### Phase 7 — AI + DBLab (Sprint 14–16, ~3 weeks)

- [ ] `src/ai/explain.ts`: LLM-powered migration explainer (OpenAI/Anthropic/Ollama)
- [ ] `stitch explain <migration>`: plain-English + risk summary
- [ ] `stitch review`: Markdown PR comment with analysis results + explanation
- [ ] `stitch suggest-revert <migration>`: LLM-assisted revert generation
- [ ] DBLab integration: `--dblab-url`, `--dblab-token` flags on `deploy`
- [ ] Clone provisioning, deploy+verify+revert on clone, report before touching prod
- [ ] Tests: mock LLM responses, DBLab API mock

---

## Prior art summary

| Tool | What we take | What we don't |
|------|-------------|---------------|
| Sqitch | CLI interface, plan format, tracking schema | Perl runtime, multi-DB |
| pgroll | Expand/contract pattern concept | Full table recreation |
| migrationpilot | Dangerous pattern detection ideas | Thin implementation |
| GitLab migration helpers | Batched DML design, throttling, retry | Rails dependency |
| SkyTools PGQ | 3-partition queue architecture | External daemon |
| Flyway / Liquibase | Nothing | Wrong philosophy |

---

*This spec is a living document. Update it before writing code.*
