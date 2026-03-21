# sqlever -- Sqitch-compatible PostgreSQL migration tool

[![CI](https://github.com/NikolayS/sqlever/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/sqlever/actions/workflows/ci.yml) [![npm version](https://img.shields.io/npm/v/sqlever)](https://www.npmjs.com/package/sqlever) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE) [![Bun](https://img.shields.io/badge/bun-1.1%2B-orange.svg)](https://bun.sh)
<!-- TODO: add codecov badge once coverage reporting is wired up -->

Sqitch-compatible PostgreSQL migration tool with static analysis, expand/contract support, batched DML, and AI-powered explanations.

---

## Why sqlever

- **Sqitch compatible** -- drop-in CLI replacement. Existing `sqitch.plan` files, tracking schemas, and workflows work unchanged.
- **Static analysis built in** -- 22 rules catch dangerous migration patterns (lock-heavy DDL, data loss, table rewrites) before deploy, not after.
- **Expand/contract migrations** -- generate paired expand + contract changes with bidirectional sync triggers.
- **Batched DML** -- backfill millions of rows without locking, with replication lag and VACUUM pressure monitoring.
- **AI-powered** -- `sqlever explain` summarizes migrations in plain English; `sqlever review` generates structured PR comments.
- **Single binary** -- compiled with Bun, no runtime dependencies. Sub-50ms startup. No Perl, no JVM, no Docker required.
- **100% open source** -- every feature ships under Apache 2.0. No paywalled "Pro" tier for safety rules or CI integrations.

## Quick start

Install (see [Distribution](#distribution) for all options):

```bash
# Run without installing
npx sqlever --help
bunx sqlever --help

# Install globally
npm install -g sqlever

# Or download binary from GitHub Releases
# https://github.com/NikolayS/sqlever/releases
```

Create a project, add a migration, deploy, and analyze:

```bash
# Initialize a new project
sqlever init myapp --engine pg

# Add a migration
sqlever add create_users -n "Create users table"

# Edit the generated SQL files
cat > deploy/create_users.sql << 'SQL'
CREATE TABLE users (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
SQL

# Analyze before deploying -- catch problems early
sqlever analyze

# Deploy to the database
sqlever deploy db:pg://localhost/myapp

# Verify the deployment
sqlever verify db:pg://localhost/myapp

# Check status
sqlever status db:pg://localhost/myapp
```

<!-- GIF demos rendered from demos/*.tape with https://github.com/charmbracelet/vhs -->
<!-- Uncomment once GIFs are rendered:
![Deploy demo](demos/demo1_deploy.gif)
![Analyze demo](demos/demo2_analyze.gif)
-->

## Features

### Snapshot includes

Deploy scripts that use `\i` or `\ir` to include shared SQL files get automatic git-correlated resolution. When sqlever deploys a migration, each `\i` resolves to the file version from when the migration was written, not the current HEAD. This means deploying on a fresh database produces the same result as deploying when the migration was originally written, even if the included files have changed since.

```sql
-- deploy/add_audit_trigger.sql
BEGIN;
\ir ../shared/audit_trigger.sql
COMMIT;
```

When this migration was added on January 15, `shared/audit_trigger.sql` contained v1 of the trigger function. By March, that file was rewritten for v2. Without snapshot includes, deploying on a fresh database would apply v2 of the trigger with v1's assumptions -- a subtle and dangerous mismatch. sqlever resolves `\ir ../shared/audit_trigger.sql` to the January 15 version automatically.

Pass `--no-snapshot` to disable this behavior and use current HEAD versions (Sqitch-compatible).

### TUI deploy dashboard

When stdout is a TTY, `sqlever deploy` shows a live-updating progress dashboard with per-change status, timing, analysis warnings, and a progress bar. Pipe-friendly plain text output is used automatically when stdout is not a TTY, or when `--no-tui` is passed.

### Static analysis at deploy time

`sqlever deploy` runs all 22 analysis rules before executing SQL and blocks on error-severity findings. Bypass with `--force`. Run standalone with `sqlever analyze` against any `.sql` file or directory -- no `sqitch.plan` required.

### Project health checks

`sqlever doctor` validates your project setup in one command: plan file parsing, change ID chain consistency, script file presence, psql metacommand detection, and syntax version checks.

## Commands

All Sqitch commands are supported with identical flags and semantics, plus sqlever extensions.

| Command | Description |
|---------|-------------|
| `sqlever init` | Initialize project, create `sqitch.conf` and `sqitch.plan` |
| `sqlever add` | Add a new migration change |
| `sqlever deploy` | Deploy changes to a database (runs analysis first) |
| `sqlever revert` | Revert changes from a database |
| `sqlever verify` | Run verify scripts against a database |
| `sqlever status` | Show deployment status |
| `sqlever log` | Show deployment history |
| `sqlever tag` | Tag the current deployment state |
| `sqlever rework` | Rework an existing change |
| `sqlever show` | Display change/tag details or script contents |
| `sqlever plan` | Display plan contents |
| `sqlever analyze` | Analyze migration SQL for dangerous patterns |
| `sqlever doctor` | Validate project setup, plan file, and script consistency |
| `sqlever diff` | Show pending changes or differences between two tags |
| `sqlever batch` | Run batched DML with progress, lag monitoring, and backpressure |
| `sqlever explain` | AI-powered plain-English summary of a migration file |
| `sqlever review` | Generate structured PR review comments from analysis findings |
| `sqlever deploy --dblab` | Deploy against a DBLab thin clone for safe testing |

All commands support `--format json` for machine-readable output.

## Analysis rules

`sqlever analyze` runs 22 rules against your migration SQL. Rules are classified as **static** (SQL-only, no database connection needed), **connected** (requires live database), or **hybrid** (static check always runs; connected check refines when a database is available).

| Rule | Severity | Type | Description |
|------|----------|------|-------------|
| SA001 | error | static | `ADD COLUMN ... NOT NULL` without `DEFAULT` -- fails on populated tables |
| SA002 | error | static | `ADD COLUMN ... DEFAULT <volatile>` -- full table rewrite on all PG versions |
| SA002b | warn | static | `ADD COLUMN ... DEFAULT` on PG < 11 -- table rewrite on older versions |
| SA003 | error | static | `ALTER COLUMN ... TYPE` with unsafe cast -- table rewrite + `AccessExclusiveLock` |
| SA004 | warn | static | `CREATE INDEX` without `CONCURRENTLY` -- blocks writes for duration |
| SA005 | warn | static | `DROP INDEX` without `CONCURRENTLY` -- takes `AccessExclusiveLock` |
| SA006 | warn | static | `DROP COLUMN` -- irreversible data loss |
| SA007 | error | static | `DROP TABLE` -- data loss (exempt in revert scripts) |
| SA008 | warn | static | `TRUNCATE` -- data loss |
| SA009 | warn | hybrid | `ADD FOREIGN KEY` without `NOT VALID` -- holds locks on both tables |
| SA010 | warn | static | `UPDATE` / `DELETE` without `WHERE` -- full table DML |
| SA011 | warn | connected | `UPDATE` / `DELETE` on large table -- needs row count from `pg_class` |
| SA012 | info | static | `ALTER SEQUENCE RESTART` -- may break application assumptions |
| SA013 | warn | static | Missing `SET lock_timeout` before risky DDL |
| SA014 | warn | static | `VACUUM FULL` / `CLUSTER` -- full table lock and rewrite |
| SA015 | warn | static | `ALTER TABLE ... RENAME` -- breaks running applications |
| SA016 | error | static | `ADD CONSTRAINT ... CHECK` without `NOT VALID` -- full table scan under lock |
| SA017 | warn | hybrid | `ALTER COLUMN ... SET NOT NULL` -- table scan on PG < 12; safe with valid CHECK |
| SA018 | warn | hybrid | `ADD PRIMARY KEY` without pre-existing index -- extends lock duration |
| SA019 | warn | static | `REINDEX` without `CONCURRENTLY` -- takes `AccessExclusiveLock` |
| SA020 | error | static | `CONCURRENTLY` inside transactional deploy -- fails at runtime |
| SA021 | warn | static | `LOCK TABLE` -- explicit locking is a code smell in migrations |

### Suppressing rules

Per-statement with SQL comments:

```sql
-- sqlever:disable SA010
UPDATE users SET tier = 'free';
-- sqlever:enable SA010
```

Single-line: `UPDATE users SET tier = 'free'; -- sqlever:disable SA010`

Per-file in `sqlever.toml`:

```toml
[analysis.overrides."deploy/backfill_tiers.sql"]
skip = ["SA010"]
```

Globally:

```toml
[analysis]
skip = ["SA002b"]
pg_version = 14
```

## Migration from Sqitch

sqlever reads `sqitch.conf`, `sqitch.plan`, and the `sqitch.*` tracking schema without modification. To switch:

```bash
alias sqitch=sqlever
```

**What works unchanged:**

- All plan file formats, pragmas, and dependency syntax
- Deploy/revert/verify workflows with identical flags
- Tracking schema -- sqlever reads and writes the same `sqitch.changes`, `sqitch.tags`, `sqitch.events` tables
- `--db-uri`, `--target`, `--set`, `--log-only`, `--registry`, and all other standard flags
- `rework`, cross-project dependencies, `@tag` references

**What sqlever adds:**

- `deploy` runs static analysis before executing SQL and blocks on error-severity findings (bypass with `--force`)
- `analyze` command for standalone linting (works without a `sqitch.plan` -- point it at any `.sql` file or directory)
- `--format json` on all commands for CI integration
- `--format github-annotations` and `--format gitlab-codequality` for native CI annotations
- Lock timeout guard auto-prepended before risky DDL (configurable in `sqlever.toml`)

## Comparison

| | sqlever | Sqitch | Atlas | Flyway |
|---|---------|--------|-------|--------|
| Migration style | Imperative (plain SQL) | Imperative (plain SQL) | Declarative + versioned | Sequential numbered files |
| Static analysis | 22 rules, built in | None | ~12 rules (Pro-only for PG) | None |
| PostgreSQL depth | Advisory locks, PgBouncer detection, replication lag monitoring | Basic | Good | Basic |
| Sqitch compatibility | Full | -- | None | None |
| Runtime | Single binary (Bun) | Perl + CPAN | Go binary | JVM |
| License | Apache 2.0 (all features) | MIT | Apache 2.0 (core) + proprietary Pro | Apache 2.0 (Community) |
| Non-transactional DDL | Write-ahead tracking with crash recovery | Manual | `--tx-mode none` (no recovery) | Manual |
| Expand/contract | Built in (sync triggers, phase tracking) | None | None | None |
| Batched DML | Built in (PGQ, lag monitoring, backpressure) | None | None | None |
| AI explanations | Built in (`explain`, `review`) | None | None | None |

## Configuration

### `sqitch.conf`

Standard Sqitch INI-format configuration. sqlever reads it as-is:

```ini
[core]
    engine = pg
    plan_file = sqitch.plan
    top_dir = .

[engine "pg"]
    target = db:pg://localhost/myapp
    registry = sqitch
```

### `sqlever.toml`

sqlever-specific configuration. Optional -- sensible defaults apply:

```toml
[analysis]
pg_version = 14               # minimum PG version to target
error_on_warn = false          # treat warnings as errors
skip = []                      # globally skip these rules
max_affected_rows = 10_000     # threshold for SA011

[analysis.rules.SA002b]
severity = "off"               # disable a specific rule

[analysis.overrides."deploy/seed_data.sql"]
skip = ["SA010"]               # suppress per file
```

## Distribution

### npm

```bash
npm install -g sqlever
```

### Docker

```bash
docker run --rm sqlever/sqlever deploy db:pg://host.docker.internal/myapp
```

The image is based on Alpine with `psql` included.

### GitHub Releases

Pre-built binaries for 4 platforms are attached to every [GitHub Release](https://github.com/NikolayS/sqlever/releases):

| Binary | Platform |
|--------|----------|
| `sqlever-linux-amd64` | Linux x86_64 |
| `sqlever-linux-arm64` | Linux ARM64 |
| `sqlever-macos-amd64` | macOS x86_64 |
| `sqlever-macos-arm64` | macOS Apple Silicon |

### Build from source

```bash
bun install
bun build src/cli.ts --compile --outfile dist/sqlever
```

The output is a single self-contained binary with no runtime dependencies.

## Contributing

See [spec/SPEC.md](spec/SPEC.md) for the full design specification.

Run tests:

```bash
bun install
bun test                       # all tests
bun test tests/unit/           # unit tests only
bun test tests/integration/    # integration tests (requires PostgreSQL)
```

Type-check:

```bash
bun x tsc --noEmit
```

Build:

```bash
bun run build                  # produces dist/sqlever
```

## License

[Apache 2.0](LICENSE)
