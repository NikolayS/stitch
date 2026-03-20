# stitch — Product Specification

**Version:** 0.1 (draft)
**Status:** Pre-development

---

## 1. Problem

Schema migrations are one of the highest-risk operations in production databases. Existing tools solve the *logistics* of migrations (ordering, tracking, rollback) but ignore the *safety* of what the migration actually does to a running system.

Sqitch is the best tool in this space — dependency-aware, database-native, no ORM coupling — but it is written in Perl (hard to distribute, hard to contribute to), has no static analysis, and has no primitives for zero-downtime changes or large-table surgery.

Engineers working on PostgreSQL at scale need:

1. A reliable migration runner they can trust (Sqitch compat)
2. A linter that catches dangerous patterns before they reach production
3. Primitives for schema changes that don't lock tables
4. Background DML tooling for large-table data migrations
5. git-aware tooling that understands migrations as historical artifacts

---

## 2. Goals

- Drop-in replacement for Sqitch (same CLI surface, same plan format)
- PostgreSQL-only — depth over breadth
- Single binary via Bun compile — no runtime deps, fast startup
- Static analysis as a first-class citizen, not an afterthought
- All advanced features (zero-downtime, batching) are opt-in — v1.0 is safe to adopt without understanding them

---

## 3. Non-goals

- Multi-database support (MySQL, SQLite, Oracle) — explicitly out of scope
- ORM integration (ActiveRecord, Django, Alembic) — out of scope
- GUI / web dashboard — CLI only
- Cloud-hosted migration service — out of scope for now

---

## 4. Tech stack

- **Language:** TypeScript
- **Runtime/bundler:** Bun (single compiled binary output)
- **Database driver:** `pg` (node-postgres)
- **Testing:** `bun test`
- **Target platforms:** Linux x86_64, Linux aarch64, macOS x86_64, macOS aarch64

---

## 5. Sqitch compatibility

stitch must be a drop-in CLI replacement. Engineers should be able to alias `sqitch` → `stitch` and have everything work.

### Commands

| Command | Sqitch behavior | stitch v1.0 |
|---------|----------------|-------------|
| `add <name>` | Create migration files | ✓ |
| `deploy [target]` | Apply pending migrations | ✓ |
| `revert [target]` | Roll back migrations | ✓ |
| `verify [target]` | Run verify scripts | ✓ |
| `status` | Show deployment state | ✓ |
| `log` | Show deployment history | ✓ |
| `init` | Initialize project | ✓ |
| `engine` | Manage database engines | ✓ |
| `target` | Manage deploy targets | ✓ |

### Plan file format

`sqitch.plan` files must be parsed and written in the existing Sqitch format. No format migration required.

### Change tracking table

Sqitch uses `sqitch.changes`, `sqitch.events`, `sqitch.tags`, `sqitch.projects`. stitch uses the same schema by default so existing deployments can be adopted without re-deploying all migrations.

---

## 6. Static analysis (v1.1)

Before deploying, stitch analyzes migration SQL and flags dangerous patterns.

### Severity levels

- **error** — blocks deploy (configurable to warn)
- **warn** — prints warning, deploy proceeds
- **info** — informational only

### Rules

| Rule | Severity | Description |
|------|----------|-------------|
| `lock-not-null` | error | `ALTER TABLE ... ADD COLUMN ... NOT NULL` without default or pre-existing data check |
| `lock-add-column-default` | warn | `ADD COLUMN` with non-volatile default on PG < 11 (rewrites table) |
| `lock-alter-type` | error | Changing column type (rewrites table or takes `AccessExclusiveLock`) |
| `missing-concurrent` | warn | `CREATE INDEX` or `DROP INDEX` without `CONCURRENT` |
| `drop-column` | warn | `DROP COLUMN` — data loss, irreversible |
| `drop-table` | error | `DROP TABLE` without `IF EXISTS` or in revert-only context |
| `truncate` | warn | `TRUNCATE` — data loss |
| `unindexed-fk` | warn | `ADD FOREIGN KEY` with no index on referencing column |
| `bulk-dml-no-where` | error | `UPDATE` or `DELETE` without `WHERE` clause |
| `bulk-dml-no-estimate` | warn | `UPDATE` or `DELETE` touching estimated > N rows (configurable) |
| `sequence-gap` | info | `ALTER SEQUENCE` — may cause application errors if gaps not expected |

Rules are inspired by and reimplemented from:
- https://github.com/mickelsamuel/migrationpilot
- GitLab's migration linter

### Configuration

```toml
# stitch.toml
[analysis]
error_on_warn = false          # promote all warns to errors
max_affected_rows = 10000      # threshold for bulk-dml-no-estimate
skip = ["missing-concurrent"]  # disable specific rules
```

---

## 7. Snapshot includes (v1.2)

Sqitch supports `\i` and `\ir` in migration files to include shared SQL (e.g. common functions, enums). The problem: if the shared file changes after a migration is written, replaying that migration (e.g. on a fresh database) uses the *current* version of the included file, not the version that existed when the migration was created.

stitch resolves `\i` / `\ir` includes using the git history of the file at the commit where the migration was added to `sqitch.plan`.

### Behavior

- On `deploy`, for each migration, stitch resolves its git commit from the plan file
- `\i path/to/file.sql` is resolved to `git show <commit>:path/to/file.sql`
- The historically-correct version of the file is used, not HEAD
- Falls back to HEAD if the file has no git history (new repo, no commits)

### Feature name

**Snapshot includes** — included files are resolved from a point-in-time snapshot of the repository, correlated to the migration's commit.

### Configuration

```toml
[includes]
snapshot = true    # default: true when git repo detected
```

---

## 8. Zero-downtime migrations — expand/contract (v2.0)

Large-scale schema changes require a two-phase approach so application code and database schema can be deployed independently.

### Pattern

**Expand phase** (backward-compatible schema change):
- Add new column alongside old column
- Add trigger to sync writes: old → new and new → old
- Deploy new application code that writes to both columns

**Contract phase** (cleanup after application fully deployed):
- Verify all rows backfilled
- Remove sync trigger
- Drop old column

### Implementation

- stitch tracks expand/contract migrations as linked pairs in the plan
- `stitch deploy --phase expand` / `stitch deploy --phase contract`
- Trigger-based sync is generated automatically from column type metadata
- Automatic view shim: old column name → new column location (for read compatibility)
- Inspired by https://github.com/xataio/pgroll but surgical — no full table recreation

### When to use

- Renaming a column
- Changing a column type (where the cast is safe)
- Splitting one column into two
- Adding a NOT NULL constraint to an existing column

---

## 9. Batched background DML (v2.1)

Large-table data migrations (e.g. backfilling a new column, transforming values) cannot run in a single transaction without locking the table for minutes or hours. stitch provides a queue-based batching primitive that runs entirely inside Postgres.

### Queue design

3-partition rotating table (PGQ-inspired, from SkyTools):
- No external queue (no Redis, no Kafka, no RabbitMQ)
- Batches are rows in a Postgres table, visible to monitoring tools
- Partition rotation provides automatic cleanup of completed batches
- All state in Postgres — survives application restarts

### Job lifecycle

```
pending → running → done
                 → failed → retry → done
                                 → dead
```

### Features

- Configurable batch size (default: 1000 rows)
- Configurable sleep between batches (default: 100ms)
- Pause / resume / cancel via `stitch batch <job> pause|resume|cancel`
- Progress tracking: rows done, rows remaining, ETA
- Per-batch transaction scope — each batch commits independently
- Maximum lock timeout per batch (configurable, default: 2s)
- Inspired by GitLab's `BatchedMigration` framework

### Study references

- GitLab background migration helpers (Rails/ActiveRecord)
- SkyTools PGQ architecture
- https://gitlab.com/gitlab-org/gitlab/-/tree/master/lib/gitlab/database/migration_helpers

---

## 10. Intelligence layer (v3.0)

- **Migration explainer:** plain-English description of what a migration does and its risk profile, powered by LLM
- **Conflict detection:** two pending migrations touching the same table/column, flagged before deploy
- **Rollback inference:** auto-generate revert script from deploy script using LLM + schema introspection
- **DBLab integration:** test deploy+revert against a full-size production clone before touching prod — the PostgresAI native advantage. No other migration tool can offer this.

---

## 11. Versioning and releases

Semantic versioning. `main` is unstable. Install from release tags.

```bash
# Install v1.0.0 (example — not yet released)
git clone --branch v1.0.0 --depth 1 https://github.com/NikolayS/stitch.git
cd stitch
bun install
bun run build
sudo cp dist/stitch /usr/local/bin/
```

---

## 12. Prior art

| Tool | What we take | What we don't |
|------|-------------|---------------|
| Sqitch | CLI interface, plan format, change tracking schema | Perl runtime, multi-DB surface area |
| pgroll | Expand/contract pattern concept | Full table recreation approach |
| migrationpilot | Dangerous pattern detection ideas | Node/JS implementation |
| GitLab migration helpers | Batched DML design, throttling, retry logic | Rails dependency |
| Flyway / Liquibase | Nothing — wrong philosophy (sequential numbered files) | — |

---

*This spec is a living document. Update it before writing code.*
