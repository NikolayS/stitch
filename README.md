# stitch

A drop-in replacement for [Sqitch](https://sqitch.org/) — PostgreSQL-only, written in TypeScript/Bun, with static analysis and zero-downtime migration primitives.

## Why

Sqitch is solid but it's Perl, has no awareness of what a migration *does* to a running database, and has no path toward zero-downtime schema changes. stitch keeps everything that works and adds what's missing.

## Status

Early development. Not ready for production use.

## Roadmap

**v1.0 — drop-in replacement**
- Sqitch command compatibility: `add`, `deploy`, `revert`, `verify`, `status`, `log`
- PostgreSQL-only
- TypeScript/Bun: single binary, fast startup, no Perl dependency
- `sqitch.plan` file format compatibility

**v1.1 — static analysis**
- DDL/DML analyzer flags dangerous operations before deploy
- Lock-heavy: `ALTER TABLE ADD COLUMN` with default (PG < 11), `NOT NULL` without default
- Full table rewrites: `ALTER TYPE`, changing column type
- Missing `CONCURRENT`: `CREATE/DROP INDEX` without `CONCURRENT`
- Data loss risk: `DROP COLUMN`, `DROP TABLE`, `TRUNCATE`
- Unindexed FK: foreign key with no supporting index
- Unsafe bulk DML: `UPDATE`/`DELETE` without `WHERE` or row estimate
- Severity levels: error (blocks deploy) / warn / info

**v1.2 — snapshot includes**
- `\i` / `\ir` with git-aware resolution: included files resolved as they existed at the migration's commit, not their current HEAD version
- Enables safe replay of historical migrations in repos where shared SQL files have evolved since the migration was written

**v2.0 — zero-downtime migrations (expand/contract)**
- Expand/contract pattern: dual-schema phases keep old + new columns alive simultaneously so application and schema deploys are decoupled
- Trigger-based sync between old and new columns during transition window
- Automatic view layer for backward compatibility
- Surgical column/constraint changes — not full table recreation

**v2.1 — batched background DML**
- Queue-based batched mutations for large tables (no long locks)
- Queue mechanism: 3-partition rotating table inside Postgres (PGQ-inspired) — no external dependencies
- Configurable batch size, sleep interval, cancellation
- Progress tracking via `pg_stat_activity` / advisory locks
- Inspired by GitLab's BatchedMigration framework: throttling, pause/resume, retry, per-batch transaction scope

**v3.0 — intelligence layer**
- AI migration review: plain-English explanation of what a migration does
- Conflict detection: concurrent migrations touching the same table/column
- Rollback inference: auto-generate revert from deploy
- DBLab integration: test deploy+revert against a production clone before touching prod

## Prior art studied

- https://sqitch.org
- https://github.com/xataio/pgroll
- https://github.com/mickelsamuel/migrationpilot
- GitLab background migration helpers (Rails)

## License

Apache 2.0
