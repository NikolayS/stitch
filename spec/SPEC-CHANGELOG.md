# Changelog

All notable changes to the stitch spec and codebase will be documented here.

## [Unreleased]

## [SPEC 0.3] — 2026-03-20

Comprehensive update based on expert review from four specialists: PG internals expert, Sqitch power user, production SRE, and static analysis engineer. All critical and important findings addressed.

### Critical fixes

- **Non-transactional DDL support (C1):** Added `--no-transaction` flag to `stitch add` and plan file pragma. Deploy data flow updated to execute non-transactional changes without `BEGIN`/`COMMIT` wrapper. Tracking updates happen in a separate transaction. Covers `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, `ALTER TYPE ADD VALUE` (PG < 12), `REINDEX CONCURRENTLY`. Added SA020 rule to detect `CONCURRENTLY` inside transactional deploys.

- **Advisory locks for deploy coordination (C2):** Deploy data flow now acquires `pg_advisory_xact_lock` (or `pg_advisory_lock`) before executing changes. Second concurrent deploy exits with code 4. Crash recovery: PG auto-releases advisory locks on disconnect. Added integration tests for concurrent deploy scenarios.

- **SA002 volatile defaults (C3):** Split into SA002 (volatile defaults cause rewrite on ALL PG versions, promoted to `error`) and SA002b (non-volatile defaults cause rewrite only on PG < 11, `warn`). Fixed Problem 1 example to use a non-volatile default to accurately illustrate the PG < 11 behavior.

- **SA003 safe cast allowlist (C4):** Defined "non-trivial cast" explicitly with a safe-cast allowlist (varchar widening, varchar→text, numeric precision widening). Everything else flags. Reference to `pg_cast` for connected analysis. OPEN marker for comprehensive allowlist audit.

- **Missing Sqitch commands (C5):** Added `rework`, `rebase`, `bundle`, `checkout`, `show`, `plan`, `upgrade` to R1 command table. Added `rework.ts`, `rebase.ts`, `bundle.ts`, `checkout.ts`, `show.ts`, `plan.ts`, `upgrade.ts` to architecture. Documented rework semantics including `@tag` syntax and plan file format. Added `reworked/` test fixture.

- **Tracking schema corrected (C6):** R3 now includes full DDL for all five tables (`sqitch.changes`, `sqitch.dependencies`, `sqitch.events`, `sqitch.tags`, `sqitch.projects`). Fixed column names: `committer_name`/`committer_email` (not `deployed_by`), `planner_name`/`planner_email`, `planned_at`, `note`. Added `sqitch.dependencies` table. Oracle comparison table updated to match actual Sqitch schema.

- **psql vs node-postgres (C7):** Added DD12 documenting the fundamental architecture decision. Three options presented (shell to psql, pre-processing layer, both modes). Marked as OPEN — must be resolved before Sprint 2. Added impact analysis on data flow, `--mode` semantics, `ON_ERROR_STOP`, `--set` variables, and snapshot includes.

- **Change ID computation (C8):** Documented that change IDs are SHA-1 hashes computed from change content. Added OPEN marker for exact algorithm specification. Added change ID verification tests to unit test plan and oracle comparison.

- **PgBouncer compatibility (C9):** Added DD13 covering PgBouncer detection, `pg_advisory_xact_lock` preference, SET re-issue per transaction, and recommendation for direct connections during deploy/batch operations.

- **pgsql-parser + bun build --compile validation (C10):** Added Phase 0 validation spike for native C addon bundling. Evaluate WASM alternatives if bundling fails. Marked as go/no-go for architecture.

- **Inline suppression for analysis (C11):** Added `-- stitch:disable SA010` comment syntax and per-file overrides in `stitch.toml`. Documented in Section 5.1 and included in unit test plan.

- **Expand/contract trigger edge cases (C12):** Added subsection covering: infinite recursion (`pg_trigger_depth()` guard), logical replication (triggers don't fire on subscribers), partitioned tables (PG 13+ for trigger inheritance), COPY performance, trigger installation lock (`AccessExclusiveLock`), concurrency control via advisory locks.

### Important fixes

- **SA001 description corrected (I1):** Changed from "Takes AccessExclusiveLock" to "Fails outright on populated tables." The issue is a DDL error, not a lock concern.

- **New analysis rules (I2):** Added SA016 (`ADD CONSTRAINT CHECK` without `NOT VALID`), SA017 (`SET NOT NULL` on existing column), SA018 (`ADD PRIMARY KEY` without pre-existing index), SA019 (`REINDEX` without `CONCURRENTLY`), SA020 (`CREATE INDEX CONCURRENTLY` in transaction), SA021 (explicit `LOCK TABLE`).

- **Static vs connected rules (I3):** Added rule type classification. SA009 and SA011 marked as "connected" (require DB context). Static rules work in standalone linter mode. Connected rules silently skipped when no connection is available.

- **PL/pgSQL body exclusion (I4):** DML inside `CREATE FUNCTION`, `CREATE PROCEDURE`, and `DO` blocks excluded from SA010/SA011/SA008. Analysis rules operate on top-level statements only.

- **sqitch.conf format documented (I5):** Added Git-style INI format description with subsections (`[engine "pg"]`), `db:` URI scheme, config precedence hierarchy (system < user < project < stitch.toml < env < flags).

- **Missing flags added (I6):** `--set`/`-s` (template variables), `--log-only` (adopt existing schemas), `--target`, `--no-verify`, `--verify` added to R1 flags list with descriptions.

- **Session settings (I7):** Added DD14 covering `statement_timeout=0` and `idle_in_transaction_session_timeout=0` for deploy connections.

- **Lock timeout guard moved to v1.0 (I8):** Moved from v1.1 to v1.0. Core safety infrastructure, not optional.

- **Batch job dead state recovery (I9):** Added `stitch batch retry` command. Dead jobs can be manually retried. Last processed PK tracked so retried jobs resume from where they stopped.

- **Replication lag monitoring (I10):** Added to batched DML features. Query `pg_stat_replication.replay_lag`, pause when lag exceeds configurable threshold (default 10s).

- **Reverse handoff test (I11):** Added stitch→Sqitch compatibility test: deploy with stitch, verify Sqitch reads tracking tables correctly, add/revert changes with Sqitch.

- **stitch analyze composability (I12):** `stitch analyze file.sql` works with zero config. No `sqitch.plan` required. Standalone linter mode for teams using other migration tools. Added `--changed` flag for CI.

- **Plan file entry format documented (I13):** `change_name [deps] YYYY-MM-DDTHH:MM:SSZ planner_name <planner_email> # note`.

- **Plan file pragmas documented (I14):** `%syntax-version`, `%project`, `%uri` — all documented with descriptions.

- **Cross-project dependencies (I15):** `project:change` syntax documented in R2, test cases added.

- **SA010 downgraded to warn (I16):** Full-table DML is often intentional in migrations. Use inline suppression for acknowledged cases.

- **SA015 expanded and downgraded (I17):** Now covers both table and column renames. Downgraded to `warn` until expand/contract (v2.0) exists, since there is no way to satisfy the rule before then.

- **Rule interface contract (I18):** Defined `Rule`, `AnalysisContext`, and `Finding` interfaces in Section 5.1.

- **--mode transaction semantics (I19):** Documented `all`/`change`/`tag` transaction scope differences. Non-transactional changes always execute outside any transaction regardless of mode.

- **Exit code 127 replaced (I20):** Changed "database unreachable" from 127 to 10. Added exit codes 4 (concurrent deploy) and 5 (lock timeout). Exit code table added to R6.

- **stitch.* schema documented (I21):** Clarified in DD3 that `stitch.*` schema is created only when stitch-specific features are used. Independent of `sqitch.*`. Can be safely dropped if reverting to Sqitch.

- **Batch worker connections (I22):** Documented that batch worker requires direct PostgreSQL connection (not PgBouncer in transaction mode). SET statements re-issued per batch transaction.

- **VACUUM pressure and bloat (I23):** Added dead tuple monitoring (`pg_stat_user_tables.n_dead_tup`) to batched DML features. Pause if accumulation exceeds threshold.

- **SA009 expanded (I24):** Now also flags `ADD FOREIGN KEY` without `NOT VALID` as the primary concern (lock during validation). Recommends two-step `NOT VALID` + `VALIDATE CONSTRAINT` pattern.

- **script_hash computation (I25):** Documented in R3. OPEN marker for exact algorithm and interaction with snapshot includes.

- **SA014 expanded (I26):** Now also covers `CLUSTER` (same lock + rewrite behavior as `VACUUM FULL`).

- **search_path handling (I27):** Added as OPEN in DD14. Options documented.

- **Problem 1 example fixed (I28):** Changed from `DEFAULT now()` (volatile, rewrites on ALL versions) to `DEFAULT '2024-01-01'::timestamptz` (non-volatile, correctly illustrates PG < 11 rewrite behavior).

### Other changes

- Version bumped to 0.3
- Static analysis moved from v1.1 to v1.0 (core safety infrastructure)
- CI integration moved from v1.1+ to v1.0+ (aligns with analysis move)
- Conflict dependency semantics documented in DD6
- `stitch analyze` scope defined (file, directory, pending, --all, --changed)
- Registry schema creation specified (IF NOT EXISTS + advisory lock for concurrent first-deploy)
- Added OPEN markers for: change ID algorithm, script_hash computation, search_path handling, logical replication + expand/contract, PGQ vs SKIP LOCKED queue design, comprehensive SA003 safe-cast list
- Added `preprocess.ts` to architecture for psql metacommand handling
- Test fixtures expanded: `reworked/`, `cross-project/`, `conflicts/`, `non-transactional/`
- Dry-run mode: explicitly documented what it does and does NOT guarantee

## [SPEC 0.2] — 2026-03-20

- Full rewrite of testing strategy (section 8): unit, integration, Sqitch oracle/compat, analysis fixture corpus, performance tests, CI configuration, local dev workflow
- Added DD11: Sqitch as oracle for compatibility testing
- Added links to GitLab migration_helpers.rb, batched background migrations docs, migration style guide, migration pipeline docs
- Fixed header metadata rendering

## [SPEC 0.1] — 2026-03-20

- Initial spec: problem statement, goals, requirements, feature ideas (10 features), design decisions (DD1–DD10), architecture, basic testing notes, phased implementation plan (7 phases, 16 sprints)
- Repo scaffolded: README, package.json, src/cli.ts skeleton, LICENSE (Apache 2.0)
