# Changelog

All notable changes to the sqlever spec and codebase will be documented here.

## [Unreleased]

## [SPEC 0.9] — 2026-03-20

- Renamed project from sqevo to sqlever.

## [SPEC 0.8] — 2026-03-20

- Renamed project from stitch to sqevo.

## [SPEC 0.7.1] — 2026-03-20

Customer zero validation: PostgresAI Console (`postgres-ai/platform-ui/db`).

- Added `postgres-ai-console/` to test fixture corpus as named customer zero fixture
- Added edge case fixtures: `planner-edge-cases/`, `missing-verify/`, `non-revertable/`, `heavy-includes/`
- Specified missing verify script behavior: `--verify` skips gracefully when verify file absent
- Specified non-revertable migration handling: log failure, record `fail` event
- Strengthened Problem 5 with real-world example (130+ shared files included via `\i`)

## [SPEC 0.7] — 2026-03-20

- **DD12 RESOLVED: Shell out to psql.** 100% psql metacommand compatibility — no subset, no reimplementation. `\i`, `\ir`, `\set`, `\copy`, `\if` all work exactly as in Sqitch. `node-postgres` used only for sqlever's own DB operations (tracking tables, advisory locks, introspection, batch DML). This was the last major OPEN blocking implementation.

## [SPEC 0.6.2] — 2026-03-20

- **DD9: 3-partition queue WITH SKIP LOCKED, not vs.** These are complementary, not alternatives. Partition rotation solves bloat (TRUNCATE vs DELETE). SKIP LOCKED solves worker concurrency (lock-free dequeue). Removed the OPEN marker — the design is sound when both are used together as PGQ intended.

## [SPEC 0.6.1] — 2026-03-20

Round 5 convergence check. All four expert domains confirmed CONVERGED.

- Removed incorrect `dependencies_check` CHECK constraint from `sqitch.dependencies` DDL — actual Sqitch has no CHECK constraint on this table. `dependency_id` can be NULL for cross-project dependencies regardless of type, and NOT NULL for conflicts referencing known changes.

## [SPEC 0.6] — 2026-03-20

Round 4 expert review findings addressed (convergence check). Same four reviewers. Surgical fixes — the spec is nearly converged. All reviewers confirmed zero critical behavioral issues with PG claims, no contradictions, and all OPEN markers remain appropriate.

### Critical fixes

- **Change ID `parent` line corrected:** The `parent` line in the change ID info string applies to ALL changes that have a preceding change in the plan file, not just reworked changes. Every change except the first has a parent (the immediately preceding change). The v0.5 spec incorrectly stated "only if this is a reworked change" — this would have produced incorrect change IDs for every change after the first in any plan with 2+ changes.

- **`dependencies.change_id` FK: added `ON DELETE CASCADE`:** Without this, reverting a change (which deletes from `sqitch.changes`) fails with a foreign key violation on the dependencies table.

### Important fixes

- **`dependencies.dependency_id` FK added:** The `dependency_id` column now has `REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE`, matching Sqitch's actual DDL. Without this, a `require` dependency could reference a non-existent change ID. The CHECK constraint is now named `dependencies_check`.

- **Tag info string `@` prefix clarified:** Tag `format_name` prepends `@` to the tag name. The info string line is `tag @v1.0`, not `tag v1.0`. Incorrect tag IDs would result from omitting the prefix.

- **SA016 lock level corrected (again):** `ADD CHECK` takes `ShareLock` on PG < 16, NOT `AccessExclusiveLock`. The v0.5 spec overcorrected from v0.4. `ShareLock` still blocks writes, so the rule recommendation (use `NOT VALID`) and severity (`error`) remain correct.

- **Advisory lock unlock on all exit paths:** Added explicit note that `pg_advisory_unlock` must be called on ALL exit paths (success, failure, analysis abort), not just the happy path. Disconnect-based release is the safety net for crashes, not the primary unlock mechanism.

- **`sqlever.pending_changes` protected by deploy advisory lock:** Added explicit note linking the session-level advisory lock to `sqlever.pending_changes` — the lock prevents concurrent access to pending records.

### Minor fixes

- SA003: clarified that `USING` clause presence always triggers SA003 regardless of the safe cast allowlist
- SA001: confirmed it does NOT fire when a `DEFAULT` is present (that case is SA002/SA002b territory)
- `sqlever.*` schema creation: clarified that the `sqlever` schema is created on first non-transactional deploy (not just expand/contract and batched DML)
- Version bumped to 0.6

## [SPEC 0.5] — 2026-03-20

Round 3 expert review findings addressed. Same four reviewers: PG internals expert, Sqitch power user, production SRE, static analysis engineer. Additional research: pg_index_pilot architecture analysis. Focus on correcting ID algorithms verified against Sqitch source, fixing tracking schema to match actual Sqitch DDL, and resolving advisory lock API mismatch.

### Critical fixes

- **Change ID algorithm corrected:** The v0.4 format was wrong. Fixed to match `App::Sqitch::Plan::Change->info` exactly: `uri` line conditional on `%uri` pragma, `parent` line for reworked changes, requires/conflicts use section headers with indented `  + dep` / `  - dep` entries (not `require <dep>` per line), note is raw text after a blank line (not `note <text>`). Every field format difference produces a different SHA-1.

- **Tag ID algorithm corrected:** Fixed to match `App::Sqitch::Plan::Tag->info`: added conditional `uri` line and conditional note (raw text after blank line).

- **script_hash is plain SHA-1, NOT git-style:** Sqitch does `SHA1(raw_file_content)` with NO `"blob <size>\0"` prefix. The v0.4 spec incorrectly claimed git-style blob hashing. Fixed. Clarified that for reworked changes, the hash is computed from the file at deploy time.

### Important fixes

- **Tracking schema corrections:** Added missing `sqitch.releases` table (registry versioning). Fixed `projects.uri` to include `UNIQUE` constraint. Changed all default timestamps from `NOW()` to `clock_timestamp()` (wall-clock time, advances within transaction). Added `ON UPDATE CASCADE` on all foreign key references. Added `UNIQUE(project, script_hash)` on changes table. Fixed events table — it DOES have `PRIMARY KEY (change_id, committed_at)`. Added `merge` to events type CHECK constraint. Added missing CHECK constraint on dependencies table. Reordered DDL to show projects first (referenced by other tables).

- **`pg_advisory_lock` replaced with `pg_try_advisory_lock`:** Default mode is now non-blocking — `pg_try_advisory_lock()` returns false immediately if lock held, matching "exit 4" behavior. Added configurable `advisory_lock_timeout` (default 30s) for CI wait mode using `pg_advisory_lock` with `SET lock_timeout`. Default: try, fail fast.

- **`hashtext()` instability resolved:** Replaced `hashtext('sqlever_deploy_' || project_name)` with application-level hash. Lock key uses two-argument form `pg_advisory_lock(constant_namespace, project_hash)` with a stable application-computed hash. `hashtext()` output is not guaranteed stable across PG major versions.

- **SET LOCAL trigger guard replaced:** `SET LOCAL sqlever.syncing = 'true'` remains true for the entire transaction, suppressing ALL subsequent trigger fires. Replaced with `pg_trigger_depth()` scoped to sqlever triggers via `TG_NAME LIKE 'sqlever_sync_%'`. All sqlever-generated sync triggers must use the `sqlever_sync_` name prefix.

- **SA016 lock level corrected:** Reverted PG < 16 lock from `ShareLock` to `AccessExclusiveLock`. `ADD CONSTRAINT ... CHECK` (with immediate validation) takes `AccessExclusiveLock` on PG < 16, `ShareUpdateExclusiveLock` on PG 16+.

- **`--mode all` is NOT a single transaction in Sqitch:** Sqitch's `_deploy_all` uses per-change transactions with explicit revert on failure, NOT a single wrapping transaction. Documented accurately. sqlever's true single-transaction `--mode all` is a sqlever improvement.

- **`-- sqitch-no-transaction` does NOT exist in Sqitch:** No evidence found in Sqitch source. Changed to `-- sqlever:no-transaction` as a sqlever-only convention. SA020 reference updated.

- **Sqitch uses `LOCK TABLE changes IN EXCLUSIVE MODE`, not advisory locks:** Documented that sqlever's advisory lock approach is a sqlever improvement providing stronger coordination (spans full deploy session vs. per-transaction table lock).

- **`sqlever.pending_changes` schema defined:** DDL specified: `change_id`, `change_name`, `project`, `script_path`, `started_at`, `status` (pending/complete/failed), `error_message`.

- **Non-transactional verify logic specified:** For index operations, check `pg_index.indisvalid`. For other DDL, run the change's verify script. Documented that automated verification only works for known DDL patterns.

- **Batch worker heartbeat added:** `heartbeat_at` column updated each batch. Configurable staleness threshold (default 5 minutes). Dead workers detected and jobs marked failed.

- **pg_index_pilot added to prior art:** Key patterns: write-ahead tracking (`in_progress` → `completed` | `failed`) for crash recovery, advisory lock using `pg_try_advisory_lock`, invalid index cleanup via `pg_index.indisvalid`. Added to Section 1 and prior art summary table.

- **Hybrid rule interface convention documented:** Hybrid rules check `context.db !== undefined` internally. Suppression filtering happens in analyzer entry point after rules return findings. Rules may produce multiple findings from one statement.

- **`--mode all` + non-transactional partial state documented:** Non-transactional changes that committed before a later failure remain deployed. `sqlever status` reports partial state correctly.

- **`--strict` and `error_on_warn` relationship documented:** `--strict` is the CLI equivalent of `error_on_warn = true` in config.

### Minor fixes

- SA009: corrected lock on referenced table to "brief" (still blocks concurrent DDL)
- script_hash for reworked changes: clarified it's computed from the file at deploy time
- `SHOW pool_mode`: documented as best-effort detection, `connection_type` config is the reliable mechanism
- GitLab Code Quality severity mapping specified: `error` → `critical`, `warn` → `major`, `info` → `minor`
- GitLab fingerprint specified: SHA-1 of `(ruleId, filePath, line)`
- Unused suppression warnings: `-- sqlever:disable` matching no finding produces a warning
- SA001: removed confusing parenthetical about PG < 11 defaults (that case is SA002b's territory)
- Change ID requires/conflicts: documented they preserve declaration order (not sorted)

### Other changes

- Version bumped to 0.5
- Remaining OPEN markers (4): SA003 safe-cast list (needs `pg_cast` audit), logical replication + expand/contract, PGQ vs SKIP LOCKED, DD12 psql vs node-postgres

## [SPEC 0.4] — 2026-03-20

Round 2 expert review findings addressed. Four reviewers: PG internals expert, Sqitch power user, production SRE, static analysis engineer. Focus on resolving contradictions, closing OPEN markers, and correcting factual errors.

### Critical fixes

- **Advisory lock design resolved:** Resolved xact vs session lock contradiction. Use `pg_advisory_lock` (session-level) as default — only option that works across multi-transaction deploys (`--mode change`) and non-transactional changes. Lock key: `pg_advisory_lock(hashtext('sqlever_deploy_' || project_name))`. Require direct connections for deploy (not PgBouncer in transaction mode). Apply to revert/rebase/checkout too, not just deploy.

- **`now()` is STABLE, not VOLATILE:** Removed `now()` from SA002 volatile examples — `now()` returns transaction start time and is classified STABLE. Corrected Problem 1 example text. Correct volatile examples: `random()`, `gen_random_uuid()`, `clock_timestamp()`, `txid_current()`. Updated SA002 test fixtures accordingly.

- **Change ID algorithm documented:** Closed OPEN marker. Algorithm: SHA-1 of `"change <length>\0project <project>\nchange <name>\nnote <note>\nplanner <planner> <<email>>\ndate <date>\nrequire <dep>\nconflict <dep>\n\n"`. Source: `App::Sqitch::Plan::Change->id`.

- **Tag ID computation added:** New addition. SHA-1 of `"tag <length>\0project <project>\ntag <tag_name>\nchange <change_id>\nplanner <planner> <<email>>\ndate <date>\n\n"`.

- **script_hash algorithm documented:** Closed OPEN marker. Uses git-style blob hashing: `SHA-1("blob <size>\0<content>")`. Raw file bytes, no line-ending normalization.

### Important fixes

- **SA003 USING clause corrected:** Changed from "always requires a rewrite" to "PostgreSQL rewrites the table to evaluate the expression, even when types are binary-compatible." Added missing safe casts: `char(N)` to `varchar`/`text`, `numeric(P,S)` to unconstrained `numeric`. Documented that `int` to `bigint` is NOT safe (rewrite required).

- **SA016 lock level corrected:** Fixed from `AccessExclusiveLock` to `ShareLock` (PG < 16) / `ShareUpdateExclusiveLock` (PG 16+).

- **Hybrid rule classification introduced:** New `type: "hybrid"` for rules with both static and connected concerns. SA009 (static: NOT VALID detection; connected: index check), SA017 (static: fire on SET NOT NULL; connected: check for CHECK constraint), SA018 (static: fire on ADD PRIMARY KEY; connected: check for pre-existing index).

- **`--mode all` + non-transactional behavior specified:** Non-transactional changes break the transaction (COMMIT before, execute, BEGIN after). Warning emitted when `--mode all` used with non-transactional changes.

- **Non-transactional write-ahead tracking:** Before executing non-transactional DDL, write "pending" record to `sqlever.pending_changes`. After success, update to "complete" and write sqitch tracking. On next deploy, check for pending non-transactional changes and verify state.

- **`--no-transaction` is a script comment in Sqitch:** Fixed — Sqitch uses `-- sqitch-no-transaction` comment in deploy script first line. sqlever supports both the script comment (Sqitch compat) and plan file pragma.

- **Inline suppression scoping specified:** Unclosed block extends to EOF with warning, single-line comment attaches to preceding statement, comma-separated rule IDs supported, unknown rules produce warning, `all` not supported.

- **SA020 expanded scope:** Now covers `DROP INDEX CONCURRENTLY` and `REINDEX CONCURRENTLY` in addition to `CREATE INDEX CONCURRENTLY`. Standalone mode behavior specified: warn on any CONCURRENTLY usage.

- **Reporter format schemas defined:** JSON output schema (envelope with metadata, findings, summary), GitHub annotations (`::error file=...` format), GitLab Code Quality JSON schema documented. `--exit-code` renamed to `--strict`.

- **Tracking schema DDL completeness:** Added `PRIMARY KEY (change_id, dependency)` to `sqitch.dependencies`. Added `CHECK` constraint and note about no PK on `sqitch.events`. Added `UNIQUE (project, tag)` to `sqitch.tags`. Added `ON DELETE CASCADE` where Sqitch uses it.

- **ALTER TYPE ADD VALUE PG 12+ gotcha documented:** Even in PG 12+ where it can run in a transaction, the new enum value is not usable within the same transaction.

- **Trigger recursion guard changed:** Replaced `pg_trigger_depth() < 2` with session variable approach: `SET LOCAL sqlever.syncing = 'true'` / `current_setting('sqlever.syncing', true)`. More robust in environments with existing triggers.

- **search_path OPEN resolved:** Use database/role default (Sqitch-compatible). Override available via `sqlever.toml`.

- **application_name added:** Set `application_name = 'sqlever/<command>/<project>'` on deploy/batch connections for production debugging.

- **Advisory locks for revert/rebase/checkout:** Not just deploy — any command modifying tracking state or executing DDL.

- **idle_in_transaction_session_timeout:** Changed from 0 (unlimited) to configurable generous value (default 10 minutes).

- **Non-transactional statement_timeout:** Separate configurable timeout (default 4 hours) for non-transactional DDL.

- **Lock retry for CI:** Added `--lock-retries N` with exponential backoff (default 0 = no retry).

- **PgBouncer detection improved:** Use `SHOW pool_mode` (PgBouncer-specific). Added `connection_type` config option for non-PgBouncer poolers.

- **PG 13 partition discussion simplified:** Since test matrix is PG 14+, trigger inheritance is always available. Removed per-partition installation discussion.

- **`--force-rule SA003` added:** Per-rule deploy-time override alongside blanket `--force`.

- **VACUUM pressure threshold defined:** Ratio `n_dead_tup / (n_live_tup + n_dead_tup)` exceeding configurable percentage (default 10%).

### Other changes

- Version bumped to 0.4
- Architecture tree fixed: `SPEC.md` path updated to `spec/SPEC.md`
- `sqitch.conf` `[deploy]` section documented (`verify`, `mode` defaults)
- SA002 test fixtures updated: `now()` moved from trigger/ to no_trigger/, `clock_timestamp()` added to trigger/
- Remaining OPEN markers (4): SA003 safe-cast list (needs `pg_cast` audit), logical replication + expand/contract, PGQ vs SKIP LOCKED, DD12 psql vs node-postgres

## [SPEC 0.3] — 2026-03-20

Comprehensive update based on expert review from four specialists: PG internals expert, Sqitch power user, production SRE, and static analysis engineer. All critical and important findings addressed.

### Critical fixes

- **Non-transactional DDL support (C1):** Added `--no-transaction` flag to `sqlever add` and plan file pragma. Deploy data flow updated to execute non-transactional changes without `BEGIN`/`COMMIT` wrapper. Tracking updates happen in a separate transaction. Covers `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, `ALTER TYPE ADD VALUE` (PG < 12), `REINDEX CONCURRENTLY`. Added SA020 rule to detect `CONCURRENTLY` inside transactional deploys.

- **Advisory locks for deploy coordination (C2):** Deploy data flow now acquires `pg_advisory_xact_lock` (or `pg_advisory_lock`) before executing changes. Second concurrent deploy exits with code 4. Crash recovery: PG auto-releases advisory locks on disconnect. Added integration tests for concurrent deploy scenarios.

- **SA002 volatile defaults (C3):** Split into SA002 (volatile defaults cause rewrite on ALL PG versions, promoted to `error`) and SA002b (non-volatile defaults cause rewrite only on PG < 11, `warn`). Fixed Problem 1 example to use a non-volatile default to accurately illustrate the PG < 11 behavior.

- **SA003 safe cast allowlist (C4):** Defined "non-trivial cast" explicitly with a safe-cast allowlist (varchar widening, varchar→text, numeric precision widening). Everything else flags. Reference to `pg_cast` for connected analysis. OPEN marker for comprehensive allowlist audit.

- **Missing Sqitch commands (C5):** Added `rework`, `rebase`, `bundle`, `checkout`, `show`, `plan`, `upgrade` to R1 command table. Added `rework.ts`, `rebase.ts`, `bundle.ts`, `checkout.ts`, `show.ts`, `plan.ts`, `upgrade.ts` to architecture. Documented rework semantics including `@tag` syntax and plan file format. Added `reworked/` test fixture.

- **Tracking schema corrected (C6):** R3 now includes full DDL for all five tables (`sqitch.changes`, `sqitch.dependencies`, `sqitch.events`, `sqitch.tags`, `sqitch.projects`). Fixed column names: `committer_name`/`committer_email` (not `deployed_by`), `planner_name`/`planner_email`, `planned_at`, `note`. Added `sqitch.dependencies` table. Oracle comparison table updated to match actual Sqitch schema.

- **psql vs node-postgres (C7):** Added DD12 documenting the fundamental architecture decision. Three options presented (shell to psql, pre-processing layer, both modes). Marked as OPEN — must be resolved before Sprint 2. Added impact analysis on data flow, `--mode` semantics, `ON_ERROR_STOP`, `--set` variables, and snapshot includes.

- **Change ID computation (C8):** Documented that change IDs are SHA-1 hashes computed from change content. Added OPEN marker for exact algorithm specification. Added change ID verification tests to unit test plan and oracle comparison.

- **PgBouncer compatibility (C9):** Added DD13 covering PgBouncer detection, `pg_advisory_xact_lock` preference, SET re-issue per transaction, and recommendation for direct connections during deploy/batch operations.

- **pgsql-parser + bun build --compile validation (C10):** Added Phase 0 validation spike for native C addon bundling. Evaluate WASM alternatives if bundling fails. Marked as go/no-go for architecture.

- **Inline suppression for analysis (C11):** Added `-- sqlever:disable SA010` comment syntax and per-file overrides in `sqlever.toml`. Documented in Section 5.1 and included in unit test plan.

- **Expand/contract trigger edge cases (C12):** Added subsection covering: infinite recursion (`pg_trigger_depth()` guard), logical replication (triggers don't fire on subscribers), partitioned tables (PG 13+ for trigger inheritance), COPY performance, trigger installation lock (`AccessExclusiveLock`), concurrency control via advisory locks.

### Important fixes

- **SA001 description corrected (I1):** Changed from "Takes AccessExclusiveLock" to "Fails outright on populated tables." The issue is a DDL error, not a lock concern.

- **New analysis rules (I2):** Added SA016 (`ADD CONSTRAINT CHECK` without `NOT VALID`), SA017 (`SET NOT NULL` on existing column), SA018 (`ADD PRIMARY KEY` without pre-existing index), SA019 (`REINDEX` without `CONCURRENTLY`), SA020 (`CREATE INDEX CONCURRENTLY` in transaction), SA021 (explicit `LOCK TABLE`).

- **Static vs connected rules (I3):** Added rule type classification. SA009 and SA011 marked as "connected" (require DB context). Static rules work in standalone linter mode. Connected rules silently skipped when no connection is available.

- **PL/pgSQL body exclusion (I4):** DML inside `CREATE FUNCTION`, `CREATE PROCEDURE`, and `DO` blocks excluded from SA010/SA011/SA008. Analysis rules operate on top-level statements only.

- **sqitch.conf format documented (I5):** Added Git-style INI format description with subsections (`[engine "pg"]`), `db:` URI scheme, config precedence hierarchy (system < user < project < sqlever.toml < env < flags).

- **Missing flags added (I6):** `--set`/`-s` (template variables), `--log-only` (adopt existing schemas), `--target`, `--no-verify`, `--verify` added to R1 flags list with descriptions.

- **Session settings (I7):** Added DD14 covering `statement_timeout=0` and `idle_in_transaction_session_timeout=0` for deploy connections.

- **Lock timeout guard moved to v1.0 (I8):** Moved from v1.1 to v1.0. Core safety infrastructure, not optional.

- **Batch job dead state recovery (I9):** Added `sqlever batch retry` command. Dead jobs can be manually retried. Last processed PK tracked so retried jobs resume from where they stopped.

- **Replication lag monitoring (I10):** Added to batched DML features. Query `pg_stat_replication.replay_lag`, pause when lag exceeds configurable threshold (default 10s).

- **Reverse handoff test (I11):** Added sqlever→Sqitch compatibility test: deploy with sqlever, verify Sqitch reads tracking tables correctly, add/revert changes with Sqitch.

- **sqlever analyze composability (I12):** `sqlever analyze file.sql` works with zero config. No `sqitch.plan` required. Standalone linter mode for teams using other migration tools. Added `--changed` flag for CI.

- **Plan file entry format documented (I13):** `change_name [deps] YYYY-MM-DDTHH:MM:SSZ planner_name <planner_email> # note`.

- **Plan file pragmas documented (I14):** `%syntax-version`, `%project`, `%uri` — all documented with descriptions.

- **Cross-project dependencies (I15):** `project:change` syntax documented in R2, test cases added.

- **SA010 downgraded to warn (I16):** Full-table DML is often intentional in migrations. Use inline suppression for acknowledged cases.

- **SA015 expanded and downgraded (I17):** Now covers both table and column renames. Downgraded to `warn` until expand/contract (v2.0) exists, since there is no way to satisfy the rule before then.

- **Rule interface contract (I18):** Defined `Rule`, `AnalysisContext`, and `Finding` interfaces in Section 5.1.

- **--mode transaction semantics (I19):** Documented `all`/`change`/`tag` transaction scope differences. Non-transactional changes always execute outside any transaction regardless of mode.

- **Exit code 127 replaced (I20):** Changed "database unreachable" from 127 to 10. Added exit codes 4 (concurrent deploy) and 5 (lock timeout). Exit code table added to R6.

- **sqlever.* schema documented (I21):** Clarified in DD3 that `sqlever.*` schema is created only when sqlever-specific features are used. Independent of `sqitch.*`. Can be safely dropped if reverting to Sqitch.

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
- `sqlever analyze` scope defined (file, directory, pending, --all, --changed)
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
