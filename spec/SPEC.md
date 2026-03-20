# stitch — Product Specification

- **Version:** 0.5 (draft)
- **Status:** Pre-development — spec review in progress, no implementation yet
- **License:** Apache 2.0
- **Changelog:** [SPEC-CHANGELOG.md](SPEC-CHANGELOG.md)
- **Location:** `spec/`

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

Source: https://gitlab.com/gitlab-org/gitlab/-/blob/master/lib/gitlab/database/migration_helpers.rb
Batched background migrations docs: https://docs.gitlab.com/development/database/batched_background_migrations/
Migration style guide: https://docs.gitlab.com/development/migration_style_guide/
Migration pipeline (how GitLab tests migrations): https://docs.gitlab.com/development/database/database_migration_pipeline/

Battle-tested at massive scale. `BatchedMigration` framework: throttled background DML, pause/resume, per-batch transactions, state tracked in Postgres. The gold standard for large-table data migrations. We extract the concepts, drop the Rails dependency.

### SkyTools / PGQ

https://github.com/pgq/pgq — 3-partition rotating queue table, entirely inside Postgres. Proven architecture for durable queuing without external systems. Inspiration for our batched DML queue.

### pg_index_pilot

https://gitlab.com/postgres-ai/postgresai/-/tree/main/components/index_pilot — pure PL/pgSQL tool for managing `REINDEX INDEX CONCURRENTLY` operations. Key design patterns relevant to stitch: (1) write-ahead tracking with three-state lifecycle (`in_progress` → `completed` | `failed`) for crash recovery of non-transactional DDL, (2) advisory lock coordination using schema OID and `pg_try_advisory_lock()` to prevent concurrent operations, (3) invalid index cleanup by checking `pg_index.indisvalid` and dropping `_ccnew` suffixed indexes left by failed `REINDEX CONCURRENTLY`, (4) explicit pre-DDL commit to release held locks before executing concurrent DDL. These patterns informed stitch's non-transactional write-ahead tracking and `stitch.pending_changes` design.

### Flyway / Liquibase

Sequential-numbered files, XML/YAML config, JVM runtime. Wrong philosophy. We take nothing.

---

## 2. Problems we're solving

### Problem 1: Dangerous migrations reach production undetected

`ALTER TABLE orders ADD COLUMN processed_at timestamptz NOT NULL DEFAULT '2024-01-01'::timestamptz` — on PostgreSQL < 11 this rewrites the entire table (any `ADD COLUMN ... DEFAULT` causes a table rewrite). On a 500GB orders table at 3am, that's an outage. And even on PG 11+, volatile defaults like `DEFAULT gen_random_uuid()` still cause a full table rewrite (note: `now()` is `STABLE`, not volatile, so it does NOT cause a rewrite on PG 11+). No existing migration tool catches this before deploy.

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
- Composable — each major feature usable independently, without adopting the full tool. Teams using Flyway, Alembic, Rails migrations, or raw psql scripts should be able to run `stitch analyze` as a standalone linter in their CI pipeline without touching their migration runner. The batched DML worker should be invokable standalone. No forced adoption of the whole stack.

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
| `stitch add <name> [-n note] [-r dep] [--conflict dep]` | Add new change (supports `--no-transaction` pragma) |
| `stitch deploy [target] [--to change] [--mode [all\|change\|tag]]` | Deploy changes |
| `stitch revert [target] [--to change] [-y]` | Revert changes |
| `stitch verify [target] [--from change] [--to change]` | Run verify scripts |
| `stitch status [target]` | Show deployment status |
| `stitch log [target]` | Show deployment history |
| `stitch tag [name]` | Tag current deployment state |
| `stitch rework <name>` | Rework an existing change (create new version with same name) |
| `stitch rebase [--onto change]` | Revert then re-deploy (convenience for `revert` + `deploy`) |
| `stitch bundle [--dest-dir dir]` | Package project for distribution |
| `stitch checkout <branch>` | Deploy/revert changes to match a VCS branch |
| `stitch show <type> <name>` | Display change/tag details or script contents |
| `stitch plan [filter]` | Display plan contents in human-readable format |
| `stitch upgrade` | Upgrade the Sqitch registry schema to current version |
| `stitch engine add\|alter\|remove\|show\|list` | Manage database engines |
| `stitch target add\|alter\|remove\|show\|list` | Manage deploy targets |
| `stitch config` | Read/write configuration |
| `stitch help [command]` | Show help |

Flags that must be supported: `--db-uri`, `--db-client`, `--plan-file`, `--top-dir`, `--registry`, `--quiet`, `--verbose`, `--target`, `--set` / `-s` (template variables), `--log-only`, `--no-verify`, `--verify`, `--no-prompt` / `-y`.

**`--set` / `-s`:** Template variable substitution. Sqitch supports passing variables at deploy time that are substituted in scripts via psql's `:variable` syntax. Commonly used for environment-specific schema names, roles, or tablespaces.

**`--log-only`:** Record a change as deployed without executing the script. Critical for adopting stitch on databases where changes were already applied manually or by another tool.

**`--registry`:** Specifies the schema name for tracking tables (default: `sqitch`). Some teams use non-default registry schemas (e.g., `_sqitch` or `migrations`).

**`rework`:** Allows re-deploying a change that has already been deployed by creating a new version of it. Appends a reworked copy to the plan file with the same change name but a new change ID. A tag must exist between the old and new versions. The old version is referenceable via `change@tag` syntax. This is heavily used for iteratively evolving stored procedures, views, and functions.

### R2 — Plan file format compatibility

`sqitch.plan` format must be parsed and written without modification. Existing Sqitch projects must be adoptable with zero file changes.

**Plan file pragmas:** The plan file begins with pragmas:
```
%syntax-version=1
%project=myproject
%uri=urn:uuid:...
```
The `%uri` pragma is set during `sqitch init --uri <uri>` and is used as the stable project identifier in `sqitch.projects.uri`.

**Change entry format:** Each change entry has the form:
```
change_name [dependencies] YYYY-MM-DDTHH:MM:SSZ planner_name <planner_email> # note
```

**Reworked changes:** The plan file supports duplicate change names separated by a tag. References to specific versions use `change@tag` syntax:
```
add_users 2024-01-01T00:00:00Z user <user@example.com> # add users table
@v1.0 2024-01-01T00:01:00Z user <user@example.com> # tag v1.0
add_users [add_users@v1.0] 2024-02-01T00:00:00Z user <user@example.com> # rework users
```

**Cross-project dependencies:** Dependencies may reference changes in other projects using `project:change` syntax. At deploy time, stitch checks the tracking tables for the other project's changes.

**Change ID computation:** Each change has a unique change ID. Sqitch computes this as a SHA-1 hash using an object format (from `App::Sqitch::Plan::Change->info` and `->id`). The input to SHA-1 is:

```
change <content_length>\0<content>
```

Where `\0` is a null byte, `<content_length>` is the decimal string length of `<content>`, and `<content>` is the concatenation of the following lines (each terminated by `\n`):
```
project <project_name>
uri <project_uri>                    ← conditional: only if project has a URI (%uri pragma)
change <change_name>
parent <parent_change_id>            ← conditional: only if this is a reworked change
planner <planner_name> <<planner_email>>
date <planned_at_iso8601>
requires                             ← conditional: only if change has requires dependencies
  + dep1                             ← indented with "  + " prefix, one per line
  + dep2
conflicts                            ← conditional: only if change has conflict dependencies
  - dep1                             ← indented with "  - " prefix, one per line
                                     ← blank line separator before note
<note text>                          ← conditional: raw note text (no "note" prefix), only if note is non-empty
```

Key differences from earlier spec versions: (1) `uri` line is conditional on the project having a `%uri` pragma, (2) `parent` line appears only for reworked changes, (3) requires and conflicts use section headers with indented entries (not `require <dep>` per line), (4) note is raw text after a blank line separator (not `note <text>`), (5) the blank line and note are only present when the note is non-empty. Requires and conflicts entries preserve declaration order (not sorted).

stitch must compute identical IDs. Since `change_id` is the primary key in `sqitch.changes`, any divergence will break the mid-deploy handoff scenario.

**Tag ID computation:** Each tag also has a SHA-1 ID (stored as `tag_id` in `sqitch.tags`). The format is:

```
tag <content_length>\0<content>
```

Where `<content>` is the concatenation of the following lines (each terminated by `\n`):
```
project <project_name>
uri <project_uri>                    ← conditional: only if project has a URI
tag <tag_name>
change <change_id>
planner <planner_name> <<planner_email>>
date <planned_at_iso8601>
                                     ← blank line separator before note
<note text>                          ← conditional: raw note text, only if note is non-empty
```

Like change IDs, the `uri` line is conditional, and the note appears as raw text after a blank line separator (only when non-empty).

### R3 — Tracking schema compatibility

Sqitch tracking tables must be used as-is. Teams currently using Sqitch must be able to switch to stitch mid-project without re-deploying all migrations. The tracking schema includes the following tables:

**`sqitch.projects`:**
```sql
CREATE TABLE sqitch.projects (
    project         TEXT        PRIMARY KEY,
    uri             TEXT        NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    creator_name    TEXT        NOT NULL,
    creator_email   TEXT        NOT NULL
);
```

Note: `uri` has a `UNIQUE` constraint (projects are uniquely identified by URI when present). `clock_timestamp()` is used instead of `NOW()` — `clock_timestamp()` returns wall-clock time and advances within a transaction, so in `--mode all` each change gets a distinct timestamp rather than all sharing the transaction start time.

**`sqitch.releases`:**
```sql
CREATE TABLE sqitch.releases (
    version         REAL        PRIMARY KEY,
    installed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    installer_name  TEXT        NOT NULL,
    installer_email TEXT        NOT NULL
);
```

Note: The `releases` table tracks registry schema versions and is used by `stitch upgrade` to determine if the tracking schema needs migration.

**`sqitch.changes`:**
```sql
CREATE TABLE sqitch.changes (
    change_id       TEXT        PRIMARY KEY,
    script_hash     TEXT,
    change          TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    UNIQUE (project, script_hash)
);
```

**`sqitch.tags`:**
```sql
CREATE TABLE sqitch.tags (
    tag_id          TEXT        PRIMARY KEY,
    tag             TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    change_id       TEXT        NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    UNIQUE (project, tag)
);
```

**`sqitch.dependencies`:**
```sql
CREATE TABLE sqitch.dependencies (
    change_id    TEXT NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE,
    type         TEXT NOT NULL,  -- 'require' or 'conflict'
    dependency   TEXT NOT NULL,
    dependency_id TEXT,
    PRIMARY KEY (change_id, dependency),
    CHECK (
        (type = 'require' AND dependency_id IS NOT NULL)
        OR (type = 'conflict' AND dependency_id IS NULL)
    )
);
```

**`sqitch.events`:**
```sql
CREATE TABLE sqitch.events (
    event           TEXT        NOT NULL CHECK (event IN ('deploy', 'revert', 'fail', 'merge')),
    change_id       TEXT        NOT NULL,
    change          TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    requires        TEXT[]      NOT NULL DEFAULT '{}',
    conflicts       TEXT[]      NOT NULL DEFAULT '{}',
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    PRIMARY KEY (change_id, committed_at)
);
```

**`script_hash` computation:** Sqitch computes `script_hash` as a plain SHA-1 hash of the raw file content: `SHA-1(<raw_file_bytes>)`. There is NO git-style `"blob <size>\0"` prefix — Sqitch reads the file in raw binary mode and feeds it directly to SHA-1 (see `App::Sqitch::Plan::Change->_deploy_hash`). stitch must compute identical hashes. If stitch adds a `"blob <size>\0"` prefix, `stitch status` will falsely report every script as "modified" compared to what Sqitch recorded.

The hash is computed from the raw file bytes (no line-ending normalization). For snapshot includes, the hash is computed from the deploy script file itself, not the assembled content after `\i` resolution. For reworked changes, the hash reflects the file content at the time of deployment — the current file on disk at deploy time, not the file as it existed when the change was first added to the plan.

**Registry schema creation:** When stitch first deploys to a database, it must create the `sqitch` schema and all tracking tables using DDL identical to what Sqitch produces. The creation should use `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` for idempotent first-deploy, and should be protected by an advisory lock to handle concurrent first-deploys from multiple CI runners.

### R4 — Static analysis on deploy

`stitch deploy` must run static analysis before executing any SQL. On `error`-severity findings, deploy must be blocked (unless `--force` is passed). On `warn`, deploy proceeds with output. `--force-rule SA003` bypasses a specific rule while keeping all other guards active (can be specified multiple times). `--force` remains as the blanket "bypass everything" escape hatch.

### R5 — Machine-readable output

All commands must support `--format json` for structured output. Risk reports must be JSON-serializable.

### R6 — Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Deploy failed |
| 2 | Analysis blocked deploy (also used by standalone `stitch analyze` when error-level findings exist) |
| 3 | Verification failed |
| 4 | Concurrent deploy detected (another deploy is in progress) |
| 5 | Lock timeout exceeded (retry may succeed) |
| 10 | Database unreachable |

Note: Previous draft used exit code 127 for "database unreachable." Changed to 10 because POSIX reserves 127 for "command not found," which would make the two conditions indistinguishable in shell scripts and CI systems.

---

## 5. Feature ideas

### 5.1 Static analysis (v1.0)

Analyze migration SQL before deploy and flag dangerous patterns. Moved from v1.1 to v1.0 — analysis is core safety infrastructure, not optional.

**Severity levels:**
- `error` — blocks deploy
- `warn` — prints warning, deploy proceeds
- `info` — informational

**Rule classification:**
- **Static rules** — can run on SQL alone, no database connection needed. These work in standalone linter mode (`stitch analyze file.sql`).
- **Connected rules** — require a database connection for schema introspection (e.g., checking indexes, row counts). When no connection is available, connected rules are silently skipped with an `info`-level note.
- **Hybrid rules** — have both a static check (always runs) and a connected check (runs when a database connection is available). In standalone mode, only the static portion fires. With a connection, the connected portion may refine, suppress, or add to the static findings.

**Rules:**

| Rule ID | Severity | Type | Trigger | Why dangerous |
|---------|----------|------|---------|---------------|
| `SA001` | error | static | `ADD COLUMN ... NOT NULL` without default | Fails outright on populated tables (`ERROR: column contains null values`). |
| `SA002` | error | static | `ADD COLUMN ... DEFAULT <volatile>` (any PG version) | Volatile defaults (e.g., `random()`, `gen_random_uuid()`, `clock_timestamp()`, `txid_current()`) cause a full table rewrite on ALL PostgreSQL versions, including PG 11+. The PG 11 optimization only applies to immutable/stable defaults. Note: `now()` is `STABLE` (returns transaction start time), not volatile — `DEFAULT now()` does NOT cause a rewrite on PG 11+. |
| `SA002b` | warn | static | `ADD COLUMN ... DEFAULT <non-volatile>` on PG < 11 | Non-volatile defaults cause a full table rewrite on PG < 11. Safe on PG 11+ (metadata-only). |
| `SA003` | error | static | `ALTER COLUMN ... TYPE` (unsafe cast) | Full table rewrite + `AccessExclusiveLock`. See safe cast allowlist below. |
| `SA004` | warn | static | `CREATE INDEX` without `CONCURRENTLY` | Takes `ShareLock`, blocks INSERT/UPDATE/DELETE for duration. |
| `SA005` | warn | static | `DROP INDEX` without `CONCURRENTLY` | Takes `AccessExclusiveLock`. |
| `SA006` | warn | static | `DROP COLUMN` | Data loss, irreversible. |
| `SA007` | error | static | `DROP TABLE` (non-revert context) | Data loss. In sqitch project context, files under `revert/` are exempt. In standalone mode, always fires. |
| `SA008` | warn | static | `TRUNCATE` | Data loss. |
| `SA009` | warn | hybrid | `ADD FOREIGN KEY` without `NOT VALID` | Static: detects `ADD FOREIGN KEY` without `NOT VALID` (lock concern — takes `ShareRowExclusiveLock` on both referencing and referenced tables; lock on the referenced table is brief but still blocks concurrent DDL). Connected: also flags missing index on referencing column (ongoing performance concern). Recommend two-step: `ADD CONSTRAINT ... NOT VALID` then `VALIDATE CONSTRAINT` (takes only `ShareUpdateExclusiveLock`, does not block writes). |
| `SA010` | warn | static | `UPDATE` or `DELETE` without `WHERE` | Full table DML. Downgraded from `error` to `warn` — full-table DML is often intentional in migrations (backfills, cleanups). Use inline suppression for acknowledged cases. |
| `SA011` | warn | connected | `UPDATE` or `DELETE` on large table (estimated rows > threshold) | Long-running DML, table bloat. Requires `pg_class.reltuples` from live database. |
| `SA012` | info | static | `ALTER SEQUENCE RESTART` | May break application assumptions. |
| `SA013` | warn | static | `SET lock_timeout` missing before risky DDL | Runaway lock wait. "Risky DDL" = any DDL taking `AccessExclusiveLock` or `ShareLock`. If lock timeout guard (5.9) auto-prepends, SA013 does not fire. |
| `SA014` | warn | static | `VACUUM FULL` or `CLUSTER` | Full table lock + rewrite, avoid in migrations. |
| `SA015` | warn | static | `ALTER TABLE ... RENAME` (table or column) | Breaks running application. Severity is `warn` (not `error`) until expand/contract (v2.0) exists, since there is no way to satisfy the rule before then. After v2.0, promote to `error` for renames not part of an expand/contract pair. |
| `SA016` | error | static | `ADD CONSTRAINT ... CHECK` without `NOT VALID` | Full table scan under `AccessExclusiveLock` (PG < 16) / `ShareUpdateExclusiveLock` (PG 16+) — blocks all concurrent access on older versions, and scan duration keeps the lock held. Safe pattern: `ADD CONSTRAINT ... NOT VALID` then `VALIDATE CONSTRAINT`. |
| `SA017` | error | hybrid | `ALTER COLUMN ... SET NOT NULL` (existing column) | Static: fires on any `SET NOT NULL` (on PG < 12, full table scan under `AccessExclusiveLock`; on PG 12+, metadata-only if a valid CHECK constraint exists). Connected: checks catalog for existing valid `CHECK (col IS NOT NULL)` constraint and suppresses if found. Recommend three-step: add CHECK NOT VALID, validate, then SET NOT NULL. |
| `SA018` | warn | hybrid | `ADD PRIMARY KEY` without pre-existing index | Static: fires on `ADD PRIMARY KEY` without `USING INDEX` clause (`ALTER TABLE` takes `AccessExclusiveLock`, and the implicit index creation extends lock duration). Connected: checks catalog for pre-existing unique index on the PK columns and suppresses if found. Safe pattern: create index concurrently first, then `ADD CONSTRAINT ... USING INDEX`. |
| `SA019` | warn | static | `REINDEX` without `CONCURRENTLY` | Takes `AccessExclusiveLock`. PG 12+ supports `REINDEX CONCURRENTLY`. |
| `SA020` | error | static | `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, or `REINDEX CONCURRENTLY` inside transactional deploy | Cannot run inside a transaction block — will fail at runtime. Change must be marked non-transactional. In project mode: checks plan file for non-transactional marker. In standalone mode: warns on any `CONCURRENTLY` usage with message "Ensure this runs outside a transaction block." Also recognizes `-- stitch:no-transaction` script comment (stitch-only convention, see non-transactional changes below). |
| `SA021` | warn | static | `LOCK TABLE` (any mode) | Explicit locking in migrations is a code smell and dangerous in production. |

**SA003 safe cast allowlist:** The following type changes are known to be safe (no table rewrite, binary-compatible):
- `varchar(N)` to `varchar(M)` where M > N (widening)
- `varchar(N)` to `varchar` (removing limit)
- `varchar` to `text`
- `char(N)` to `varchar` or `text`
- `numeric(P,S)` to `numeric(P2,S)` where P2 > P (widening precision)
- `numeric(P,S)` to unconstrained `numeric` (removing precision/scale constraint)

Known unsafe casts that require a rewrite (commonly assumed safe but are not):
- `int` to `bigint` — different binary representation, always rewrites
- `timestamp` to `timestamptz` — rewrite required

All other type changes are flagged. When a `USING` clause is present, PostgreSQL rewrites the table to evaluate the expression, even when the source and target types are binary-compatible. In the absence of a database connection, the rule is conservative and flags all type changes not in the allowlist. With a connection, the rule can consult `pg_cast` to determine if the cast is binary-coercible.

**OPEN:** Build a comprehensive safe-cast list by auditing `pg_cast.castmethod` across PG 14-18. The allowlist above is a starting point.

**PL/pgSQL body exclusion:** DML inside `CREATE FUNCTION`, `CREATE PROCEDURE`, and `DO $$ ... $$` blocks is excluded from SA010, SA011, and SA008. These statements define function bodies, not direct migration operations. Analysis rules operate on top-level statements only.

**Inline suppression:** Rules can be suppressed per-statement using SQL comments:
```sql
-- stitch:disable SA010
UPDATE users SET tier = 'free';
-- stitch:enable SA010
```
Or single-line: `UPDATE users SET tier = 'free'; -- stitch:disable SA010`

**Inline suppression scoping rules:**
- **Block form:** `-- stitch:disable` ... `-- stitch:enable` suppresses findings for all statements between the markers. An unclosed block (no matching `enable`) extends to end of file and produces a warning.
- **Single-line form:** A trailing `-- stitch:disable` comment attaches to the immediately preceding statement (determined by source range from the parser).
- **Multiple rules:** Comma-separated rule IDs are supported: `-- stitch:disable SA010,SA011`.
- **Unknown rules:** `-- stitch:disable SA999` (nonexistent rule) produces a warning, not a silent ignore.
- **`all` keyword:** `-- stitch:disable all` is NOT supported — suppressing all rules silently is too dangerous. Suppress rules individually.

**Per-file overrides in `stitch.toml`:**
```toml
[analysis.overrides."deploy/backfill_tiers.sql"]
skip = ["SA010"]
```

**Configuration via `stitch.toml`:**
```toml
[analysis]
error_on_warn = false
max_affected_rows = 10_000
skip = []
pg_version = 14               # minimum PG version migrations must support
```

`pg_version` represents the minimum supported version. If upgrading from PG 14 to PG 17, set `pg_version = 14` so rules fire for patterns unsafe on the oldest supported version.

**Rule interface contract:**
```
interface Rule {
  id: string;           // "SA001"
  severity: Severity;
  type: "static" | "connected" | "hybrid";
  check(context: AnalysisContext): Finding[];
}
interface AnalysisContext {
  ast: ParseResult;
  rawSql: string;
  filePath: string;
  pgVersion: number;
  config: AnalysisConfig;
  db?: DatabaseClient;   // present only for connected rules with active connection
}
interface Finding {
  ruleId: string;
  severity: Severity;
  message: string;
  location: { file: string; line: number; column: number; endLine?: number; endColumn?: number };
  suggestion?: string;
}
```

Note: `libpg_query` provides byte offsets that must be converted to line/column using the original source text. This conversion is part of `analysis/parser.ts`.

**Hybrid rule convention:** Hybrid rules implement a single `check()` method and internally branch on `context.db !== undefined`. When `db` is present, the connected portion may refine, suppress, or add to the static findings. A hybrid rule may produce multiple findings from one statement (e.g., SA009 can produce both a "missing NOT VALID" finding and a "missing index" finding). Suppression filtering (inline `-- stitch:disable` and per-file overrides) happens in the analyzer entry point after rules return findings — rules do not see or reason about suppressions.

**Unused suppression warnings:** If a `-- stitch:disable SA010` comment matches no actual finding in its scope, the analyzer emits a warning: "Unused suppression for SA010." This prevents dead suppression comments from accumulating.

**`stitch analyze` scope:**
- `stitch analyze <file>` — analyze a single SQL file (standalone linter mode, no `sqitch.plan` required).
- `stitch analyze <directory>` — analyze all `.sql` files in the directory.
- `stitch analyze` (no args, in sqitch project) — analyze all pending (undeployed) migrations from `sqitch.plan`.
- `stitch analyze --all` — analyze all migrations in the project.
- `stitch analyze --changed` — analyze only files changed in the current git diff (useful in CI for PRs).

When no `sqitch.plan` exists and no arguments are given, analyze all `.sql` files in the current directory. This supports the composability goal — teams using other migration tools can run `stitch analyze` as a standalone linter.

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

**Trigger edge cases and mitigations:**

1. **Infinite recursion:** Bidirectional sync triggers (old→new, new→old) can recurse infinitely. All generated sync triggers must include a recursion guard. The guard uses `pg_trigger_depth()` scoped to stitch triggers by checking the trigger name: `IF pg_trigger_depth() < 2 AND TG_NAME LIKE 'stitch_sync_%' THEN ... END IF`. This is preferred over the session variable approach (`SET LOCAL stitch.syncing = 'true'`) because `SET LOCAL` remains true for the entire transaction, which would suppress ALL subsequent trigger fires within the transaction — not just recursive ones. The `pg_trigger_depth()` approach correctly allows multiple stitch sync triggers on different tables to fire independently within the same transaction while still preventing recursion. All stitch-generated sync triggers must use the `stitch_sync_` name prefix.

2. **Logical replication:** Triggers do not fire on logical replication subscribers by default. If the target database is a subscriber, sync triggers will not fire, leaving columns out of sync. stitch should document this limitation. Using `ALTER TABLE ... ENABLE ALWAYS TRIGGER` is possible but risky (may cause loops). **OPEN:** Determine the recommended approach for logical replication environments.

3. **Partitioned tables:** Since stitch targets PG 14+ (test matrix), trigger inheritance from the partitioned parent table is always available (PG 13+ feature). stitch installs sync triggers on the parent table; they automatically apply to all partitions. Backfills must be partition-aware (iterate per-partition for progress tracking and to avoid lock escalation).

4. **COPY performance:** `BEFORE INSERT` triggers fire during `COPY`, which may significantly impact bulk load performance during the expand phase. Document this trade-off.

5. **Trigger installation lock:** Creating a trigger takes `AccessExclusiveLock` on the table. This is the same lock type that the expand/contract pattern is designed to avoid for the overall migration. The lock is brief (metadata-only), but on a high-traffic table with long-running queries, it may require `lock_timeout` and retry logic.

6. **Concurrency control:** The expand/contract phase tracker must use advisory locks to prevent concurrent phase transitions (e.g., two operators running `--phase contract` simultaneously).

### 5.5 Batched background DML (v2.1)

Queue-based large-table data migrations, entirely inside Postgres.

**Queue architecture:**
- 3-partition rotating table (PGQ-inspired from SkyTools)
- No external dependencies (no Redis, no Kafka)
- All job state visible in Postgres via querying `stitch.*` tables. `pg_stat_activity` shows the currently executing batch query and its duration/wait events, but job-level state (pending/running/done/failed) is in the queue tables.

**Job lifecycle:**
```
pending → running → done
               ↓
             failed → (retry) → done
                             → dead (max retries exceeded)
                                  ↓
                              (manual retry) → running
```

A `dead` job can be manually retried after the operator fixes the underlying issue. stitch tracks the last processed primary key so that retried jobs resume from where they stopped, not from the beginning.

**Worker heartbeat:** The batch queue table includes a `heartbeat_at` column updated at the start of each batch. A configurable staleness threshold (default: 5 minutes, configurable via `stitch.toml` `[batch] heartbeat_staleness`) determines when a worker is considered dead. On `stitch batch status` and at the start of any batch operation, stitch checks for running jobs with stale heartbeats and marks them as `failed` with an error message indicating the worker was unresponsive. This handles the case where a batch worker process dies silently (OOM kill, network partition) and leaves a job in `running` state indefinitely.

**CLI:**
```bash
stitch batch add backfill_user_tier --table users --batch-size 500 --sleep 100ms
stitch batch list
stitch batch status backfill_user_tier
stitch batch pause backfill_user_tier
stitch batch resume backfill_user_tier
stitch batch cancel backfill_user_tier
stitch batch retry backfill_user_tier    # manual retry of dead job
```

**Features:**
- Configurable batch size, sleep interval, lock timeout, and statement timeout per batch
- Progress: rows done / rows remaining / ETA
- Per-batch transaction — each batch commits independently
- Replication lag monitoring: query `pg_stat_replication.replay_lag` and pause the batch job when lag exceeds a configurable threshold (default: 10s). Most production databases have replicas; unthrottled batched writes will cause replica lag incidents.
- VACUUM pressure awareness: monitor `pg_stat_user_tables.n_dead_tup` and pause if dead tuple ratio exceeds a configurable percentage (default: 10%). The ratio is computed as `n_dead_tup / (n_live_tup + n_dead_tup)`. Using a ratio rather than an absolute count ensures the threshold is meaningful regardless of table size. Configurable via `stitch.toml` `[batch] max_dead_tuple_ratio`. Many small transactions create dead tuples; autovacuum may not keep up on hot tables.
- Connection management: the batch worker requires a direct PostgreSQL connection (not through PgBouncer in transaction mode) because it uses session-level settings and the connection must persist across sleep intervals. SET statements (`lock_timeout`, `statement_timeout`, `search_path`) are re-issued at the start of each batch transaction as a safety measure.
- Inspired by GitLab `BatchedMigration`: throttling, pause/resume, retry, state tracking in Postgres
  - https://gitlab.com/gitlab-org/gitlab/-/blob/master/lib/gitlab/database/migration_helpers.rb
  - https://docs.gitlab.com/development/database/batched_background_migrations/

### 5.6 CI integration (v1.0+)

```bash
# GitHub Actions
stitch analyze --format github-annotations  # native GH annotation format
stitch analyze --format json | jq .         # structured for any CI

# GitLab CI
stitch analyze --format gitlab-codequality  # native GL code quality report

# General
stitch analyze --strict                     # exit non-zero on any finding (warnings treated as errors)
```

Note: `stitch analyze` returns exit code 2 when error-level findings exist (default behavior). The `--strict` flag additionally treats warnings as errors for exit code purposes. `--strict` is the CLI equivalent of `error_on_warn = true` in `stitch.toml` `[analysis]` config. This replaces the earlier `--exit-code` flag which was redundant with the default behavior.

**Reporter format specifications:**

- **`text`** (default): Human-readable output with colors when stdout is a TTY.
- **`json`**: Structured output following a defined schema:
  ```json
  {
    "version": 1,
    "metadata": { "files_analyzed": 3, "rules_checked": 21, "duration_ms": 42 },
    "findings": [ { "ruleId": "SA004", "severity": "warn", "message": "...", "location": { "file": "...", "line": 5, "column": 1 }, "suggestion": "..." } ],
    "summary": { "errors": 0, "warnings": 1, "info": 0 }
  }
  ```
- **`github-annotations`**: GitHub Actions workflow commands: `::error file={file},line={line},col={col}::{message}` and `::warning ...`. Appear inline in PR diffs.
- **`gitlab-codequality`**: GitLab Code Quality JSON schema: `[{"description": "...", "check_name": "SA004", "fingerprint": "...", "severity": "major", "location": {"path": "...", "lines": {"begin": 5}}}]`. Severity mapping from stitch to GitLab Code Quality: `error` → `critical`, `warn` → `major`, `info` → `minor`. Fingerprint computation: SHA-1 of `(ruleId, filePath, line)` — this produces stable fingerprints for deduplication across CI runs.

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

### 5.9 Lock timeout guard (v1.0)

Moved from v1.1 to v1.0 — this is core safety infrastructure.

Automatically prepend `SET lock_timeout = '5s'` before any DDL that could take a long lock, unless the migration already sets it. Configurable via `stitch.toml`. Can be disabled.

Detection: stitch scans the deploy script for any `SET lock_timeout` statement at the top level. If found, the auto-prepend is skipped for that script.

**Timeout behavior on failure:** When `lock_timeout` fires, the statement fails and the transaction rolls back. stitch reports the error with actionable guidance: which lock was contended, suggestion to retry, and optionally identify the blocking query via `pg_stat_activity`.

**Lock retry for CI:** `stitch deploy --lock-retries N` (default: 0, no retry) retries acquiring the lock up to N times with exponential backoff (starting at 1 second, doubling each retry, capped at 30 seconds). This is designed for CI pipelines where the operator is not present to manually re-trigger a deploy after a transient lock conflict. Configurable via `stitch.toml` `[deploy] lock_retries`.

**Per-migration override:** Individual migrations can set their own `lock_timeout` (e.g., a `VALIDATE CONSTRAINT` that needs a longer timeout). The auto-prepend is suppressed when the script contains its own `SET lock_timeout`.

### 5.10 Dry-run mode

```bash
stitch deploy --dry-run   # prints what would be deployed, runs analysis, exits
```

**What dry-run does:** Validates the plan, checks dependency order, runs static analysis, reports what changes would be deployed. Zero database modifications (verified in tests via table counts before/after).

**What dry-run does NOT do:** Predict lock contention (depends on concurrent queries), estimate DDL duration (depends on table size), verify data-dependent operations (constraints, COPY), or check disk space. Dry-run validates the plan and runs static analysis but does not simulate execution.

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

**`stitch.*` schema:** When stitch-specific features are used (expand/contract, batched DML), a separate `stitch.*` schema is created. This schema is created only on first use of these features — never during basic deploy/revert/verify operations. The `stitch.*` schema is independent of `sqitch.*` and can be safely dropped if reverting to Sqitch (advanced features will stop working, but core migration tracking is unaffected).

### DD4 — SQL parser

We use a PostgreSQL-aware SQL parser for static analysis, not regex.

Options evaluated:
- `pgsql-parser` (npm) — JS wrapper around the actual PG parser (`libpg_query`). Exact fidelity, same AST as Postgres. Use this.
- `pg-query-parser` — older, same approach
- Hand-rolled regex — too fragile for production rules

Decision: **`pgsql-parser`** (or equivalent). If AST is unavailable for some construct, fall back to regex with a clear comment.

**psql metacommand pre-processing:** `pgsql-parser` / `libpg_query` parses SQL, not psql metacommands. `\i`, `\ir`, `\set`, `\copy`, etc. are client-side directives. Before passing SQL to the parser, a pre-processing stage must: (1) scan for `\i`/`\ir` lines and resolve includes, (2) strip or record other psql metacommands (`\set`, `\pset`, `\timing`, etc.) with an info-level note, (3) pass the assembled SQL to `pgsql-parser`.

### DD5 — Plan file is source of truth

stitch never modifies `sqitch.plan` without an explicit command. The plan file is append-only during `add`, never rewritten during deploy/revert.

### DD6 — No magic sequencing

Like Sqitch, stitch uses explicit dependency declarations (`-r dep1 -r dep2`), not sequential numbers. Sequential numbers create false ordering assumptions and merge conflicts. Dependencies are explicit.

**Conflict dependencies:** Sqitch supports `--conflict dep` (or `!dep` in the plan file), meaning "this change cannot be deployed if `dep` is currently deployed." Before deploying a change, stitch must check that all requires are deployed and no conflicts are deployed. If a conflict is deployed, deploy fails with an error.

### DD7 — Expand/contract is opt-in

The expand/contract pattern requires application-side changes. stitch never automatically applies it. It provides the primitives and tracks state. Engineers choose when to use it.

### DD8 — All state in Postgres

No lock files, no local state files, no `.stitch/` directory with runtime state. Everything that matters (what's deployed, batch job state, expand/contract phase) lives in the database. This makes stitch safe to run from multiple machines (CI + developer laptop) without coordination.

### DD9 — 3-partition queue, not SKIP LOCKED

For batched DML, we use a PGQ-style 3-partition rotating table rather than `SELECT ... FOR UPDATE SKIP LOCKED`. Reason: partition rotation provides automatic cleanup, explicit job state tracking, and visibility into queue depth without scanning all rows.

**OPEN:** The PGQ rotation model may not be a natural fit for job queues that need state updates, random access, and pause/resume. The stated reasons for rejecting `SKIP LOCKED` (cleanup, state tracking, depth visibility) can also be achieved with simpler designs (DELETE of completed jobs, status column, partial index on status). Revisit during implementation and validate the 3-partition approach against a simpler `SKIP LOCKED` design in a spike.

### DD10 — No hidden network calls

stitch never calls external services without explicit configuration. No telemetry, no update checks, no LLM calls unless `stitch explain`/`stitch review` is explicitly invoked.

### DD11 — Sqitch as the oracle for compatibility testing

We run Sqitch and stitch side-by-side against identical databases and compare output, tracking table state, and exit codes. Sqitch is the ground truth. Any divergence is a bug in stitch.

This means Sqitch must be installed in CI. It is available as a Docker image (`sqitch/sqitch`) and as a Perl cpan package. We use the Docker image to avoid Perl runtime management.

We maintain a corpus of real-world Sqitch projects as test fixtures (anonymized where needed). Each fixture is tested against both tools and outputs compared.

### DD12 — Script execution model: psql vs node-postgres

**The problem:** Sqitch shells out to `psql` to execute migration scripts. It does NOT use a programmatic database driver. This matters because many real-world Sqitch migration scripts use psql metacommands: `\i` (include), `\ir` (include relative), `\set` (variable substitution), `\copy` (client-side copy), `\if`/`\elif`/`\else`/`\endif` (conditionals), `\echo`, etc. `node-postgres` cannot handle any of these — they are client-side directives, not SQL.

Sqitch also sets `ON_ERROR_STOP=1` when invoking psql, which aborts on the first error. It disables `.psqlrc` via environment variables.

**Decision: OPEN.** This is the most consequential architecture decision in the project. Three options:

**(a) Shell out to psql (like Sqitch).** Full compatibility. psql must be installed on the deploy machine. The `--db-client` flag specifies the psql path. This is what Sqitch does and is the safest path to compatibility. Drawback: requires psql binary, limits control over execution, complicates error handling.

**(b) Use node-postgres with a psql metacommand pre-processing layer.** Parse migration scripts for psql metacommands before execution. Handle `\i`/`\ir` (inline the included file), `\set` (variable substitution), strip `\echo`/`\timing`, error on unsupported commands (`\copy`, `\if`). This gives stitch full control over execution but is a significant implementation effort and will inevitably have compatibility gaps.

**(c) Support both modes.** Default to psql (full compat), with a `--engine native` flag that uses node-postgres directly (faster, no psql dependency, but limited metacommand support). Scripts using only standard SQL work in both modes.

Resolve before Sprint 2 (plan + tracking). This decision affects the data flow, error handling, `--mode` transaction semantics, `ON_ERROR_STOP` behavior, `--set` variable substitution, and snapshot includes.

**`--mode` transaction semantics (depends on DD12):**
- `change` mode (default): each change in its own transaction.
- `all` mode: see note below on Sqitch behavior.
- `tag` mode: changes grouped by tag, each tag-group in a transaction.

**Sqitch `--mode all` behavior (important):** Sqitch's `_deploy_all` does NOT use a single wrapping transaction. It uses per-change transactions with explicit revert on failure — if change N fails, Sqitch explicitly reverts changes N-1, N-2, etc. that were already committed. This means intermediate committed states are visible to other sessions between changes. stitch may choose to improve upon this by offering true single-transaction semantics for `--mode all` (where supported by the execution model), but must document the behavioral difference. When stitch uses single-transaction `--mode all`, failure rolls back atomically (no partial state visible); Sqitch's approach shows intermediate states and relies on explicit revert.

If using psql, Sqitch does NOT wrap deploy scripts in a transaction managed by Sqitch — it passes transaction control to psql and manages tracking separately. If using node-postgres, stitch manages `BEGIN`/`COMMIT` directly. The transaction boundary differs by mode.

### DD13 — PgBouncer compatibility and advisory locks

**The problem:** Most production PostgreSQL deployments use PgBouncer for connection pooling. PgBouncer in transaction mode has significant implications for stitch:

- `pg_advisory_lock` (session-level) does not work through PgBouncer in transaction mode — the lock is tied to the backend connection, which PgBouncer may reassign between transactions.
- `pg_advisory_xact_lock` (transaction-level) releases at transaction end — it cannot span the entire deploy in `--mode change` (each change is a separate transaction) or across non-transactional changes. This makes it unsuitable for deploy coordination.
- Session-level `SET` commands (`lock_timeout`, `statement_timeout`, `search_path`) may leak to other connections or be lost between transactions.
- The batch worker's sleep interval between batches causes the connection to return to the pool; the next batch may run on a different backend.

**Decision:** stitch deploy, revert, rebase, checkout, and batch operations require direct PostgreSQL connections, not PgBouncer in transaction mode. stitch will:

1. **Use session-level advisory locks:** Deploy coordination uses session-level advisory locks. The default mode is non-blocking: `pg_try_advisory_lock(<lock_key>)`, which returns `false` immediately if the lock is held by another session. If the lock is not acquired, stitch exits with code 4 (concurrent deploy detected). For CI environments where waiting is preferred, an alternative wait mode is available: `SET lock_timeout = '<advisory_lock_timeout>'` followed by `pg_advisory_lock(<lock_key>)`. The `advisory_lock_timeout` is configurable (default: 30 seconds) via `stitch.toml` `[deploy] advisory_lock_timeout`. If the timeout expires, stitch exits with code 5 (lock timeout). The lock key is computed in the application layer as `pg_advisory_lock(<namespace_constant>, <project_hash>)` using the two-argument form with a fixed namespace constant and a stable application-computed hash of the project name. This avoids using `hashtext()`, whose output is NOT guaranteed stable across PostgreSQL major versions (the hash function can change during upgrades). The lock is held for the entire deploy session and released explicitly on completion via `pg_advisory_unlock()` (or automatically on disconnect for crash recovery). The same lock must be acquired for `revert`, `rebase`, and `checkout` — any command that modifies tracking state or executes DDL/DML. **Note:** Sqitch uses `LOCK TABLE sqitch.changes IN EXCLUSIVE MODE` (a table-level lock inside each change's transaction) for concurrency control. stitch's advisory lock approach is a stitch improvement that provides stronger coordination (spans the full deploy session, not just individual transactions).
2. **Detect PgBouncer:** Attempt `SHOW pool_mode` (PgBouncer-specific command that returns the pool mode; errors on direct PG connections). This is a best-effort detection — it works for standard PgBouncer installations but may not detect all pooler configurations. If PgBouncer in transaction mode is detected, emit an **error** (not just a warning) for deploy/revert/rebase/checkout operations, as session-level advisory locks are not safe. The `connection_type` config option in `stitch.toml` is the reliable mechanism: `connection_type = "direct"` for non-PgBouncer poolers, PgBouncer in session mode, or when `SHOW pool_mode` detection is unreliable.
3. **Re-issue SET commands:** At the start of each transaction, re-issue any session-level settings (`lock_timeout`, `statement_timeout`, `search_path`) as a safety measure.
4. **Document:** Require direct PostgreSQL connections for deploy/batch operations. Application traffic can continue to use PgBouncer.

### DD14 — Deploy connection session settings

Deploy connections should set:
- `application_name = 'stitch/<command>/<project>'` — e.g., `stitch/deploy/myproject`. Visible in `pg_stat_activity`, critical for DBAs diagnosing lock contention or long-running queries during incidents.
- `statement_timeout = 0` (or a configurable high value) — migrations are inherently long-running; a global `statement_timeout` (common in production, e.g., 30s) will kill legitimate operations like `VALIDATE CONSTRAINT`. For non-transactional DDL (e.g., `CREATE INDEX CONCURRENTLY`), a separate configurable timeout applies (default: 4 hours, configurable via `stitch.toml` `[deploy] non_transactional_statement_timeout`). This prevents indefinite hangs while allowing legitimately long operations.
- `idle_in_transaction_session_timeout` — set to a configurable generous value (default: 10 minutes), not unlimited. This provides a safety net against hung deploy processes (e.g., operator walks away during a TUI prompt) without interfering with normal operation. Configurable via `stitch.toml` `[deploy] idle_in_transaction_session_timeout`.
- `lock_timeout` — set by the lock timeout guard (5.9), per-migration configurable.
- `search_path` — respect the database/role default (Sqitch-compatible behavior). Sqitch does not set `search_path`; stitch follows suit. Override available via `stitch.toml` `[deploy] search_path` for teams that want explicit control.

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
│   │   ├── rework.ts           # Rework existing change
│   │   ├── rebase.ts           # Revert + deploy convenience
│   │   ├── bundle.ts           # Package for distribution
│   │   ├── checkout.ts         # VCS branch switch
│   │   ├── show.ts             # Display change/tag details
│   │   ├── plan.ts             # Display plan contents
│   │   ├── upgrade.ts          # Upgrade registry schema
│   │   ├── analyze.ts          # Static analysis (standalone)
│   │   ├── explain.ts          # AI explain
│   │   ├── batch.ts            # Batched DML commands
│   │   └── diff.ts
│   ├── plan/
│   │   ├── parser.ts           # sqitch.plan parser (pragmas, reworked changes, @tag refs)
│   │   ├── writer.ts           # sqitch.plan writer
│   │   └── types.ts            # Change, Tag, Dependency types
│   ├── db/
│   │   ├── client.ts           # pg connection wrapper
│   │   ├── registry.ts         # sqitch.* table operations
│   │   └── introspect.ts       # Schema introspection for connected analysis rules
│   ├── analysis/
│   │   ├── index.ts            # Analyzer entry point, rule registry
│   │   ├── parser.ts           # SQL AST parsing (pgsql-parser), byte-offset→line/col conversion
│   │   ├── preprocess.ts       # psql metacommand pre-processing (\i, \set, etc.)
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
├── spec/
│   ├── SPEC.md
│   └── SPEC-CHANGELOG.md
├── README.md
├── package.json
├── tsconfig.json
└── stitch.toml.example
```

**`sqitch.conf` format:** Sqitch uses a Git-style INI configuration format (not TOML — parsed by `Config::GitLike`). stitch must parse this format including sections, subsections, multi-valued keys, and includes:
```ini
[core]
    engine = pg
    top_dir = migrations
    plan_file = migrations/sqitch.plan
[engine "pg"]
    target = db:pg:mydb
    client = /usr/bin/psql
[deploy]
    verify = true
    mode = change
[target "production"]
    uri = db:pg://user@host/dbname
```

The `[deploy]` section controls default deploy behavior: `verify` (default: `true`, run verify scripts after each change), `mode` (default: `change`, transaction scope). These correspond to `--verify`/`--no-verify` and `--mode` command-line flags.

**Configuration precedence:** system (`$(prefix)/etc/sqitch/sqitch.conf`) < user (`~/.sqitch/sqitch.conf`) < project (`./sqitch.conf`) < `stitch.toml` (stitch-only features) < environment variables < command-line flags.

**Target URI scheme:** Sqitch uses a `db:` URI scheme: `db:pg://user:pass@host:port/dbname`. stitch must accept both `db:pg:` URIs (Sqitch compat) and standard PostgreSQL URIs (`postgresql://...`).

**Default paths:** plan file = `./sqitch.plan`, top dir = `.`, deploy dir = `./deploy`, revert dir = `./revert`, verify dir = `./verify`. All overridable in `sqitch.conf` under `[core]`.

### Data flow — deploy

```
stitch deploy
  → parse sqitch.conf + stitch.toml
  → connect to database (set application_name, statement_timeout=0, idle_in_transaction_session_timeout=10min)
  → acquire session-level advisory lock (default: non-blocking)
    → pg_try_advisory_lock(<lock_key>) — returns false immediately if lock held
    → if lock not acquired: exit 4 (concurrent deploy detected)
    → alternative wait mode (CI): SET lock_timeout = '<advisory_lock_timeout>'; pg_advisory_lock(<lock_key>)
      → if timeout exceeded: exit 5 (lock timeout)
    → lock_key: application-computed stable hash (see I3 note in DD13)
    → (requires direct connection — not PgBouncer in transaction mode)
  → read sqitch.* tracking tables
  → compute pending changes (topological sort by dependency)
  → check conflict dependencies (no conflicts may be currently deployed)
  → for each pending change:
      → resolve \i includes (snapshot or HEAD)
      → pre-process psql metacommands
      → run static analysis
      → if error: abort (unless --force)
      → if warn: print, continue
      → if change is non-transactional:
          → execute deploy script WITHOUT transaction wrapper
          → on success: BEGIN; update sqitch.changes, sqitch.events, sqitch.dependencies; COMMIT
          → on failure: report error, note that partial DDL may remain (e.g., INVALID index)
      → else (normal transactional change):
          → BEGIN
          → SET lock_timeout (if guard enabled and script doesn't set its own)
          → execute deploy script
          → update sqitch.changes, sqitch.events, sqitch.dependencies
          → COMMIT
  → release advisory lock: pg_advisory_unlock(<lock_key>)
  → print summary
```

**Non-transactional changes:** stitch marks non-transactional changes via a plan file pragma added by `stitch add --no-transaction`. Additionally, stitch recognizes a `-- stitch:no-transaction` comment on the first line of the deploy script as a stitch-only convention. **Note:** Sqitch does NOT have a `-- sqitch-no-transaction` convention — no evidence of this mechanism exists in the Sqitch source code. Sqitch always wraps changes in `begin_work`/`finish_work`. The script comment convention is a stitch-only innovation for standalone linter mode (SA020 detection) and should not be described as Sqitch-compatible. During deploy, non-transactional changes execute without `BEGIN`/`COMMIT` wrapping. A separate configurable `statement_timeout` applies to non-transactional DDL (default: 4 hours, configurable via `stitch.toml` `[deploy] non_transactional_statement_timeout`).

**Non-transactional write-ahead tracking:** Before executing non-transactional DDL, stitch writes a "pending" record to `stitch.pending_changes` (in its own committed transaction). After the DDL succeeds, the record is updated to "complete" and the sqitch tracking tables are updated. On the next deploy, stitch checks for any "pending" non-transactional changes and verifies their state before deciding to skip or retry. This handles the case where stitch crashes between DDL execution and tracking table update.

**`stitch.pending_changes` schema:**
```sql
CREATE TABLE stitch.pending_changes (
    change_id       TEXT        PRIMARY KEY,
    change_name     TEXT        NOT NULL,
    project         TEXT        NOT NULL,
    script_path     TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'complete', 'failed')),
    error_message   TEXT
);
```

**Non-transactional verify logic:** When stitch finds a "pending" record on the next deploy, it verifies the change's state. For index operations (`CREATE INDEX CONCURRENTLY`), stitch checks `pg_index.indisvalid` to determine if the index was successfully created. For other DDL, stitch runs the change's verify script (if one exists). Automated verification only works for known DDL patterns — for arbitrary DDL without a verify script, stitch reports the pending state and requires manual resolution.

Failure recovery for non-transactional DDL is fundamentally different: a failed `CREATE INDEX CONCURRENTLY` leaves an `INVALID` index that must be cleaned up. The error message must include the exact command to drop the INVALID index before retrying.

**`ALTER TYPE ... ADD VALUE` note:** On PG < 12, `ALTER TYPE ... ADD VALUE` cannot run inside a transaction block and must be marked non-transactional. On PG 12+, it can run inside a transaction, but the new enum value is **not usable within the same transaction** — an `INSERT` using the new value in the same transaction will fail. If a deploy script does `ALTER TYPE ... ADD VALUE 'x'` followed by `INSERT ... VALUES ('x')`, they must be in separate changes or the change must be non-transactional.

**Transaction scope by `--mode`:**
- `change` (default): each change in its own transaction (as shown above).
- `all`: all changes in a single transaction. Failure rolls back everything including tracking table updates. Note: this is a stitch improvement — Sqitch uses per-change transactions with explicit revert on failure (see DD12).
- `tag`: changes grouped by tag, each tag-group in a single transaction.

Non-transactional changes always execute outside any transaction regardless of `--mode`. In `--mode all`, non-transactional changes break the surrounding transaction: stitch issues `COMMIT` before the non-transactional DDL, executes it, then issues `BEGIN` to continue with subsequent changes. This means `--mode all` cannot guarantee atomicity when non-transactional changes are present. If a transactional change fails after a non-transactional change has already committed, the non-transactional DDL remains deployed (its tracking update was committed separately). The subsequent transactional changes roll back, including their tracking records. This leaves a partially-deployed state where `stitch status` correctly reports which changes are deployed and which are not. stitch emits a warning at the start of deploy when `--mode all` is used with a plan containing non-transactional changes.

---

## 8. Testing strategy

Reliability is non-negotiable for a migration tool. A bug in a migration runner can corrupt production databases. We invest heavily in testing infrastructure from day one.

### 8.1 Test pyramid

```
         ┌──────────────┐
         │  E2E compat  │  ← Sqitch oracle tests (slowest, highest confidence)
        ┌┴──────────────┴┐
        │  Integration   │  ← Real Postgres, real filesystem
       ┌┴────────────────┴┐
       │    Unit tests    │  ← Pure functions, no I/O (fastest)
       └──────────────────┘
```

### 8.2 Unit tests

Fast, no I/O, run on every commit.

**Plan parser**
- Round-trip: parse every valid sqitch.plan format → serialize → parse again, result identical
- All comment styles (`#`, blank lines)
- All pragmas (`%syntax-version`, `%project`, `%uri`)
- Reworked changes: duplicate change names with `@tag` references, change ID disambiguation
- All dependency forms (`@tag`, `change`, `project:change`, `!conflict`)
- Cross-project dependencies (`project:change` syntax)
- Change entry format: timestamp + planner name + email + note
- Unicode in change names and notes
- Edge cases: empty plan, plan with only pragmas, plan with tags only
- Change name character set validation (alphanumeric, hyphens, underscores, forward slashes)
- Note parsing: `#` in middle of change line starts the note vs. `#` at beginning of line is a comment

**Change ID computation**
- Compute change IDs for known Sqitch test cases and verify byte-for-byte match

**Config parser**
- `sqitch.conf` Git-style INI format: sections, subsections (`[engine "pg"]`), multi-valued keys
- `db:pg:` URI scheme parsing and conversion to standard PostgreSQL URI
- `stitch.toml` overrides: precedence rules (system < user < project < stitch.toml < env vars < flags)
- Invalid config: clear error messages, no panics

**Analysis rules**
For each rule SA001–SA021:
- SQL strings that must trigger the rule (positive cases, with location info)
- SQL strings that must NOT trigger (false positive prevention)
- Version-aware cases: SQL safe on PG 17 but dangerous on PG 14 (e.g. SA002b)
- Multi-statement scripts: rule fires on correct statement, not adjacent ones
- Rule interactions: two rules on same statement both fire independently
- PL/pgSQL body exclusion: DML inside CREATE FUNCTION / DO blocks does not fire SA010/SA011/SA008
- Inline suppression: `-- stitch:disable SA010` prevents rule from firing
- SA003 safe-cast allowlist: verify each safe cast does NOT fire, each unsafe cast does fire
- SA020: CREATE INDEX CONCURRENTLY detection in transactional context

For connected rules (SA009, SA011): fixtures include companion `.context.json` files with mock introspection data (table schemas, row estimates, index lists).

**Snapshot includes**
- Mock git: given commit hash → file content mapping, verify correct version resolved
- Fallback: no git repo → HEAD used, no error
- Missing file: clear error, not silent skip
- Nested includes: `\i a.sql` where `a.sql` also has `\i b.sql`

**Topological sort**
- Linear deps: A → B → C deploys in order
- Diamond deps: A → B, A → C, D → B, D → C deploys correctly
- Cycle detection: circular deps produce clear error before any deploy
- Partial deploy `--to <change>`: correct subset selected
- Conflict dependencies: change with `!conflict` fails if conflict is deployed

**Exit codes**
- Each exit code scenario produces the correct code
- Analysis error (2) vs deploy failure (1) vs verification failure (3) vs concurrent deploy (4) vs lock timeout (5) vs database unreachable (10) are distinct

### 8.3 Integration tests

Real Postgres via Docker. No mocks for DB layer. Each test gets a fresh database.

**PG version matrix: 14, 15, 16, 17, 18.**

PG < 14 is best-effort/untested. Version-aware rules (SA002b) still fire based on `pg_version` config without integration tests on old versions.

**Command coverage (per PG version):**

| Command | What we test |
|---------|-------------|
| `init` | Creates sqitch.conf, sqitch.plan, deploy/ revert/ verify/ dirs |
| `add` | Creates correctly named files, appends to plan, handles `-r` deps and `--conflict` |
| `add --no-transaction` | Plan entry contains no-transaction pragma |
| `deploy` | Executes SQL, updates sqitch.changes + sqitch.events + sqitch.dependencies, correct timestamps |
| `deploy --to` | Stops at specified change, tracking state correct |
| `deploy --dry-run` | Zero DB changes (verified via table counts before/after) |
| `deploy --mode change` | Each change in own transaction, stops on first failure, tracking state consistent |
| `deploy --mode all` | All changes in single transaction (stitch improvement over Sqitch's per-change txn + explicit revert), failure rolls back everything |
| `deploy --mode tag` | Changes grouped by tag, each group in a transaction |
| `deploy` (non-transactional) | `CREATE INDEX CONCURRENTLY` executes without transaction wrapper, tracking updated separately |
| `revert` | Reverts in reverse dependency order, updates tracking tables |
| `revert --to` | Reverts to specified change, not further |
| `verify` | Runs verify script, PASS/FAIL per change, correct exit code |
| `status` | Correct pending count, deployed count, last deployed change |
| `log` | Full history in correct order, timestamps reasonable |
| `tag` | Tag appears in plan, visible in status/log |
| `rework` | Creates reworked change, plan file has duplicate name with @tag reference |

**Failure scenarios:**
- Deploy script fails (SQL error): tracking tables left consistent, revert possible
- Deploy script fails mid-batch (multiple changes): partial state recoverable
- Verify script fails: correct exit code 3, clear output
- Database unreachable: exit code 10, clear error message
- Concurrent deploy from two processes: second process gets exit code 4, first completes
- Non-transactional deploy fails: INVALID index detected, clear guidance on cleanup
- Lock timeout exceeded: exit code 5, actionable error message

**Advisory lock tests:**
- `pg_try_advisory_lock(<lock_key>)` acquired at deploy start, released on completion
- Same lock acquired for revert, rebase, and checkout operations
- Second concurrent deploy fails immediately (non-blocking mode), reports exit code 4
- Wait mode: `pg_advisory_lock` with `lock_timeout` times out, reports exit code 5
- Crashed deploy: advisory lock auto-released on disconnect, next deploy succeeds
- PgBouncer detection: `SHOW pool_mode` triggers error for deploy/revert in transaction mode

**Dependency scenarios:**
- Diamond dependency deploys correctly
- Missing dependency detected before deploy starts
- Circular dependency detected before deploy starts
- Conflict dependency: deploy blocked if conflicting change is currently deployed

**Schema isolation:**
- `sqitch.*` schema created correctly on first deploy (using IF NOT EXISTS)
- Existing `sqitch.*` schema from prior Sqitch deployment used without modification
- Concurrent first-deploy (two processes, fresh DB): advisory lock prevents race condition

### 8.4 Sqitch oracle / compatibility tests

The most important test suite. Sqitch is the ground truth. We run identical operations against identical databases with both tools and compare results.

**Infrastructure:**
- Sqitch runs via Docker image `sqitch/sqitch:latest` — no Perl runtime needed
- Both tools share the same Postgres container
- Test harness executes a command with Sqitch, snapshots DB state, resets, executes same command with stitch, snapshots DB state, diffs

**What we compare:**

| Artifact | How we compare |
|----------|----------------|
| `sqitch.plan` output | Byte-for-byte identical after `add`, `tag`, `rework` |
| `sqitch.changes` table | All columns: change_id, script_hash, change, project, note, committed_at (within tolerance), committer_name, committer_email, planned_at, planner_name, planner_email |
| `sqitch.dependencies` table | All columns: change_id, type, dependency, dependency_id |
| `sqitch.events` table | All columns: event, change_id, change, project, note, requires, conflicts, tags, committed_at (within tolerance), committer_name, committer_email, planned_at, planner_name, planner_email |
| `sqitch.tags` table | All columns: tag_id, tag, project, change_id, note, committed_at (within tolerance), committer_name, committer_email, planned_at, planner_name, planner_email |
| `stitch status` stdout | Semantically equivalent (pending count, deployed count, last change name) |
| `stitch log` stdout | Same changes in same order, same metadata |
| Exit codes | Identical for all success and failure scenarios |

**Timestamp tolerance:** committed_at / planned_at compared within 5 seconds (wall clock differences between runs).

**Test fixture corpus** — maintained in `tests/fixtures/sqitch-projects/`:

| Fixture | Description |
|---------|-------------|
| `minimal/` | 1 change, no deps, no tags |
| `linear/` | 10 changes, linear deps A→B→C... |
| `diamond/` | Diamond dependency graph |
| `tagged/` | Multiple tags, partial deploy to tag |
| `with-includes/` | `\i` shared SQL files |
| `reworked/` | Changes reworked with `@tag` references |
| `cross-project/` | Cross-project dependencies (`project:change`) |
| `conflicts/` | Changes with conflict dependencies |
| `non-transactional/` | Changes marked `--no-transaction` |
| `mid-deploy/` | Project partially deployed by Sqitch, stitch continues |
| `real-world-1/` | Anonymized real project, ~50 changes |
| `real-world-2/` | Anonymized real project, ~200 changes, multiple tags |

**The "mid-deploy handoff" test** — most important for adoption:
1. Deploy first half of project with real Sqitch
2. Switch to stitch for second half
3. Verify tracking tables consistent, all remaining changes deploy correctly
4. Verify stitch status matches what sqitch status would show for the full deployment
5. Verify change IDs computed by stitch match those computed by Sqitch for the same changes

**The "reverse handoff" test** — safety net for adoption:
1. Deploy full project with stitch
2. Switch to Sqitch
3. Verify `sqitch status` reads stitch-written tracking tables correctly
4. Verify `sqitch log` shows correct history
5. Add a new change with Sqitch, deploy it, verify tracking tables are consistent
6. Revert a stitch-deployed change with Sqitch, verify tracking state

This bidirectional test validates that teams can safely evaluate stitch and revert to Sqitch if needed.

### 8.5 Analysis correctness tests

For each analysis rule, maintain a fixture directory:

```
tests/fixtures/analysis/
  SA001/
    trigger/
      add_column_not_null_no_default.sql        # must trigger
    no_trigger/
      add_column_nullable.sql                   # must NOT trigger
      add_column_not_null_with_default.sql      # must NOT trigger (PG 17)
    version_aware/
      add_column_not_null_with_default.pg11.trigger.sql  # triggers on PG < 11
  SA002/
    trigger/
      add_column_default_random.sql             # must trigger (volatile, all versions)
      add_column_default_gen_random_uuid.sql    # must trigger (volatile, all versions)
      add_column_default_clock_timestamp.sql    # must trigger (volatile, all versions)
    no_trigger/
      add_column_default_now.sql                # must NOT trigger on PG >= 11 (now() is STABLE)
      add_column_default_literal.pg17.sql       # must NOT trigger on PG >= 11
    ...
  SA009/
    trigger/
      add_fk_no_index.sql                       # must trigger
      add_fk_no_index.context.json              # mock introspection data
    ...
  SA020/
    trigger/
      concurrent_index_in_transaction.sql       # must trigger
    no_trigger/
      concurrent_index_no_transaction.sql       # must NOT trigger
    ...
```

Test runner:
- For every `trigger/` file: analysis must produce the rule ID at correct severity
- For every `no_trigger/` file: analysis must produce zero findings for that rule
- Version-aware files: tested against relevant PG versions in matrix

False positive rate tracked as a metric. Any new rule must have >=5 no_trigger fixtures. Complex rules (SA003 with many type pairs, SA002 with version/volatility matrix) should have 10-20+.

### 8.6 Performance tests

Migration tooling must be fast even on large plan files.

- Plan parse: 10,000-change plan file parses in < 500ms
- `stitch status` on 1,000-change deployed project: < 1s (single query, not N queries)
- Analysis: 1,000-line migration SQL analyzed in < 200ms
- `stitch log` with 10,000 entries: < 2s (pagination by default)

Run on every release, not every commit.

### 8.7 CI configuration

```yaml
# .github/workflows/ci.yml

on: [push, pull_request]

jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v2
      - run: bun install
      - run: bun test tests/unit/

  integration:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        pg: ["14", "15", "16", "17", "18"]
    services:
      postgres:
        image: postgres:${{ matrix.pg }}
        env:
          POSTGRES_PASSWORD: test
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v2
      - run: bun install
      - run: bun test tests/integration/
        env:
          PGPASSWORD: test
          PGHOST: localhost

  compat:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        pg: ["14", "15", "16", "17", "18"]
    services:
      postgres:
        image: postgres:${{ matrix.pg }}
        env:
          POSTGRES_PASSWORD: test
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v2
      - run: docker pull sqitch/sqitch:latest
      - run: bun install
      - run: bun test tests/compat/
        env:
          PGPASSWORD: test
          PGHOST: localhost
          SQITCH_IMAGE: sqitch/sqitch:latest

  analysis:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        pg: ["14", "17"]   # oldest + newest for version-aware rules
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v2
      - run: bun install
      - run: bun test tests/analysis/

  build:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v2
      - run: bun install
      - run: bun run build
      - run: ./dist/stitch --version
```

**CI policy:**
- All jobs must pass before merge to `main`
- `compat` job is the gate — no merge if Sqitch oracle tests fail
- Performance tests run on release tags only
- Test coverage reported to codecov

### 8.8 Local development

```bash
# Run all tests locally
bun test

# Run only unit tests (fast, no Docker needed)
bun test tests/unit/

# Run integration tests against local PG
PGURI=postgres://postgres:test@localhost/stitch_test bun test tests/integration/

# Run Sqitch compat tests (requires Docker)
bun test tests/compat/

# Run a specific rule's analysis tests
bun test tests/analysis/SA001

# Run the full matrix (slow — use before release)
bash scripts/test-matrix.sh
```

`docker-compose.yml` in repo root spins up PG 14–18 on ports 5414–5418 for local matrix testing.

---

## 9. Implementation plan

### Phase 0 — foundation (Sprint 1, ~1 week)

Core infrastructure. Nothing user-visible yet.

- [ ] Repo structure: `src/`, `tests/`, `fixtures/`
- [ ] `tsconfig.json`, `package.json` with bun
- [ ] `bun build --compile` producing single binary
- [ ] **Validation spike: `pgsql-parser` + `bun build --compile`** — verify that the native C addon (`libpg_query`) compiles on macOS and Linux, bundles correctly in the compiled binary, and works on a machine without build tools. If it fails, evaluate WASM alternatives (`pg-query-emscripten` or similar). This is a go/no-go for the architecture.
- [ ] CI: GitHub Actions running `bun test` on push
- [ ] Docker Compose for local PG test matrix (PG 14–18)
- [ ] `src/config.ts`: parse `sqitch.conf` (Git-style INI with subsections) and `stitch.toml`
- [ ] `src/db/client.ts`: pg connection wrapper, URI parsing (`db:pg:` and `postgresql://`), error handling
- [ ] `src/output.ts`: shared print/error/json output helpers
- [ ] `src/cli.ts`: command router with `--help`, `--version`, `--format`
- [ ] **Resolve DD12 (psql vs node-postgres execution model)** — spike both approaches, decide before Phase 1

### Phase 1 — Sqitch parity (Sprint 2–4, ~3 weeks)

After this phase: drop-in replacement for all Sqitch commands.

**Sprint 2 — plan + tracking**
- [ ] `src/plan/parser.ts`: full sqitch.plan format parser (pragmas, reworked changes, `@tag` refs, cross-project deps)
- [ ] `src/plan/writer.ts`: sqitch.plan writer (append-only for `add`, rework support)
- [ ] `src/plan/types.ts`: Change, Tag, Dependency, Project types
- [ ] Change ID computation: SHA-1 algorithm matching Sqitch byte-for-byte
- [ ] `src/db/registry.ts`: read/write `sqitch.changes`, `sqitch.events`, `sqitch.tags`, `sqitch.projects`, `sqitch.dependencies`
- [ ] `stitch init`: creates sqitch.conf, sqitch.plan, deploy/revert/verify dirs
- [ ] `stitch add`: creates migration files, appends to plan (supports `--no-transaction`, `--conflict`)
- [ ] Tests: plan round-trip, init, add, change ID verification against Sqitch

**Sprint 3 — deploy + revert**
- [ ] `src/commands/deploy.ts`: topological sort, execute deploy scripts, update tracking
- [ ] Advisory lock acquisition at deploy start: `pg_try_advisory_lock(<lock_key>)` (non-blocking default) or `pg_advisory_lock(<lock_key>)` with `lock_timeout` (wait mode) — session-level, released on completion or disconnect. Lock key: application-computed stable hash (not `hashtext()`)
- [ ] Non-transactional change support (execute without BEGIN/COMMIT, track separately)
- [ ] `src/commands/revert.ts`: execute revert scripts in reverse order, update tracking
- [ ] `--to <change>` flag for both
- [ ] `--dry-run` flag
- [ ] `--mode [all|change|tag]` flag with correct transaction boundaries
- [ ] `--log-only` flag (record as deployed without executing)
- [ ] `--set` variable substitution
- [ ] Deploy connection session settings (application_name, statement_timeout=0, idle_in_transaction_session_timeout=10min, non_transactional_statement_timeout=4h)
- [ ] Lock timeout guard: auto-prepend `SET lock_timeout` before risky DDL (configurable, enabled by default)
- [ ] Conflict dependency checking
- [ ] Partial deploy / revert with dependency validation
- [ ] Tests: deploy/revert, partial, dry-run, failed deploy recovery, advisory locks, non-transactional deploy

**Sprint 4 — verify + status + log + remaining commands**
- [ ] `stitch verify`: run verify scripts, report pass/fail per change
- [ ] `stitch status`: pending count, deployed count, last deployed, target info, modified script detection
- [ ] `stitch log`: deployment history with timestamps and committers
- [ ] `stitch tag`: create tag at current deployment state
- [ ] `stitch rework`: create reworked version of existing change
- [ ] `stitch rebase`: revert + deploy convenience command
- [ ] `stitch bundle`: package project for distribution
- [ ] `stitch checkout`: deploy/revert to match VCS branch
- [ ] `stitch show`: display change/tag details
- [ ] `stitch plan`: display plan contents
- [ ] `stitch upgrade`: upgrade registry schema
- [ ] `stitch engine`, `stitch target`, `stitch config`: manage configuration
- [ ] Compatibility test: adopt existing Sqitch project, verify parity
- [ ] Reverse handoff test: deploy with stitch, verify Sqitch reads tracking tables correctly
- [ ] Tests: verify, status, log, tag, rework, all compat tests

### Phase 2 — static analysis (Sprint 5–6, ~2 weeks)

**Sprint 5 — analysis engine**
- [ ] Integrate `pgsql-parser` (npm): parse SQL to AST (or WASM alternative if Phase 0 spike failed)
- [ ] `src/analysis/preprocess.ts`: psql metacommand pre-processing (`\i`/`\ir` resolution, `\set` handling, strip unsupported metacommands with warning)
- [ ] `src/analysis/index.ts`: analyzer entry point, rule registry, static/connected rule distinction
- [ ] `src/analysis/reporter.ts`: text / json / github-annotations / gitlab-codequality output
- [ ] `stitch analyze <file>`: standalone analysis command (works without sqitch.plan)
- [ ] `stitch analyze` (no args): analyze pending migrations
- [ ] `stitch analyze --changed`: analyze git-changed files
- [ ] Inline suppression: `-- stitch:disable SA010` comment syntax
- [ ] Per-file overrides in `stitch.toml`
- [ ] Analysis integrated into `stitch deploy` (pre-deploy, blocks on error)
- [ ] `--force` flag to bypass analysis errors
- [ ] `--format github-annotations` for CI
- [ ] Rules SA001–SA010 (column safety, index, drop, DML)
- [ ] PL/pgSQL body exclusion for SA008/SA010/SA011

**Sprint 6 — remaining rules + version awareness**
- [ ] Rules SA011–SA021 (connected rules, sequence, lock timeout, rename, constraints, concurrency)
- [ ] PG version awareness in rules (SA002/SA002b volatility + version, SA017 PG 12+ CHECK pattern, SA019 PG 12+ REINDEX CONCURRENTLY)
- [ ] `stitch.toml` `[analysis]` config: skip, error_on_warn, max_affected_rows, pg_version
- [ ] Tests: all rules trigger/no-trigger, version-aware behavior, inline suppression, PL/pgSQL exclusion
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
- [ ] PgBouncer detection and warning (DD13)
- [ ] Man page generation
- [ ] Homebrew formula

### Phase 5 — expand/contract (Sprint 9–10, ~2 weeks)

- [ ] `src/expand-contract/generator.ts`: generate expand + contract migration pair from column rename/type change
- [ ] `src/expand-contract/tracker.ts`: track phase state in Postgres (new table in `stitch.*` schema)
- [ ] `stitch add --expand`: generate linked pair
- [ ] `stitch deploy --phase expand|contract`
- [ ] Trigger generation for old↔new column sync (with `pg_trigger_depth()` + `TG_NAME LIKE 'stitch_sync_%'` recursion guard)
- [ ] Partitioned table detection: install sync triggers on parent table (PG 14+ always supports trigger inheritance)
- [ ] View shim for backward compat during transition
- [ ] Backfill verification before contract phase
- [ ] Advisory lock-based concurrency control for phase transitions
- [ ] Tests: full expand/contract cycle, rollback, trigger correctness, partitioned tables

### Phase 6 — batched background DML (Sprint 11–13, ~3 weeks)

- [ ] `src/batch/queue.ts`: 3-partition rotating queue schema (PGQ-inspired), DDL migration, partition rotation logic
- [ ] `src/batch/worker.ts`: batch execution loop, lock_timeout + statement_timeout per batch, sleep, retry
- [ ] `src/batch/progress.ts`: row counting (last processed PK tracking), ETA, dead tuple monitoring
- [ ] Replication lag monitoring: query `pg_stat_replication`, pause when lag exceeds threshold
- [ ] `stitch batch add`: register job, create queue entry
- [ ] `stitch batch list`: show all jobs and state
- [ ] `stitch batch status <job>`: progress, ETA, recent errors
- [ ] `stitch batch pause|resume|cancel <job>`
- [ ] `stitch batch retry <job>`: manual retry of dead jobs, resume from last processed PK
- [ ] Connection management: direct connection required, SET re-issued per batch
- [ ] Tests: full job lifecycle, pause/resume, retry on failure, max retries → dead → manual retry, replication lag pause

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
| GitLab migration helpers | Batched DML design, throttling, retry, replication lag monitoring | Rails dependency |
| SkyTools PGQ | 3-partition queue architecture | External daemon |
| pg_index_pilot | Write-ahead tracking, advisory lock patterns, invalid index cleanup | PL/pgSQL-only, dblink architecture |
| Flyway / Liquibase | Nothing | Wrong philosophy |

---

*This spec is a living document. Update it before writing code.*
