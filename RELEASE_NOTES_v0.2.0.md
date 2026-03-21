# sqlever v0.2.0 Release Notes

The first production-quality release of sqlever. This version has been validated
against a real Sqitch oracle (running identical plans through both sqlever and
Sqitch and comparing tracking-schema state), and includes all planned v0.2
features.

## What's new

### Bug fixes from Sqitch oracle testing

Five bugs were discovered and fixed by running sqlever side-by-side with Sqitch
on identical migration plans and comparing results:

1. **Change/tag ID content trailing newline** (`6a00eaa`) -- Sqitch does not
   include a trailing newline in the content that gets hashed into change and
   tag IDs. sqlever was adding one, producing different IDs. Fixed to match
   Sqitch exactly.

2. **`name@tag` dependency resolution** (`89f0ab3`) -- Dependencies written as
   `change_name@tag_name` were not resolved correctly during topological sort
   and validation. Fixed to parse the `@tag` suffix and resolve to the tagged
   version of the change.

3. **Reworked change deploy paths** (`01a4f0f`) -- When deploying a reworked
   change, sqlever was looking for the script at the base name instead of the
   versioned path (e.g., `deploy/foo@bar.sql`). Fixed to use correct versioned
   script paths.

4. **Revert ignoring `no-transaction` directive** (`35ffc53`) -- The
   `-- sqlever:no-transaction` pragma was honored during deploy but silently
   ignored during revert. Fixed to respect the directive in both directions.

5. **Advisory lock leak on revert --to** (`6c861de`) -- When `revert --to` hit
   a validation error, the advisory lock was not released. Fixed to always
   release the lock in a `finally` block.

### Features (18 commands, 22 analysis rules)

**Core Sqitch-compatible commands:**
- `init`, `add`, `deploy`, `revert`, `verify`, `status`, `log`, `tag`,
  `rework`, `show`, `plan`

**sqlever extensions:**
- `analyze` -- 22 static analysis rules (SA001-SA021) for dangerous migration
  patterns: lock-heavy DDL, data loss, table rewrites, missing `CONCURRENTLY`,
  unsafe casts, and more. Works on any `.sql` file -- no `sqitch.plan` required.
- `doctor` -- project health check: plan parsing, change ID chain, script
  presence, psql metacommand detection, syntax version.
- `diff` -- pending changes or differences between two tags.
- `batch` -- batched DML execution with PGQ 3-partition queue, `SKIP LOCKED`
  workers, replication lag monitoring, and VACUUM pressure backpressure.
- `explain` -- AI-powered plain-English migration summary (OpenAI, Anthropic).
- `review` -- structured PR review comment generation from analysis findings.
- `deploy --dblab` -- deploy against a DBLab thin clone for safe testing.

**Expand/contract migrations:**
- `add --expand` / `add --contract` generate paired migration changes.
- Bidirectional sync trigger generation keeps old and new schemas in sync.
- Phase tracker manages expand/contract state in PostgreSQL.
- `deploy --phase expand|contract` deploys only the specified phase.

**Other highlights:**
- Snapshot includes: `\i`/`\ir` resolved to the git version from when the
  migration was written, not current HEAD. Pass `--no-snapshot` to disable.
- TUI deploy dashboard with live progress, per-change timing, and analysis
  warnings (auto-disables when stdout is not a TTY).
- `--format json`, `--format github-annotations`, `--format gitlab-codequality`
  on all commands.
- Lock timeout guard auto-prepended before risky DDL.
- Non-transactional DDL with write-ahead tracking and crash recovery.
- Advisory lock support to prevent concurrent deploys.
- PgBouncer transaction-mode detection.

## Breaking changes

None. This is the first real release -- there is no prior version to break
compatibility with.

## Installation

```bash
# Run without installing
npx sqlever --help
bunx sqlever --help

# Install globally
npm install -g sqlever

# Or download a binary from GitHub Releases
# https://github.com/NikolayS/sqlever/releases
```

## Links

- [README](README.md)
- [Full specification](spec/SPEC.md)
- [Spec changelog](spec/SPEC-CHANGELOG.md)
- [License](LICENSE) (Apache 2.0)
