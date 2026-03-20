/**
 * SA013: SET lock_timeout missing before risky DDL
 *
 * Severity: warn
 * Type: static
 *
 * Detects risky DDL statements that are not preceded by a SET lock_timeout
 * statement. "Risky DDL" means any DDL that takes AccessExclusiveLock or
 * ShareLock, which can cause runaway lock waits on busy tables.
 *
 * Risky DDL includes:
 * - ALTER TABLE (AccessExclusiveLock for most operations)
 * - DROP TABLE / DROP INDEX (AccessExclusiveLock)
 * - CREATE INDEX without CONCURRENTLY (ShareLock)
 * - TRUNCATE (AccessExclusiveLock)
 * - CLUSTER (AccessExclusiveLock)
 * - VACUUM FULL (AccessExclusiveLock)
 * - REINDEX without CONCURRENTLY (AccessExclusiveLock)
 *
 * If a SET lock_timeout (or SET LOCAL lock_timeout) appears before the
 * risky DDL in the same file, this rule does not fire.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

/**
 * Check if a statement is a SET lock_timeout.
 */
function isLockTimeoutSet(stmt: any): boolean {
  if (!stmt?.VariableSetStmt) return false;
  const setStmt = stmt.VariableSetStmt;
  return (
    setStmt.kind === "VAR_SET_VALUE" && setStmt.name === "lock_timeout"
  );
}

/**
 * Check if a statement is risky DDL that takes a heavy lock.
 */
function isRiskyDDL(stmt: any): { risky: boolean; description: string } {
  // ALTER TABLE (most operations take AccessExclusiveLock)
  if (stmt?.AlterTableStmt) {
    const tableName = stmt.AlterTableStmt.relation?.relname ?? "unknown";
    return { risky: true, description: `ALTER TABLE on "${tableName}"` };
  }

  // DROP TABLE
  if (stmt?.DropStmt) {
    const dropStmt = stmt.DropStmt;
    if (dropStmt.removeType === "OBJECT_TABLE") {
      return { risky: true, description: "DROP TABLE" };
    }
    // DROP INDEX (without CONCURRENTLY)
    if (dropStmt.removeType === "OBJECT_INDEX" && !dropStmt.concurrent) {
      return { risky: true, description: "DROP INDEX" };
    }
  }

  // CREATE INDEX without CONCURRENTLY (ShareLock)
  if (stmt?.IndexStmt && !stmt.IndexStmt.concurrent) {
    return { risky: true, description: "CREATE INDEX" };
  }

  // TRUNCATE (AccessExclusiveLock)
  if (stmt?.TruncateStmt) {
    return { risky: true, description: "TRUNCATE" };
  }

  // CLUSTER (AccessExclusiveLock)
  if (stmt?.ClusterStmt) {
    return { risky: true, description: "CLUSTER" };
  }

  // VACUUM FULL (AccessExclusiveLock)
  if (stmt?.VacuumStmt) {
    const options = (stmt.VacuumStmt.options ?? []) as any[];
    const hasFull = options.some(
      (opt: any) => opt?.DefElem?.defname === "full",
    );
    if (hasFull) {
      return { risky: true, description: "VACUUM FULL" };
    }
  }

  // REINDEX without CONCURRENTLY (AccessExclusiveLock)
  if (stmt?.ReindexStmt) {
    const params = (stmt.ReindexStmt.params ?? []) as any[];
    const hasConcurrently = params.some(
      (p: any) => p?.DefElem?.defname === "concurrently",
    );
    if (!hasConcurrently) {
      return { risky: true, description: "REINDEX" };
    }
  }

  return { risky: false, description: "" };
}

export const SA013: Rule = {
  id: "SA013",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    // Scan all statements to find SET lock_timeout positions
    let lockTimeoutSeen = false;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Track SET lock_timeout
      if (isLockTimeoutSet(stmt)) {
        lockTimeoutSeen = true;
        continue;
      }

      // Check for risky DDL
      const { risky, description } = isRiskyDDL(stmt);
      if (risky && !lockTimeoutSeen) {
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        findings.push({
          ruleId: "SA013",
          severity: "warn",
          message: `${description} without a preceding SET lock_timeout. Without a lock timeout, this statement may wait indefinitely for locks.`,
          location,
          suggestion:
            "Add SET lock_timeout = '5s'; (or appropriate duration) before risky DDL to prevent runaway lock waits.",
        });
      }
    }

    return findings;
  },
};

export default SA013;
