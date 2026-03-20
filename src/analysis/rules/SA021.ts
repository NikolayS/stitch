/**
 * SA021: Explicit LOCK TABLE
 *
 * Severity: warn
 * Type: static
 *
 * Detects LOCK TABLE statements in any lock mode. Explicit locking in
 * migrations is a code smell and dangerous in production — it can cause
 * deadlocks, block other operations, and is usually unnecessary.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

/**
 * Map lock mode number to human-readable name.
 * These correspond to PostgreSQL's LockMode enum values.
 */
function lockModeName(mode: number): string {
  switch (mode) {
    case 1:
      return "ACCESS SHARE";
    case 2:
      return "ROW SHARE";
    case 3:
      return "ROW EXCLUSIVE";
    case 4:
      return "SHARE UPDATE EXCLUSIVE";
    case 5:
      return "SHARE";
    case 6:
      return "SHARE ROW EXCLUSIVE";
    case 7:
      return "EXCLUSIVE";
    case 8:
      return "ACCESS EXCLUSIVE";
    default:
      return `mode ${mode}`;
  }
}

export const SA021: Rule = {
  id: "SA021",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.LockStmt) continue;

      const lockStmt = stmt.LockStmt;
      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // Extract table names
      const tableNames: string[] = [];
      for (const rel of (lockStmt.relations ?? []) as any[]) {
        const rv = rel?.RangeVar;
        if (rv?.relname) {
          const schema = rv.schemaname ? `${rv.schemaname}.` : "";
          tableNames.push(`${schema}${rv.relname}`);
        }
      }

      const nameStr =
        tableNames.length > 0 ? tableNames.join(", ") : "unknown";
      const mode = lockModeName(lockStmt.mode as number);

      findings.push({
        ruleId: "SA021",
        severity: "warn",
        message: `Explicit LOCK TABLE on ${nameStr} in ${mode} mode is dangerous in production migrations.`,
        location,
        suggestion:
          "Avoid explicit LOCK TABLE in migrations. If locking is truly needed, use the most permissive lock mode possible and set a lock_timeout.",
      });
    }

    return findings;
  },
};

export default SA021;
