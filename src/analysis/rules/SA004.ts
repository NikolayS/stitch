/**
 * SA004: CREATE INDEX without CONCURRENTLY
 *
 * Severity: warn
 * Type: static
 *
 * Detects CREATE INDEX statements that do not use the CONCURRENTLY option.
 * Without CONCURRENTLY, CREATE INDEX takes a ShareLock on the table, blocking
 * INSERT/UPDATE/DELETE for the duration of the index build.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA004: Rule = {
  id: "SA004",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.IndexStmt) continue;

      const indexStmt = stmt.IndexStmt;

      // Skip if CONCURRENTLY is already used
      if (indexStmt.concurrent) continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const idxName = indexStmt.idxname ?? "unnamed";
      const tableName = indexStmt.relation?.relname ?? "unknown";

      findings.push({
        ruleId: "SA004",
        severity: "warn",
        message: `CREATE INDEX "${idxName}" on table "${tableName}" without CONCURRENTLY takes a ShareLock, blocking writes for the duration.`,
        location,
        suggestion:
          "Use CREATE INDEX CONCURRENTLY to avoid blocking writes. Note: CONCURRENTLY cannot run inside a transaction block.",
      });
    }

    return findings;
  },
};

export default SA004;
