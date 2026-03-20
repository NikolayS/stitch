/**
 * SA008: TRUNCATE
 *
 * Severity: warn
 * Type: static
 *
 * Detects TRUNCATE statements. TRUNCATE removes all rows from a table and
 * is effectively irreversible without a backup. It also takes an
 * AccessExclusiveLock.
 *
 * PL/pgSQL body exclusion: TRUNCATE inside CREATE FUNCTION, CREATE PROCEDURE,
 * and DO blocks is excluded — these define function bodies, not direct
 * migration operations.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA008: Rule = {
  id: "SA008",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Skip PL/pgSQL bodies (CREATE FUNCTION, CREATE PROCEDURE, DO blocks)
      if (stmt?.CreateFunctionStmt || stmt?.DoStmt) continue;

      if (!stmt?.TruncateStmt) continue;

      const truncateStmt = stmt.TruncateStmt;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // Extract table names
      const tableNames: string[] = [];
      for (const rel of truncateStmt.relations ?? []) {
        const rv = rel.RangeVar;
        if (rv?.relname) {
          const schema = rv.schemaname ? `${rv.schemaname}.` : "";
          tableNames.push(`${schema}${rv.relname}`);
        }
      }

      const nameStr =
        tableNames.length > 0 ? tableNames.join(", ") : "unknown";

      findings.push({
        ruleId: "SA008",
        severity: "warn",
        message: `TRUNCATE on ${nameStr} removes all data and takes an AccessExclusiveLock.`,
        location,
        suggestion:
          "Ensure this is intentional. Use DELETE with a WHERE clause for partial removal, or ensure a backup exists.",
      });
    }

    return findings;
  },
};

export default SA008;
