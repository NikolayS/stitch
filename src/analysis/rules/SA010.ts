/**
 * SA010: UPDATE/DELETE without WHERE
 *
 * Severity: warn
 * Type: static
 *
 * Detects UPDATE or DELETE statements that have no WHERE clause. Full-table
 * DML can be intentional in migrations (backfills, cleanups), so this is a
 * warning rather than an error. Use inline suppression for acknowledged cases.
 *
 * PL/pgSQL body exclusion: DML inside CREATE FUNCTION, CREATE PROCEDURE,
 * and DO blocks is excluded — these define function bodies, not direct
 * migration operations.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA010: Rule = {
  id: "SA010",
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

      // Check UPDATE without WHERE
      if (stmt?.UpdateStmt) {
        const updateStmt = stmt.UpdateStmt;
        if (!updateStmt.whereClause) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const tableName = updateStmt.relation?.relname ?? "unknown";

          findings.push({
            ruleId: "SA010",
            severity: "warn",
            message: `UPDATE on table "${tableName}" without a WHERE clause affects all rows.`,
            location,
            suggestion:
              "Add a WHERE clause to limit the scope, or suppress this warning with -- sqlever:disable SA010 if the full-table update is intentional.",
          });
        }
      }

      // Check DELETE without WHERE
      if (stmt?.DeleteStmt) {
        const deleteStmt = stmt.DeleteStmt;
        if (!deleteStmt.whereClause) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const tableName = deleteStmt.relation?.relname ?? "unknown";

          findings.push({
            ruleId: "SA010",
            severity: "warn",
            message: `DELETE on table "${tableName}" without a WHERE clause affects all rows.`,
            location,
            suggestion:
              "Add a WHERE clause to limit the scope, or suppress this warning with -- sqlever:disable SA010 if the full-table delete is intentional.",
          });
        }
      }
    }

    return findings;
  },
};

export default SA010;
