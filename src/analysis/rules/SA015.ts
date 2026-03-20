/**
 * SA015: ALTER TABLE RENAME (table or column)
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... RENAME (both table rename and column rename).
 * Renames break running application code that references the old name.
 * Severity is warn (not error) until expand/contract (v2.0) exists,
 * since there is no way to satisfy the rule before then.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA015: Rule = {
  id: "SA015",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.RenameStmt) continue;

      const renameStmt = stmt.RenameStmt;
      const renameType = renameStmt.renameType;

      // Table rename: renameType === "OBJECT_TABLE"
      if (renameType === "OBJECT_TABLE") {
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        const oldName = renameStmt.relation?.relname ?? "unknown";
        const newName = renameStmt.newname ?? "unknown";

        findings.push({
          ruleId: "SA015",
          severity: "warn",
          message: `Renaming table "${oldName}" to "${newName}" will break running application code that references the old name.`,
          location,
          suggestion:
            "Use the expand/contract pattern: create a view with the old name pointing to the new table, then remove the view after all application code is updated.",
        });
        continue;
      }

      // Column rename: renameType === "OBJECT_COLUMN" with relationType === "OBJECT_TABLE"
      if (
        renameType === "OBJECT_COLUMN" &&
        renameStmt.relationType === "OBJECT_TABLE"
      ) {
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        const tableName = renameStmt.relation?.relname ?? "unknown";
        const oldCol = renameStmt.subname ?? "unknown";
        const newCol = renameStmt.newname ?? "unknown";

        findings.push({
          ruleId: "SA015",
          severity: "warn",
          message: `Renaming column "${oldCol}" to "${newCol}" on table "${tableName}" will break running application code that references the old column name.`,
          location,
          suggestion:
            "Use the expand/contract pattern: add the new column, backfill, update application code, then drop the old column.",
        });
      }
    }

    return findings;
  },
};

export default SA015;
