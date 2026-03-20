/**
 * SA006: DROP COLUMN
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... DROP COLUMN statements. Dropping a column is
 * irreversible data loss. While PostgreSQL marks the column as dropped
 * (metadata-only, no rewrite), the data is gone and cannot be recovered
 * without a backup.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA006: Rule = {
  id: "SA006",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.AlterTableStmt) continue;

      const alterStmt = stmt.AlterTableStmt;
      if (alterStmt.objtype !== "OBJECT_TABLE") continue;

      const cmds = alterStmt.cmds ?? [];
      for (const cmdEntry of cmds) {
        const cmd = cmdEntry.AlterTableCmd;
        if (!cmd || cmd.subtype !== "AT_DropColumn") continue;

        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        const tableName = alterStmt.relation?.relname ?? "unknown";
        const colName = cmd.name ?? "unknown";

        findings.push({
          ruleId: "SA006",
          severity: "warn",
          message: `Dropping column "${colName}" from table "${tableName}" causes irreversible data loss.`,
          location,
          suggestion:
            "Ensure a backup exists and that no application code depends on this column. Consider a deprecation period before dropping.",
        });
      }
    }

    return findings;
  },
};

export default SA006;
