/**
 * SA001: ADD COLUMN NOT NULL without DEFAULT
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TABLE ... ADD COLUMN with a NOT NULL constraint but no DEFAULT.
 * This fails outright on populated tables because existing rows would have NULL
 * for the new column, violating the NOT NULL constraint.
 *
 * Does NOT fire when a DEFAULT is present — that case is covered by SA002/SA002b.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA001: Rule = {
  id: "SA001",
  severity: "error",
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
        if (!cmd || cmd.subtype !== "AT_AddColumn") continue;

        const colDef = cmd.def?.ColumnDef;
        if (!colDef) continue;

        const constraints = colDef.constraints ?? [];
        let hasNotNull = false;
        let hasDefault = false;

        for (const c of constraints) {
          const constraint = c.Constraint;
          if (!constraint) continue;
          if (constraint.contype === "CONSTR_NOTNULL") hasNotNull = true;
          if (constraint.contype === "CONSTR_DEFAULT") hasDefault = true;
        }

        if (hasNotNull && !hasDefault) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );
          const tableName = alterStmt.relation?.relname ?? "unknown";
          const colName = colDef.colname ?? "unknown";

          findings.push({
            ruleId: "SA001",
            severity: "error",
            message: `Adding NOT NULL column "${colName}" to table "${tableName}" without a DEFAULT will fail on populated tables.`,
            location,
            suggestion:
              "Add a DEFAULT value, or add the column as nullable first, backfill, then set NOT NULL.",
          });
        }
      }
    }

    return findings;
  },
};

export default SA001;
