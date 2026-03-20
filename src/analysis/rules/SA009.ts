/**
 * SA009: ADD FOREIGN KEY without NOT VALID
 *
 * Severity: warn
 * Type: hybrid (static check for NOT VALID, connected check for index)
 *
 * Static: Detects ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY without
 * NOT VALID. Without NOT VALID, the constraint takes a ShareRowExclusiveLock
 * on both referencing and referenced tables and validates all existing rows
 * before completing.
 *
 * Connected: Also checks for a missing index on the referencing column(s)
 * (ongoing performance concern). This portion only runs when a DB connection
 * is available.
 *
 * Recommended pattern:
 *   ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES t2(id) NOT VALID;
 *   ALTER TABLE t VALIDATE CONSTRAINT fk;  -- takes ShareUpdateExclusiveLock only
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA009: Rule = {
  id: "SA009",
  severity: "warn",
  type: "hybrid",

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
        if (!cmd || cmd.subtype !== "AT_AddConstraint") continue;

        const constraint = cmd.def?.Constraint;
        if (!constraint || constraint.contype !== "CONSTR_FOREIGN") continue;

        // Check for NOT VALID: in the AST, skip_validation = true means NOT VALID was used
        const hasNotValid = constraint.skip_validation === true;

        if (!hasNotValid) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const tableName = alterStmt.relation?.relname ?? "unknown";
          const constraintName = constraint.conname ?? "unnamed";
          const refTable = constraint.pktable?.relname ?? "unknown";

          // Extract FK column names
          const fkCols = (constraint.fk_attrs ?? [])
            .map((attr: any) => attr?.String?.sval)
            .filter(Boolean)
            .join(", ");

          findings.push({
            ruleId: "SA009",
            severity: "warn",
            message: `Adding foreign key "${constraintName}" on ${tableName}(${fkCols}) referencing ${refTable} without NOT VALID takes a ShareRowExclusiveLock and validates all existing rows.`,
            location,
            suggestion:
              "Use ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT in a separate statement (takes only ShareUpdateExclusiveLock, does not block writes).",
          });
        }

        // Connected check: index on referencing columns
        // This would run when context.db is available
        // Left as a stub for the analysis engine integration
      }
    }

    return findings;
  },
};

export default SA009;
