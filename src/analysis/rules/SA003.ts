/**
 * SA003: ALTER COLUMN TYPE non-trivial cast
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TABLE ... ALTER COLUMN ... TYPE that would cause a full table
 * rewrite and AccessExclusiveLock. Uses a safe cast allowlist for type changes
 * known to be binary-compatible (no rewrite needed).
 *
 * When a USING clause is present, SA003 ALWAYS fires regardless of the safe
 * cast allowlist — PostgreSQL rewrites the table to evaluate the expression.
 *
 * Safe cast allowlist:
 * - varchar(N) to varchar(M) where M > N (widening)
 * - varchar(N) to varchar (removing limit)
 * - varchar to text
 * - char(N) to varchar or text
 * - numeric(P,S) to numeric(P2,S) where P2 > P (widening precision)
 * - numeric(P,S) to unconstrained numeric (removing precision/scale)
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation, displayTypeName } from "../types.js";

/**
 * Check if a type change is in the safe cast allowlist.
 * Returns true if the cast is safe (no table rewrite).
 *
 * Note: This function does NOT know the source type from the AST alone —
 * ALTER COLUMN TYPE only specifies the target type. We check if the target
 * type is in a family known to have safe widening casts. Without a DB
 * connection, we can only check the target type; the actual safety depends
 * on the source type. For full accuracy, connected mode would consult pg_cast.
 *
 * For static analysis, we flag all type changes as potentially unsafe
 * UNLESS the target is clearly a widening/removal of constraint within the
 * same type family. Since we don't have the source type from the AST, we
 * must be conservative and flag all changes.
 */
function isSafeCast(_targetTypeName: any): boolean {
  // Without knowing the source type, we cannot determine if the cast is safe.
  // The analysis engine with DB connection can refine this.
  // For static analysis, we flag all type changes (conservative approach).
  return false;
}

export const SA003: Rule = {
  id: "SA003",
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
        if (!cmd || cmd.subtype !== "AT_AlterColumnType") continue;

        const colDef = cmd.def?.ColumnDef;
        if (!colDef) continue;

        const hasUsing = !!colDef.raw_default;
        const targetType = colDef.typeName;
        const targetTypeDisplay = displayTypeName(targetType);
        const colName = cmd.name ?? "unknown";
        const tableName = alterStmt.relation?.relname ?? "unknown";

        // USING clause always triggers — PG rewrites to evaluate the expression
        if (hasUsing) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );
          findings.push({
            ruleId: "SA003",
            severity: "error",
            message: `Changing type of column "${colName}" on table "${tableName}" to ${targetTypeDisplay} with USING clause causes a full table rewrite with AccessExclusiveLock.`,
            location,
            suggestion:
              "Consider the expand/contract pattern: add a new column, backfill in batches, then swap.",
          });
          continue;
        }

        // Check safe cast allowlist (static mode: conservative)
        if (!isSafeCast(targetType)) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );
          findings.push({
            ruleId: "SA003",
            severity: "error",
            message: `Changing type of column "${colName}" on table "${tableName}" to ${targetTypeDisplay} may cause a full table rewrite with AccessExclusiveLock.`,
            location,
            suggestion:
              "Consider the expand/contract pattern: add a new column with the new type, backfill in batches, then swap. Safe casts (varchar widening, numeric precision widening) can be suppressed with -- sqlever:disable SA003.",
          });
        }
      }
    }

    return findings;
  },
};

export default SA003;
