/**
 * SA002b: ADD COLUMN DEFAULT non-volatile on PG < 11
 *
 * Severity: warn
 * Type: static
 *
 * Detects ALTER TABLE ... ADD COLUMN with a non-volatile default expression
 * when targeting PostgreSQL < 11. On PG < 11, ANY default on ADD COLUMN
 * causes a full table rewrite. On PG 11+, non-volatile (immutable/stable)
 * defaults are metadata-only operations.
 *
 * This rule only fires when pgVersion < 11.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

// Import volatile function set from SA002 to reuse the detection logic
const VOLATILE_FUNCTIONS: ReadonlySet<string> = new Set([
  "random",
  "gen_random_uuid",
  "clock_timestamp",
  "txid_current",
  "timeofday",
  "uuid_generate_v1",
  "uuid_generate_v1mc",
  "uuid_generate_v4",
  "statement_timestamp",
  "setseed",
  "nextval",
  "currval",
  "lastval",
]);

/**
 * Check if an expression contains a volatile function call.
 * Returns true if ANY function in the expression is volatile.
 */
function containsVolatileFunction(node: any): boolean {
  if (!node || typeof node !== "object") return false;

  if (node.FuncCall) {
    const funcNames = node.FuncCall.funcname ?? [];
    for (const fn of funcNames) {
      const name = fn?.String?.sval?.toLowerCase();
      if (name && VOLATILE_FUNCTIONS.has(name)) {
        return true;
      }
    }
    // Check function arguments
    for (const arg of node.FuncCall.args ?? []) {
      if (containsVolatileFunction(arg)) return true;
    }
    return false;
  }

  if (node.TypeCast) {
    return containsVolatileFunction(node.TypeCast.arg);
  }

  if (node.A_Expr) {
    return (
      containsVolatileFunction(node.A_Expr.lexpr) ||
      containsVolatileFunction(node.A_Expr.rexpr)
    );
  }

  if (node.CoalesceExpr) {
    for (const arg of node.CoalesceExpr.args ?? []) {
      if (containsVolatileFunction(arg)) return true;
    }
  }

  return false;
}

export const SA002b: Rule = {
  id: "SA002b",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, pgVersion } = context;

    // Only fires when targeting PG < 11
    if (pgVersion >= 11) return findings;

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
        for (const c of constraints) {
          const constraint = c.Constraint;
          if (!constraint || constraint.contype !== "CONSTR_DEFAULT") continue;

          const rawExpr = constraint.raw_expr;

          // Skip volatile defaults — those are handled by SA002
          if (rawExpr && containsVolatileFunction(rawExpr)) continue;

          // This is a non-volatile default on PG < 11
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );
          const tableName = alterStmt.relation?.relname ?? "unknown";
          const colName = colDef.colname ?? "unknown";

          findings.push({
            ruleId: "SA002b",
            severity: "warn",
            message: `Adding column "${colName}" to table "${tableName}" with a default causes a full table rewrite on PostgreSQL < 11 (target version: ${pgVersion}).`,
            location,
            suggestion:
              "Upgrade to PostgreSQL 11+ where non-volatile defaults are metadata-only, or add the column without a default and backfill.",
          });
        }
      }
    }

    return findings;
  },
};

export default SA002b;
