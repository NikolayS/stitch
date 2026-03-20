/**
 * SA002: ADD COLUMN DEFAULT volatile on any PG version
 *
 * Severity: error
 * Type: static
 *
 * Detects ALTER TABLE ... ADD COLUMN with a volatile default expression.
 * Volatile defaults (e.g. random(), gen_random_uuid(), clock_timestamp(),
 * txid_current()) cause a full table rewrite on ALL PostgreSQL versions,
 * including PG 11+. The PG 11 optimization only applies to immutable/stable
 * defaults.
 *
 * Note: now() is STABLE (returns transaction start time), not volatile —
 * DEFAULT now() does NOT cause a rewrite on PG 11+.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

/**
 * Known volatile functions that cause table rewrites on all PG versions.
 * These are checked case-insensitively.
 */
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
 * Recursively check if an AST expression node contains a volatile function call.
 */
function containsVolatileFunction(node: any): string | null {
  if (!node || typeof node !== "object") return null;

  // Direct function call
  if (node.FuncCall) {
    const funcNames = node.FuncCall.funcname ?? [];
    for (const fn of funcNames) {
      const name = fn?.String?.sval?.toLowerCase();
      if (name && VOLATILE_FUNCTIONS.has(name)) {
        return name;
      }
    }
    // Check function arguments recursively
    const args = node.FuncCall.args ?? [];
    for (const arg of args) {
      const result = containsVolatileFunction(arg);
      if (result) return result;
    }
    return null;
  }

  // TypeCast wrapping a volatile function (e.g. random()::int)
  if (node.TypeCast) {
    return containsVolatileFunction(node.TypeCast.arg);
  }

  // Check nested expressions
  if (node.A_Expr) {
    const left = containsVolatileFunction(node.A_Expr.lexpr);
    if (left) return left;
    return containsVolatileFunction(node.A_Expr.rexpr);
  }

  // CoalesceExpr
  if (node.CoalesceExpr) {
    for (const arg of node.CoalesceExpr.args ?? []) {
      const result = containsVolatileFunction(arg);
      if (result) return result;
    }
  }

  return null;
}

export const SA002: Rule = {
  id: "SA002",
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
        for (const c of constraints) {
          const constraint = c.Constraint;
          if (!constraint || constraint.contype !== "CONSTR_DEFAULT") continue;

          const rawExpr = constraint.raw_expr;
          if (!rawExpr) continue;

          const volatileFunc = containsVolatileFunction(rawExpr);
          if (volatileFunc) {
            const location = offsetToLocation(
              rawSql,
              stmtEntry.stmt_location ?? 0,
              filePath,
            );
            const tableName = alterStmt.relation?.relname ?? "unknown";
            const colName = colDef.colname ?? "unknown";

            findings.push({
              ruleId: "SA002",
              severity: "error",
              message: `Adding column "${colName}" to table "${tableName}" with volatile default ${volatileFunc}() causes a full table rewrite on all PostgreSQL versions.`,
              location,
              suggestion:
                "Add the column without a default, then backfill in batches using UPDATE.",
            });
          }
        }
      }
    }

    return findings;
  },
};

export default SA002;
