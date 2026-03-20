/**
 * SA007: DROP TABLE in non-revert context
 *
 * Severity: error
 * Type: static
 *
 * Detects DROP TABLE statements outside of revert scripts. Dropping a table
 * is irreversible data loss. In sqitch project context, files under revert/
 * are exempt since DROP TABLE is expected in revert scripts. In standalone
 * mode, always fires.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA007: Rule = {
  id: "SA007",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, isRevertContext } = context;

    // In revert context, DROP TABLE is expected and exempt
    if (isRevertContext) return findings;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.DropStmt) continue;

      const dropStmt = stmt.DropStmt;

      // Only care about DROP TABLE
      if (dropStmt.removeType !== "OBJECT_TABLE") continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // Extract table name(s) from the objects list
      const tableNames: string[] = [];
      for (const obj of dropStmt.objects ?? []) {
        if (obj?.List?.items) {
          const names = obj.List.items
            .map((item: any) => item?.String?.sval)
            .filter(Boolean);
          tableNames.push(names.join("."));
        }
      }

      const nameStr =
        tableNames.length > 0 ? tableNames.join(", ") : "unknown";

      findings.push({
        ruleId: "SA007",
        severity: "error",
        message: `DROP TABLE ${nameStr} causes irreversible data loss.`,
        location,
        suggestion:
          "Ensure a backup exists. In a sqitch project, DROP TABLE is expected only in revert/ scripts.",
      });
    }

    return findings;
  },
};

export default SA007;
