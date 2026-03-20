/**
 * SA012: ALTER SEQUENCE RESTART
 *
 * Severity: info
 * Type: static
 *
 * Detects ALTER SEQUENCE ... RESTART statements. Restarting a sequence can
 * break application assumptions about ID uniqueness or ordering, and may
 * cause primary key conflicts if the sequence is used for auto-incrementing
 * columns.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA012: Rule = {
  id: "SA012",
  severity: "info",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.AlterSeqStmt) continue;

      const alterSeq = stmt.AlterSeqStmt;
      const options = (alterSeq.options ?? []) as any[];

      // Check if any option is "restart"
      const hasRestart = options.some(
        (opt: any) => opt?.DefElem?.defname === "restart",
      );

      if (!hasRestart) continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const seqName = alterSeq.sequence?.relname ?? "unknown";

      findings.push({
        ruleId: "SA012",
        severity: "info",
        message: `ALTER SEQUENCE RESTART on "${seqName}" may break application assumptions about ID uniqueness or ordering.`,
        location,
        suggestion:
          "Ensure no existing rows conflict with the restarted sequence values, and that all application code handles potential ID collisions.",
      });
    }

    return findings;
  },
};

export default SA012;
