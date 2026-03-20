/**
 * SA019: REINDEX without CONCURRENTLY
 *
 * Severity: warn
 * Type: static
 *
 * Detects REINDEX statements that do not use the CONCURRENTLY option.
 * Without CONCURRENTLY, REINDEX takes an AccessExclusiveLock on the
 * table/index. PG 12+ supports REINDEX CONCURRENTLY.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA019: Rule = {
  id: "SA019",
  severity: "warn",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;
      if (!stmt?.ReindexStmt) continue;

      const reindexStmt = stmt.ReindexStmt;

      // Check if CONCURRENTLY is used
      const params = (reindexStmt.params ?? []) as any[];
      const hasConcurrently = params.some(
        (p: any) => p?.DefElem?.defname === "concurrently",
      );

      if (hasConcurrently) continue;

      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      // Determine target description
      const kind = reindexStmt.kind as string;
      let target: string;
      if (kind === "REINDEX_OBJECT_INDEX") {
        target = `index "${reindexStmt.relation?.relname ?? "unknown"}"`;
      } else if (kind === "REINDEX_OBJECT_TABLE") {
        target = `table "${reindexStmt.relation?.relname ?? "unknown"}"`;
      } else if (kind === "REINDEX_OBJECT_SCHEMA") {
        target = `schema "${reindexStmt.name ?? "unknown"}"`;
      } else if (kind === "REINDEX_OBJECT_DATABASE") {
        target = `database "${reindexStmt.name ?? "unknown"}"`;
      } else {
        target = reindexStmt.relation?.relname ?? "unknown";
      }

      findings.push({
        ruleId: "SA019",
        severity: "warn",
        message: `REINDEX on ${target} without CONCURRENTLY takes an AccessExclusiveLock.`,
        location,
        suggestion:
          "Use REINDEX CONCURRENTLY (PG 12+) to avoid blocking reads and writes. Note: CONCURRENTLY cannot run inside a transaction block.",
      });
    }

    return findings;
  },
};

export default SA019;
