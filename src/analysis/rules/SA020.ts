/**
 * SA020: CONCURRENTLY operations in transactional context
 *
 * Severity: error
 * Type: static
 *
 * Detects CREATE INDEX CONCURRENTLY, DROP INDEX CONCURRENTLY, or
 * REINDEX CONCURRENTLY usage. These operations cannot run inside a
 * transaction block and will fail at runtime if attempted.
 *
 * In project mode, the analyzer would check the plan file for a
 * non-transactional marker. In standalone mode, this rule warns on
 * any CONCURRENTLY usage with guidance to ensure it runs outside a
 * transaction block.
 *
 * Also recognizes the -- sqlever:no-transaction script comment
 * (sqlever-only convention) to suppress the warning.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

/**
 * Check if the SQL contains a -- sqlever:no-transaction comment.
 */
function hasNoTransactionComment(rawSql: string): boolean {
  return /--\s*sqlever:no-transaction/i.test(rawSql);
}

export const SA020: Rule = {
  id: "SA020",
  severity: "error",
  type: "static",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath } = context;

    if (!ast?.stmts) return findings;

    // If the file has a -- sqlever:no-transaction comment, skip
    if (hasNoTransactionComment(rawSql)) return findings;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // CREATE INDEX CONCURRENTLY
      if (stmt?.IndexStmt?.concurrent) {
        const indexStmt = stmt.IndexStmt;
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );
        const idxName = indexStmt.idxname ?? "unnamed";

        findings.push({
          ruleId: "SA020",
          severity: "error",
          message: `CREATE INDEX CONCURRENTLY "${idxName}" cannot run inside a transaction block.`,
          location,
          suggestion:
            "Mark this migration as non-transactional, or add a -- sqlever:no-transaction comment. In sqitch, use a non-transactional change.",
        });
      }

      // DROP INDEX CONCURRENTLY
      if (
        stmt?.DropStmt?.removeType === "OBJECT_INDEX" &&
        stmt.DropStmt.concurrent
      ) {
        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        // Extract index name(s)
        const indexNames: string[] = [];
        for (const obj of stmt.DropStmt.objects ?? []) {
          if (obj?.List?.items) {
            const names = obj.List.items
              .map((item: any) => item?.String?.sval)
              .filter(Boolean);
            indexNames.push(names.join("."));
          }
        }
        const nameStr =
          indexNames.length > 0 ? indexNames.join(", ") : "unnamed";

        findings.push({
          ruleId: "SA020",
          severity: "error",
          message: `DROP INDEX CONCURRENTLY ${nameStr} cannot run inside a transaction block.`,
          location,
          suggestion:
            "Mark this migration as non-transactional, or add a -- sqlever:no-transaction comment. In sqitch, use a non-transactional change.",
        });
      }

      // REINDEX CONCURRENTLY
      if (stmt?.ReindexStmt) {
        const params = (stmt.ReindexStmt.params ?? []) as any[];
        const hasConcurrently = params.some(
          (p: any) => p?.DefElem?.defname === "concurrently",
        );

        if (hasConcurrently) {
          const location = offsetToLocation(
            rawSql,
            stmtEntry.stmt_location ?? 0,
            filePath,
          );

          const reindexStmt = stmt.ReindexStmt;
          const target =
            reindexStmt.relation?.relname ?? reindexStmt.name ?? "unknown";

          findings.push({
            ruleId: "SA020",
            severity: "error",
            message: `REINDEX CONCURRENTLY on "${target}" cannot run inside a transaction block.`,
            location,
            suggestion:
              "Mark this migration as non-transactional, or add a -- sqlever:no-transaction comment. In sqitch, use a non-transactional change.",
          });
        }
      }
    }

    return findings;
  },
};

export default SA020;
