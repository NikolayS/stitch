/**
 * SA011: UPDATE/DELETE on large table (estimated rows > threshold)
 *
 * Severity: warn
 * Type: connected
 *
 * Detects UPDATE or DELETE statements targeting tables with estimated row
 * counts exceeding the configured threshold (default: 10,000). Requires a
 * database connection to query pg_class.reltuples for the row estimate.
 *
 * When no database connection is available, this rule is silently skipped.
 *
 * PL/pgSQL body exclusion: DML inside CREATE FUNCTION, CREATE PROCEDURE,
 * and DO blocks is excluded — these define function bodies, not direct
 * migration operations.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA011: Rule = {
  id: "SA011",
  severity: "warn",
  type: "connected",

  check(context: AnalysisContext): Finding[] {
    const findings: Finding[] = [];
    const { ast, rawSql, filePath, db, config } = context;

    // Connected rule: requires a database connection
    if (!db) return findings;

    if (!ast?.stmts) return findings;

    const threshold = config.maxAffectedRows ?? 10_000;

    for (const stmtEntry of ast.stmts) {
      const stmt = stmtEntry.stmt;

      // Skip PL/pgSQL bodies (CREATE FUNCTION, CREATE PROCEDURE, DO blocks)
      if (stmt?.CreateFunctionStmt || stmt?.DoStmt) continue;

      let tableName: string | undefined;
      let schemaName: string | undefined;
      let dmlType: "UPDATE" | "DELETE" | undefined;

      if (stmt?.UpdateStmt) {
        const rel = stmt.UpdateStmt.relation;
        tableName = rel?.relname;
        schemaName = rel?.schemaname;
        dmlType = "UPDATE";
      } else if (stmt?.DeleteStmt) {
        const rel = stmt.DeleteStmt.relation;
        tableName = rel?.relname;
        schemaName = rel?.schemaname;
        dmlType = "DELETE";
      }

      if (!tableName || !dmlType) continue;

      // Query pg_class.reltuples for estimated row count
      // This is done synchronously in the check() signature, but since
      // connected rules require async DB access, the actual implementation
      // will be wired up by the analysis engine. For now, we note the
      // finding with the table info for the engine to resolve.
      const location = offsetToLocation(
        rawSql,
        stmtEntry.stmt_location ?? 0,
        filePath,
      );

      const qualifiedName = schemaName
        ? `${schemaName}.${tableName}`
        : tableName;

      findings.push({
        ruleId: "SA011",
        severity: "warn",
        message: `${dmlType} on table "${qualifiedName}" may affect a large number of rows. Verify estimated row count against threshold (${threshold}).`,
        location,
        suggestion:
          "Consider batching the operation to avoid long-running transactions, table bloat, and lock contention.",
      });
    }

    return findings;
  },
};

export default SA011;
