/**
 * SA018: ADD PRIMARY KEY without pre-existing index
 *
 * Severity: warn
 * Type: hybrid
 *
 * Static: Fires on ALTER TABLE ... ADD PRIMARY KEY that does not use the
 * USING INDEX clause. Without USING INDEX, ALTER TABLE takes
 * AccessExclusiveLock and the implicit index creation extends the lock
 * duration.
 *
 * Connected: When a database connection is available, checks the catalog
 * for a pre-existing unique index on the PK columns and suppresses if found.
 *
 * Safe pattern: Create the index concurrently first, then use
 * ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY USING INDEX.
 */

import type { Rule, Finding, AnalysisContext } from "../types.js";
import { offsetToLocation } from "../types.js";

export const SA018: Rule = {
  id: "SA018",
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
        if (!constraint || constraint.contype !== "CONSTR_PRIMARY") continue;

        // If USING INDEX is specified, indexname will be set
        if (constraint.indexname) continue;

        const location = offsetToLocation(
          rawSql,
          stmtEntry.stmt_location ?? 0,
          filePath,
        );

        const tableName = alterStmt.relation?.relname ?? "unknown";

        // Extract PK column names
        const pkCols = (constraint.keys ?? [])
          .map((key: any) => key?.String?.sval)
          .filter(Boolean)
          .join(", ");

        // Connected check: if DB is available, check for pre-existing
        // unique index on PK columns and suppress if found
        // This is left as a stub for the analysis engine integration

        findings.push({
          ruleId: "SA018",
          severity: "warn",
          message: `ADD PRIMARY KEY on "${tableName}" (${pkCols}) without USING INDEX holds AccessExclusiveLock while creating the index.`,
          location,
          suggestion:
            "Create the unique index concurrently first, then use ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY USING INDEX.",
        });
      }
    }

    return findings;
  },
};

export default SA018;
