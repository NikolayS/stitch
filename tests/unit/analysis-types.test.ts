/**
 * Tests for src/analysis/types.ts — type contracts and interface conformance.
 *
 * These tests validate that the type definitions work correctly at runtime
 * (structural checks), ensuring the interfaces can be implemented and used
 * as expected by rule authors and the analyzer.
 */
import { describe, test, expect } from "bun:test";
import type {
  Rule,
  AnalysisContext,
  Finding,
  Severity,
  RuleType,
  AnalysisConfig,
  DatabaseClient,
  ParseResult,
} from "../../src/analysis/types";

describe("analysis types", () => {
  test("Severity type accepts error, warn, info", () => {
    const severities: Severity[] = ["error", "warn", "info"];
    expect(severities).toHaveLength(3);
  });

  test("RuleType accepts static, connected, hybrid", () => {
    const types: RuleType[] = ["static", "connected", "hybrid"];
    expect(types).toHaveLength(3);
  });

  test("Finding can be constructed with required fields", () => {
    const finding: Finding = {
      ruleId: "SA001",
      severity: "error",
      message: "Test message",
      location: { file: "test.sql", line: 1, column: 1 },
    };
    expect(finding.ruleId).toBe("SA001");
    expect(finding.suggestion).toBeUndefined();
  });

  test("Finding can include optional suggestion", () => {
    const finding: Finding = {
      ruleId: "SA004",
      severity: "warn",
      message: "Missing CONCURRENTLY",
      location: { file: "test.sql", line: 5, column: 1, endLine: 5, endColumn: 40 },
      suggestion: "Use CREATE INDEX CONCURRENTLY instead.",
    };
    expect(finding.suggestion).toBe("Use CREATE INDEX CONCURRENTLY instead.");
    expect(finding.location.endLine).toBe(5);
  });

  test("Rule interface can be implemented", () => {
    const rule: Rule = {
      id: "SA999",
      severity: "warn",
      type: "static",
      check(_context: AnalysisContext): Finding[] {
        return [];
      },
    };
    expect(rule.id).toBe("SA999");
    expect(rule.type).toBe("static");
  });

  test("AnalysisConfig supports all optional fields", () => {
    const config: AnalysisConfig = {
      skip: ["SA001"],
      errorOnWarn: true,
      maxAffectedRows: 10_000,
      pgVersion: 16,
      rules: {
        SA003: { severity: "off" },
      },
      overrides: {
        "deploy/backfill.sql": { skip: ["SA010"] },
      },
    };
    expect(config.skip).toContain("SA001");
    expect(config.rules?.SA003?.severity).toBe("off");
    expect(config.overrides?.["deploy/backfill.sql"]?.skip).toContain("SA010");
  });

  test("AnalysisContext can be built with minimal fields", () => {
    const ast: ParseResult = { stmts: [] };
    const context: AnalysisContext = {
      ast,
      rawSql: "SELECT 1;",
      filePath: "test.sql",
      pgVersion: 14,
      config: {},
    };
    expect(context.db).toBeUndefined();
    expect(context.pgVersion).toBe(14);
  });

  test("AnalysisContext can include db client", () => {
    const mockDb: DatabaseClient = {
      query: async () => ({ rows: [] }),
    };
    const context: AnalysisContext = {
      ast: { stmts: [] },
      rawSql: "",
      filePath: "test.sql",
      pgVersion: 14,
      config: {},
      db: mockDb,
    };
    expect(context.db).toBeDefined();
  });
});
