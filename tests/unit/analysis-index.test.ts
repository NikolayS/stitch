/**
 * Tests for src/analysis/index.ts — Analyzer entry point.
 *
 * Integration tests that exercise the full pipeline:
 * preprocessing -> parsing -> rule execution -> suppression filtering.
 */
import { describe, test, expect, beforeAll } from "bun:test";
import { loadModule } from "libpg-query";
import { Analyzer, RuleRegistry } from "../../src/analysis/index";
import type {
  Rule,
  Finding,
  AnalysisContext,
  AnalysisConfig,
} from "../../src/analysis/types";
import { join } from "path";
import { writeFileSync, mkdirSync, rmSync } from "fs";

// Ensure WASM module is loaded before tests
beforeAll(async () => {
  await loadModule();
});

const TMP_DIR = join(import.meta.dir, "..", ".tmp-analysis-tests");

/** Helper: create a test rule that flags SELECT statements. */
function selectRule(): Rule {
  return {
    id: "TEST001",
    severity: "warn",
    type: "static",
    check(ctx: AnalysisContext): Finding[] {
      const findings: Finding[] = [];
      for (const entry of ctx.ast.stmts) {
        if (entry.stmt.SelectStmt) {
          const offset = entry.stmt_location ?? 0;
          // Convert byte offset to line/column
          let line = 1;
          let col = 1;
          for (let i = 0; i < offset && i < ctx.rawSql.length; i++) {
            if (ctx.rawSql[i] === "\n") {
              line++;
              col = 1;
            } else {
              col++;
            }
          }
          findings.push({
            ruleId: "TEST001",
            severity: "warn",
            message: "SELECT statement detected",
            location: { file: ctx.filePath, line, column: col },
          });
        }
      }
      return findings;
    },
  };
}

/** Helper: create a test rule that flags CREATE TABLE. */
function createTableRule(): Rule {
  return {
    id: "TEST002",
    severity: "error",
    type: "static",
    check(ctx: AnalysisContext): Finding[] {
      const findings: Finding[] = [];
      for (const entry of ctx.ast.stmts) {
        if (entry.stmt.CreateStmt) {
          const offset = entry.stmt_location ?? 0;
          let line = 1;
          let col = 1;
          for (let i = 0; i < offset && i < ctx.rawSql.length; i++) {
            if (ctx.rawSql[i] === "\n") { line++; col = 1; } else { col++; }
          }
          findings.push({
            ruleId: "TEST002",
            severity: "error",
            message: "CREATE TABLE detected",
            location: { file: ctx.filePath, line, column: col },
          });
        }
      }
      return findings;
    },
  };
}

/** Helper: create a connected rule (requires db). */
function connectedRule(): Rule {
  return {
    id: "TEST003",
    severity: "warn",
    type: "connected",
    check(ctx: AnalysisContext): Finding[] {
      if (!ctx.db) return [];
      return [
        {
          ruleId: "TEST003",
          severity: "warn",
          message: "Connected rule fired",
          location: { file: ctx.filePath, line: 1, column: 1 },
        },
      ];
    },
  };
}

/** Helper: create an analyzer with test rules. */
function makeAnalyzer(rules?: Rule[]): Analyzer {
  const reg = new RuleRegistry();
  reg.registerAll(rules ?? [selectRule(), createTableRule()]);
  return new Analyzer(reg);
}

/** Helper: write SQL to a temp file. */
function writeTempSql(name: string, sql: string): string {
  mkdirSync(TMP_DIR, { recursive: true });
  const path = join(TMP_DIR, name);
  writeFileSync(path, sql, "utf-8");
  return path;
}

// Clean up temp dir after all tests
afterAll(() => {
  try {
    rmSync(TMP_DIR, { recursive: true, force: true });
  } catch {
    // ignore
  }
});

// Need to import afterAll
import { afterAll } from "bun:test";

describe("Analyzer", () => {
  describe("analyzeSql", () => {
    test("returns no findings for empty SQL", () => {
      const analyzer = makeAnalyzer();
      const findings = analyzer.analyzeSql("", "test.sql");
      expect(findings).toHaveLength(0);
    });

    test("runs rules and returns findings", () => {
      const analyzer = makeAnalyzer();
      const findings = analyzer.analyzeSql(
        "SELECT 1;",
        "test.sql",
      );

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(1);
      expect(selectFindings[0]!.message).toContain("SELECT");
    });

    test("returns parse error for invalid SQL", () => {
      const analyzer = makeAnalyzer();
      const findings = analyzer.analyzeSql(
        "SELCT 1;",
        "test.sql",
      );

      expect(findings).toHaveLength(1);
      expect(findings[0]!.ruleId).toBe("parse-error");
      expect(findings[0]!.severity).toBe("error");
    });

    test("preprocesses psql metacommands before parsing", () => {
      const analyzer = makeAnalyzer();
      const sql = "\\set ON_ERROR_STOP on\nSELECT 1;";
      const findings = analyzer.analyzeSql(sql, "test.sql");

      // Should parse successfully (metacommand stripped)
      const parseErrors = findings.filter((f) => f.ruleId === "parse-error");
      expect(parseErrors).toHaveLength(0);

      // Should find the SELECT
      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(1);
    });

    test("skips globally disabled rules", () => {
      const analyzer = makeAnalyzer();
      const config: AnalysisConfig = { skip: ["TEST001"] };
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql", { config });

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(0);
    });

    test("skips connected rules when no db is provided", () => {
      const analyzer = makeAnalyzer([selectRule(), connectedRule()]);
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql");

      const connectedFindings = findings.filter((f) => f.ruleId === "TEST003");
      expect(connectedFindings).toHaveLength(0);
    });

    test("runs connected rules when db is provided", () => {
      const analyzer = makeAnalyzer([connectedRule()]);
      const mockDb = { query: async () => ({ rows: [] }) };
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql", { db: mockDb });

      const connectedFindings = findings.filter((f) => f.ruleId === "TEST003");
      expect(connectedFindings).toHaveLength(1);
    });

    test("applies inline suppression (block form)", () => {
      const analyzer = makeAnalyzer();
      const sql = [
        "-- sqlever:disable TEST001",
        "SELECT 1;",
        "-- sqlever:enable TEST001",
      ].join("\n");

      const findings = analyzer.analyzeSql(sql, "test.sql");

      // TEST001 should be suppressed
      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(0);
    });

    test("applies inline suppression (single-line form)", () => {
      const analyzer = makeAnalyzer();
      const sql = "SELECT 1; -- sqlever:disable TEST001";
      const findings = analyzer.analyzeSql(sql, "test.sql");

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(0);
    });

    test("applies per-file override skip list", () => {
      const analyzer = makeAnalyzer();
      const config: AnalysisConfig = {
        overrides: {
          "deploy/test.sql": { skip: ["TEST001"] },
        },
      };
      const findings = analyzer.analyzeSql(
        "SELECT 1;",
        "deploy/test.sql",
        { config },
      );

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(0);
    });

    test("applies severity override from config", () => {
      const analyzer = makeAnalyzer();
      const config: AnalysisConfig = {
        rules: { TEST001: { severity: "error" } },
      };
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql", { config });

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(1);
      expect(selectFindings[0]!.severity).toBe("error");
    });

    test("turns off rules via severity=off", () => {
      const analyzer = makeAnalyzer();
      const config: AnalysisConfig = {
        rules: { TEST001: { severity: "off" } },
      };
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql", { config });

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(0);
    });

    test("promotes warn to error when errorOnWarn is true", () => {
      const analyzer = makeAnalyzer();
      const config: AnalysisConfig = { errorOnWarn: true };
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql", { config });

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(1);
      expect(selectFindings[0]!.severity).toBe("error");
    });

    test("reports unused suppression warnings", () => {
      const analyzer = makeAnalyzer();
      const sql = [
        "-- sqlever:disable TEST001",
        "CREATE TABLE t (id int);",
        "-- sqlever:enable TEST001",
      ].join("\n");

      const findings = analyzer.analyzeSql(sql, "test.sql");

      // TEST001 is not triggered (no SELECT), so the suppression is unused
      const unusedWarnings = findings.filter(
        (f) => f.message.includes("Unused suppression") && f.message.includes("TEST001"),
      );
      expect(unusedWarnings).toHaveLength(1);
    });

    test("handles rule that throws an error gracefully", () => {
      const throwingRule: Rule = {
        id: "THROW001",
        severity: "error",
        type: "static",
        check(): Finding[] {
          throw new Error("Rule exploded");
        },
      };
      const analyzer = makeAnalyzer([throwingRule]);
      const findings = analyzer.analyzeSql("SELECT 1;", "test.sql");

      const errorFindings = findings.filter((f) => f.ruleId === "THROW001");
      expect(errorFindings).toHaveLength(1);
      expect(errorFindings[0]!.message).toContain("threw an error");
      expect(errorFindings[0]!.message).toContain("Rule exploded");
    });

    test("multiple rules run independently", () => {
      const analyzer = makeAnalyzer();
      const sql = "SELECT 1;\nCREATE TABLE t (id int);";
      const findings = analyzer.analyzeSql(sql, "test.sql");

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      const createFindings = findings.filter((f) => f.ruleId === "TEST002");
      expect(selectFindings).toHaveLength(1);
      expect(createFindings).toHaveLength(1);
    });

    test("uses default pgVersion of 14", () => {
      let capturedVersion = 0;
      const versionRule: Rule = {
        id: "VERSION001",
        severity: "info",
        type: "static",
        check(ctx: AnalysisContext): Finding[] {
          capturedVersion = ctx.pgVersion;
          return [];
        },
      };
      const analyzer = makeAnalyzer([versionRule]);
      analyzer.analyzeSql("SELECT 1;", "test.sql");
      expect(capturedVersion).toBe(14);
    });

    test("respects custom pgVersion from options", () => {
      let capturedVersion = 0;
      const versionRule: Rule = {
        id: "VERSION001",
        severity: "info",
        type: "static",
        check(ctx: AnalysisContext): Finding[] {
          capturedVersion = ctx.pgVersion;
          return [];
        },
      };
      const analyzer = makeAnalyzer([versionRule]);
      analyzer.analyzeSql("SELECT 1;", "test.sql", { pgVersion: 16 });
      expect(capturedVersion).toBe(16);
    });
  });

  describe("analyze (file-based)", () => {
    test("reads and analyzes a SQL file", () => {
      const path = writeTempSql("read-test.sql", "SELECT 1;");
      const analyzer = makeAnalyzer();
      const findings = analyzer.analyze(path);

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(1);
    });

    test("analyzes a file with metacommands", () => {
      const sql = "\\set ON_ERROR_STOP on\n\\i shared.sql\nSELECT 1;";
      const path = writeTempSql("meta-test.sql", sql);
      const analyzer = makeAnalyzer();
      const findings = analyzer.analyze(path);

      const parseErrors = findings.filter((f) => f.ruleId === "parse-error");
      expect(parseErrors).toHaveLength(0);

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      expect(selectFindings).toHaveLength(1);
    });
  });

  describe("multi-statement analysis", () => {
    test("finds issues across multiple statements", () => {
      const analyzer = makeAnalyzer();
      const sql = [
        "CREATE TABLE users (id int);",
        "SELECT * FROM users;",
        "CREATE TABLE posts (id int);",
      ].join("\n");

      const findings = analyzer.analyzeSql(sql, "test.sql");

      const selectFindings = findings.filter((f) => f.ruleId === "TEST001");
      const createFindings = findings.filter((f) => f.ruleId === "TEST002");
      expect(selectFindings).toHaveLength(1);
      expect(createFindings).toHaveLength(2);
    });
  });
});
