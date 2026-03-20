/**
 * Tests for src/analysis/suppression.ts — inline suppression parsing and filtering.
 */
import { describe, test, expect } from "bun:test";
import {
  parseSuppressions,
  resolveSuppressionRanges,
  filterFindings,
} from "../../src/analysis/suppression";
import type { Finding } from "../../src/analysis/types";

const FILE = "test.sql";

/** Helper: create a finding at a given line. */
function makeFinding(
  ruleId: string,
  line: number,
  severity: "error" | "warn" | "info" = "warn",
): Finding {
  return {
    ruleId,
    severity,
    message: `Test finding for ${ruleId}`,
    location: { file: FILE, line, column: 1 },
  };
}

describe("parseSuppressions", () => {
  test("parses a disable directive", () => {
    const sql = "-- sqlever:disable SA001\nSELECT 1;";
    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.action).toBe("disable");
    expect(directives[0]!.ruleIds).toEqual(["SA001"]);
    expect(directives[0]!.line).toBe(1);
  });

  test("parses an enable directive", () => {
    const sql = "SELECT 1;\n-- sqlever:enable SA001";
    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.action).toBe("enable");
    expect(directives[0]!.ruleIds).toEqual(["SA001"]);
    expect(directives[0]!.line).toBe(2);
  });

  test("parses comma-separated rule IDs", () => {
    const sql = "-- sqlever:disable SA010,SA011";
    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.ruleIds).toEqual(["SA010", "SA011"]);
  });

  test("parses comma-separated with spaces", () => {
    const sql = "-- sqlever:disable SA010, SA011, SA012";
    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.ruleIds).toEqual(["SA010", "SA011", "SA012"]);
  });

  test("parses block form (disable + enable)", () => {
    const sql = [
      "-- sqlever:disable SA010",
      "UPDATE users SET tier = 'free';",
      "-- sqlever:enable SA010",
    ].join("\n");

    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(2);
    expect(directives[0]!.action).toBe("disable");
    expect(directives[1]!.action).toBe("enable");
  });

  test("parses single-line form (trailing comment)", () => {
    const sql = "UPDATE users SET tier = 'free'; -- sqlever:disable SA010";
    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.ruleIds).toEqual(["SA010"]);
    expect(directives[0]!.line).toBe(1);
  });

  test("handles no suppression directives", () => {
    const sql = "SELECT 1;\n-- Just a regular comment\nSELECT 2;";
    const directives = parseSuppressions(sql);
    expect(directives).toHaveLength(0);
  });
});

describe("resolveSuppressionRanges", () => {
  const knownRules = new Set(["SA001", "SA010", "SA011"]);

  test("resolves block form range", () => {
    const sql = [
      "-- sqlever:disable SA010",
      "UPDATE users SET tier = 'free';",
      "-- sqlever:enable SA010",
    ].join("\n");

    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges, warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      FILE,
    );

    expect(ranges).toHaveLength(1);
    expect(ranges[0]!.ruleId).toBe("SA010");
    expect(ranges[0]!.startLine).toBe(1);
    expect(ranges[0]!.endLine).toBe(3);
    expect(warnings).toHaveLength(0);
  });

  test("resolves single-line form range", () => {
    const sql = "UPDATE users SET tier = 'free'; -- sqlever:disable SA010";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      FILE,
    );

    expect(ranges).toHaveLength(1);
    expect(ranges[0]!.ruleId).toBe("SA010");
    expect(ranges[0]!.startLine).toBe(1);
    expect(ranges[0]!.endLine).toBe(1); // single-line
  });

  test("warns on unknown rule ID", () => {
    const sql = "-- sqlever:disable SA999\nSELECT 1;\n-- sqlever:enable SA999";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      FILE,
    );

    const unknownWarnings = warnings.filter((w) =>
      w.message.includes("Unknown rule ID"),
    );
    expect(unknownWarnings.length).toBeGreaterThan(0);
    expect(unknownWarnings[0]!.message).toContain("SA999");
  });

  test("warns on unclosed block", () => {
    const sql = "-- sqlever:disable SA010\nUPDATE users SET tier = 'free';";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges, warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      FILE,
    );

    expect(ranges).toHaveLength(1);
    expect(ranges[0]!.endLine).toBe(sqlLines.length); // extends to EOF
    const unclosedWarnings = warnings.filter((w) =>
      w.message.includes("Unclosed"),
    );
    expect(unclosedWarnings).toHaveLength(1);
  });

  test("warns on 'all' keyword", () => {
    const sql = "-- sqlever:disable all\nSELECT 1;\n-- sqlever:enable all";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      FILE,
    );

    const allWarnings = warnings.filter((w) =>
      w.message.includes("all"),
    );
    expect(allWarnings.length).toBeGreaterThan(0);
  });

  test("resolves multiple comma-separated rules in block", () => {
    const sql = [
      "-- sqlever:disable SA010,SA011",
      "UPDATE users SET tier = 'free';",
      "DELETE FROM logs;",
      "-- sqlever:enable SA010,SA011",
    ].join("\n");

    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      FILE,
    );

    expect(ranges).toHaveLength(2);
    const ruleIds = ranges.map((r) => r.ruleId).sort();
    expect(ruleIds).toEqual(["SA010", "SA011"]);
  });
});

describe("filterFindings", () => {
  test("passes through findings with no suppressions", () => {
    const findings = [makeFinding("SA010", 2)];
    const { filtered, suppressed, warnings } = filterFindings(
      findings,
      [],
      [],
    );

    expect(filtered).toHaveLength(1);
    expect(suppressed).toHaveLength(0);
    expect(warnings).toHaveLength(0);
  });

  test("suppresses findings in block range", () => {
    const findings = [makeFinding("SA010", 2)];
    const ranges = [
      {
        ruleId: "SA010",
        startLine: 1,
        endLine: 3,
        directive: { action: "disable" as const, ruleIds: ["SA010"], line: 1 },
        used: false,
      },
    ];

    const { filtered, suppressed } = filterFindings(findings, ranges, []);
    expect(filtered).toHaveLength(0);
    expect(suppressed).toHaveLength(1);
    expect(ranges[0]!.used).toBe(true);
  });

  test("does not suppress findings outside range", () => {
    const findings = [makeFinding("SA010", 5)];
    const ranges = [
      {
        ruleId: "SA010",
        startLine: 1,
        endLine: 3,
        directive: { action: "disable" as const, ruleIds: ["SA010"], line: 1 },
        used: false,
      },
    ];

    const { filtered, suppressed } = filterFindings(findings, ranges, []);
    expect(filtered).toHaveLength(1);
    expect(suppressed).toHaveLength(0);
  });

  test("does not suppress findings for different rule ID", () => {
    const findings = [makeFinding("SA001", 2)];
    const ranges = [
      {
        ruleId: "SA010",
        startLine: 1,
        endLine: 3,
        directive: { action: "disable" as const, ruleIds: ["SA010"], line: 1 },
        used: false,
      },
    ];

    const { filtered } = filterFindings(findings, ranges, []);
    expect(filtered).toHaveLength(1);
  });

  test("suppresses findings via per-file skip list", () => {
    const findings = [makeFinding("SA010", 2), makeFinding("SA001", 3)];
    const { filtered, suppressed } = filterFindings(
      findings,
      [],
      [],
      ["SA010"],
    );

    expect(filtered).toHaveLength(1);
    expect(filtered[0]!.ruleId).toBe("SA001");
    expect(suppressed).toHaveLength(1);
    expect(suppressed[0]!.ruleId).toBe("SA010");
  });

  test("reports unused suppression warnings", () => {
    const findings: Finding[] = [];
    const ranges = [
      {
        ruleId: "SA010",
        startLine: 1,
        endLine: 3,
        directive: { action: "disable" as const, ruleIds: ["SA010"], line: 1 },
        used: false,
      },
    ];

    const { warnings } = filterFindings(findings, ranges, []);
    const unusedWarnings = warnings.filter((w) =>
      w.message.includes("Unused suppression"),
    );
    expect(unusedWarnings).toHaveLength(1);
    expect(unusedWarnings[0]!.message).toContain("SA010");
  });

  test("does not report used suppression as unused", () => {
    const findings = [makeFinding("SA010", 2)];
    const ranges = [
      {
        ruleId: "SA010",
        startLine: 1,
        endLine: 3,
        directive: { action: "disable" as const, ruleIds: ["SA010"], line: 1 },
        used: false,
      },
    ];

    const { warnings } = filterFindings(findings, ranges, []);
    const unusedWarnings = warnings.filter((w) =>
      w.message.includes("Unused suppression"),
    );
    expect(unusedWarnings).toHaveLength(0);
  });

  test("passes through directive warnings", () => {
    const directiveWarnings: Finding[] = [
      {
        ruleId: "suppression",
        severity: "warn",
        message: "Unknown rule ID",
        location: { file: FILE, line: 1, column: 1 },
      },
    ];

    const { warnings } = filterFindings([], [], directiveWarnings);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.message).toContain("Unknown rule ID");
  });
});
