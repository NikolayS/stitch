/**
 * Tests for src/analysis/preprocessor.ts — psql metacommand stripping.
 */
import { describe, test, expect } from "bun:test";
import {
  preprocessSql,
  byteOffsetToLocation,
} from "../../src/analysis/preprocessor";

describe("preprocessSql", () => {
  test("passes through plain SQL unchanged", () => {
    const sql = "SELECT 1;\nCREATE TABLE t (id int);";
    const result = preprocessSql(sql);
    expect(result.cleanedSql).toBe(sql);
    expect(result.strippedLines).toEqual([]);
  });

  test("strips \\i metacommand", () => {
    const sql = "\\i shared/functions.sql\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.cleanedSql).not.toContain("\\i");
    expect(result.strippedLines).toEqual([1]);
    // Line 2 should be unchanged
    expect(result.cleanedSql.split("\n")[1]).toBe("SELECT 1;");
  });

  test("strips \\ir metacommand", () => {
    const sql = "\\ir ../shared/utils.sql\nSELECT 2;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips \\set metacommand", () => {
    const sql = "\\set ON_ERROR_STOP on\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips \\echo metacommand", () => {
    const sql = "\\echo 'deploying migration'\nCREATE TABLE t (id int);";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips multiple metacommands", () => {
    const sql = [
      "\\set ON_ERROR_STOP on",
      "\\echo 'start'",
      "CREATE TABLE users (id int);",
      "\\i shared/roles.sql",
      "ALTER TABLE users ADD COLUMN name text;",
    ].join("\n");

    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1, 2, 4]);
    // SQL lines should remain intact
    const lines = result.cleanedSql.split("\n");
    expect(lines[2]).toBe("CREATE TABLE users (id int);");
    expect(lines[4]).toBe("ALTER TABLE users ADD COLUMN name text;");
  });

  test("preserves line count (blank lines for stripped metacommands)", () => {
    const sql = "\\set foo bar\nSELECT 1;\n\\echo done\nSELECT 2;";
    const result = preprocessSql(sql);

    const originalLineCount = sql.split("\n").length;
    const cleanedLineCount = result.cleanedSql.split("\n").length;
    expect(cleanedLineCount).toBe(originalLineCount);
  });

  test("handles leading whitespace before metacommand", () => {
    const sql = "  \\set foo bar\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("does not strip backslash in SQL strings", () => {
    // This is a SQL string containing a backslash, not a metacommand
    const sql = "SELECT E'hello\\nworld';";
    const result = preprocessSql(sql);
    expect(result.cleanedSql).toBe(sql);
    expect(result.strippedLines).toEqual([]);
  });

  test("strips \\pset metacommand", () => {
    const sql = "\\pset format csv\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips \\! (shell command)", () => {
    const sql = "\\! echo hello\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips \\if/\\elif/\\else/\\endif conditionals", () => {
    const sql = [
      "\\if :is_production",
      "SELECT 'prod';",
      "\\else",
      "SELECT 'dev';",
      "\\endif",
    ].join("\n");

    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1, 3, 5]);
    const lines = result.cleanedSql.split("\n");
    expect(lines[1]).toBe("SELECT 'prod';");
    expect(lines[3]).toBe("SELECT 'dev';");
  });

  test("preserves originalSql in result", () => {
    const sql = "\\set foo bar\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.originalSql).toBe(sql);
    expect(result.originalSql).not.toBe(result.cleanedSql);
  });

  test("handles empty input", () => {
    const result = preprocessSql("");
    expect(result.cleanedSql).toBe("");
    expect(result.strippedLines).toEqual([]);
  });

  test("handles input with only metacommands", () => {
    const sql = "\\set foo bar\n\\echo hello";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1, 2]);
  });
});

describe("byteOffsetToLocation", () => {
  test("offset 0 maps to line 1, column 1", () => {
    const loc = byteOffsetToLocation("SELECT 1;", 0);
    expect(loc).toEqual({ line: 1, column: 1 });
  });

  test("maps offset within first line", () => {
    const loc = byteOffsetToLocation("SELECT 1;", 7);
    expect(loc).toEqual({ line: 1, column: 8 });
  });

  test("maps offset on second line", () => {
    const sql = "SELECT 1;\nSELECT 2;";
    const loc = byteOffsetToLocation(sql, 10);
    expect(loc).toEqual({ line: 2, column: 1 });
  });

  test("maps offset mid-second-line", () => {
    const sql = "SELECT 1;\nSELECT 2;";
    const loc = byteOffsetToLocation(sql, 17);
    expect(loc).toEqual({ line: 2, column: 8 });
  });

  test("handles multi-line SQL", () => {
    const sql = "A\nBB\nCCC\n";
    // Offset 5 = start of "CCC" line (A\n = 2 bytes, BB\n = 3 bytes, total = 5)
    const loc = byteOffsetToLocation(sql, 5);
    expect(loc).toEqual({ line: 3, column: 1 });
  });
});
