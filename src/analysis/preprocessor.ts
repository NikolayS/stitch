// src/analysis/preprocessor.ts — Strip psql metacommands before parsing
//
// psql metacommands (\i, \ir, \set, \echo, etc.) are not valid SQL and will
// cause libpg-query to fail. We strip them before parsing while preserving
// line numbers so that findings map back to the original source locations.
//
// Strategy: replace each metacommand line with a blank line (preserving \n)
// so that byte offsets and line numbers remain stable.

/** Result of preprocessing: cleaned SQL and a mapping for location fixup. */
export interface PreprocessResult {
  /** SQL with metacommands replaced by blank lines. */
  cleanedSql: string;
  /** Original SQL text (before preprocessing). */
  originalSql: string;
  /** Lines that were stripped (1-indexed line numbers). */
  strippedLines: number[];
}

/**
 * Regex matching psql metacommands at the start of a line.
 *
 * Covers:
 *   \i, \ir, \include, \include_relative — file includes
 *   \set — variable assignment
 *   \echo — echo text
 *   \pset, \timing, \x — output formatting
 *   \connect, \c — change connection
 *   \cd — change directory
 *   \encoding — set encoding
 *   \password — set password
 *   \prompt — prompt user
 *   \! — shell command
 *   \copy — client-side copy
 *   \q, \quit — quit
 *   \if, \elif, \else, \endif — conditional execution
 *   \warn — print to stderr
 *
 * Pattern: backslash followed by a word or '!', then rest of line.
 * This is intentionally broad — any unrecognized \word will be stripped
 * as well. libpg-query cannot parse them either way.
 */
const METACOMMAND_RE = /^\\(?:[a-zA-Z_]\w*|!)(?:\b|\s|$).*$/;

/**
 * Strip psql metacommands from SQL text.
 *
 * Each metacommand line is replaced with an empty line to preserve
 * line numbers and byte offsets of subsequent lines.
 */
export function preprocessSql(sql: string): PreprocessResult {
  const lines = sql.split("\n");
  const strippedLines: number[] = [];
  const cleanedLines: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const trimmed = lines[i]!.trimStart();

    if (METACOMMAND_RE.test(trimmed)) {
      // Replace with a blank line of the same byte length
      // to preserve offsets for everything after this line
      cleanedLines.push(" ".repeat(lines[i]!.length));
      strippedLines.push(i + 1); // 1-indexed
    } else {
      cleanedLines.push(lines[i]!);
    }
  }

  return {
    cleanedSql: cleanedLines.join("\n"),
    originalSql: sql,
    strippedLines,
  };
}

/**
 * Convert a byte offset in the SQL text to a 1-indexed line and column.
 */
export function byteOffsetToLocation(
  sql: string,
  byteOffset: number,
): { line: number; column: number } {
  let line = 1;
  let col = 1;

  for (let i = 0; i < byteOffset && i < sql.length; i++) {
    if (sql[i] === "\n") {
      line++;
      col = 1;
    } else {
      col++;
    }
  }

  return { line, column: col };
}
