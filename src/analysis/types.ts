// src/analysis/types.ts — Core types for the sqlever analysis engine
//
// Defines the Rule interface, AnalysisContext, Finding, and Severity types
// per SPEC section 5.1. These types are the contract between the analyzer
// entry point, the rule registry, and individual rule implementations.

// ---------------------------------------------------------------------------
// Severity
// ---------------------------------------------------------------------------

/** Severity level for analysis findings. */
export type Severity = "error" | "warn" | "info";

// ---------------------------------------------------------------------------
// Finding
// ---------------------------------------------------------------------------

/** Source location for a finding. */
export interface FindingLocation {
  file: string;
  line: number;
  column: number;
  endLine?: number;
  endColumn?: number;
}

/** Alias for FindingLocation — used by rule implementations. */
export type Location = FindingLocation;

/** A single finding produced by a rule. */
export interface Finding {
  ruleId: string;
  severity: Severity;
  message: string;
  location: FindingLocation;
  suggestion?: string;
}

// ---------------------------------------------------------------------------
// AnalysisContext
// ---------------------------------------------------------------------------

/** Parsed SQL AST from libpg-query. */
export interface ParseResult {
  stmts: StmtEntry[];
}

/** A single statement entry from the parser. */
export interface StmtEntry {
  stmt: Record<string, unknown>;
  stmt_location?: number;
  stmt_len?: number;
}

/** Configuration for the analysis engine. */
export interface AnalysisConfig {
  /** Rules to skip globally. */
  skip?: string[];
  /** Treat warnings as errors. */
  errorOnWarn?: boolean;
  /** Max affected rows threshold for batch-related rules. */
  maxAffectedRows?: number;
  /** Minimum PG version migrations must support. */
  pgVersion?: number;
  /** Per-rule configuration. */
  rules?: Record<string, RuleConfig>;
  /** Per-file overrides keyed by file path. */
  overrides?: Record<string, FileOverride>;
}

/** Per-rule configuration. */
export interface RuleConfig {
  /** Override max_affected_rows for this rule. */
  maxAffectedRows?: number;
  /** Severity override. */
  severity?: Severity | "off";
}

/** Per-file override. */
export interface FileOverride {
  /** Rules to skip for this file. */
  skip?: string[];
}

/** Minimal database client interface for connected/hybrid rules. */
export interface DatabaseClient {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

/** Context passed to every rule's check() method. */
export interface AnalysisContext {
  /** Parsed AST from libpg-query. */
  ast: ParseResult;
  /** Original SQL text (after preprocessor strips metacommands). */
  rawSql: string;
  /** Path to the file being analyzed. */
  filePath: string;
  /** Minimum PG version to target. */
  pgVersion: number;
  /** Analysis configuration. */
  config: AnalysisConfig;
  /** Database client, present only for connected/hybrid rules with active connection. */
  db?: DatabaseClient;
  /** Whether this file is in a revert context (e.g. under revert/ in a sqitch project). */
  isRevertContext?: boolean;
}

// ---------------------------------------------------------------------------
// Rule
// ---------------------------------------------------------------------------

/** Rule type classification. */
export type RuleType = "static" | "connected" | "hybrid";

/**
 * Rule interface — the contract every analysis rule must implement.
 *
 * Rules receive an AnalysisContext and return an array of findings.
 * Suppression filtering happens in the analyzer entry point AFTER
 * rules return findings — rules do not see or reason about suppressions.
 */
export interface Rule {
  /** Unique rule identifier, e.g., "SA001". */
  id: string;
  /** Default severity level. */
  severity: Severity;
  /** Whether this rule is static, connected, or hybrid. */
  type: RuleType;
  /** Run the rule against the given context, returning any findings. */
  check(context: AnalysisContext): Finding[];
}

// ---------------------------------------------------------------------------
// AnalyzeOptions
// ---------------------------------------------------------------------------

/** Options passed to the Analyzer.analyze() entry point. */
export interface AnalyzeOptions {
  /** Analysis configuration (from sqlever.toml). */
  config?: AnalysisConfig;
  /** Database client for connected/hybrid rules. */
  db?: DatabaseClient;
  /** Minimum PG version to target. Default: 14. */
  pgVersion?: number;
  /** Whether to treat the file as a revert script (affects SA007 etc.). */
  isRevert?: boolean;
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/**
 * Convert a byte offset in the source SQL to a 1-based line and column.
 */
export function offsetToLocation(
  rawSql: string,
  byteOffset: number,
  filePath: string,
): Location {
  let line = 1;
  let col = 1;
  const len = Math.min(byteOffset, rawSql.length);
  for (let i = 0; i < len; i++) {
    if (rawSql[i] === "\n") {
      line++;
      col = 1;
    } else {
      col++;
    }
  }
  return { file: filePath, line, column: col };
}

/**
 * Extract the type name string from a libpg-query TypeName node.
 * Returns the last name part (e.g. "varchar", "int4", "text").
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function extractTypeName(typeName: any): string | null {
  if (!typeName?.names) return null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const names = typeName.names as any[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const last = names[names.length - 1];
  return last?.String?.sval ?? null;
}

/**
 * Extract type modifiers (e.g. length for varchar, precision/scale for numeric).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function extractTypeMods(typeName: any): number[] {
  if (!typeName?.typmods) return [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (typeName.typmods as any[])
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    .map((m: any) => m?.A_Const?.ival?.ival)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    .filter((v: any): v is number => typeof v === "number");
}

/**
 * Get the fully-qualified type name for display purposes.
 * Skips "pg_catalog" schema prefix.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function displayTypeName(typeName: any): string {
  if (!typeName?.names) return "unknown";
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const names = (typeName.names as any[])
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    .map((n: any) => n?.String?.sval)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    .filter((s: any): s is string => !!s)
    .filter((s: string) => s !== "pg_catalog");
  const base = names.join(".");
  const mods = extractTypeMods(typeName);
  if (mods.length > 0) {
    return `${base}(${mods.join(",")})`;
  }
  return base;
}
