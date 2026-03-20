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
