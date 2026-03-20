// src/analysis/index.ts — Analyzer entry point for sqlever static analysis
//
// The Analyzer class is the main entry point. Given a file path and options,
// it:
//   1. Reads the SQL file
//   2. Preprocesses (strips psql metacommands)
//   3. Parses via libpg-query
//   4. Loads rules from the registry
//   5. Runs each applicable rule
//   6. Filters findings through inline suppressions and per-file overrides
//   7. Returns the final set of findings

import { parseSync, loadModule } from "libpg-query";
import { readFileSync } from "fs";
import { preprocessSql } from "./preprocessor";
import {
  parseSuppressions,
  resolveSuppressionRanges,
  filterFindings,
} from "./suppression";
import { RuleRegistry, defaultRegistry } from "./registry";
import type {
  Finding,
  AnalyzeOptions,
  AnalysisContext,
  ParseResult,
} from "./types";

// Re-export types for consumer convenience
export type {
  Finding,
  AnalyzeOptions,
  AnalysisContext,
  AnalysisConfig,
  Rule,
  RuleType,
  Severity,
  DatabaseClient,
  ParseResult,
  StmtEntry,
  FindingLocation,
  RuleConfig,
  FileOverride,
} from "./types";
export { RuleRegistry, defaultRegistry } from "./registry";
export { preprocessSql, byteOffsetToLocation } from "./preprocessor";
export {
  parseSuppressions,
  resolveSuppressionRanges,
  filterFindings,
} from "./suppression";

/** Default PG version when none is specified. */
const DEFAULT_PG_VERSION = 14;

/**
 * Analyzer — the main entry point for sqlever static analysis.
 *
 * Usage:
 *   const analyzer = new Analyzer();
 *   // or: const analyzer = new Analyzer(customRegistry);
 *   const findings = analyzer.analyze("deploy/001_create_users.sql");
 */
export class Analyzer {
  private readonly registry: RuleRegistry;
  private wasmLoaded = false;

  constructor(registry?: RuleRegistry) {
    this.registry = registry ?? defaultRegistry;
  }

  /**
   * Ensure the libpg-query WASM module is loaded.
   * Safe to call multiple times — only loads once.
   */
  async ensureWasm(): Promise<void> {
    if (!this.wasmLoaded) {
      await loadModule();
      this.wasmLoaded = true;
    }
  }

  /**
   * Analyze a SQL file and return findings.
   *
   * @param filePath — path to the SQL file
   * @param options — analysis options (config, db client, pgVersion, etc.)
   * @returns array of findings (filtered through suppressions)
   */
  analyze(filePath: string, options?: AnalyzeOptions): Finding[] {
    const config = options?.config ?? {};
    const pgVersion = options?.pgVersion ?? config.pgVersion ?? DEFAULT_PG_VERSION;
    const db = options?.db;

    // 1. Read the file
    const rawSql = readFileSync(filePath, "utf-8");

    // 2. Analyze SQL content directly
    return this.analyzeSql(rawSql, filePath, {
      ...options,
      pgVersion,
      config,
      db,
    });
  }

  /**
   * Analyze raw SQL text and return findings.
   *
   * Useful when the SQL is already in memory (e.g., from stdin or tests).
   */
  analyzeSql(
    sql: string,
    filePath: string,
    options?: AnalyzeOptions,
  ): Finding[] {
    const config = options?.config ?? {};
    const pgVersion = options?.pgVersion ?? config.pgVersion ?? DEFAULT_PG_VERSION;
    const db = options?.db;

    // 1. Preprocess: strip psql metacommands
    const { cleanedSql, originalSql } = preprocessSql(sql);

    // 1b. Short-circuit on empty/whitespace-only SQL
    if (cleanedSql.trim().length === 0) {
      return [];
    }

    // 2. Parse via libpg-query (synchronous — WASM must be loaded first)
    let ast: ParseResult;
    try {
      ast = parseSync(cleanedSql) as ParseResult;
    } catch (err: unknown) {
      // If parsing fails, return a single finding with the parse error
      const message =
        err instanceof Error ? err.message : String(err);
      return [
        {
          ruleId: "parse-error",
          severity: "error",
          message: `Failed to parse SQL: ${message}`,
          location: { file: filePath, line: 1, column: 1 },
        },
      ];
    }

    // 3. Build analysis context
    const context: AnalysisContext = {
      ast,
      rawSql: originalSql,
      filePath,
      pgVersion,
      config,
      db,
    };

    // 4. Determine which rules to run
    const allRules = this.registry.all();
    const globalSkip = new Set(config.skip ?? []);

    // Per-file overrides
    const fileOverride = config.overrides?.[filePath];
    const fileSkipRules = fileOverride?.skip;

    // 5. Run applicable rules
    const allFindings: Finding[] = [];

    for (const rule of allRules) {
      // Skip globally disabled rules
      if (globalSkip.has(rule.id)) continue;

      // Skip connected rules when no db is available
      if (rule.type === "connected" && !db) continue;

      try {
        const findings = rule.check(context);
        allFindings.push(...findings);
      } catch (err: unknown) {
        // Rule threw an error — report it as an internal finding
        const message =
          err instanceof Error ? err.message : String(err);
        allFindings.push({
          ruleId: rule.id,
          severity: "warn",
          message: `Rule "${rule.id}" threw an error: ${message}`,
          location: { file: filePath, line: 1, column: 1 },
        });
      }
    }

    // 6. Apply severity overrides from config
    for (const finding of allFindings) {
      const ruleConfig = config.rules?.[finding.ruleId];
      if (ruleConfig?.severity) {
        if (ruleConfig.severity === "off") {
          // Will be filtered out below
          continue;
        }
        finding.severity = ruleConfig.severity;
      }
      // error_on_warn promotion
      if (config.errorOnWarn && finding.severity === "warn") {
        finding.severity = "error";
      }
    }

    // Filter out "off" rules
    const activeFindings = allFindings.filter((f) => {
      const ruleConfig = config.rules?.[f.ruleId];
      return ruleConfig?.severity !== "off";
    });

    // 7. Parse inline suppressions
    const directives = parseSuppressions(originalSql);
    const sqlLines = originalSql.split("\n");
    const knownRuleIds = new Set(this.registry.ids());
    const { ranges, warnings: directiveWarnings } =
      resolveSuppressionRanges(
        directives,
        sqlLines,
        sqlLines.length,
        knownRuleIds,
        filePath,
      );

    // 8. Filter findings through suppressions
    const { filtered, warnings: suppressionWarnings } = filterFindings(
      activeFindings,
      ranges,
      directiveWarnings,
      fileSkipRules,
    );

    // 9. Return findings + suppression warnings
    return [...filtered, ...suppressionWarnings];
  }
}
