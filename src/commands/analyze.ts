// src/commands/analyze.ts — sqlever analyze command
//
// Static analysis of SQL migration files for dangerous patterns.
//
// Usage:
//   sqlever analyze file.sql       — analyze a single file
//   sqlever analyze dir/           — analyze all .sql files in a directory
//   sqlever analyze                — analyze pending migrations from sqitch.plan
//   sqlever analyze --all          — analyze all migrations from sqitch.plan
//   sqlever analyze --changed      — analyze files changed in git diff
//
// Options:
//   --format text|json|github-annotations|gitlab-codequality
//   --strict                       — treat warnings as errors for exit code
//   --force-rule SA003             — bypass a specific rule (repeatable)
//
// Exit codes:
//   0 — no error-level findings
//   2 — one or more error-level findings
//
// Implements S5-5 (GitHub issue #54).

import { existsSync, readFileSync, statSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { Analyzer } from "../analysis/index";
import { defaultRegistry } from "../analysis/registry";
import { allRules } from "../analysis/rules/index";
import {
  formatFindings,
  computeSummary,
  type ReportFormat,
  type ReportMetadata,
  type Finding,
} from "../analysis/reporter";
import type { AnalysisConfig } from "../analysis/types";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface AnalyzeOptions {
  /** Positional arguments (file paths / directories). */
  targets: string[];
  /** Output format. */
  format: ReportFormat;
  /** Treat warnings as errors for exit code. */
  strict: boolean;
  /** Analyze all migrations, not just pending. */
  all: boolean;
  /** Analyze files changed in git diff. */
  changed: boolean;
  /** Rules to forcibly skip (--force-rule). */
  forceRules: string[];
  /** Project top directory. */
  topDir?: string;
  /** Plan file path override. */
  planFile?: string;
}

export interface AnalyzeResult {
  /** All findings across all analyzed files. */
  findings: Finding[];
  /** Number of files analyzed. */
  filesAnalyzed: number;
  /** Exit code: 0 if clean, 2 if errors found. */
  exitCode: number;
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/**
 * Parse analyze-specific arguments from the rest array.
 */
export function parseAnalyzeArgs(rest: string[]): AnalyzeOptions {
  const opts: AnalyzeOptions = {
    targets: [],
    format: "text",
    strict: false,
    all: false,
    changed: false,
    forceRules: [],
  };

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--format") {
      const val = rest[i + 1];
      if (
        val === "text" ||
        val === "json" ||
        val === "github-annotations" ||
        val === "gitlab-codequality"
      ) {
        opts.format = val;
      } else {
        throw new Error(
          `Invalid --format value '${val ?? ""}'. Expected text, json, github-annotations, or gitlab-codequality.`,
        );
      }
      i += 2;
      continue;
    }

    if (arg === "--strict") {
      opts.strict = true;
      i++;
      continue;
    }

    if (arg === "--all") {
      opts.all = true;
      i++;
      continue;
    }

    if (arg === "--changed") {
      opts.changed = true;
      i++;
      continue;
    }

    if (arg === "--force-rule") {
      const val = rest[i + 1];
      if (!val) {
        throw new Error("--force-rule requires a rule ID argument");
      }
      opts.forceRules.push(val);
      i += 2;
      continue;
    }

    if (arg === "--top-dir") {
      opts.topDir = rest[i + 1];
      i += 2;
      continue;
    }

    if (arg === "--plan-file") {
      opts.planFile = rest[i + 1];
      i += 2;
      continue;
    }

    // Positional argument — file or directory target
    opts.targets.push(arg);
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// File resolution
// ---------------------------------------------------------------------------

/**
 * Collect all .sql files from a directory (non-recursive).
 */
function collectSqlFiles(dirPath: string): string[] {
  const entries = readdirSync(dirPath, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.endsWith(".sql"))
    .map((e) => join(dirPath, e.name))
    .sort();
}

/**
 * Resolve explicit targets (files and directories) to a list of .sql file paths.
 */
function resolveExplicitTargets(targets: string[]): string[] {
  const files: string[] = [];
  for (const target of targets) {
    const resolved = resolve(target);
    if (!existsSync(resolved)) {
      throw new Error(`Path not found: ${target}`);
    }
    const stat = statSync(resolved);
    if (stat.isDirectory()) {
      files.push(...collectSqlFiles(resolved));
    } else {
      files.push(resolved);
    }
  }
  return files;
}

/**
 * Get migration file paths from sqitch.plan.
 *
 * Without a database connection we cannot determine deployment state,
 * so all change deploy scripts are returned.
 */
function resolveFromPlan(
  planPath: string,
  deployDir: string,
): string[] {
  if (!existsSync(planPath)) {
    throw new Error(`Plan file not found: ${planPath}`);
  }

  const { parsePlan } = require("../plan/parser") as typeof import("../plan/parser");
  const planContent = readFileSync(planPath, "utf-8");
  const plan = parsePlan(planContent);

  const files: string[] = [];
  for (const change of plan.changes) {
    const deployFile = join(deployDir, `${change.name}.sql`);
    if (existsSync(deployFile)) {
      files.push(resolve(deployFile));
    }
  }

  return files;
}

/**
 * Get files changed in git diff (unstaged + staged vs HEAD).
 * Only returns .sql files.
 */
function resolveChangedFiles(): string[] {
  try {
    const proc = Bun.spawnSync(["git", "diff", "--name-only", "HEAD"], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const diffOutput = proc.stdout.toString().trim();

    // Also include staged files
    const stagedProc = Bun.spawnSync(
      ["git", "diff", "--name-only", "--cached"],
      { stdout: "pipe", stderr: "pipe" },
    );
    const stagedOutput = stagedProc.stdout.toString().trim();

    // Also include untracked files
    const untrackedProc = Bun.spawnSync(
      ["git", "ls-files", "--others", "--exclude-standard"],
      { stdout: "pipe", stderr: "pipe" },
    );
    const untrackedOutput = untrackedProc.stdout.toString().trim();

    const allFiles = new Set<string>();
    for (const output of [diffOutput, stagedOutput, untrackedOutput]) {
      if (output) {
        for (const f of output.split("\n")) {
          if (f.endsWith(".sql")) {
            const abs = resolve(f);
            if (existsSync(abs)) {
              allFiles.add(abs);
            }
          }
        }
      }
    }

    return Array.from(allFiles).sort();
  } catch {
    throw new Error("Failed to determine changed files from git");
  }
}

// ---------------------------------------------------------------------------
// Core analysis
// ---------------------------------------------------------------------------

/**
 * Run the analyze command.
 *
 * @returns AnalyzeResult with findings, file count, and exit code.
 */
export async function runAnalyze(opts: AnalyzeOptions): Promise<AnalyzeResult> {
  // Register all rules into the default registry (idempotent — skips already-registered)
  for (const rule of allRules) {
    if (!defaultRegistry.has(rule.id)) {
      defaultRegistry.register(rule);
    }
  }

  const analyzer = new Analyzer(defaultRegistry);
  await analyzer.ensureWasm();

  // Resolve file list
  let files: string[];

  if (opts.targets.length > 0) {
    // Explicit file/directory targets
    files = resolveExplicitTargets(opts.targets);
  } else if (opts.changed) {
    // Files changed in git
    files = resolveChangedFiles();
  } else {
    // From sqitch.plan (pending or all)
    const topDir = opts.topDir ?? ".";
    const planFile = opts.planFile ?? join(topDir, "sqitch.plan");
    const deployDir = join(topDir, "deploy");

    if (!existsSync(planFile)) {
      if (opts.planFile) {
        // Explicitly specified plan file — throw if not found
        throw new Error(`Plan file not found: ${planFile}`);
      }
      // No plan file and no explicit targets — nothing to analyze
      return { findings: [], filesAnalyzed: 0, exitCode: 0 };
    }

    files = resolveFromPlan(planFile, deployDir);
  }

  if (files.length === 0) {
    return { findings: [], filesAnalyzed: 0, exitCode: 0 };
  }

  // Build analysis config
  const config: AnalysisConfig = {
    skip: [...opts.forceRules],
    errorOnWarn: opts.strict,
  };

  // Analyze all files
  const allFindings: Finding[] = [];
  const startTime = performance.now();

  for (const filePath of files) {
    try {
      const findings = analyzer.analyze(filePath, { config });
      allFindings.push(...findings);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      allFindings.push({
        ruleId: "analyze-error",
        severity: "error",
        message: `Failed to analyze file: ${message}`,
        location: { file: filePath, line: 1, column: 1 },
      });
    }
  }

  const duration = performance.now() - startTime;

  // Compute summary for exit code
  const summary = computeSummary(allFindings);

  // Determine exit code
  const hasErrors = summary.errors > 0;
  const hasWarnings = summary.warnings > 0;
  const exitCode = hasErrors || (opts.strict && hasWarnings) ? 2 : 0;

  // Format and print output
  const metadata: ReportMetadata = {
    files_analyzed: files.length,
    rules_checked: defaultRegistry.size,
    duration_ms: Math.round(duration),
  };

  const output = formatFindings(opts.format, allFindings, {
    metadata,
    useColors: process.stdout.isTTY ?? false,
  });

  process.stdout.write(output);

  return {
    findings: allFindings,
    filesAnalyzed: files.length,
    exitCode,
  };
}
