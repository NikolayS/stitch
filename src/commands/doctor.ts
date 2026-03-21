// src/commands/doctor.ts — sqlever doctor command
//
// Validates the project setup and reports potential issues:
//   1. Plan file parsing — can the plan file be parsed without errors?
//   2. Change ID consistency — do recomputed IDs match the parent chain?
//   3. Script file presence — do deploy/revert/verify scripts exist for each change?
//   4. psql metacommand detection — do any deploy/revert scripts contain psql metacommands?
//   5. Syntax version check — is %syntax-version set and supported?
//
// Exit codes:
//   0 — all checks passed
//   1 — one or more checks failed

import { readFileSync, existsSync } from "node:fs";
import { resolve, join } from "node:path";
import type { ParsedArgs } from "../cli";
import { parsePlan, PlanParseError } from "../plan/parser";
import type { Plan } from "../plan/types";
import { loadConfig } from "../config/index";
import { info, error as logError, verbose, json as jsonOut, getConfig } from "../output";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface DoctorOptions {
  /** Project root directory. */
  topDir: string;
  /** Plan file path override. */
  planFile?: string;
  /** Output format override. */
  format?: "text" | "json";
}

export type CheckSeverity = "ok" | "warn" | "error";

export interface CheckResult {
  /** Name of the check. */
  check: string;
  /** Severity: ok, warn, or error. */
  severity: CheckSeverity;
  /** Human-readable message. */
  message: string;
  /** Optional list of details (e.g. missing files, metacommand locations). */
  details?: string[];
}

export interface DoctorReport {
  /** All check results. */
  checks: CheckResult[];
  /** Summary counts. */
  summary: {
    ok: number;
    warn: number;
    error: number;
  };
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

export function parseDoctorArgs(args: ParsedArgs): DoctorOptions {
  const opts: DoctorOptions = {
    topDir: args.topDir ?? ".",
    planFile: args.planFile,
  };

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--format") {
      const val = rest[++i];
      if (val === "json" || val === "text") {
        opts.format = val;
      }
      i++;
      continue;
    }

    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// psql metacommand detection (reuses the same regex from preprocessor)
// ---------------------------------------------------------------------------

const METACOMMAND_RE = /^\\(?:[a-zA-Z_]\w*|!)(?:\b|\s|$).*$/;

/**
 * Scan a SQL file for psql metacommand lines.
 * Returns an array of "line N: <content>" strings for each metacommand found.
 */
function detectMetacommands(content: string): string[] {
  const lines = content.split("\n");
  const found: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const trimmed = lines[i]!.trimStart();
    if (METACOMMAND_RE.test(trimmed)) {
      found.push(`line ${i + 1}: ${trimmed.slice(0, 80)}`);
    }
  }

  return found;
}

// ---------------------------------------------------------------------------
// Individual checks
// ---------------------------------------------------------------------------

/**
 * Check 1: Can the plan file be parsed?
 */
function checkPlanParsing(planPath: string): CheckResult & { plan?: Plan } {
  if (!existsSync(planPath)) {
    return {
      check: "plan-file",
      severity: "error",
      message: `Plan file not found: ${planPath}`,
    };
  }

  try {
    const content = readFileSync(planPath, "utf-8");
    const plan = parsePlan(content);
    return {
      check: "plan-file",
      severity: "ok",
      message: `Plan file parsed successfully: ${plan.changes.length} change(s), ${plan.tags.length} tag(s).`,
      plan,
    };
  } catch (err) {
    const msg = err instanceof PlanParseError
      ? err.message
      : (err instanceof Error ? err.message : String(err));
    return {
      check: "plan-file",
      severity: "error",
      message: `Plan file parse error: ${msg}`,
    };
  }
}

/**
 * Check 2: Syntax version validation.
 */
function checkSyntaxVersion(plan: Plan): CheckResult {
  const version = plan.pragmas.get("syntax-version");

  if (version === undefined) {
    return {
      check: "syntax-version",
      severity: "warn",
      message: "No %syntax-version pragma found. Recommend adding %syntax-version=1.0.0.",
    };
  }

  const major = parseInt(version.split(".")[0] ?? "", 10);
  if (isNaN(major) || major !== 1) {
    return {
      check: "syntax-version",
      severity: "error",
      message: `Unsupported %syntax-version '${version}'. Only syntax-version 1.x is supported.`,
    };
  }

  return {
    check: "syntax-version",
    severity: "ok",
    message: `%syntax-version=${version} (supported).`,
  };
}

/**
 * Check 3: Change ID parent chain consistency.
 *
 * Verifies that each change's parent field correctly references
 * the preceding change's change_id.
 */
function checkChangeIdChain(plan: Plan): CheckResult {
  const issues: string[] = [];

  for (let i = 0; i < plan.changes.length; i++) {
    const change = plan.changes[i]!;

    if (i === 0) {
      if (change.parent !== undefined) {
        issues.push(`Change '${change.name}' is the first change but has a parent set.`);
      }
    } else {
      const expectedParent = plan.changes[i - 1]!.change_id;
      if (change.parent !== expectedParent) {
        issues.push(
          `Change '${change.name}' (index ${i}): expected parent ${expectedParent.slice(0, 8)}..., got ${(change.parent ?? "none").slice(0, 8)}...`,
        );
      }
    }
  }

  if (issues.length > 0) {
    return {
      check: "change-id-chain",
      severity: "error",
      message: `${issues.length} change ID chain issue(s) found.`,
      details: issues,
    };
  }

  return {
    check: "change-id-chain",
    severity: "ok",
    message: `Change ID parent chain is consistent across ${plan.changes.length} change(s).`,
  };
}

/**
 * Check 4: Script file presence.
 *
 * For each change, verifies that deploy and revert SQL files exist.
 * Verify scripts are optional (warn if missing).
 */
function checkScriptFiles(
  plan: Plan,
  topDir: string,
  deployDir: string,
  revertDir: string,
  verifyDir: string,
): CheckResult {
  const missingDeploy: string[] = [];
  const missingRevert: string[] = [];
  const missingVerify: string[] = [];

  // Deduplicate by change name (reworked changes share script names)
  const seen = new Set<string>();

  for (const change of plan.changes) {
    if (seen.has(change.name)) continue;
    seen.add(change.name);

    const deployPath = join(topDir, deployDir, `${change.name}.sql`);
    const revertPath = join(topDir, revertDir, `${change.name}.sql`);
    const verifyPath = join(topDir, verifyDir, `${change.name}.sql`);

    if (!existsSync(deployPath)) missingDeploy.push(change.name);
    if (!existsSync(revertPath)) missingRevert.push(change.name);
    if (!existsSync(verifyPath)) missingVerify.push(change.name);
  }

  const details: string[] = [];
  if (missingDeploy.length > 0) {
    details.push(`Missing deploy scripts (${missingDeploy.length}): ${missingDeploy.slice(0, 5).join(", ")}${missingDeploy.length > 5 ? `, ... and ${missingDeploy.length - 5} more` : ""}`);
  }
  if (missingRevert.length > 0) {
    details.push(`Missing revert scripts (${missingRevert.length}): ${missingRevert.slice(0, 5).join(", ")}${missingRevert.length > 5 ? `, ... and ${missingRevert.length - 5} more` : ""}`);
  }
  if (missingVerify.length > 0) {
    details.push(`Missing verify scripts (${missingVerify.length}): ${missingVerify.slice(0, 5).join(", ")}${missingVerify.length > 5 ? `, ... and ${missingVerify.length - 5} more` : ""}`);
  }

  if (missingDeploy.length > 0 || missingRevert.length > 0) {
    return {
      check: "script-files",
      severity: "error",
      message: `Missing deploy/revert script files detected.`,
      details,
    };
  }

  if (missingVerify.length > 0) {
    return {
      check: "script-files",
      severity: "warn",
      message: `All deploy/revert scripts present. ${missingVerify.length} verify script(s) missing.`,
      details,
    };
  }

  return {
    check: "script-files",
    severity: "ok",
    message: `All script files present for ${seen.size} unique change(s).`,
  };
}

/**
 * Check 5: psql metacommand usage in deploy/revert scripts.
 *
 * Detects psql-specific commands (\i, \set, \echo, etc.) that may cause
 * compatibility issues or require psql as the execution client.
 */
function checkPsqlMetacommands(
  plan: Plan,
  topDir: string,
  deployDir: string,
  revertDir: string,
): CheckResult {
  const findings: string[] = [];
  const seen = new Set<string>();

  for (const change of plan.changes) {
    if (seen.has(change.name)) continue;
    seen.add(change.name);

    for (const [dir, label] of [[deployDir, "deploy"], [revertDir, "revert"]] as const) {
      const scriptPath = join(topDir, dir, `${change.name}.sql`);
      if (!existsSync(scriptPath)) continue;

      try {
        const content = readFileSync(scriptPath, "utf-8");
        const metacmds = detectMetacommands(content);
        if (metacmds.length > 0) {
          findings.push(`${label}/${change.name}.sql: ${metacmds.length} metacommand(s)`);
          for (const m of metacmds.slice(0, 3)) {
            findings.push(`  ${m}`);
          }
          if (metacmds.length > 3) {
            findings.push(`  ... and ${metacmds.length - 3} more`);
          }
        }
      } catch {
        // Skip unreadable files
      }
    }
  }

  if (findings.length > 0) {
    return {
      check: "psql-metacommands",
      severity: "warn",
      message: `psql metacommands detected in script files. These require psql as the execution client.`,
      details: findings,
    };
  }

  return {
    check: "psql-metacommands",
    severity: "ok",
    message: "No psql metacommands detected in deploy/revert scripts.",
  };
}

// ---------------------------------------------------------------------------
// Main doctor logic
// ---------------------------------------------------------------------------

/**
 * Run all doctor checks and produce a report.
 */
export function runDoctorChecks(opts: DoctorOptions): DoctorReport {
  const topDir = resolve(opts.topDir);
  const checks: CheckResult[] = [];

  // Load config to get directory settings
  let deployDir = "deploy";
  let revertDir = "revert";
  let verifyDir = "verify";
  let planFile = opts.planFile ?? "sqitch.plan";

  try {
    const config = loadConfig(topDir);
    deployDir = config.core.deploy_dir;
    revertDir = config.core.revert_dir;
    verifyDir = config.core.verify_dir;
    if (!opts.planFile) {
      planFile = config.core.plan_file;
    }
  } catch {
    // Config loading may fail (no sqitch.conf). Use defaults.
    verbose("Could not load config, using defaults.");
  }

  const planPath = resolve(topDir, planFile);

  // Check 1: Plan file parsing
  const planResult = checkPlanParsing(planPath);
  checks.push({
    check: planResult.check,
    severity: planResult.severity,
    message: planResult.message,
    ...(planResult.details ? { details: planResult.details } : {}),
  });

  // Remaining checks require a successfully parsed plan
  if (planResult.plan) {
    const plan = planResult.plan;

    // Check 2: Syntax version
    checks.push(checkSyntaxVersion(plan));

    // Check 3: Change ID chain
    checks.push(checkChangeIdChain(plan));

    // Check 4: Script files
    checks.push(checkScriptFiles(plan, topDir, deployDir, revertDir, verifyDir));

    // Check 5: psql metacommands
    checks.push(checkPsqlMetacommands(plan, topDir, deployDir, revertDir));
  }

  // Build summary
  const summary = { ok: 0, warn: 0, error: 0 };
  for (const c of checks) {
    summary[c.severity]++;
  }

  return { checks, summary };
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

const SEVERITY_SYMBOLS: Record<CheckSeverity, string> = {
  ok: "[ok]",
  warn: "[warn]",
  error: "[ERROR]",
};

function printReportText(report: DoctorReport): void {
  info("sqlever doctor\n");

  for (const check of report.checks) {
    const symbol = SEVERITY_SYMBOLS[check.severity];
    info(`  ${symbol} ${check.check}: ${check.message}`);
    if (check.details) {
      for (const detail of check.details) {
        info(`    ${detail}`);
      }
    }
  }

  info("");
  const { ok, warn, error: errCount } = report.summary;
  const parts: string[] = [];
  if (ok > 0) parts.push(`${ok} passed`);
  if (warn > 0) parts.push(`${warn} warning(s)`);
  if (errCount > 0) parts.push(`${errCount} error(s)`);
  info(`Summary: ${parts.join(", ")}`);
}

function printReportJson(report: DoctorReport): void {
  jsonOut(report);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/**
 * Execute the `doctor` command.
 *
 * @returns exit code: 0 if all checks pass (no errors), 1 if any errors.
 */
export function runDoctor(args: ParsedArgs): number {
  const opts = parseDoctorArgs(args);
  const report = runDoctorChecks(opts);

  const config = getConfig();
  const format = opts.format ?? config.format;

  if (format === "json") {
    printReportJson(report);
  } else {
    printReportText(report);
  }

  return report.summary.error > 0 ? 1 : 0;
}
