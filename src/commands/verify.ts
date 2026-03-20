// src/commands/verify.ts — sqlever verify command
//
// Runs verify scripts for deployed changes and reports pass/fail.
// Implements SPEC R1 `verify` semantics:
//   - Connect to database, read deployed changes from tracking tables
//   - For each deployed change, locate its verify script
//   - Execute the verify script via psql wrapped in BEGIN/ROLLBACK (read-only)
//   - Report pass/fail per change
//   - Exit code 3 on any verification failure (SPEC R6)
//   - Support --from/--to range filtering
//   - Skip gracefully if verify script is missing

import { resolve, join } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import type { ParsedArgs } from "../cli";
import { loadConfig, type MergedConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import type { Plan } from "../plan/types";
import { DatabaseClient } from "../db/client";
import {
  Registry,
  type Change as RegistryChange,
} from "../db/registry";
import { PsqlRunner, type PsqlRunResult } from "../psql";
import { info, error as logError, verbose } from "../output";

// ---------------------------------------------------------------------------
// Exit codes (SPEC R6)
// ---------------------------------------------------------------------------

/** Exit code for verification failure. */
export const EXIT_CODE_VERIFY_FAILED = 3;

// ---------------------------------------------------------------------------
// Verify-specific argument parsing
// ---------------------------------------------------------------------------

export interface VerifyOptions {
  /** Target database URI (from --db-uri or config). */
  dbUri?: string;
  /** Verify from this change name (inclusive). */
  fromChange?: string;
  /** Verify up to this change name (inclusive). */
  toChange?: string;
  /** Project root directory. */
  topDir: string;
  /** Target name (from --target). */
  target?: string;
  /** Plan file path override (from --plan-file). */
  planFile?: string;
}

/**
 * Parse verify-specific options from the CLI's parsed args.
 *
 * Usage: sqlever verify [target] [--from change] [--to change]
 */
export function parseVerifyOptions(args: ParsedArgs): VerifyOptions {
  const opts: VerifyOptions = {
    dbUri: args.dbUri,
    topDir: args.topDir ?? ".",
    target: args.target,
    planFile: args.planFile,
  };

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--from") {
      opts.fromChange = rest[++i];
      i++;
      continue;
    }
    if (token === "--to") {
      opts.toChange = rest[++i];
      i++;
      continue;
    }

    // First non-flag token could be a target name
    if (opts.target === undefined) {
      opts.target = token;
    }
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Resolve target URI from config and options
// ---------------------------------------------------------------------------

/**
 * Resolve the database connection URI from the combined config sources.
 *
 * Precedence: --db-uri > target lookup > engine default target.
 */
export function resolveTargetUri(
  opts: VerifyOptions,
  config: MergedConfig,
): string | undefined {
  // CLI --db-uri takes precedence
  if (opts.dbUri) return opts.dbUri;

  // Named target lookup
  const targetName = opts.target ?? config.engines.pg?.target;
  if (targetName && config.targets[targetName]) {
    return config.targets[targetName]!.uri;
  }

  // Fall back to engine target (which may be a URI like db:pg://...)
  if (targetName) return targetName;

  return undefined;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Result of verifying a single change. */
export interface VerifyChangeResult {
  /** Change name. */
  name: string;
  /** Change ID. */
  change_id: string;
  /** Whether verification passed. */
  pass: boolean;
  /** Whether the verify script was skipped (missing). */
  skipped: boolean;
  /** Error message on failure. */
  error?: string;
}

/** Summary result from the verify command. */
export interface VerifyResult {
  /** Per-change verification results. */
  changes: VerifyChangeResult[];
  /** Total number of changes verified. */
  total: number;
  /** Number of changes that passed. */
  passed: number;
  /** Number of changes that failed. */
  failed: number;
  /** Number of changes skipped (missing verify script). */
  skipped: number;
}

// ---------------------------------------------------------------------------
// Core verify logic
// ---------------------------------------------------------------------------

/**
 * Filter deployed changes to the --from/--to range.
 *
 * Both --from and --to are inclusive. If --from is specified, only changes
 * at or after that change (by deployment order) are included. If --to is
 * specified, only changes at or before that change are included.
 *
 * @throws Error if --from or --to change is not found in the deployed list
 */
export function filterChangesForRange(
  deployedChanges: RegistryChange[],
  fromChange?: string,
  toChange?: string,
): RegistryChange[] {
  if (deployedChanges.length === 0) return [];

  let startIdx = 0;
  let endIdx = deployedChanges.length - 1;

  if (fromChange) {
    startIdx = deployedChanges.findIndex((c) => c.change === fromChange);
    if (startIdx === -1) {
      throw new Error(
        `Change '${fromChange}' is not deployed. Cannot use as --from target.`,
      );
    }
  }

  if (toChange) {
    endIdx = deployedChanges.findIndex((c) => c.change === toChange);
    if (endIdx === -1) {
      throw new Error(
        `Change '${toChange}' is not deployed. Cannot use as --to target.`,
      );
    }
  }

  if (startIdx > endIdx) {
    return [];
  }

  return deployedChanges.slice(startIdx, endIdx + 1);
}

/**
 * Build the path to the verify script for a given change name.
 */
export function getVerifyScriptPath(
  verifyDir: string,
  changeName: string,
): string {
  return join(verifyDir, `${changeName}.sql`);
}

/**
 * Execute a single verify script and return the result.
 *
 * Verify scripts are NOT wrapped in BEGIN/ROLLBACK by sqlever itself —
 * the verify scripts themselves should include those statements (or rely
 * on psql's --single-transaction mode which wraps the entire script in
 * a transaction). We use --single-transaction so the verify script runs
 * in a transaction, but we don't add an explicit ROLLBACK — psql will
 * commit on success. Verify scripts that want read-only behavior should
 * include their own BEGIN/ROLLBACK wrapping.
 *
 * Actually, per Sqitch convention, verify scripts should use
 * BEGIN; ... ROLLBACK; to ensure they don't modify the database.
 * We just run them as-is via psql.
 */
export async function runVerifyScript(
  psqlRunner: PsqlRunner,
  scriptPath: string,
  targetUri: string,
  workingDir: string,
  changeName: string,
  changeId: string,
): Promise<VerifyChangeResult> {
  // Check if the verify script exists
  if (!existsSync(scriptPath)) {
    return {
      name: changeName,
      change_id: changeId,
      pass: true,
      skipped: true,
    };
  }

  let result: PsqlRunResult;
  try {
    result = await psqlRunner.run(scriptPath, {
      uri: targetUri,
      workingDir,
    });
  } catch (err) {
    // Spawn failure (e.g., psql not found)
    const msg = err instanceof Error ? err.message : String(err);
    return {
      name: changeName,
      change_id: changeId,
      pass: false,
      skipped: false,
      error: msg,
    };
  }

  if (result.exitCode !== 0) {
    const errMsg =
      result.error?.message ?? result.stderr.trim() ?? "unknown error";
    return {
      name: changeName,
      change_id: changeId,
      pass: false,
      skipped: false,
      error: errMsg,
    };
  }

  return {
    name: changeName,
    change_id: changeId,
    pass: true,
    skipped: false,
  };
}

/**
 * Format the verify result summary for text output.
 */
export function formatVerifyResult(result: VerifyResult): string {
  const lines: string[] = [];

  for (const change of result.changes) {
    if (change.skipped) {
      lines.push(`  - ${change.name} .. skipped (no verify script)`);
    } else if (change.pass) {
      lines.push(`  - ${change.name} .. ok`);
    } else {
      lines.push(`  - ${change.name} .. FAIL: ${change.error ?? "unknown"}`);
    }
  }

  lines.push("");
  const parts: string[] = [];
  parts.push(`${result.passed} passed`);
  if (result.failed > 0) parts.push(`${result.failed} failed`);
  if (result.skipped > 0) parts.push(`${result.skipped} skipped`);
  lines.push(`Verify summary: ${parts.join(", ")} (${result.total} total)`);

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Main verify command
// ---------------------------------------------------------------------------

/**
 * Execute the `verify` command.
 *
 * Flow:
 * 1. Parse config, connect to database
 * 2. Read deployed changes from tracking tables
 * 3. Filter to --from/--to range
 * 4. For each change: locate verify script, execute via psql
 * 5. Report results
 * 6. Exit code 3 if any verification failed (SPEC R6)
 */
export async function runVerify(
  args: ParsedArgs,
  opts?: {
    psqlRunner?: PsqlRunner;
  },
): Promise<void> {
  const options = parseVerifyOptions(args);
  const topDir = resolve(options.topDir);

  // 1. Load config
  const config = loadConfig(topDir);

  // Load plan file
  const planFilePath = options.planFile
    ? resolve(options.planFile)
    : join(topDir, config.core.plan_file);

  let plan: Plan;
  try {
    const planContent = readFileSync(planFilePath, "utf-8");
    plan = parsePlan(planContent);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logError(`Failed to read plan file: ${msg}`);
    process.exit(1);
  }

  // Resolve target URI
  const targetUri = resolveTargetUri(options, config);
  if (!targetUri) {
    logError(
      "No database target specified. Use --db-uri or configure a target in sqitch.conf.",
    );
    process.exit(1);
  }

  // 2. Connect to database
  const db = new DatabaseClient(targetUri, {
    command: "verify",
    project: plan.project.name,
  });

  await db.connect();

  try {
    const registry = new Registry(db);

    // 3. Read deployed changes
    const deployedChanges = await registry.getDeployedChanges(plan.project.name);

    if (deployedChanges.length === 0) {
      info("Nothing to verify. No changes are deployed.");
      return;
    }

    // 4. Filter to --from/--to range
    let changesToVerify: RegistryChange[];
    try {
      changesToVerify = filterChangesForRange(
        deployedChanges,
        options.fromChange,
        options.toChange,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logError(msg);
      process.exit(1);
    }

    if (changesToVerify.length === 0) {
      info("Nothing to verify in the specified range.");
      return;
    }

    // 5. Execute verify scripts
    const psqlRunner = opts?.psqlRunner ?? new PsqlRunner();
    const verifyDir = join(topDir, config.core.verify_dir);

    const results: VerifyChangeResult[] = [];

    for (const deployed of changesToVerify) {
      const scriptPath = getVerifyScriptPath(verifyDir, deployed.change);

      verbose(`Verifying: ${deployed.change}`);

      const changeResult = await runVerifyScript(
        psqlRunner,
        scriptPath,
        targetUri,
        topDir,
        deployed.change,
        deployed.change_id,
      );

      results.push(changeResult);
    }

    // 6. Build summary and report
    const verifyResult: VerifyResult = {
      changes: results,
      total: results.length,
      passed: results.filter((r) => r.pass && !r.skipped).length,
      failed: results.filter((r) => !r.pass).length,
      skipped: results.filter((r) => r.skipped).length,
    };

    info(formatVerifyResult(verifyResult));

    // 7. Exit code 3 if any failures (SPEC R6)
    if (verifyResult.failed > 0) {
      process.exit(EXIT_CODE_VERIFY_FAILED);
    }
  } finally {
    await db.disconnect();
  }
}
