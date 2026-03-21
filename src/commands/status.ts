// src/commands/status.ts — sqlever status command
//
// Shows deployment status: pending count, deployed count, last deployed
// change, target info, and modified script detection (script_hash comparison).
//
// Supports --format json for machine-readable output.
//
// Implements S4-2 (GitHub issue #44).

import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { loadConfig, type MergedConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import { computeScriptHash, type Plan } from "../plan/types";
import type { Change as RegistryChange } from "../db/registry";
import { info, error, json as jsonOut } from "../output";
import type { ParsedArgs } from "../cli";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A script whose on-disk hash no longer matches the deployed script_hash. */
export interface ModifiedScript {
  /** Change name. */
  change: string;
  /** Change ID. */
  change_id: string;
  /** Hash stored in the registry (from deployment time). */
  registry_hash: string;
  /** Current hash of the on-disk deploy script. */
  current_hash: string;
}

/** Expand/contract operation status for the status command. */
export interface ExpandContractStatus {
  /** Base change name (e.g., "rename_users_name"). */
  change_name: string;
  /** Current phase: expanding, expanded, contracting, completed. */
  phase: string;
  /** Schema-qualified table name. */
  table: string;
  /** When the operation started. */
  started_at: string;
  /** Who started the operation. */
  started_by: string;
}

/** Full status result used for both text and JSON output. */
export interface StatusResult {
  /** Project name from the plan. */
  project: string;
  /** Target URI or name (if available). */
  target: string | null;
  /** Number of changes already deployed. */
  deployed_count: number;
  /** Number of changes in the plan not yet deployed. */
  pending_count: number;
  /** Names of pending changes, in plan order. */
  pending_changes: string[];
  /** Last deployed change info, or null if none deployed. */
  last_deployed: {
    change: string;
    change_id: string;
    committed_at: string;
    committer_name: string;
  } | null;
  /** Scripts modified since deployment (script_hash mismatch). */
  modified_scripts: ModifiedScript[];
  /** Active expand/contract operations (non-completed). */
  expand_contract_operations: ExpandContractStatus[];
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface StatusOptions {
  /** Project root directory. */
  topDir: string;
  /** Format: "text" or "json". */
  format: "text" | "json";
  /** Database URI override. */
  dbUri?: string;
  /** Target name override. */
  target?: string;
  /** Plan file override. */
  planFile?: string;
}

/**
 * Parse status-specific options from CLI parsed args.
 */
export function parseStatusOptions(args: ParsedArgs): StatusOptions {
  return {
    topDir: args.topDir ?? ".",
    format: args.format,
    dbUri: args.dbUri,
    target: args.target,
    planFile: args.planFile,
  };
}

// ---------------------------------------------------------------------------
// Core logic (pure, testable)
// ---------------------------------------------------------------------------

/**
 * Resolve the target URI string from config and CLI overrides.
 *
 * Priority:
 *   1. --db-uri flag
 *   2. --target flag => look up in config targets
 *   3. Default engine target from config
 *   4. null (no target configured)
 */
export function resolveTargetUri(
  config: MergedConfig,
  dbUri?: string,
  targetName?: string,
): string | null {
  // 1. Explicit --db-uri
  if (dbUri) return dbUri;

  // 2. Explicit --target => look up in config
  if (targetName) {
    const t = config.targets[targetName];
    if (t?.uri) return t.uri;
    // Target name might itself be a URI
    if (targetName.includes("://")) return targetName;
    return null;
  }

  // 3. Default engine target
  const engineName = config.core.engine;
  if (engineName) {
    const engine = config.engines[engineName];
    if (engine?.target) {
      // engine.target could be a target name or URI
      const t = config.targets[engine.target];
      if (t?.uri) return t.uri;
      if (engine.target.includes("://")) return engine.target;
    }
  }

  return null;
}

/**
 * Compute deployment status by comparing a plan against deployed changes.
 *
 * This is the pure, side-effect-free core of the status command.
 * It takes already-loaded data and returns a StatusResult.
 */
export function computeStatus(
  plan: Plan,
  deployedChanges: RegistryChange[],
  targetUri: string | null,
  deployDir: string,
  ecOperations: ExpandContractStatus[] = [],
): StatusResult {
  const deployedIds = new Set(deployedChanges.map((c) => c.change_id));
  const deployedMap = new Map(
    deployedChanges.map((c) => [c.change_id, c]),
  );

  // Pending = plan changes not yet deployed
  const pendingPlanChanges = plan.changes.filter(
    (c) => !deployedIds.has(c.change_id),
  );

  // Last deployed change = most recent committed_at among deployed
  let lastDeployed: StatusResult["last_deployed"] = null;
  if (deployedChanges.length > 0) {
    const last = deployedChanges[deployedChanges.length - 1]!;
    lastDeployed = {
      change: last.change,
      change_id: last.change_id,
      committed_at:
        last.committed_at instanceof Date
          ? last.committed_at.toISOString()
          : String(last.committed_at),
      committer_name: last.committer_name,
    };
  }

  // Modified script detection: compare script_hash for deployed changes
  const modifiedScripts: ModifiedScript[] = [];
  for (const planChange of plan.changes) {
    const deployed = deployedMap.get(planChange.change_id);
    if (!deployed || !deployed.script_hash) continue;

    const scriptPath = join(deployDir, `${planChange.name}.sql`);
    if (!existsSync(scriptPath)) continue;

    try {
      const currentHash = computeScriptHash(scriptPath);
      if (currentHash !== deployed.script_hash) {
        modifiedScripts.push({
          change: planChange.name,
          change_id: planChange.change_id,
          registry_hash: deployed.script_hash,
          current_hash: currentHash,
        });
      }
    } catch {
      // Can't read file — skip
    }
  }

  return {
    project: plan.project.name,
    target: targetUri,
    deployed_count: deployedChanges.length,
    pending_count: pendingPlanChanges.length,
    pending_changes: pendingPlanChanges.map((c) => c.name),
    last_deployed: lastDeployed,
    modified_scripts: modifiedScripts,
    expand_contract_operations: ecOperations,
  };
}

// ---------------------------------------------------------------------------
// Text formatting
// ---------------------------------------------------------------------------

/**
 * Format a StatusResult as human-readable text lines.
 */
export function formatStatusText(result: StatusResult): string {
  const lines: string[] = [];

  lines.push(`# Project: ${result.project}`);

  if (result.target) {
    lines.push(`# Target:  ${result.target}`);
  }

  lines.push("");

  if (result.last_deployed) {
    lines.push(`# Last deployed change:`);
    lines.push(`#   ${result.last_deployed.change}`);
    lines.push(`#   deployed at: ${result.last_deployed.committed_at}`);
    lines.push(`#   by: ${result.last_deployed.committer_name}`);
    lines.push("");
  }

  lines.push(`Deployed: ${result.deployed_count}`);
  lines.push(`Pending:  ${result.pending_count}`);

  if (result.pending_changes.length > 0) {
    lines.push("");
    lines.push("Pending changes:");
    for (const name of result.pending_changes) {
      lines.push(`  * ${name}`);
    }
  }

  if (result.modified_scripts.length > 0) {
    lines.push("");
    lines.push("Modified scripts (hash mismatch):");
    for (const mod of result.modified_scripts) {
      lines.push(`  ! ${mod.change}`);
    }
  }

  if (result.expand_contract_operations.length > 0) {
    lines.push("");
    lines.push("Expand/contract operations:");
    for (const op of result.expand_contract_operations) {
      lines.push(`  ~ ${op.change_name} [${op.phase}] on ${op.table}`);
    }
  }

  if (
    result.pending_count === 0 &&
    result.modified_scripts.length === 0 &&
    result.expand_contract_operations.length === 0
  ) {
    lines.push("");
    lines.push("Nothing to deploy. Everything is up-to-date.");
  }

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Main command runner (side-effectful: reads files, connects to DB)
// ---------------------------------------------------------------------------

/**
 * Run the status command.
 *
 * Loads plan from disk, connects to the database to read deployed changes,
 * then computes and prints the status.
 */
export async function runStatus(args: ParsedArgs): Promise<void> {
  const options = parseStatusOptions(args);
  const topDir = resolve(options.topDir);

  // Load config
  const config = loadConfig(topDir);

  // Resolve target
  const targetUri = resolveTargetUri(config, options.dbUri, options.target);

  // Read the plan file
  const planFilePath = options.planFile
    ? resolve(options.planFile)
    : join(topDir, config.core.plan_file);

  if (!existsSync(planFilePath)) {
    error(`Plan file not found: ${planFilePath}`);
    error("Run 'sqlever init' to initialize a project.");
    process.exit(1);
  }

  const planContent = readFileSync(planFilePath, "utf-8");
  const plan = parsePlan(planContent);

  // If no target URI, show status without DB info (plan-only mode)
  if (!targetUri) {
    const result = computeStatus(plan, [], null, join(topDir, config.core.deploy_dir));
    if (options.format === "json") {
      jsonOut(result);
    } else {
      info(formatStatusText(result));
    }
    return;
  }

  // Connect to database and read deployed changes
  const { DatabaseClient } = await import("../db/client");
  const { Registry } = await import("../db/registry");
  const { ExpandContractTracker } = await import("../expand-contract/tracker");

  const client = new DatabaseClient(targetUri, {
    command: "status",
    project: plan.project.name,
  });
  await client.connect();

  try {
    const registry = new Registry(client);
    const deployedChanges = await registry.getDeployedChanges(plan.project.name);

    // Query expand/contract state (best-effort — table may not exist)
    let ecOperations: ExpandContractStatus[] = [];
    try {
      const tracker = new ExpandContractTracker(client);
      const activeOps = await tracker.listActiveOperations(plan.project.name);
      ecOperations = activeOps.map((op) => ({
        change_name: op.change_name,
        phase: op.phase,
        table: `${op.table_schema}.${op.table_name}`,
        started_at: op.started_at instanceof Date
          ? op.started_at.toISOString()
          : String(op.started_at),
        started_by: op.started_by,
      }));
    } catch {
      // expand_contract_state table may not exist yet — that's fine
    }

    const deployDir = join(topDir, config.core.deploy_dir);
    const result = computeStatus(plan, deployedChanges, targetUri, deployDir, ecOperations);

    if (options.format === "json") {
      jsonOut(result);
    } else {
      info(formatStatusText(result));
    }
  } finally {
    await client.disconnect();
  }
}
