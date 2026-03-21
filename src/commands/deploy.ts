// src/commands/deploy.ts — sqlever deploy command
//
// The core command: executes pending migration scripts against a PostgreSQL
// database, tracking state in sqitch.* registry tables.
//
// See SPEC.md Section 7 (Data flow — deploy), DD12, DD13, DD14.

import { readFileSync, existsSync } from "node:fs";
import { join, resolve } from "node:path";
import type { ParsedArgs } from "../cli";
import { loadConfig, type MergedConfig } from "../config/index";
import { DatabaseClient } from "../db/client";
import { Registry, type RecordDeployInput } from "../db/registry";
import { parsePlan } from "../plan/parser";
import { topologicalSort, filterPending, filterToTarget, validateDependencies } from "../plan/sort";
import { computeScriptHash } from "../plan/types";
import type { Change, Plan, Tag } from "../plan/types";
import { PsqlRunner, type PsqlRunResult } from "../psql";
import { shouldSetLockTimeout } from "../lock-guard";
import { resolveDeployIncludes } from "../includes/snapshot";
import { shutdownManager } from "../signals";
import { info, error as logError, verbose, getConfig } from "../output";
import { sqitchToStandard } from "../db/uri";
import { DeployProgress, shouldUseTUI } from "../tui/deploy";

// ---------------------------------------------------------------------------
// Exit codes (SPEC R6)
// ---------------------------------------------------------------------------

export const EXIT_DEPLOY_FAILED = 1;
export const EXIT_CONCURRENT_DEPLOY = 4;
export const EXIT_LOCK_TIMEOUT = 5;
export const EXIT_DB_UNREACHABLE = 10;

// ---------------------------------------------------------------------------
// Advisory lock
// ---------------------------------------------------------------------------

/**
 * Namespace constant for the two-argument advisory lock form.
 * Stable across PG versions (application-defined, not hashtext).
 * ASCII bytes of "sqlv" = 0x73716C76.
 */
export const ADVISORY_LOCK_NAMESPACE = 0x7371_6C76;

/**
 * Compute a stable 32-bit integer hash of a project name for use as
 * the second argument to pg_advisory_lock(namespace, key).
 *
 * Uses a simple DJB2-style hash. The result is always positive 32-bit
 * so it fits in PostgreSQL's int4 argument.
 */
export function projectLockKey(projectName: string): number {
  let hash = 5381;
  for (let i = 0; i < projectName.length; i++) {
    // hash * 33 + charCode, keep within 32-bit signed range
    hash = ((hash << 5) + hash + projectName.charCodeAt(i)) | 0;
  }
  // Ensure positive value for pg_advisory_lock int4 argument
  return hash & 0x7fff_ffff;
}

// ---------------------------------------------------------------------------
// Deploy options
// ---------------------------------------------------------------------------

export interface DeployOptions {
  /** Deploy up to and including this change name. */
  to?: string;
  /** Transaction scope: change (default), all, or tag. */
  mode: "change" | "all" | "tag";
  /** Print what would be deployed, make no changes. */
  dryRun: boolean;
  /** Run verify scripts after each change. */
  verify: boolean;
  /** Key-value pairs passed as psql -v variables. */
  variables: Record<string, string>;
  /** Database connection URI. */
  dbUri?: string;
  /** Named target from config. */
  target?: string;
  /** Path to the psql binary. */
  dbClient?: string;
  /** Lock timeout in milliseconds for deploy scripts. */
  lockTimeout?: number;
  /** Project directory. */
  projectDir: string;
  /** Committer name (from git config or env). */
  committerName: string;
  /** Committer email (from git config or env). */
  committerEmail: string;
  /** Disable TUI dashboard even when stdout is a TTY. */
  noTui: boolean;
  /** Skip snapshot include resolution; use HEAD/current files (Sqitch-compatible). */
  noSnapshot: boolean;
}

/**
 * Parse deploy-specific options from CLI args.
 */
export function parseDeployOptions(args: ParsedArgs): DeployOptions {
  let to: string | undefined;
  let mode: "change" | "all" | "tag" = "change";
  let dryRun = false;
  let verify: boolean | undefined;
  let dbClient: string | undefined;
  let lockTimeout: number | undefined;
  let noTui = false;
  let noSnapshot = false;
  const variables: Record<string, string> = {};

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--to") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--to requires a change name");
      }
      to = val;
      i++;
      continue;
    }
    if (token === "--mode") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--mode requires a value (change, all, or tag)");
      }
      if (val !== "change" && val !== "all" && val !== "tag") {
        throw new Error(`Unknown mode: ${val}. Must be one of: change, all, tag`);
      }
      if (val !== "change") {
        throw new Error(`--mode ${val} is not yet implemented. Only --mode change is supported.`);
      }
      mode = val;
      i++;
      continue;
    }
    if (token === "--dry-run") {
      dryRun = true;
      i++;
      continue;
    }
    if (token === "--verify") {
      verify = true;
      i++;
      continue;
    }
    if (token === "--no-verify") {
      verify = false;
      i++;
      continue;
    }
    if (token === "--set") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--set requires a key=value argument");
      }
      const eqIdx = val.indexOf("=");
      if (eqIdx !== -1) {
        variables[val.slice(0, eqIdx)] = val.slice(eqIdx + 1);
      }
      i++;
      continue;
    }
    if (token === "--db-client" || token === "--client") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--db-client requires a path to the psql binary");
      }
      dbClient = val;
      i++;
      continue;
    }
    if (token === "--lock-timeout") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--lock-timeout requires a value in milliseconds");
      }
      lockTimeout = parseInt(val, 10);
      i++;
      continue;
    }
    if (token === "--no-tui") {
      noTui = true;
      i++;
      continue;
    }
    if (token === "--no-snapshot") {
      noSnapshot = true;
      i++;
      continue;
    }
    // Positional: treat as target name
    if (!args.target && !args.dbUri) {
      // Could be a target name or URI
      args.target = token;
    }
    i++;
  }

  // Load merged config to get defaults
  const projectDir = args.topDir ?? ".";
  const config = loadConfig(projectDir);

  // Resolve verify: CLI flag > config
  if (verify === undefined) {
    verify = config.deploy.verify;
  }

  // Resolve mode: CLI flag already set above, but check config if still default
  // (mode was already set from CLI --mode, config was handled during loadConfig)

  // Resolve DB URI from: --db-uri flag > --target flag lookup > config engine target
  let dbUri = args.dbUri;
  if (!dbUri) {
    const targetName = args.target;
    if (targetName) {
      // Look up target in config
      const targetConfig = config.targets[targetName];
      if (targetConfig?.uri) {
        dbUri = targetConfig.uri;
      } else {
        // Maybe it's a URI directly
        if (targetName.includes("://")) {
          dbUri = targetName;
        }
      }
    }
    if (!dbUri) {
      // Fall back to engine target
      const engineName = config.core.engine ?? "pg";
      const engineConfig = config.engines[engineName];
      if (engineConfig?.target) {
        const targetRef = engineConfig.target;
        // Could be a target name or a URI
        if (targetRef.includes("://")) {
          dbUri = targetRef;
        } else {
          const t = config.targets[targetRef];
          if (t?.uri) dbUri = t.uri;
        }
      }
    }
  }

  // Resolve psql client
  if (!dbClient) {
    const engineName = config.core.engine ?? "pg";
    const engineConfig = config.engines[engineName];
    dbClient = engineConfig?.client;
  }

  // Committer info: env > git config fallback
  const committerName = process.env.SQITCH_FULLNAME
    ?? process.env.USER
    ?? "sqlever";
  const committerEmail = process.env.SQITCH_EMAIL
    ?? process.env.EMAIL
    ?? "sqlever@localhost";

  return {
    to,
    mode,
    dryRun,
    verify,
    variables,
    dbUri,
    target: args.target,
    dbClient,
    lockTimeout,
    projectDir,
    committerName,
    committerEmail,
    noTui,
    noSnapshot,
  };
}

// ---------------------------------------------------------------------------
// Script helpers
// ---------------------------------------------------------------------------

/**
 * Check if a deploy script is marked as non-transactional.
 * Looks for `-- sqlever:no-transaction` on the first line.
 */
export function isNonTransactional(scriptContent: string): boolean {
  const firstLine = scriptContent.split("\n")[0] ?? "";
  return /--\s*sqlever:no-transaction/i.test(firstLine);
}

/**
 * Resolve the path to a deploy/verify script.
 */
function scriptPath(
  topDir: string,
  dir: string,
  changeName: string,
): string {
  return join(resolve(topDir), dir, `${changeName}.sql`);
}

// ---------------------------------------------------------------------------
// Deploy result
// ---------------------------------------------------------------------------

export interface DeployResult {
  /** Total changes deployed. */
  deployed: number;
  /** Total changes skipped (already deployed). */
  skipped: number;
  /** The change that failed, if any. */
  failedChange?: string;
  /** Error message if deploy failed. */
  error?: string;
  /** Whether this was a dry-run. */
  dryRun: boolean;
}

// ---------------------------------------------------------------------------
// Core deploy logic (testable, receives dependencies)
// ---------------------------------------------------------------------------

export interface DeployDeps {
  db: DatabaseClient;
  registry: Registry;
  psqlRunner: PsqlRunner;
  config: MergedConfig;
  shutdownMgr: typeof shutdownManager;
}

/**
 * Execute the deploy workflow.
 *
 * This is the pure logic, separated from I/O setup so it can be unit-tested
 * with mocked dependencies.
 */
export async function executeDeploy(
  options: DeployOptions,
  deps: DeployDeps,
): Promise<DeployResult> {
  const { db, registry, psqlRunner, config, shutdownMgr } = deps;
  const topDir = resolve(options.projectDir);
  const deployDir = config.core.deploy_dir;
  const verifyDir = config.core.verify_dir;
  const planFilePath = join(topDir, config.core.plan_file);

  // 1. Parse plan file
  if (!existsSync(planFilePath)) {
    return { deployed: 0, skipped: 0, dryRun: options.dryRun, error: `Plan file not found: ${planFilePath}` };
  }
  const planContent = readFileSync(planFilePath, "utf-8");
  const plan = parsePlan(planContent);
  const projectName = plan.project.name;

  // 2. Resolve DB URI
  const dbUri = options.dbUri;
  if (!dbUri) {
    return { deployed: 0, skipped: 0, dryRun: options.dryRun, error: "No database URI specified. Use --db-uri or configure a target." };
  }

  // Convert to standard URI for psql
  const standardUri = sqitchToStandard(dbUri);

  // 2a. Compute plan-level pending changes for dry-run (no DB needed)
  let allChanges = plan.changes;
  if (options.to) {
    allChanges = filterToTarget(allChanges, options.to);
  }

  // Dry-run: show what would be deployed without touching the database.
  // Per spec, --dry-run makes zero DB changes.
  if (options.dryRun) {
    const sortedChanges = topologicalSort(allChanges);
    info(`Dry-run: ${sortedChanges.length} change(s) would be deployed:`);
    for (const change of sortedChanges) {
      const deployPath = scriptPath(topDir, deployDir, change.name);
      const noTxn = existsSync(deployPath) && isNonTransactional(readFileSync(deployPath, "utf-8"));
      const marker = noTxn ? " [no-transaction]" : "";
      info(`  + ${change.name}${marker}`);
    }
    return { deployed: 0, skipped: 0, dryRun: true };
  }

  // 3. Connect to database
  await db.connect();

  // 4. Acquire advisory lock
  const lockKey = projectLockKey(projectName);
  let lockAcquired = false;

  try {
    const lockResult = await db.query<{ pg_try_advisory_lock: boolean }>(
      "SELECT pg_try_advisory_lock($1, $2)",
      [ADVISORY_LOCK_NAMESPACE, lockKey],
    );
    lockAcquired = lockResult.rows[0]?.pg_try_advisory_lock === true;

    if (!lockAcquired) {
      logError("Another deploy is already running for this project (advisory lock held).");
      return {
        deployed: 0,
        skipped: 0,
        dryRun: options.dryRun,
        error: "Concurrent deploy detected",
      };
    }

    verbose(`Advisory lock acquired: namespace=${ADVISORY_LOCK_NAMESPACE}, key=${lockKey}`);

    // Register cleanup for signal handling
    shutdownMgr.onShutdown(async () => {
      try {
        await db.query("SELECT pg_advisory_unlock($1, $2)", [ADVISORY_LOCK_NAMESPACE, lockKey]);
        verbose("Advisory lock released (shutdown)");
      } catch {
        // Best effort — PG releases on disconnect anyway
      }
      try {
        await db.disconnect();
      } catch {
        // Best effort
      }
    });

    // 5. Create registry schema if needed
    await registry.createRegistry();

    // 6. Register project
    await registry.getProject({
      project: projectName,
      uri: plan.project.uri ?? null,
      creator_name: options.committerName,
      creator_email: options.committerEmail,
    });

    // 7. Read deployed changes
    const deployedChanges = await registry.getDeployedChanges(projectName);
    const deployedIds = new Set(deployedChanges.map((c) => c.change_id));
    const deployedNames = deployedChanges.map((c) => c.change);

    // 8. Compute pending changes (re-filter with DB state)
    const pendingChanges = filterPending(allChanges, Array.from(deployedIds));

    if (pendingChanges.length === 0) {
      info("Nothing to deploy. Database is up to date.");
      return { deployed: 0, skipped: allChanges.length, dryRun: options.dryRun };
    }

    // Validate dependencies (pending changes may depend on already-deployed)
    validateDependencies(pendingChanges, deployedNames);

    // Topological sort
    const sortedChanges = topologicalSort(pendingChanges);

    // Build tag lookup: change_id -> tags attached to it
    const changeTagMap = buildChangeTagMap(plan);

    // 9. Set up TUI progress dashboard
    const outputCfg = getConfig();
    const useTUI = shouldUseTUI({ noTui: options.noTui, quiet: outputCfg.quiet });
    const progress = new DeployProgress({ isTTY: useTUI });
    const deployStartTime = Date.now();
    progress.start(sortedChanges.length);

    // 10. Execute each pending change
    let deployedCount = 0;
    let failedCount = 0;

    for (const change of sortedChanges) {
      // Check for shutdown request
      if (shutdownMgr.isShuttingDown()) {
        progress.finish({
          totalDeployed: deployedCount,
          totalFailed: failedCount,
          totalSkipped: 0,
          elapsedMs: Date.now() - deployStartTime,
        });
        return {
          deployed: deployedCount,
          skipped: 0,
          dryRun: false,
          error: "Deploy interrupted by signal",
        };
      }

      const deployScript = scriptPath(topDir, deployDir, change.name);
      if (!existsSync(deployScript)) {
        progress.updateChange(change.name, "failed");
        failedCount++;
        progress.finish({
          totalDeployed: deployedCount,
          totalFailed: failedCount,
          totalSkipped: 0,
          elapsedMs: Date.now() - deployStartTime,
        });
        return {
          deployed: deployedCount,
          skipped: 0,
          dryRun: false,
          failedChange: change.name,
          error: `Deploy script not found: ${deployScript}`,
        };
      }

      const scriptContent = readFileSync(deployScript, "utf-8");
      const noTransaction = isNonTransactional(scriptContent);
      const scriptHash = computeScriptHash(deployScript);

      // Resolve lock_timeout for this script
      let effectiveLockTimeout: number | undefined = options.lockTimeout;
      if (effectiveLockTimeout != null && !shouldSetLockTimeout(scriptContent)) {
        // Script already sets lock_timeout — skip auto-set
        effectiveLockTimeout = undefined;
      }

      // Determine transaction mode for psql
      const useSingleTransaction = !noTransaction && options.mode === "change";

      // Mark change as running in TUI
      progress.updateChange(change.name, "running");
      const changeStartTime = Date.now();

      if (!useTUI) {
        info(`Deploying change: ${change.name}`);
      }

      // Resolve snapshot includes (if any) before executing
      const resolved = resolveDeployIncludes(
        deployScript,
        change.planned_at,
        topDir,
        undefined, // commitHash — let resolveDeployIncludes look it up from planned_at
        options.noSnapshot,
      );

      // Execute via psql — use assembled content when includes were resolved,
      // otherwise pass the original script file (preserving psql's own \i handling
      // when --no-snapshot is set or there are no includes).
      let psqlResult: PsqlRunResult;
      if (resolved && !options.noSnapshot) {
        psqlResult = await psqlRunner.runContent(resolved.content, {
          uri: standardUri,
          singleTransaction: useSingleTransaction,
          variables: options.variables,
          dbClient: options.dbClient,
          workingDir: topDir,
          lockTimeout: effectiveLockTimeout,
        });
      } else {
        psqlResult = await psqlRunner.run(deployScript, {
          uri: standardUri,
          singleTransaction: useSingleTransaction,
          variables: options.variables,
          dbClient: options.dbClient,
          workingDir: topDir,
          lockTimeout: effectiveLockTimeout,
        });
      }

      const changeDuration = Date.now() - changeStartTime;

      if (psqlResult.exitCode !== 0) {
        // Deploy script failed
        const errMsg = psqlResult.error?.message ?? psqlResult.stderr;
        progress.updateChange(change.name, "failed", changeDuration);
        failedCount++;
        logError(`Deploy failed on change "${change.name}": ${errMsg}`);

        // Record fail event
        try {
          await registry.recordFail({
            change_id: change.change_id,
            script_hash: scriptHash,
            change: change.name,
            project: projectName,
            note: change.note,
            committer_name: options.committerName,
            committer_email: options.committerEmail,
            planned_at: new Date(change.planned_at),
            planner_name: change.planner_name,
            planner_email: change.planner_email,
            requires: change.requires,
            conflicts: change.conflicts,
            tags: changeTagMap.get(change.change_id) ?? [],
            dependencies: buildDependencies(change, allChanges),
          });
        } catch {
          // Best effort — don't mask the original error
        }

        progress.finish({
          totalDeployed: deployedCount,
          totalFailed: failedCount,
          totalSkipped: 0,
          elapsedMs: Date.now() - deployStartTime,
        });

        return {
          deployed: deployedCount,
          skipped: 0,
          dryRun: false,
          failedChange: change.name,
          error: errMsg,
        };
      }

      // Mark change as done in TUI
      progress.updateChange(change.name, "done", changeDuration);

      // Record successful deploy in tracking tables
      const recordInput: RecordDeployInput = {
        change_id: change.change_id,
        script_hash: scriptHash,
        change: change.name,
        project: projectName,
        note: change.note,
        committer_name: options.committerName,
        committer_email: options.committerEmail,
        planned_at: new Date(change.planned_at),
        planner_name: change.planner_name,
        planner_email: change.planner_email,
        requires: change.requires,
        conflicts: change.conflicts,
        tags: changeTagMap.get(change.change_id) ?? [],
        dependencies: buildDependencies(change, allChanges),
      };

      // Record tracking update in its own transaction (psql runs in a
      // separate process, so the tracking connection always needs its own
      // transaction regardless of the script's transaction mode).
      await db.transaction(async () => {
        await registry.recordDeploy(recordInput);
      });

      // Record any tags attached to this change
      const changeTags = plan.tags.filter((t) => t.change_id === change.change_id);
      for (const tag of changeTags) {
        await registry.recordTag({
          tag_id: tag.tag_id,
          tag: `@${tag.name}`,
          project: projectName,
          change_id: change.change_id,
          note: tag.note,
          committer_name: options.committerName,
          committer_email: options.committerEmail,
          planned_at: new Date(tag.planned_at),
          planner_name: tag.planner_name,
          planner_email: tag.planner_email,
        });
      }

      deployedCount++;

      // Run verify if enabled
      if (options.verify) {
        const verifyScript = scriptPath(topDir, verifyDir, change.name);
        if (existsSync(verifyScript)) {
          verbose(`Verifying change: ${change.name}`);
          const verifyResult = await psqlRunner.run(verifyScript, {
            uri: standardUri,
            variables: options.variables,
            dbClient: options.dbClient,
            workingDir: topDir,
          });
          if (verifyResult.exitCode !== 0) {
            const errMsg = verifyResult.error?.message ?? verifyResult.stderr;
            logError(`Verify failed for change "${change.name}": ${errMsg}`);
            progress.finish({
              totalDeployed: deployedCount,
              totalFailed: 1,
              totalSkipped: 0,
              elapsedMs: Date.now() - deployStartTime,
            });
            return {
              deployed: deployedCount,
              skipped: 0,
              dryRun: false,
              failedChange: change.name,
              error: `Verify failed: ${errMsg}`,
            };
          }
        }
      }
    }

    // 11. Print summary
    progress.finish({
      totalDeployed: deployedCount,
      totalFailed: failedCount,
      totalSkipped: 0,
      elapsedMs: Date.now() - deployStartTime,
    });
    if (!useTUI) {
      info(`Deployed ${deployedCount} change(s) successfully.`);
    }

    return { deployed: deployedCount, skipped: 0, dryRun: false };
  } finally {
    // Always release advisory lock
    if (lockAcquired) {
      try {
        await db.query("SELECT pg_advisory_unlock($1, $2)", [ADVISORY_LOCK_NAMESPACE, lockKey]);
        verbose("Advisory lock released");
      } catch {
        // Best effort — PG releases on disconnect
      }
    }

    await db.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Build the change -> tag name mapping from the plan.
 * Returns tags formatted as "@tagname" for the events table.
 */
function buildChangeTagMap(plan: Plan): Map<string, string[]> {
  const map = new Map<string, string[]>();
  for (const tag of plan.tags) {
    const existing = map.get(tag.change_id) ?? [];
    existing.push(`@${tag.name}`);
    map.set(tag.change_id, existing);
  }
  return map;
}

/**
 * Build dependency records for a change, resolving dependency IDs from
 * the full change set.
 */
function buildDependencies(
  change: Change,
  allChanges: Change[],
): RecordDeployInput["dependencies"] {
  const changeMap = new Map<string, string>();
  for (const c of allChanges) {
    changeMap.set(c.name, c.change_id);
  }

  const deps: RecordDeployInput["dependencies"] = [];

  for (const req of change.requires) {
    deps.push({
      type: "require",
      dependency: req,
      dependency_id: changeMap.get(req) ?? null,
    });
  }

  for (const conflict of change.conflicts) {
    deps.push({
      type: "conflict",
      dependency: conflict,
      dependency_id: changeMap.get(conflict) ?? null,
    });
  }

  return deps;
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

/**
 * Run the deploy command from CLI args.
 *
 * Returns an exit code instead of calling process.exit() directly,
 * so that callers (and finally blocks) can run cleanup before exiting.
 */
export async function runDeploy(args: ParsedArgs): Promise<number> {
  const options = parseDeployOptions(args);

  if (!options.dbUri) {
    logError("No database URI specified. Use --db-uri or configure a target in sqitch.conf.");
    return EXIT_DEPLOY_FAILED;
  }

  // Set up signal handling
  shutdownManager.register({ quiet: false });

  // Read project name from plan file for session settings
  const projectDir = resolve(options.projectDir);
  const config = loadConfig(options.projectDir);
  const planFilePath = join(projectDir, config.core.plan_file);
  let projectName = "unknown";
  if (existsSync(planFilePath)) {
    const planContent = readFileSync(planFilePath, "utf-8");
    const plan = parsePlan(planContent);
    projectName = plan.project.name;
  }

  const db = new DatabaseClient(options.dbUri, {
    command: "deploy",
    project: projectName,
    statementTimeout: 0,
    idleInTransactionSessionTimeout: 600_000,
  });
  const registry = new Registry(db);
  const psqlRunner = new PsqlRunner(options.dbClient);

  const result = await executeDeploy(options, {
    db,
    registry,
    psqlRunner,
    config,
    shutdownMgr: shutdownManager,
  });

  if (result.error && !result.dryRun) {
    if (result.error === "Concurrent deploy detected") {
      return EXIT_CONCURRENT_DEPLOY;
    }
    return EXIT_DEPLOY_FAILED;
  }

  return 0;
}
