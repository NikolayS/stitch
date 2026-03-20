// src/commands/rework.ts — sqlever rework command
//
// Reworks an existing change by creating a new version with the same name.
// Sqitch rework semantics (SPEC R1, R2):
//   1. Verify the change exists in the plan
//   2. Verify a tag exists after the change's last occurrence
//   3. Copy current deploy/revert/verify scripts to <change>@<tag>.sql
//   4. Create fresh deploy/revert/verify files for the new version
//   5. Append a new plan entry with the same name, depending on change@tag

import { existsSync, readFileSync, mkdirSync, copyFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { loadConfig, type MergedConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import { computeChangeId, type ChangeIdInput, type Change, type Tag } from "../plan/types";
import { appendChange } from "../plan/writer";
import {
  getPlannerIdentity,
  nowTimestamp,
  deployTemplate,
  revertTemplate,
  verifyTemplate,
} from "./add";
import { info, error, verbose } from "../output";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ReworkOptions {
  /** Change name to rework (required positional arg). */
  name: string;
  /** Note for the reworked change (from -n / --note). */
  note: string;
  /** Project root directory (from --top-dir or cwd). */
  topDir?: string;
}

// ---------------------------------------------------------------------------
// Argument parsing for the rework subcommand
// ---------------------------------------------------------------------------

/**
 * Parse the `rest` array from the CLI into ReworkOptions.
 *
 * Expected usage:
 *   sqlever rework <name> [-n note]
 */
export function parseReworkArgs(rest: string[]): ReworkOptions {
  const opts: ReworkOptions = {
    name: "",
    note: "",
  };

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "-n" || arg === "--note") {
      opts.note = rest[++i] ?? "";
      i++;
      continue;
    }

    // First non-flag argument is the change name
    if (opts.name === "") {
      opts.name = arg;
    }
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Plan analysis helpers
// ---------------------------------------------------------------------------

export interface ReworkContext {
  /** The last occurrence of the change in the plan. */
  lastChange: Change;
  /** Index of the last occurrence in the changes array. */
  lastChangeIndex: number;
  /** The tag that appears after the last occurrence. */
  tagAfterChange: Tag;
  /** All tags in the plan (for finding tag after change). */
  allTags: Tag[];
  /** Project name from pragmas. */
  projectName: string;
  /** Project URI from pragmas (may be undefined). */
  projectUri?: string;
  /** The change_id of the last change in the plan (for parent linking). */
  lastPlanChangeId: string;
}

/**
 * Find the rework context: the last occurrence of the change name,
 * and the tag that must exist after it.
 *
 * @throws Error if the change doesn't exist or has no tag after it
 */
export function findReworkContext(
  planContent: string,
  changeName: string,
): ReworkContext {
  const plan = parsePlan(planContent);

  // Find all occurrences of this change name
  const occurrences: { change: Change; index: number }[] = [];
  for (let i = 0; i < plan.changes.length; i++) {
    if (plan.changes[i]!.name === changeName) {
      occurrences.push({ change: plan.changes[i]!, index: i });
    }
  }

  if (occurrences.length === 0) {
    throw new ReworkError(
      `Unknown change: "${changeName}". The change must exist in the plan to be reworked.`,
    );
  }

  const lastOccurrence = occurrences[occurrences.length - 1]!;

  // Build a map from change_id to tags that follow that change
  // We need to find a tag that appears after the last occurrence
  // In Sqitch semantics, a tag is "after" a change if the tag's change_id
  // matches ANY change from the last occurrence onward (including changes
  // that come after the last occurrence but before any tag).
  //
  // More precisely: we need a tag that is attached to a change at or after
  // the last occurrence index. The tag we want is the FIRST such tag.
  const changeIdsAtOrAfterLast = new Set<string>();
  for (let i = lastOccurrence.index; i < plan.changes.length; i++) {
    changeIdsAtOrAfterLast.add(plan.changes[i]!.change_id);
  }

  let tagAfterChange: Tag | undefined;
  for (const tag of plan.tags) {
    if (changeIdsAtOrAfterLast.has(tag.change_id)) {
      tagAfterChange = tag;
      break;
    }
  }

  if (!tagAfterChange) {
    throw new ReworkError(
      `Cannot rework "${changeName}": no tag exists after the change. ` +
      `Use 'sqlever tag' to create a tag before reworking.`,
    );
  }

  // The last change in the plan for parent linking
  const lastPlanChange = plan.changes[plan.changes.length - 1]!;

  return {
    lastChange: lastOccurrence.change,
    lastChangeIndex: lastOccurrence.index,
    tagAfterChange,
    allTags: plan.tags,
    projectName: plan.project.name,
    projectUri: plan.project.uri,
    lastPlanChangeId: lastPlanChange.change_id,
  };
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

export class ReworkError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReworkError";
  }
}

// ---------------------------------------------------------------------------
// Main rework logic
// ---------------------------------------------------------------------------

/**
 * Execute the `rework` command.
 *
 * @param opts    - Parsed rework options
 * @param config  - Merged configuration (if not provided, loaded from cwd)
 * @param env     - Environment variables (defaults to process.env)
 */
export async function runRework(
  opts: ReworkOptions,
  config?: MergedConfig,
  env?: Record<string, string | undefined>,
): Promise<void> {
  const environment = env ?? process.env;

  // Validate change name
  if (!opts.name) {
    error("Error: change name is required. Usage: sqlever rework <name> [-n note]");
    process.exit(1);
  }

  // Load config if not provided
  const cfg = config ?? loadConfig(opts.topDir, undefined, environment);

  // Resolve directories relative to top_dir
  const topDir = resolve(opts.topDir ?? cfg.core.top_dir);
  const deployDir = resolve(topDir, cfg.core.deploy_dir);
  const revertDir = resolve(topDir, cfg.core.revert_dir);
  const verifyDir = resolve(topDir, cfg.core.verify_dir);
  const planPath = resolve(topDir, cfg.core.plan_file);

  // Ensure plan file exists
  if (!existsSync(planPath)) {
    error(`Error: plan file not found at ${planPath}. Run 'sqlever init' first.`);
    process.exit(1);
  }

  // Read and analyze the plan
  const planContent = readFileSync(planPath, "utf-8");
  let ctx: ReworkContext;
  try {
    ctx = findReworkContext(planContent, opts.name);
  } catch (err) {
    if (err instanceof ReworkError) {
      error(`Error: ${err.message}`);
      process.exit(1);
    }
    throw err;
  }

  const tagName = ctx.tagAfterChange.name;

  // --- Step 1: Copy current scripts to @tag versions (backup) ---
  const deployScript = join(deployDir, `${opts.name}.sql`);
  const revertScript = join(revertDir, `${opts.name}.sql`);
  const verifyScript = join(verifyDir, `${opts.name}.sql`);

  const deployBackup = join(deployDir, `${opts.name}@${tagName}.sql`);
  const revertBackup = join(revertDir, `${opts.name}@${tagName}.sql`);
  const verifyBackup = join(verifyDir, `${opts.name}@${tagName}.sql`);

  // Ensure directories exist
  mkdirSync(deployDir, { recursive: true });
  mkdirSync(revertDir, { recursive: true });
  mkdirSync(verifyDir, { recursive: true });

  // Copy existing scripts to @tag versions
  if (existsSync(deployScript)) {
    copyFileSync(deployScript, deployBackup);
    verbose(`Copied ${deployScript} -> ${deployBackup}`);
  }
  if (existsSync(revertScript)) {
    copyFileSync(revertScript, revertBackup);
    verbose(`Copied ${revertScript} -> ${revertBackup}`);
  }
  if (existsSync(verifyScript)) {
    copyFileSync(verifyScript, verifyBackup);
    verbose(`Copied ${verifyScript} -> ${verifyBackup}`);
  }

  // --- Step 2: Overwrite the original scripts with fresh templates ---
  writeFileSync(
    deployScript,
    deployTemplate(opts.name, [`${opts.name}@${tagName}`]),
    "utf-8",
  );
  verbose(`Created fresh ${deployScript}`);

  writeFileSync(revertScript, revertTemplate(opts.name), "utf-8");
  verbose(`Created fresh ${revertScript}`);

  writeFileSync(verifyScript, verifyTemplate(opts.name), "utf-8");
  verbose(`Created fresh ${verifyScript}`);

  // --- Step 3: Append the reworked change to the plan ---
  const planner = getPlannerIdentity(environment);
  const timestamp = nowTimestamp();

  // The reworked change depends on change@tag
  const requires = [`${opts.name}@${tagName}`];

  const changeIdInput: ChangeIdInput = {
    project: ctx.projectName,
    uri: ctx.projectUri,
    change: opts.name,
    parent: ctx.lastPlanChangeId,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: timestamp,
    requires,
    conflicts: [],
    note: opts.note,
  };

  const changeId = computeChangeId(changeIdInput);

  const change: Change = {
    change_id: changeId,
    name: opts.name,
    project: ctx.projectName,
    note: opts.note,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: timestamp,
    requires,
    conflicts: [],
    parent: ctx.lastPlanChangeId,
  };

  await appendChange(planPath, change);
  verbose(`Appended reworked change to ${planPath}`);

  info(`Reworked "${opts.name}" referencing @${tagName}`);
}
