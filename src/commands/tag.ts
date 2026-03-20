// src/commands/tag.ts — sqlever tag command
//
// Creates a tag at the current deployment state. Tags mark a specific
// point in the plan (attached to the last change). The tag is both
// appended to sqitch.plan and (when a DB connection is available)
// recorded in the sqitch.tags tracking table.
//
// Implements SPEC R1 `tag` semantics, compatible with Sqitch plan format.

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { loadConfig, type MergedConfig } from "../config/index";
import { computeTagId, type TagIdInput, type Tag } from "../plan/types";
import { appendTag } from "../plan/writer";
import { parsePlan } from "../plan/parser";
import { info, error, verbose } from "../output";
import { getPlannerIdentity, nowTimestamp } from "./add";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface TagOptions {
  /** Tag name (required positional arg, without @ prefix). */
  name: string;
  /** Note for the tag (from -n / --note). */
  note: string;
  /** Project root directory (from --top-dir or cwd). */
  topDir?: string;
}

// ---------------------------------------------------------------------------
// Argument parsing for the tag subcommand
// ---------------------------------------------------------------------------

/**
 * Parse the `rest` array from the CLI into TagOptions.
 *
 * Expected usage:
 *   sqlever tag <name> [-n note]
 */
export function parseTagArgs(rest: string[]): TagOptions {
  const opts: TagOptions = {
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

    // First non-flag argument is the tag name
    if (opts.name === "") {
      opts.name = arg;
    }
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Tag name validation
// ---------------------------------------------------------------------------

/**
 * Validate that a tag name is acceptable.
 *
 * Tag names must start with a letter or underscore and contain only
 * letters, digits, underscores, hyphens, and dots.
 * The @ prefix is NOT included in the name passed to this function.
 */
export function isValidTagName(name: string): boolean {
  return /^[a-zA-Z_][a-zA-Z0-9_.\-]*$/.test(name);
}

// ---------------------------------------------------------------------------
// Main tag logic
// ---------------------------------------------------------------------------

/**
 * Execute the `tag` command.
 *
 * Creates a tag at the current plan state by:
 *   1. Reading the plan file to find the last change
 *   2. Computing the tag ID using computeTagId
 *   3. Appending the tag line to the plan file
 *
 * @param opts    - Parsed tag options
 * @param config  - Merged configuration (if not provided, loaded from cwd)
 * @param env     - Environment variables (defaults to process.env)
 */
export async function runTag(
  opts: TagOptions,
  config?: MergedConfig,
  env?: Record<string, string | undefined>,
): Promise<Tag> {
  const environment = env ?? process.env;

  // Validate tag name
  if (!opts.name) {
    error("Error: tag name is required. Usage: sqlever tag <name> [-n note]");
    process.exit(1);
  }

  // Strip leading @ if the user included it
  const tagName = opts.name.startsWith("@") ? opts.name.slice(1) : opts.name;

  if (!isValidTagName(tagName)) {
    error(
      `Error: invalid tag name '${tagName}'. ` +
      "Names must start with a letter or underscore and contain only " +
      "letters, digits, underscores, hyphens, and dots.",
    );
    process.exit(1);
  }

  // Load config if not provided
  const cfg = config ?? loadConfig(opts.topDir, undefined, environment);

  // Resolve plan file path
  const topDir = resolve(opts.topDir ?? cfg.core.top_dir);
  const planPath = resolve(topDir, cfg.core.plan_file);

  // Ensure plan file exists
  if (!existsSync(planPath)) {
    error(`Error: plan file not found at ${planPath}. Run 'sqlever init' first.`);
    process.exit(1);
  }

  // Parse the plan file to get project info and the last change
  const planContent = readFileSync(planPath, "utf-8");
  const plan = parsePlan(planContent);

  // Must have at least one change to tag
  if (plan.changes.length === 0) {
    error("Error: no changes in plan. Add a change before tagging.");
    process.exit(1);
  }

  // Check for duplicate tag name
  const existingTagNames = new Set(plan.tags.map((t) => t.name));
  if (existingTagNames.has(tagName)) {
    error(
      `Error: tag '@${tagName}' already exists in the plan. ` +
      "Tag names must be unique.",
    );
    process.exit(1);
  }

  // The tag attaches to the last change
  const lastChange = plan.changes[plan.changes.length - 1]!;

  // Get planner identity
  const planner = getPlannerIdentity(environment);
  verbose(`Planner: ${planner.name} <${planner.email}>`);

  // Compute timestamp and tag ID
  const timestamp = nowTimestamp();

  const tagInput: TagIdInput = {
    project: plan.project.name,
    ...(plan.project.uri ? { uri: plan.project.uri } : {}),
    tag: tagName,
    change_id: lastChange.change_id,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: timestamp,
    note: opts.note,
  };

  const tagId = computeTagId(tagInput);

  // Build Tag object
  const tag: Tag = {
    tag_id: tagId,
    name: tagName,
    project: plan.project.name,
    change_id: lastChange.change_id,
    note: opts.note,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: timestamp,
  };

  // Append tag to plan file
  await appendTag(planPath, tag);
  verbose(`Appended tag to ${planPath}`);

  info(`Tagged "${lastChange.name}" with @${tagName} in ${planPath}`);

  return tag;
}
