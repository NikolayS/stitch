// src/commands/show.ts — sqlever show command
//
// Displays change/tag metadata or deploy/revert/verify script contents.
//
// Usage:
//   sqlever show deploy <name>   — print deploy script
//   sqlever show revert <name>   — print revert script
//   sqlever show verify <name>   — print verify script
//   sqlever show change <name>   — show change metadata from the plan
//   sqlever show tag <name>      — show tag metadata from the plan

import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { loadConfig, type MergedConfig } from "../config/index";
import { parsePlan } from "../plan/parser";
import type { Change, Tag } from "../plan/types";
import { info, error, json, getConfig } from "../output";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Supported show types. */
export type ShowType = "deploy" | "revert" | "verify" | "change" | "tag";

export interface ShowOptions {
  /** What to show: deploy, revert, verify, change, or tag. */
  type: ShowType;
  /** Name of the change or tag to look up. */
  name: string;
  /** Project root directory (from --top-dir or cwd). */
  topDir?: string;
  /** Plan file path override. */
  planFile?: string;
}

// ---------------------------------------------------------------------------
// Argument parsing for the show subcommand
// ---------------------------------------------------------------------------

const VALID_TYPES = new Set<string>(["deploy", "revert", "verify", "change", "tag"]);

/**
 * Parse the `rest` array from the CLI into ShowOptions.
 *
 * Expected usage:
 *   sqlever show <type> <name>
 */
export function parseShowArgs(rest: string[]): ShowOptions {
  const opts: ShowOptions = {
    type: "" as ShowType,
    name: "",
  };

  let positionalIndex = 0;
  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    // Consume known flags
    if (arg === "--plan-file") {
      opts.planFile = rest[++i];
      i++;
      continue;
    }
    if (arg === "--top-dir") {
      opts.topDir = rest[++i];
      i++;
      continue;
    }

    // Positional arguments
    if (positionalIndex === 0) {
      opts.type = arg as ShowType;
      positionalIndex++;
    } else if (positionalIndex === 1) {
      opts.name = arg;
      positionalIndex++;
    }
    i++;
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Script display (deploy / revert / verify)
// ---------------------------------------------------------------------------

/**
 * Resolve the path to a script file for the given type and change name.
 */
export function resolveScriptPath(
  topDir: string,
  scriptDir: string,
  changeName: string,
): string {
  return join(resolve(topDir), scriptDir, `${changeName}.sql`);
}

/**
 * Read and return the contents of a script file.
 * Returns null if the file does not exist.
 */
export function readScript(scriptPath: string): string | null {
  if (!existsSync(scriptPath)) return null;
  return readFileSync(scriptPath, "utf-8");
}

// ---------------------------------------------------------------------------
// Change/tag lookup
// ---------------------------------------------------------------------------

/**
 * Find a change by name in the parsed plan.
 * Returns the last occurrence (in case of reworked changes).
 */
export function findChange(planPath: string, name: string): Change | null {
  const content = readFileSync(planPath, "utf-8");
  const plan = parsePlan(content);

  // Find the last change with this name (handles reworked changes)
  let found: Change | null = null;
  for (const change of plan.changes) {
    if (change.name === name) {
      found = change;
    }
  }
  return found;
}

/**
 * Find a tag by name in the parsed plan.
 * The name should NOT include the @ prefix.
 */
export function findTag(planPath: string, name: string): Tag | null {
  const content = readFileSync(planPath, "utf-8");
  const plan = parsePlan(content);

  for (const tag of plan.tags) {
    if (tag.name === name) {
      return tag;
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

/**
 * Format change metadata for text output.
 */
export function formatChange(change: Change): string {
  const lines: string[] = [];
  lines.push(`Change:    ${change.name}`);
  lines.push(`ID:        ${change.change_id}`);
  lines.push(`Project:   ${change.project}`);
  lines.push(`Planner:   ${change.planner_name} <${change.planner_email}>`);
  lines.push(`Planned:   ${change.planned_at}`);
  if (change.parent) {
    lines.push(`Parent:    ${change.parent}`);
  }
  if (change.requires.length > 0) {
    lines.push(`Requires:  ${change.requires.join(", ")}`);
  }
  if (change.conflicts.length > 0) {
    lines.push(`Conflicts: ${change.conflicts.join(", ")}`);
  }
  if (change.note) {
    lines.push(`Note:      ${change.note}`);
  }
  return lines.join("\n");
}

/**
 * Format tag metadata for text output.
 */
export function formatTag(tag: Tag): string {
  const lines: string[] = [];
  lines.push(`Tag:       @${tag.name}`);
  lines.push(`ID:        ${tag.tag_id}`);
  lines.push(`Project:   ${tag.project}`);
  lines.push(`Change:    ${tag.change_id}`);
  lines.push(`Planner:   ${tag.planner_name} <${tag.planner_email}>`);
  lines.push(`Planned:   ${tag.planned_at}`);
  if (tag.note) {
    lines.push(`Note:      ${tag.note}`);
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Main show logic
// ---------------------------------------------------------------------------

/**
 * Execute the `show` command.
 *
 * @param opts   - Parsed show options
 * @param config - Merged configuration (if not provided, loaded from cwd)
 */
export function runShow(
  opts: ShowOptions,
  config?: MergedConfig,
): void {
  // Validate type
  if (!opts.type || !VALID_TYPES.has(opts.type)) {
    error(
      `Error: invalid show type '${opts.type || ""}'. ` +
      "Expected one of: deploy, revert, verify, change, tag.",
    );
    process.exit(1);
  }

  // Validate name
  if (!opts.name) {
    error(
      "Error: name is required. Usage: sqlever show <type> <name>",
    );
    process.exit(1);
  }

  // Load config if not provided
  const cfg = config ?? loadConfig(opts.topDir, undefined, undefined);
  const topDir = resolve(opts.topDir ?? cfg.core.top_dir);
  const planPath = opts.planFile
    ? resolve(opts.planFile)
    : resolve(topDir, cfg.core.plan_file);

  const outputCfg = getConfig();

  // Script types: deploy, revert, verify
  if (opts.type === "deploy" || opts.type === "revert" || opts.type === "verify") {
    const dirMap: Record<string, string> = {
      deploy: cfg.core.deploy_dir,
      revert: cfg.core.revert_dir,
      verify: cfg.core.verify_dir,
    };
    const scriptDir = dirMap[opts.type]!;
    const scriptPath = resolveScriptPath(topDir, scriptDir, opts.name);
    const content = readScript(scriptPath);

    if (content === null) {
      error(`Error: ${opts.type} script not found at ${scriptPath}`);
      process.exit(1);
    }

    if (outputCfg.format === "json") {
      json({
        type: opts.type,
        name: opts.name,
        path: scriptPath,
        content,
      });
    } else {
      // Print raw script content to stdout
      process.stdout.write(content);
    }
    return;
  }

  // Metadata types: change, tag
  if (opts.type === "change") {
    if (!existsSync(planPath)) {
      error(`Error: plan file not found at ${planPath}`);
      process.exit(1);
    }

    const change = findChange(planPath, opts.name);
    if (!change) {
      error(`Error: change '${opts.name}' not found in plan`);
      process.exit(1);
    }

    if (outputCfg.format === "json") {
      json(change);
    } else {
      info(formatChange(change));
    }
    return;
  }

  if (opts.type === "tag") {
    if (!existsSync(planPath)) {
      error(`Error: plan file not found at ${planPath}`);
      process.exit(1);
    }

    const tag = findTag(planPath, opts.name);
    if (!tag) {
      error(`Error: tag '${opts.name}' not found in plan`);
      process.exit(1);
    }

    if (outputCfg.format === "json") {
      json(tag);
    } else {
      info(formatTag(tag));
    }
    return;
  }
}
