// src/commands/plan.ts — sqlever plan command
//
// Displays the contents of a sqitch.plan file in a human-readable
// format (text table or JSON). Supports optional filters by change
// name (--change) or tag name (--tag).
//
// Implements GitHub issue #48 (S4-6).

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ParsedArgs } from "../cli";
import { parsePlan } from "../plan/parser";
import type { Plan, Change, Tag } from "../plan/types";
import { info, error, json, table, getConfig } from "../output";

// ---------------------------------------------------------------------------
// Plan-specific argument parsing
// ---------------------------------------------------------------------------

export interface PlanOptions {
  /** Path to the plan file (resolved from --plan-file or default). */
  planFile: string;
  /** Filter changes to only those matching this name. */
  change?: string;
  /** Filter to show only changes up to and including this tag. */
  tag?: string;
}

/**
 * Parse plan-specific options from the CLI's parsed args.
 *
 * Usage: sqlever plan [--change <name>] [--tag <name>] [--plan-file <path>]
 */
export function parsePlanArgs(args: ParsedArgs): PlanOptions {
  const topDir = args.topDir ?? ".";
  let planFile = args.planFile ?? "sqitch.plan";
  let change: string | undefined;
  let tag: string | undefined;

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--change") {
      change = rest[++i];
      i++;
      continue;
    }
    if (token === "--tag") {
      tag = rest[++i];
      i++;
      continue;
    }
    if (token === "--plan-file") {
      planFile = rest[++i] ?? planFile;
      i++;
      continue;
    }

    // Unknown tokens are ignored (could be positional in the future)
    i++;
  }

  // Resolve plan file relative to top-dir
  const resolved = resolve(topDir, planFile);

  return { planFile: resolved, change, tag };
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

/**
 * Build a map from change_id to the tags attached to that change.
 */
function buildTagMap(tags: Tag[]): Map<string, Tag[]> {
  const map = new Map<string, Tag[]>();
  for (const tag of tags) {
    const existing = map.get(tag.change_id);
    if (existing) {
      existing.push(tag);
    } else {
      map.set(tag.change_id, [tag]);
    }
  }
  return map;
}

/**
 * Filter plan changes and tags based on --change and --tag options.
 *
 * - --change <name>: Show only changes matching `name`.
 * - --tag <name>: Show all changes up to and including the change
 *   that the named tag is attached to.
 *
 * When both are specified, --tag determines the range and --change
 * filters within that range.
 */
export function filterPlan(
  plan: Plan,
  opts: PlanOptions,
): { changes: Change[]; tags: Tag[] } {
  let { changes, tags } = plan;

  // --tag: slice changes up to and including the tagged change
  if (opts.tag) {
    const matchingTag = tags.find((t) => t.name === opts.tag);
    if (!matchingTag) {
      return { changes: [], tags: [] };
    }

    const cutoffIdx = changes.findIndex(
      (c) => c.change_id === matchingTag.change_id,
    );
    if (cutoffIdx === -1) {
      return { changes: [], tags: [] };
    }

    changes = changes.slice(0, cutoffIdx + 1);
    // Only include tags that reference changes in the sliced range
    const changeIds = new Set(changes.map((c) => c.change_id));
    tags = tags.filter((t) => changeIds.has(t.change_id));
  }

  // --change: filter to only changes with matching name
  if (opts.change) {
    changes = changes.filter((c) => c.name === opts.change);
    // Only include tags attached to the filtered changes
    const changeIds = new Set(changes.map((c) => c.change_id));
    tags = tags.filter((t) => changeIds.has(t.change_id));
  }

  return { changes, tags };
}

// ---------------------------------------------------------------------------
// Formatting — text output
// ---------------------------------------------------------------------------

/**
 * Format a dependency list for display.
 * Requires: dep1, dep2
 * Conflicts: !conflict1
 */
export function formatDeps(requires: string[], conflicts: string[]): string {
  const parts: string[] = [];
  for (const r of requires) {
    parts.push(r);
  }
  for (const c of conflicts) {
    parts.push(`!${c}`);
  }
  return parts.join(", ");
}

/**
 * Format tag names for a given change_id.
 */
function formatTags(tagMap: Map<string, Tag[]>, changeId: string): string {
  const changeTags = tagMap.get(changeId);
  if (!changeTags || changeTags.length === 0) return "";
  return changeTags.map((t) => `@${t.name}`).join(", ");
}

/**
 * Print plan in text format to stdout.
 */
export function printPlanText(
  plan: Plan,
  changes: Change[],
  tags: Tag[],
): void {
  const tagMap = buildTagMap(tags);

  // Header: project info
  info(`Project: ${plan.project.name}`);
  if (plan.project.uri) {
    info(`URI:     ${plan.project.uri}`);
  }
  info("");

  if (changes.length === 0) {
    info("No changes.");
    return;
  }

  const headers = ["Name", "Deps", "Tags", "Planner", "Date", "Note"];
  const rows: string[][] = [];

  for (const c of changes) {
    const deps = formatDeps(c.requires, c.conflicts);
    const changeTags = formatTags(tagMap, c.change_id);
    const date = c.planned_at.replace("T", " ").replace("Z", "");
    const planner = `${c.planner_name} <${c.planner_email}>`;

    rows.push([c.name, deps, changeTags, planner, date, c.note]);
  }

  table(rows, headers);
}

// ---------------------------------------------------------------------------
// Formatting — JSON output
// ---------------------------------------------------------------------------

/**
 * Build the JSON-serializable representation of plan contents.
 */
export function buildPlanJson(
  plan: Plan,
  changes: Change[],
  tags: Tag[],
): object {
  const tagMap = buildTagMap(tags);

  return {
    project: {
      name: plan.project.name,
      uri: plan.project.uri ?? null,
    },
    changes: changes.map((c) => ({
      name: c.name,
      change_id: c.change_id,
      planned_at: c.planned_at,
      planner_name: c.planner_name,
      planner_email: c.planner_email,
      note: c.note,
      requires: c.requires,
      conflicts: c.conflicts,
      parent: c.parent ?? null,
      tags: (tagMap.get(c.change_id) ?? []).map((t) => ({
        name: t.name,
        tag_id: t.tag_id,
      })),
    })),
  };
}

// ---------------------------------------------------------------------------
// Main plan command
// ---------------------------------------------------------------------------

/**
 * Execute the `plan` command.
 *
 * Reads the plan file, applies filters, and displays the result
 * in text or JSON format.
 */
export function runPlan(args: ParsedArgs): void {
  const opts = parsePlanArgs(args);

  // Read and parse plan file
  let content: string;
  try {
    content = readFileSync(opts.planFile, "utf-8");
  } catch {
    error(`Error: cannot read plan file: ${opts.planFile}`);
    process.exit(1);
    return; // unreachable, for TypeScript
  }

  let plan: Plan;
  try {
    plan = parsePlan(content);
  } catch (e) {
    error(
      `Error: failed to parse plan file: ${e instanceof Error ? e.message : String(e)}`,
    );
    process.exit(1);
    return;
  }

  // Apply filters
  const { changes, tags } = filterPlan(plan, opts);

  // Output
  const config = getConfig();
  if (config.format === "json") {
    json(buildPlanJson(plan, changes, tags));
  } else {
    printPlanText(plan, changes, tags);
  }
}
