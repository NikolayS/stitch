// src/commands/add.ts — sqlever add command
//
// Creates migration files (deploy, revert, verify) and appends a
// change entry to sqitch.plan. Implements SPEC R1 `add` semantics.

import { existsSync, readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { execSync } from "node:child_process";
import { loadConfig, type MergedConfig } from "../config/index";
import { computeChangeId, type ChangeIdInput } from "../plan/types";
import { appendChange } from "../plan/writer";
import type { Change } from "../plan/types";
import { info, error, verbose } from "../output";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface AddOptions {
  /** Change name (required positional arg). */
  name: string;
  /** Note for the change (from -n / --note). */
  note: string;
  /** Required dependencies (from -r / --requires, repeatable). */
  requires: string[];
  /** Conflict dependencies (from -c / --conflicts, repeatable). */
  conflicts: string[];
  /** Skip verify file creation (from --no-verify). */
  noVerify: boolean;
  /** Project root directory (from --top-dir or cwd). */
  topDir?: string;
}

// ---------------------------------------------------------------------------
// Argument parsing for the add subcommand
// ---------------------------------------------------------------------------

/**
 * Parse the `rest` array from the CLI into AddOptions.
 *
 * Expected usage:
 *   sqlever add <name> [-n note] [-r dep]... [-c conflict]... [--no-verify]
 */
export function parseAddArgs(rest: string[]): AddOptions {
  const opts: AddOptions = {
    name: "",
    note: "",
    requires: [],
    conflicts: [],
    noVerify: false,
  };

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "-n" || arg === "--note") {
      opts.note = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "-r" || arg === "--requires") {
      const val = rest[++i];
      if (val) opts.requires.push(val);
      i++;
      continue;
    }
    if (arg === "-c" || arg === "--conflicts") {
      const val = rest[++i];
      if (val) opts.conflicts.push(val);
      i++;
      continue;
    }
    if (arg === "--no-verify") {
      opts.noVerify = true;
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
// Plan reading (minimal — extracts project name, existing changes, last ID)
// ---------------------------------------------------------------------------

interface PlanInfo {
  /** Project name from %project pragma. */
  projectName: string;
  /** Project URI from %uri pragma (may be undefined). */
  projectUri?: string;
  /** Set of existing change names (for duplicate detection). */
  existingNames: Set<string>;
  /** The change_id of the last change in the plan (for parent linking). */
  lastChangeId?: string;
}

/**
 * Read plan file and extract minimal information needed for `add`.
 *
 * This is a lightweight reader that extracts pragmas and change entries
 * without a full parse. The plan parser module may not be available yet.
 */
export function readPlanInfo(planPath: string): PlanInfo {
  const content = readFileSync(planPath, "utf-8");
  const lines = content.split("\n");

  let projectName = "";
  let projectUri: string | undefined;
  const existingNames = new Set<string>();
  let lastChangeId: string | undefined;

  for (const line of lines) {
    const trimmed = line.trim();

    // Skip empty lines and comments
    if (trimmed === "" || trimmed.startsWith("--")) continue;

    // Pragmas
    if (trimmed.startsWith("%")) {
      const match = trimmed.match(/^%(\S+?)=(.*)$/);
      if (match) {
        const [, key, value] = match;
        if (key === "project") projectName = value!;
        if (key === "uri") projectUri = value;
      }
      continue;
    }

    // Tags (start with @) — skip
    if (trimmed.startsWith("@")) continue;

    // Change lines: name [deps] timestamp planner <email> # note
    // The name is the first non-whitespace token
    const changeName = trimmed.split(/\s+/)[0];
    if (changeName) {
      existingNames.add(changeName);

      // We need to recompute the change_id from the line to get the last one.
      // But we actually need a full parse for that. Instead, since we're
      // appending, we compute all IDs in order.
      // For now, we'll recompute from parsed line data.
      const parsedChange = parseChangeLine(trimmed, projectName, projectUri, lastChangeId);
      if (parsedChange) {
        lastChangeId = parsedChange.change_id;
      }
    }
  }

  return { projectName, projectUri, existingNames, lastChangeId };
}

/**
 * Parse a single change line from the plan file and compute its change_id.
 *
 * Format: name [deps] timestamp planner_name <email> # note
 */
function parseChangeLine(
  line: string,
  project: string,
  uri: string | undefined,
  parent: string | undefined,
): Change | null {
  // Match: name [deps]? timestamp planner <email> (# note)?
  // We need to handle optional deps block
  const depsRegex = /^(\S+)\s+(?:\[([^\]]*)\]\s+)?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(.+?)\s+<([^>]+)>(?:\s+#\s+(.*))?$/;
  const match = line.match(depsRegex);
  if (!match) return null;

  const [, name, depsStr, timestamp, plannerName, plannerEmail, note] = match;

  const requires: string[] = [];
  const conflicts: string[] = [];

  if (depsStr) {
    for (const dep of depsStr.split(/\s+/)) {
      if (dep.startsWith("!")) {
        conflicts.push(dep.slice(1));
      } else if (dep !== "") {
        requires.push(dep);
      }
    }
  }

  const changeId = computeChangeId({
    project,
    uri,
    change: name!,
    parent,
    planner_name: plannerName!,
    planner_email: plannerEmail!,
    planned_at: timestamp!,
    requires,
    conflicts,
    note: note ?? "",
  });

  return {
    change_id: changeId,
    name: name!,
    project,
    note: note ?? "",
    planner_name: plannerName!,
    planner_email: plannerEmail!,
    planned_at: timestamp!,
    requires,
    conflicts,
    parent,
  };
}

// ---------------------------------------------------------------------------
// Planner identity
// ---------------------------------------------------------------------------

export interface PlannerIdentity {
  name: string;
  email: string;
}

/**
 * Determine planner name and email.
 *
 * Precedence:
 *   1. SQLEVER_USER_NAME / SQLEVER_USER_EMAIL env vars
 *   2. git config user.name / user.email
 *   3. Fallback: "Unknown" / "unknown@example.com"
 */
export function getPlannerIdentity(
  env: Record<string, string | undefined> = process.env,
): PlannerIdentity {
  let name = env.SQLEVER_USER_NAME;
  let email = env.SQLEVER_USER_EMAIL;

  if (!name) {
    try {
      name = execSync("git config user.name", { encoding: "utf-8" }).trim();
    } catch {
      // git not available or not configured
    }
  }

  if (!email) {
    try {
      email = execSync("git config user.email", { encoding: "utf-8" }).trim();
    } catch {
      // git not available or not configured
    }
  }

  return {
    name: name || "Unknown",
    email: email || "unknown@example.com",
  };
}

// ---------------------------------------------------------------------------
// File templates
// ---------------------------------------------------------------------------

export function deployTemplate(name: string, requires: string[]): string {
  const reqLine = requires.length > 0
    ? `-- requires: ${requires.join(", ")}`
    : "-- requires:";
  return `-- Deploy ${name}\n${reqLine}\n\nBEGIN;\n\n-- XXX Add DDL here.\n\nCOMMIT;\n`;
}

export function revertTemplate(name: string): string {
  return `-- Revert ${name}\n\nBEGIN;\n\n-- XXX Add revert DDL here.\n\nCOMMIT;\n`;
}

export function verifyTemplate(name: string): string {
  return `-- Verify ${name}\n\nBEGIN;\n\n-- XXX Add verification here.\n\nROLLBACK;\n`;
}

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/**
 * Generate an ISO 8601 timestamp in the format Sqitch uses.
 * Example: 2024-01-15T10:30:00Z
 */
export function nowTimestamp(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

// ---------------------------------------------------------------------------
// Main add logic
// ---------------------------------------------------------------------------

/**
 * Execute the `add` command.
 *
 * @param opts    - Parsed add options
 * @param config  - Merged configuration (if not provided, loaded from cwd)
 * @param env     - Environment variables (defaults to process.env)
 */
export async function runAdd(
  opts: AddOptions,
  config?: MergedConfig,
  env?: Record<string, string | undefined>,
): Promise<void> {
  const environment = env ?? process.env;

  // Validate change name
  if (!opts.name) {
    error("Error: change name is required. Usage: sqlever add <name>");
    process.exit(1);
  }

  // Validate change name format (alphanumeric, underscores, hyphens)
  if (!/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(opts.name)) {
    error(
      `Error: invalid change name '${opts.name}'. ` +
      "Names must start with a letter or underscore and contain only " +
      "letters, digits, underscores, and hyphens.",
    );
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

  // Read existing plan to check for duplicates and get last change ID
  const planInfo = readPlanInfo(planPath);

  if (planInfo.existingNames.has(opts.name)) {
    error(
      `Error: change '${opts.name}' already exists in the plan. ` +
      "Use 'sqlever rework' to create a new version of an existing change.",
    );
    process.exit(1);
  }

  // Get planner identity
  const planner = getPlannerIdentity(environment);
  verbose(`Planner: ${planner.name} <${planner.email}>`);

  // Compute timestamp and change ID
  const timestamp = nowTimestamp();

  const changeIdInput: ChangeIdInput = {
    project: planInfo.projectName,
    uri: planInfo.projectUri,
    change: opts.name,
    parent: planInfo.lastChangeId,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: timestamp,
    requires: opts.requires,
    conflicts: opts.conflicts,
    note: opts.note,
  };

  const changeId = computeChangeId(changeIdInput);

  // Build Change object
  const change: Change = {
    change_id: changeId,
    name: opts.name,
    project: planInfo.projectName,
    note: opts.note,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: timestamp,
    requires: opts.requires,
    conflicts: opts.conflicts,
    parent: planInfo.lastChangeId,
  };

  // Create directories if they don't exist
  mkdirSync(deployDir, { recursive: true });
  mkdirSync(revertDir, { recursive: true });
  if (!opts.noVerify) {
    mkdirSync(verifyDir, { recursive: true });
  }

  // Create migration files (error if they already exist)
  const deployPath = join(deployDir, `${opts.name}.sql`);
  const revertPath = join(revertDir, `${opts.name}.sql`);
  const verifyPath = join(verifyDir, `${opts.name}.sql`);

  if (existsSync(deployPath)) {
    error(`Error: deploy script already exists at ${deployPath}`);
    process.exit(1);
  }
  if (existsSync(revertPath)) {
    error(`Error: revert script already exists at ${revertPath}`);
    process.exit(1);
  }
  if (!opts.noVerify && existsSync(verifyPath)) {
    error(`Error: verify script already exists at ${verifyPath}`);
    process.exit(1);
  }

  writeFileSync(deployPath, deployTemplate(opts.name, opts.requires), "utf-8");
  verbose(`Created ${deployPath}`);

  writeFileSync(revertPath, revertTemplate(opts.name), "utf-8");
  verbose(`Created ${revertPath}`);

  if (!opts.noVerify) {
    writeFileSync(verifyPath, verifyTemplate(opts.name), "utf-8");
    verbose(`Created ${verifyPath}`);
  }

  // Append change to plan
  await appendChange(planPath, change);
  verbose(`Appended change to ${planPath}`);

  info(`Added "${opts.name}" to ${planPath}`);
}
