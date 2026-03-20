// src/commands/init.ts — sqlever init command
//
// Initializes a new project by creating:
//   - sqitch.conf with [core] engine = pg and directory settings
//   - sqitch.plan with pragmas (%syntax-version, %project, optional %uri)
//   - deploy/, revert/, verify/ directories (respecting --top-dir)
//
// Matches Sqitch's `sqitch init` behavior for compatibility (SPEC R1).

import { mkdir, stat, writeFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import type { ParsedArgs } from "../cli";
import {
  serializeSqitchConf,
  confSet,
  type SqitchConf,
} from "../config/sqitch-conf";
import { info, error } from "../output";
import { serializePlan } from "../plan/writer";
import type { Plan } from "../plan/types";

// ---------------------------------------------------------------------------
// Init-specific argument parsing
// ---------------------------------------------------------------------------

export interface InitOptions {
  /** Project name. If not given, derived from the current directory name. */
  projectName: string;
  /** Top-level directory for the project (default: "."). */
  topDir: string;
  /** Database engine (default: "pg"). */
  engine: string;
  /** Optional project URI for the %uri pragma. */
  uri?: string;
  /** Path to the plan file (default: "sqitch.plan" under topDir). */
  planFile?: string;
  /** Force re-initialization even if sqitch.plan already exists. */
  force: boolean;
}

/**
 * Parse init-specific options from the CLI's parsed args.
 *
 * Usage: sqlever init [project_name] [--top-dir dir] [--engine pg] [--uri uri] [--plan-file path] [--force]
 *
 * Flags that appear in `rest` (after the command) are parsed here.
 */
export function parseInitOptions(args: ParsedArgs): InitOptions {
  const topDir = args.topDir ?? ".";
  let projectName: string | undefined;
  let engine = "pg";
  let uri: string | undefined;
  let planFile: string | undefined;
  let force = false;

  // Parse rest array for init-specific flags and positional project name
  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--engine") {
      engine = rest[++i] ?? "pg";
      i++;
      continue;
    }
    if (token === "--uri") {
      uri = rest[++i];
      i++;
      continue;
    }
    if (token === "--plan-file") {
      planFile = rest[++i];
      i++;
      continue;
    }
    if (token === "--force" || token === "-f") {
      force = true;
      i++;
      continue;
    }

    // First non-flag token is the project name
    if (projectName === undefined) {
      projectName = token;
    }
    i++;
  }

  // Use global --plan-file if not overridden locally
  if (planFile === undefined && args.planFile !== undefined) {
    planFile = args.planFile;
  }

  // Default project name = basename of the resolved top directory
  if (projectName === undefined) {
    projectName = basename(resolve(topDir));
  }

  return { projectName, topDir, engine, uri, planFile, force };
}

// ---------------------------------------------------------------------------
// File creation helpers
// ---------------------------------------------------------------------------

/**
 * Build sqitch.conf content for a new project.
 *
 * Generates:
 *   [core]
 *       engine = <engine>
 *       top_dir = <topDir>       (only if non-default)
 *       plan_file = <planFile>   (only if non-default)
 */
export function buildSqitchConf(options: InitOptions): string {
  const conf: SqitchConf = { entries: [], rawLines: [] };

  confSet(conf, "core.engine", options.engine);

  // Only write top_dir if it differs from default "."
  if (options.topDir !== ".") {
    confSet(conf, "core.top_dir", options.topDir);
  }

  // Only write plan_file if explicitly specified
  if (options.planFile !== undefined) {
    confSet(conf, "core.plan_file", options.planFile);
  }

  return serializeSqitchConf(conf);
}

/**
 * Build an empty sqitch.plan with only pragmas.
 */
export function buildInitialPlan(options: InitOptions): string {
  const pragmas = new Map<string, string>();
  pragmas.set("syntax-version", "1.0.0");
  pragmas.set("project", options.projectName);
  if (options.uri !== undefined) {
    pragmas.set("uri", options.uri);
  }

  const plan: Plan = {
    project: { name: options.projectName, uri: options.uri },
    pragmas,
    changes: [],
    tags: [],
  };

  return serializePlan(plan);
}

// ---------------------------------------------------------------------------
// Main init command
// ---------------------------------------------------------------------------

/**
 * Execute the `init` command.
 *
 * Creates sqitch.conf, sqitch.plan, and deploy/revert/verify directories.
 */
export async function runInit(args: ParsedArgs): Promise<void> {
  const options = parseInitOptions(args);
  const topDir = resolve(options.topDir);

  // Determine file paths
  const confPath = join(topDir, "sqitch.conf");
  const planPath = options.planFile
    ? resolve(options.planFile)
    : join(topDir, "sqitch.plan");

  // Check if sqitch.plan already exists (unless --force)
  if (!options.force) {
    try {
      const planStat = await stat(planPath);
      if (planStat.isFile()) {
        error(
          `Plan file already exists: ${planPath}\n` +
            `Use --force to reinitialize.`,
        );
        process.exit(1);
      }
    } catch {
      // File doesn't exist — proceed
    }
  }

  // Ensure top directory exists
  await mkdir(topDir, { recursive: true });

  // Create deploy/, revert/, verify/ directories under topDir
  const dirs = ["deploy", "revert", "verify"];
  for (const dir of dirs) {
    await mkdir(join(topDir, dir), { recursive: true });
  }

  // Write sqitch.conf
  const confContent = buildSqitchConf(options);
  await writeFile(confPath, confContent, "utf-8");
  info(`Created ${confPath}`);

  // Write sqitch.plan
  const planContent = buildInitialPlan(options);
  await writeFile(planPath, planContent, "utf-8");
  info(`Created ${planPath}`);

  // Report created directories
  for (const dir of dirs) {
    info(`Created ${join(topDir, dir)}/`);
  }

  info(`Initialized project '${options.projectName}'`);
}
