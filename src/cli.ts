#!/usr/bin/env bun
// sqlever — Sqitch-compatible PostgreSQL migration tool

import packageJson from "../package.json";
import { runInit } from "./commands/init";
import { runStatus } from "./commands/status";
import { runDeploy } from "./commands/deploy";
import { setConfig, type OutputFormat } from "./output";
import { parseAddArgs, runAdd } from "./commands/add";
import { runLogCommand } from "./commands/log";
import { runRevert } from "./commands/revert";
import { parseTagArgs, runTag } from "./commands/tag";
import { parseReworkArgs, runRework } from "./commands/rework";
import { parseShowArgs, runShow } from "./commands/show";
import { runPlan } from "./commands/plan";
import { runVerify } from "./commands/verify";
import { parseAnalyzeArgs, runAnalyze } from "./commands/analyze";
import { runDoctor } from "./commands/doctor";

// ---------------------------------------------------------------------------
// Command registry — all commands from SPEC R1 plus sqlever extensions
// ---------------------------------------------------------------------------

/** Description for each supported command, used in --help output. */
const COMMANDS: Record<string, string> = {
  init: "Initialize project, create sqitch.conf and sqitch.plan",
  add: "Add a new migration change",
  deploy: "Deploy changes to a database",
  revert: "Revert changes from a database",
  verify: "Run verify scripts against a database",
  status: "Show deployment status",
  log: "Show deployment history",
  tag: "Tag the current deployment state",
  rework: "Rework an existing change",
  rebase: "Revert then re-deploy changes",
  bundle: "Package project for distribution",
  checkout: "Deploy/revert changes to match a VCS branch",
  show: "Display change/tag details or script contents",
  plan: "Display plan contents",
  upgrade: "Upgrade the registry schema to current version",
  engine: "Manage database engines",
  target: "Manage deploy targets",
  config: "Read/write configuration",
  analyze: "Analyze migration SQL for dangerous patterns",
  explain: "Explain what a migration does in plain language",
  review: "Review migrations for issues",
  batch: "Manage batched background data migrations",
  diff: "Show differences between plan states",
  doctor: "Validate project setup, plan file, and script consistency",
  help: "Show help for a command",
};

/** Sorted command names for display. */
const COMMAND_NAMES = Object.keys(COMMANDS).sort();

// ---------------------------------------------------------------------------
// Flag parsing
// ---------------------------------------------------------------------------

export interface ParsedArgs {
  /** The command to run (e.g. "deploy"), or undefined if none given. */
  command: string | undefined;
  /** Remaining positional arguments after the command. */
  rest: string[];
  /** --help / -h */
  help: boolean;
  /** --version / -V */
  version: boolean;
  /** --format json|text */
  format: OutputFormat;
  /** --quiet / -q */
  quiet: boolean;
  /** --verbose / -v */
  verbose: boolean;
  /** --db-uri <uri> */
  dbUri: string | undefined;
  /** --plan-file <path> */
  planFile: string | undefined;
  /** --top-dir <path> */
  topDir: string | undefined;
  /** --registry <name> */
  registry: string | undefined;
  /** --target <target> */
  target: string | undefined;
}

/**
 * Parse argv into structured args. Extracts top-level flags that appear
 * before or after the command. The first non-flag token is treated as the
 * command; everything after it goes into `rest`.
 */
export function parseArgs(argv: string[]): ParsedArgs {
  const result: ParsedArgs = {
    command: undefined,
    rest: [],
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: undefined,
    planFile: undefined,
    topDir: undefined,
    registry: undefined,
    target: undefined,
  };

  let i = 0;
  while (i < argv.length) {
    const arg = argv[i]!;

    // --- Boolean flags ---
    if (arg === "--help" || arg === "-h") {
      result.help = true;
      i++;
      continue;
    }
    if (arg === "--version" || arg === "-V") {
      result.version = true;
      i++;
      continue;
    }
    if (arg === "--quiet" || arg === "-q") {
      result.quiet = true;
      i++;
      continue;
    }
    if (arg === "--verbose" || arg === "-v") {
      result.verbose = true;
      i++;
      continue;
    }

    // --- Value flags ---
    if (arg === "--format") {
      const val = argv[i + 1];
      if (val === "json" || val === "text") {
        result.format = val;
        // When a command is already set, also forward to rest so that
        // command-specific parsers (e.g. analyze) can see the flag.
        if (result.command !== undefined) {
          result.rest.push(arg, val);
        }
      } else if (result.command !== undefined) {
        // Command-specific format value (e.g. github-annotations,
        // gitlab-codequality for the analyze command) — pass through to rest
        // without rejecting.
        result.rest.push(arg, val ?? "");
      } else {
        process.stderr.write(
          `sqlever: invalid --format value '${val ?? ""}'. Expected 'text' or 'json'.\n`,
        );
        process.exit(1);
      }
      i += 2;
      continue;
    }
    if (arg === "--db-uri") {
      result.dbUri = argv[++i];
      i++;
      continue;
    }
    if (arg === "--plan-file") {
      result.planFile = argv[++i];
      i++;
      continue;
    }
    if (arg === "--top-dir") {
      result.topDir = argv[++i];
      i++;
      continue;
    }
    if (arg === "--registry") {
      result.registry = argv[++i];
      i++;
      continue;
    }
    if (arg === "--target") {
      result.target = argv[++i];
      i++;
      continue;
    }

    // --- Command or positional argument ---
    if (result.command === undefined) {
      result.command = arg;
    } else {
      result.rest.push(arg);
    }
    i++;
  }

  return result;
}

// ---------------------------------------------------------------------------
// Help text
// ---------------------------------------------------------------------------

function printTopLevelHelp(): void {
  const maxLen = Math.max(...COMMAND_NAMES.map((c) => c.length));
  const cmdLines = COMMAND_NAMES.map(
    (c) => `  ${c.padEnd(maxLen)}  ${COMMANDS[c]}`,
  ).join("\n");

  process.stdout.write(`sqlever — Sqitch-compatible PostgreSQL migration tool

Usage:
  sqlever <command> [options]

Commands:
${cmdLines}

Global options:
  --help, -h         Show this help message
  --version, -V      Show version number
  --format <fmt>     Output format: text (default) or json
  --quiet, -q        Suppress informational output
  --verbose, -v      Show verbose/debug output
  --db-uri <uri>     Database connection URI
  --plan-file <path> Path to plan file (default: sqitch.plan)
  --top-dir <path>   Path to project top directory
  --registry <name>  Registry schema name (default: sqitch)
  --target <target>  Deploy target name

https://github.com/NikolayS/sqlever
`);
}

function printCommandHelp(command: string): void {
  if (!(command in COMMANDS)) {
    process.stderr.write(`sqlever: unknown command '${command}'\n`);
    process.exit(1);
  }
  process.stdout.write(
    `sqlever ${command} — ${COMMANDS[command]}\n\nNo detailed help available yet.\n`,
  );
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

function stubHandler(command: string): never {
  process.stderr.write(`sqlever ${command}: not yet implemented\n`);
  process.exit(1);
  // TypeScript needs this even though process.exit() is noreturn
  throw new Error("unreachable");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

export function main(argv: string[] = process.argv.slice(2)): void {
  const args = parseArgs(argv);

  // --version takes precedence (matches Sqitch behavior)
  if (args.version) {
    process.stdout.write(packageJson.version + "\n");
    process.exit(0);
  }

  // Wire up the output module based on parsed flags
  setConfig({
    format: args.format,
    quiet: args.quiet,
    verbose: args.verbose,
  });

  // --help with no command => top-level help
  if (args.help && !args.command) {
    printTopLevelHelp();
    process.exit(0);
  }

  // No command at all => top-level help
  if (!args.command) {
    printTopLevelHelp();
    process.exit(0);
  }

  // --help with a command => command-specific help
  if (args.help) {
    printCommandHelp(args.command);
    process.exit(0);
  }

  // "help" command — treat like --help for the next argument
  if (args.command === "help") {
    const subcommand = args.rest[0];
    if (subcommand) {
      printCommandHelp(subcommand);
    } else {
      printTopLevelHelp();
    }
    process.exit(0);
  }

  // Unknown command
  if (!(args.command in COMMANDS)) {
    process.stderr.write(`sqlever: unknown command '${args.command}'\n`);
    process.exit(1);
  }

  // --- Dispatch to implemented commands ---
  if (args.command === "init") {
    runInit(args).catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sqlever init: ${msg}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "add") {
    const addOpts = parseAddArgs(args.rest);
    addOpts.topDir = args.topDir;
    runAdd(addOpts).catch((err: unknown) => {
      process.stderr.write(`sqlever add: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "deploy") {
    runDeploy(args).then((exitCode) => {
      if (exitCode !== 0) {
        process.exit(exitCode);
      }
    }).catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sqlever deploy: ${msg}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "log") {
    runLogCommand(args).catch((err: unknown) => {
      process.stderr.write(`sqlever log: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "revert") {
    runRevert(args)
      .then((exitCode) => {
        if (exitCode !== 0) process.exit(exitCode);
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        process.stderr.write(`sqlever revert: ${msg}\n`);
        process.exit(1);
      });
    return;
  }

  if (args.command === "tag") {
    const tagOpts = parseTagArgs(args.rest);
    tagOpts.topDir = args.topDir;
    runTag(tagOpts).catch((err: unknown) => {
      process.stderr.write(`sqlever tag: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "rework") {
    const reworkOpts = parseReworkArgs(args.rest);
    reworkOpts.topDir = args.topDir;
    runRework(reworkOpts).catch((err: unknown) => {
      process.stderr.write(`sqlever rework: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "show") {
    const showOpts = parseShowArgs(args.rest);
    if (args.topDir !== undefined) showOpts.topDir = args.topDir;
    if (args.planFile !== undefined) showOpts.planFile = args.planFile;
    try {
      runShow(showOpts);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sqlever show: ${msg}\n`);
      process.exit(1);
    }
    return;
  }

  if (args.command === "verify") {
    runVerify(args).catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sqlever verify: ${msg}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "status") {
    runStatus(args).catch((err: unknown) => {
      process.stderr.write(`sqlever status: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    });
    return;
  }

  if (args.command === "plan") {
    runPlan(args);
    return;
  }

  if (args.command === "analyze") {
    const analyzeOpts = parseAnalyzeArgs(args.rest);
    if (args.topDir !== undefined) analyzeOpts.topDir = args.topDir;
    if (args.planFile !== undefined) analyzeOpts.planFile = args.planFile;
    runAnalyze(analyzeOpts)
      .then((result) => { if (result.exitCode !== 0) process.exit(result.exitCode); })
      .catch((err: unknown) => { process.stderr.write(`sqlever analyze: ${err instanceof Error ? err.message : String(err)}\n`); process.exit(1); });
    return;
  }

  if (args.command === "doctor") {
    const exitCode = runDoctor(args);
    if (exitCode !== 0) process.exit(exitCode);
    return;
  }

  // Known command — stub handler
  stubHandler(args.command);
}

// Run when executed directly (not when imported by tests)
if (import.meta.main) {
  main();
}
