// src/psql.ts — psql shell-out wrapper for script execution
//
// See SPEC.md DD12: sqlever executes migration scripts by shelling out
// to psql, exactly like Sqitch. This guarantees 100% compatibility with
// all psql metacommands (\i, \ir, \set, \copy, \if/\elif/\endif, etc.).
//
// node-postgres (pg) is used only for tracking-table operations,
// advisory locks, and schema introspection — never for user scripts.

import { spawn, type SpawnOptions } from "child_process";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PsqlRunOptions {
  /** PostgreSQL connection URI (postgresql://user:pass@host:port/db). */
  uri: string;

  /** Key-value pairs passed as psql variables via -v key=value. */
  variables?: Record<string, string>;

  /** Wrap execution in a single transaction (--single-transaction). */
  singleTransaction?: boolean;

  /** Override psql binary path (takes precedence over constructor default). */
  dbClient?: string;

  /** Working directory for psql process (for \i relative paths). */
  workingDir?: string;
}

export interface PsqlRunResult {
  /** Process exit code (0 = success). */
  exitCode: number;

  /** Captured stdout. */
  stdout: string;

  /** Captured stderr. */
  stderr: string;

  /** Parsed error info from stderr, if any. */
  error?: PsqlError;
}

export interface PsqlError {
  /** Severity: ERROR, FATAL, PANIC. */
  severity?: string;

  /** The error message text. */
  message: string;

  /** Source file and line where the error was raised, if available. */
  location?: string;

  /** DETAIL line, if present. */
  detail?: string;

  /** HINT line, if present. */
  hint?: string;

  /** CONTEXT line, if present. */
  context?: string;

  /** STATEMENT line, if present. */
  statement?: string;
}

// ---------------------------------------------------------------------------
// Error parsing
// ---------------------------------------------------------------------------

/**
 * Best-effort extraction of structured error information from psql stderr.
 *
 * psql error output looks like:
 *   psql:path/to/file.sql:42: ERROR:  relation "foo" does not exist
 *   LINE 1: SELECT * FROM foo;
 *                          ^
 *   DETAIL:  ...
 *   HINT:  ...
 *
 * Or sometimes just:
 *   ERROR:  syntax error at or near "SELEC"
 */
export function parsePsqlStderr(stderr: string): PsqlError | undefined {
  if (!stderr.trim()) return undefined;

  // Match the primary error line. Two common formats:
  //   psql:<file>:<line>: <SEVERITY>:  <message>
  //   <SEVERITY>:  <message>
  const errorLineRe =
    /^(?:psql:([^:]+:\d+):\s*)?(ERROR|FATAL|PANIC):\s+(.+)$/m;
  const match = stderr.match(errorLineRe);

  if (!match) return undefined;

  const error: PsqlError = {
    message: match[3]!.trim(),
  };

  if (match[2]) error.severity = match[2];
  if (match[1]) error.location = match[1];

  // Extract supplementary fields (DETAIL, HINT, CONTEXT, STATEMENT).
  const detailMatch = stderr.match(/^DETAIL:\s+(.+)$/m);
  if (detailMatch) error.detail = detailMatch[1]!.trim();

  const hintMatch = stderr.match(/^HINT:\s+(.+)$/m);
  if (hintMatch) error.hint = hintMatch[1]!.trim();

  const contextMatch = stderr.match(/^CONTEXT:\s+(.+)$/m);
  if (contextMatch) error.context = contextMatch[1]!.trim();

  const stmtMatch = stderr.match(/^STATEMENT:\s+(.+)$/m);
  if (stmtMatch) error.statement = stmtMatch[1]!.trim();

  return error;
}

// ---------------------------------------------------------------------------
// URI password extraction
// ---------------------------------------------------------------------------

/**
 * Extract the password from a PostgreSQL URI and return the URI without it.
 *
 * This is critical for security: passwords must NEVER appear in process
 * arguments (visible via `ps`). Instead, we pass the password via the
 * PGPASSWORD environment variable.
 *
 * Returns { uri, password } where uri has the password removed.
 */
export function extractPassword(uri: string): {
  cleanUri: string;
  password: string | undefined;
} {
  // Match: scheme://user:password@host...
  // The password is everything between the first : after :// and the last @.
  const re = /^([a-zA-Z][a-zA-Z0-9+.:~-]*:\/\/)([^:@/]+):(.+)@(?=[^@]*$)/;
  const match = uri.match(re);

  if (!match) {
    return { cleanUri: uri, password: undefined };
  }

  const password = match[3]!;
  // Reconstruct URI without password: scheme://user@host...
  const cleanUri = uri.replace(re, "$1$2@");

  return { cleanUri, password };
}

// ---------------------------------------------------------------------------
// Command builder
// ---------------------------------------------------------------------------

export interface PsqlCommand {
  /** The psql binary to invoke. */
  bin: string;

  /** Arguments for the psql process. */
  args: string[];

  /** Environment variables to set on the child process. */
  env: Record<string, string>;

  /** Working directory for the child process. */
  cwd?: string;
}

/**
 * Build the psql command-line arguments and environment.
 *
 * This is a pure function (no I/O) so it can be unit-tested without
 * spawning processes.
 */
export function buildPsqlCommand(
  scriptPath: string,
  options: PsqlRunOptions,
  defaultBin: string,
): PsqlCommand {
  const bin = options.dbClient ?? defaultBin;
  const args: string[] = [];
  const env: Record<string, string> = {};

  // --- Disable .psqlrc ---
  // Both the env var and the flag for belt-and-suspenders safety.
  env["PSQLRC"] = "/dev/null";
  args.push("--no-psqlrc");

  // --- ON_ERROR_STOP=1 --- abort on first error
  args.push("-v", "ON_ERROR_STOP=1");

  // --- Single transaction ---
  if (options.singleTransaction) {
    args.push("--single-transaction");
  }

  // --- User variables ---
  if (options.variables) {
    for (const [key, value] of Object.entries(options.variables)) {
      args.push("-v", `${key}=${value}`);
    }
  }

  // --- Connection URI ---
  // Extract password from URI and pass via PGPASSWORD env var
  // so it never appears in the process argument list (visible via `ps`).
  const { cleanUri, password } = extractPassword(options.uri);
  if (password) {
    env["PGPASSWORD"] = password;
  }
  args.push("--dbname", cleanUri);

  // --- Script file ---
  args.push("-f", scriptPath);

  return {
    bin,
    args,
    env,
    cwd: options.workingDir,
  };
}

// ---------------------------------------------------------------------------
// Subprocess execution
// ---------------------------------------------------------------------------

/**
 * Type for the spawn function, allowing injection for testing.
 */
export type SpawnFn = (
  command: string,
  args: string[],
  options: SpawnOptions,
) => ReturnType<typeof spawn>;

// ---------------------------------------------------------------------------
// PsqlRunner class
// ---------------------------------------------------------------------------

/**
 * Executes SQL scripts by shelling out to psql.
 *
 * Usage:
 *   const runner = new PsqlRunner();
 *   const result = await runner.run("deploy/001-init.sql", {
 *     uri: "postgresql://user@localhost/mydb",
 *     singleTransaction: true,
 *     variables: { schema: "public" },
 *   });
 *
 *   if (result.exitCode !== 0) {
 *     console.error("psql failed:", result.error?.message ?? result.stderr);
 *   }
 */
export class PsqlRunner {
  private readonly defaultBin: string;
  private readonly spawnFn: SpawnFn;

  /**
   * @param psqlPath — path to the psql binary (default: "psql")
   * @param spawnFn — subprocess spawn function (for testing)
   */
  constructor(psqlPath?: string, spawnFn?: SpawnFn) {
    this.defaultBin = psqlPath ?? "psql";
    this.spawnFn = spawnFn ?? spawn;
  }

  /**
   * Execute a SQL script file via psql.
   *
   * @param scriptPath — path to the .sql file (relative to workingDir or absolute)
   * @param options — connection URI, variables, transaction mode, etc.
   * @returns exit code, captured stdout/stderr, and parsed error (if any)
   */
  async run(scriptPath: string, options: PsqlRunOptions): Promise<PsqlRunResult> {
    const cmd = buildPsqlCommand(scriptPath, options, this.defaultBin);

    return new Promise<PsqlRunResult>((resolve, reject) => {
      const child = this.spawnFn(cmd.bin, cmd.args, {
        cwd: cmd.cwd,
        env: { ...process.env, ...cmd.env },
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";

      child.stdout?.on("data", (chunk: Buffer) => {
        stdout += chunk.toString();
      });

      child.stderr?.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });

      child.on("error", (err) => {
        // Spawn failure (e.g., psql not found)
        reject(err);
      });

      child.on("close", (code) => {
        const exitCode = code ?? 1;
        const error = exitCode !== 0 ? parsePsqlStderr(stderr) : undefined;

        resolve({
          exitCode,
          stdout,
          stderr,
          error,
        });
      });
    });
  }
}
