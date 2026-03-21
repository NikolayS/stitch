// src/commands/deploy-dblab.ts — DBLab integration for sqlever deploy
//
// Test deploy+verify against a full-size production clone before touching prod.
// See SPEC Section 5.8 and issue #108.
//
// Flow:
//   1. Provision a thin clone of production via DBLab API (POST /clone)
//   2. Get the clone's connection URI from the response
//   3. Run deploy+verify against the clone
//   4. If success: report "Clone test passed, safe to deploy to production"
//   5. If failure: report errors, do NOT touch production
//   6. Destroy the clone (DELETE /clone/<id>) in a finally block

import type { ParsedArgs } from "../cli";
import { info, error as logError, verbose } from "../output";

// ---------------------------------------------------------------------------
// DBLab API types
// ---------------------------------------------------------------------------

/** Request body for POST /clone. */
export interface DblabCreateCloneRequest {
  id?: string;
  protected?: boolean;
  db?: {
    username?: string;
    password?: string;
    db_name?: string;
  };
}

/** Response from POST /clone — subset of fields we use. */
export interface DblabCloneResponse {
  id: string;
  status: {
    code: string;
    message: string;
  };
  db: {
    connStr: string;
    host: string;
    port: string;
    username: string;
    password?: string;
    db_name: string;
  };
}

/** Status response used when polling clone readiness. */
export interface DblabCloneStatus {
  id: string;
  status: {
    code: string;
    message: string;
  };
}

// ---------------------------------------------------------------------------
// DBLab client
// ---------------------------------------------------------------------------

/**
 * Minimal DBLab API client.
 *
 * Uses fetch() (globally available in Bun / Node 18+).
 * The `fetchFn` parameter enables dependency injection for testing.
 */
export class DblabClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchFn: typeof fetch;

  constructor(
    baseUrl: string,
    token: string,
    fetchFn: typeof fetch = globalThis.fetch,
  ) {
    // Strip trailing slash for consistency
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.token = token;
    this.fetchFn = fetchFn;
  }

  /**
   * Create a thin clone via POST /clone.
   *
   * @returns The clone metadata including connection URI.
   * @throws Error if the API returns a non-2xx status.
   */
  async createClone(
    request?: DblabCreateCloneRequest,
  ): Promise<DblabCloneResponse> {
    const url = `${this.baseUrl}/clone`;
    const body = request ?? {};

    const response = await this.fetchFn(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Verification-Token": this.token,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(
        `DBLab API error: POST /clone returned ${response.status}${text ? ` — ${text}` : ""}`,
      );
    }

    return (await response.json()) as DblabCloneResponse;
  }

  /**
   * Get clone status via GET /clone/<id>.
   */
  async getCloneStatus(cloneId: string): Promise<DblabCloneStatus> {
    const url = `${this.baseUrl}/clone/${encodeURIComponent(cloneId)}`;

    const response = await this.fetchFn(url, {
      method: "GET",
      headers: {
        "Verification-Token": this.token,
      },
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(
        `DBLab API error: GET /clone/${cloneId} returned ${response.status}${text ? ` — ${text}` : ""}`,
      );
    }

    return (await response.json()) as DblabCloneStatus;
  }

  /**
   * Destroy a clone via DELETE /clone/<id>.
   *
   * This is called in a finally block and should not throw on best-effort
   * cleanup failures.
   */
  async destroyClone(cloneId: string): Promise<void> {
    const url = `${this.baseUrl}/clone/${encodeURIComponent(cloneId)}`;

    const response = await this.fetchFn(url, {
      method: "DELETE",
      headers: {
        "Verification-Token": this.token,
      },
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(
        `DBLab API error: DELETE /clone/${cloneId} returned ${response.status}${text ? ` — ${text}` : ""}`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// DBLab deploy options
// ---------------------------------------------------------------------------

export interface DblabDeployOptions {
  /** DBLab instance URL. */
  dblabUrl: string;
  /** DBLab authentication token. */
  dblabToken: string;
  /** Remaining deploy args to forward. */
  deployArgs: ParsedArgs;
}

/**
 * Parse --dblab-url and --dblab-token from the deploy command's rest args.
 *
 * Returns null if neither flag is present (regular deploy).
 * Throws if only one of the two required flags is provided.
 */
export function parseDblabOptions(
  args: ParsedArgs,
): DblabDeployOptions | null {
  let dblabUrl: string | undefined;
  let dblabToken: string | undefined;
  const filteredRest: string[] = [];

  const rest = args.rest;
  let i = 0;
  while (i < rest.length) {
    const token = rest[i]!;

    if (token === "--dblab-url") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--dblab-url requires a URL value");
      }
      dblabUrl = val;
      i++;
      continue;
    }

    if (token === "--dblab-token") {
      const val = rest[++i];
      if (!val || val.startsWith("-")) {
        throw new Error("--dblab-token requires a token value");
      }
      dblabToken = val;
      i++;
      continue;
    }

    filteredRest.push(token);
    i++;
  }

  // Also check environment variables
  if (!dblabUrl) {
    dblabUrl = process.env.DBLAB_URL;
  }
  if (!dblabToken) {
    dblabToken = process.env.DBLAB_TOKEN;
  }

  // Neither specified — this is a regular deploy, not DBLab
  if (!dblabUrl && !dblabToken) {
    return null;
  }

  // One specified without the other
  if (!dblabUrl) {
    throw new Error(
      "--dblab-url is required when --dblab-token is specified (or set DBLAB_URL env var)",
    );
  }
  if (!dblabToken) {
    throw new Error(
      "--dblab-token is required when --dblab-url is specified (or set DBLAB_TOKEN env var)",
    );
  }

  return {
    dblabUrl,
    dblabToken,
    deployArgs: { ...args, rest: filteredRest },
  };
}

// ---------------------------------------------------------------------------
// DBLab deploy result
// ---------------------------------------------------------------------------

export interface DblabDeployResult {
  /** Whether the clone test passed. */
  success: boolean;
  /** Clone ID that was provisioned. */
  cloneId?: string;
  /** Connection URI used for the clone. */
  cloneUri?: string;
  /** Number of changes deployed to the clone. */
  deployed: number;
  /** Error message if the test failed. */
  error?: string;
  /** Time taken for clone provisioning (ms). */
  cloneProvisionMs?: number;
  /** Time taken for deploy+verify on the clone (ms). */
  deployMs?: number;
  /** Whether the clone was successfully destroyed. */
  cloneDestroyed: boolean;
}

// ---------------------------------------------------------------------------
// Core DBLab deploy logic
// ---------------------------------------------------------------------------

/**
 * Build a PostgreSQL connection URI from DBLab clone response.
 */
export function buildCloneUri(clone: DblabCloneResponse): string {
  const { host, port, username, password, db_name } = clone.db;
  if (clone.db.connStr) {
    return clone.db.connStr;
  }
  const userPart = password
    ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}`
    : encodeURIComponent(username);
  return `postgresql://${userPart}@${host}:${port}/${db_name}`;
}

/**
 * Execute the DBLab deploy workflow.
 *
 * 1. Provision a thin clone
 * 2. Run deploy+verify against it
 * 3. Report results
 * 4. Destroy the clone (always, in finally block)
 *
 * The `deployFn` parameter allows dependency injection for testing.
 */
export async function executeDblabDeploy(
  options: DblabDeployOptions,
  deps: {
    client: DblabClient;
    deployFn: (args: ParsedArgs) => Promise<number>;
  },
): Promise<DblabDeployResult> {
  const { client, deployFn } = deps;

  // Mutable result object — updated throughout the function and in finally.
  // We never spread-copy this; the same object reference is returned.
  const result: DblabDeployResult = {
    success: false,
    deployed: 0,
    cloneDestroyed: false,
  };

  try {
    // Step 1: Provision clone
    info("Provisioning DBLab thin clone...");
    const provisionStart = Date.now();

    let clone: DblabCloneResponse;
    try {
      clone = await client.createClone({
        id: `sqlever-test-${Date.now()}`,
        protected: false,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logError(`Failed to provision DBLab clone: ${msg}`);
      result.error = `Clone provisioning failed: ${msg}`;
      return result;
    }

    result.cloneId = clone.id;
    result.cloneProvisionMs = Date.now() - provisionStart;

    // Check clone status
    if (clone.status.code !== "OK" && clone.status.code !== "CREATING") {
      logError(`DBLab clone status: ${clone.status.code} — ${clone.status.message}`);
      result.error = `Clone not ready: ${clone.status.code} — ${clone.status.message}`;
      return result;
    }

    // Step 2: Build connection URI from clone response
    result.cloneUri = buildCloneUri(clone);

    verbose(`Clone provisioned: id=${result.cloneId}`);
    verbose(`Clone connection URI: ${result.cloneUri}`);
    info(`Clone provisioned in ${result.cloneProvisionMs}ms`);

    // Step 3: Run deploy+verify against the clone
    info("Running deploy+verify against clone...");
    const deployStart = Date.now();

    // Build args with clone URI as the target
    const deployArgs: ParsedArgs = {
      ...options.deployArgs,
      dbUri: result.cloneUri,
      rest: [...options.deployArgs.rest, "--verify", "--no-tui"],
    };

    let exitCode: number;
    try {
      exitCode = await deployFn(deployArgs);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logError(`Deploy against clone failed: ${msg}`);
      result.deployMs = Date.now() - deployStart;
      result.error = `Deploy failed on clone: ${msg}`;
      return result;
    }

    result.deployMs = Date.now() - deployStart;

    // Step 4: Report results
    if (exitCode === 0) {
      result.success = true;
      info(`Clone test passed in ${result.deployMs}ms. Safe to deploy to production.`);
    } else {
      result.error = `Deploy+verify failed on clone (exit code ${exitCode}). Production was NOT touched.`;
      logError(result.error);
    }

    return result;
  } finally {
    // Step 5: Always destroy the clone
    if (result.cloneId) {
      try {
        verbose(`Destroying clone: ${result.cloneId}`);
        await client.destroyClone(result.cloneId);
        result.cloneDestroyed = true;
        verbose("Clone destroyed successfully");
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logError(`Warning: Failed to destroy clone ${result.cloneId}: ${msg}`);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

/**
 * Run the DBLab deploy workflow from CLI args.
 *
 * This is called from the deploy command when --dblab-url/--dblab-token
 * flags are detected.
 *
 * @returns exit code (0 for success, 1 for failure)
 */
export async function runDblabDeploy(
  options: DblabDeployOptions,
  deps?: {
    client?: DblabClient;
    deployFn?: (args: ParsedArgs) => Promise<number>;
  },
): Promise<number> {
  // Lazy import to avoid circular dependency at module level
  const { runDeploy } = await import("./deploy");

  const client = deps?.client ?? new DblabClient(options.dblabUrl, options.dblabToken);
  const deployFn = deps?.deployFn ?? runDeploy;

  const result = await executeDblabDeploy(options, { client, deployFn });

  if (result.success) {
    return 0;
  }
  return 1;
}
