// src/db/client.ts — PostgreSQL connection wrapper for sqlever
//
// Wraps node-postgres (pg) Client for sqlever's own database operations:
// tracking tables, advisory locks, introspection. NOT for executing
// migration scripts (that uses psql via DD12).
//
// Key behaviors:
// - Parses both db:pg:// and postgresql:// URIs
// - Sets session settings per DD14 (statement_timeout, lock_timeout, etc.)
// - Never logs passwords (uses maskUri from output.ts)
// - Connection errors produce exit code 10 (R6)

import Client from "pg/lib/client";
import { parseUri, sqitchToStandard } from "./uri";
import { error as logError, verbose as logVerbose, maskUri } from "../output";

/** Exit code for database-unreachable errors (SPEC R6). */
export const EXIT_CODE_DB_UNREACHABLE = 10;

/** Options for configuring the DatabaseClient session. */
export interface SessionSettings {
  /** Command name for application_name (e.g., "deploy"). */
  command?: string;
  /** Project name for application_name (e.g., "myproject"). */
  project?: string;
  /** lock_timeout in milliseconds. Default: 5000 (5s). */
  lockTimeout?: number;
  /** idle_in_transaction_session_timeout in milliseconds. Default: 600000 (10 min). */
  idleInTransactionSessionTimeout?: number;
  /** statement_timeout in milliseconds. Default: 0 (disabled). */
  statementTimeout?: number;
}

/** Query result row type. */
export interface QueryResult<T = Record<string, unknown>> {
  rows: T[];
  rowCount: number | null;
  command: string;
}

/**
 * DatabaseClient wraps `pg.Client` with sqlever-specific behavior:
 *
 * - URI parsing (both db:pg:// and postgresql:// schemes)
 * - Session settings (DD14)
 * - Parameterized queries
 * - Transaction wrapper with automatic COMMIT/ROLLBACK
 * - Connection error handling (exit code 10)
 * - Password masking in all log output
 */
export class DatabaseClient {
  private client: InstanceType<typeof Client>;
  private uri: string;
  private connected = false;
  private settings: SessionSettings;

  /**
   * Create a new DatabaseClient.
   *
   * @param uri - Connection URI (db:pg:// or postgresql:// scheme)
   * @param settings - Optional session settings
   */
  constructor(uri: string, settings: SessionSettings = {}) {
    this.uri = uri;
    this.settings = settings;

    // Parse and validate the URI (will throw on bad input)
    const parsed = parseUri(uri);

    // Convert to standard postgresql:// for pg.Client
    const standardUri = sqitchToStandard(uri);

    this.client = new Client({
      connectionString: standardUri,
      // Provide explicit host/port/database so pg doesn't need to re-parse
      host: parsed.host,
      port: parsed.port,
      database: parsed.database || undefined,
      user: parsed.user,
      password: parsed.password,
    });
  }

  /**
   * Connect to the database. Applies session settings after connecting.
   *
   * On connection failure, logs a clear error and exits with code 10.
   */
  async connect(): Promise<void> {
    const maskedUri = maskUri(this.uri);
    logVerbose(`Connecting to ${maskedUri}`);

    try {
      await this.client.connect();
      this.connected = true;
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      logError(`Database unreachable: ${message}`);
      logError(`Connection URI: ${maskedUri}`);
      process.exit(EXIT_CODE_DB_UNREACHABLE);
    }

    await this.setSessionSettings();
    logVerbose(`Connected to ${maskedUri}`);
  }

  /**
   * Disconnect from the database.
   */
  async disconnect(): Promise<void> {
    if (!this.connected) return;

    try {
      await this.client.end();
    } finally {
      this.connected = false;
    }
  }

  /**
   * Execute a parameterized query.
   *
   * @param sql - SQL string, with $1, $2, ... placeholders
   * @param params - Parameter values
   * @returns Query result with rows, rowCount, and command
   */
  async query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<QueryResult<T>> {
    this.assertConnected();

    const result = await this.client.query(sql, params);
    return {
      rows: result.rows as T[],
      rowCount: result.rowCount,
      command: result.command,
    };
  }

  /**
   * Execute a callback inside a database transaction.
   *
   * - Issues BEGIN before the callback
   * - Issues COMMIT if the callback succeeds
   * - Issues ROLLBACK if the callback throws, then re-throws the error
   *
   * @param fn - Async function to execute within the transaction
   * @returns The return value of fn
   */
  async transaction<T>(fn: (client: DatabaseClient) => Promise<T>): Promise<T> {
    this.assertConnected();

    await this.client.query("BEGIN");
    try {
      const result = await fn(this);
      await this.client.query("COMMIT");
      return result;
    } catch (err) {
      await this.client.query("ROLLBACK");
      throw err;
    }
  }

  /**
   * Apply session settings per SPEC DD14.
   *
   * Sets:
   * - statement_timeout = 0 (migrations are inherently long-running)
   * - lock_timeout (configurable, default 5000ms)
   * - idle_in_transaction_session_timeout (configurable, default 600000ms / 10 min)
   * - application_name = 'sqlever/<command>/<project>'
   */
  async setSessionSettings(): Promise<void> {
    const {
      command = "unknown",
      project = "unknown",
      lockTimeout = 5000,
      idleInTransactionSessionTimeout = 600_000,
      statementTimeout = 0,
    } = this.settings;

    const appName = `sqlever/${command}/${project}`;

    // Use a single SET command batch for efficiency. Each SET is a
    // separate statement because PostgreSQL doesn't support multi-SET.
    const statements = [
      `SET statement_timeout = ${statementTimeout}`,
      `SET lock_timeout = ${lockTimeout}`,
      `SET idle_in_transaction_session_timeout = ${idleInTransactionSessionTimeout}`,
      `SET application_name = '${appName}'`,
    ];

    for (const stmt of statements) {
      await this.client.query(stmt);
    }

    logVerbose(
      `Session settings applied: statement_timeout=${statementTimeout}, ` +
        `lock_timeout=${lockTimeout}, ` +
        `idle_in_transaction_session_timeout=${idleInTransactionSessionTimeout}, ` +
        `application_name='${appName}'`,
    );
  }

  /**
   * Whether the client is currently connected.
   */
  get isConnected(): boolean {
    return this.connected;
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private assertConnected(): void {
    if (!this.connected) {
      throw new Error("DatabaseClient is not connected. Call connect() first.");
    }
  }
}
