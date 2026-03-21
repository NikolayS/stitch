// src/expand-contract/triggers.ts — Bidirectional sync trigger generation
//
// Generates CREATE/DROP SQL for bidirectional sync triggers used during the
// expand phase of expand/contract migrations. Implements SPEC Section 5.4.
//
// Trigger design:
//   - BEFORE INSERT OR UPDATE trigger on the target table
//   - Bidirectional: writes to old column sync to new, and vice versa
//   - Recursion guard: pg_trigger_depth() prevents infinite loops between
//     the two sync directions. This is preferred over SET LOCAL because
//     SET LOCAL suppresses ALL subsequent trigger fires in the transaction,
//     not just recursive ones (SPEC 5.4, point 1).
//   - All generated trigger names use the `sqlever_sync_` prefix
//
// Partitioned tables (SPEC 5.4, point 3):
//   Triggers are installed on the parent table. PG 14+ (sqlever's minimum)
//   automatically inherits triggers to all partitions.
//
// Trigger installation lock (SPEC 5.4, point 5):
//   CREATE TRIGGER takes AccessExclusiveLock on the table. This is brief
//   (metadata-only) but on high-traffic tables with long-running queries
//   it may block. Callers should consider SET lock_timeout and retry logic.
//
// Known limitation — logical replication (SPEC 5.4, point 2):
//   Triggers do not fire on logical replication subscribers by default.
//   If the target database is a subscriber, sync triggers will not fire,
//   leaving columns out of sync. ALTER TABLE ... ENABLE ALWAYS TRIGGER
//   is possible but risky (may cause loops).
//
// COPY performance (SPEC 5.4, point 4):
//   BEFORE INSERT triggers fire during COPY, which may significantly
//   impact bulk load performance during the expand phase.

import {
  syncTriggerName,
  syncTriggerFunctionName,
  type ExpandContractConfig,
} from "./generator";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Direction of sync: old->new or new->old. */
export type SyncDirection = "forward" | "reverse";

/** Options for generating a sync trigger. */
export interface SyncTriggerOptions {
  /** Target table (schema-qualified, e.g., "public.users"). */
  table: string;
  /** Old column name. */
  oldColumn: string;
  /** New column name. */
  newColumn: string;
  /** Old column type. */
  oldType?: string;
  /** New column type. */
  newType?: string;
  /** Cast expression for old -> new (forward). E.g., "NEW.name::text". */
  castForward?: string;
  /** Cast expression for new -> old (reverse). E.g., "NEW.full_name::varchar". */
  castReverse?: string;
  /** Default value expression for old column (used when new->old sync encounters NULL). */
  oldDefault?: string;
  /** Default value expression for new column (used when old->new sync encounters NULL). */
  newDefault?: string;
  /**
   * lock_timeout for trigger installation (e.g., "1s", "5s").
   * If set, wraps CREATE TRIGGER in SET lock_timeout / RESET lock_timeout.
   */
  lockTimeout?: string;
}

/** Result from generating trigger SQL. */
export interface TriggerSQL {
  /** The CREATE FUNCTION + CREATE TRIGGER SQL. */
  createSQL: string;
  /** The DROP TRIGGER + DROP FUNCTION SQL. */
  dropSQL: string;
  /** The trigger name (for reference). */
  triggerName: string;
  /** The trigger function name (for reference). */
  functionName: string;
}

// ---------------------------------------------------------------------------
// SQL generation — trigger function body
// ---------------------------------------------------------------------------

/**
 * Build the forward sync expression (old column -> new column).
 *
 * If a castForward expression is provided, wraps it in parentheses.
 * Otherwise, copies the old column value directly.
 * If a newDefault is provided, uses COALESCE for NULL handling.
 */
export function forwardSyncExpression(opts: SyncTriggerOptions): string {
  const base = opts.castForward
    ? `(${opts.castForward})`
    : `NEW.${opts.oldColumn}`;

  if (opts.newDefault) {
    return `COALESCE(${base}, ${opts.newDefault})`;
  }
  return base;
}

/**
 * Build the reverse sync expression (new column -> old column).
 *
 * If a castReverse expression is provided, wraps it in parentheses.
 * Otherwise, copies the new column value directly.
 * If an oldDefault is provided, uses COALESCE for NULL handling.
 */
export function reverseSyncExpression(opts: SyncTriggerOptions): string {
  const base = opts.castReverse
    ? `(${opts.castReverse})`
    : `NEW.${opts.newColumn}`;

  if (opts.oldDefault) {
    return `COALESCE(${base}, ${opts.oldDefault})`;
  }
  return base;
}

/**
 * Generate the PL/pgSQL trigger function body for bidirectional sync.
 *
 * The function implements:
 *   1. Recursion guard using pg_trigger_depth()
 *   2. Forward sync (old -> new) on INSERT or UPDATE of old column
 *   3. Reverse sync (new -> old) on INSERT or UPDATE of new column
 *
 * Recursion guard rationale (SPEC 5.4, point 1):
 *   We use `pg_trigger_depth() < 2` to allow the trigger to fire once
 *   (depth 0 = direct user DML, depth 1 = first cascade) but NOT
 *   recursively (depth >= 2). Combined with the `sqlever_sync_` name
 *   prefix, this allows multiple independent sync triggers on different
 *   tables to fire within the same transaction while preventing infinite
 *   recursion between forward and reverse sync on the same table.
 */
export function generateTriggerFunctionBody(opts: SyncTriggerOptions): string {
  const fwd = forwardSyncExpression(opts);
  const rev = reverseSyncExpression(opts);

  return `
BEGIN
  -- Recursion guard: pg_trigger_depth() prevents infinite recursion
  -- between bidirectional sync triggers. Depth 0 = direct DML,
  -- depth 1 = first cascade from another sqlever_sync_ trigger.
  -- At depth >= 2 we are recursing and must stop.
  IF pg_trigger_depth() < 2 THEN
    IF TG_OP = 'INSERT' THEN
      -- On INSERT: sync both directions, respecting explicit values
      IF NEW.${opts.newColumn} IS NULL THEN
        -- New column not explicitly set; sync from old
        NEW.${opts.newColumn} := ${fwd};
      END IF;
      IF NEW.${opts.oldColumn} IS NULL AND NEW.${opts.newColumn} IS NOT NULL THEN
        -- Old column not set but new is; sync from new
        NEW.${opts.oldColumn} := ${rev};
      END IF;
    ELSIF TG_OP = 'UPDATE' THEN
      -- On UPDATE: detect which column changed and sync the other
      IF NEW.${opts.oldColumn} IS DISTINCT FROM OLD.${opts.oldColumn} THEN
        NEW.${opts.newColumn} := ${fwd};
      ELSIF NEW.${opts.newColumn} IS DISTINCT FROM OLD.${opts.newColumn} THEN
        NEW.${opts.oldColumn} := ${rev};
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END;`.trim();
}

// ---------------------------------------------------------------------------
// SQL generation — CREATE statements
// ---------------------------------------------------------------------------

/**
 * Generate the CREATE FUNCTION SQL for the sync trigger function.
 */
export function generateCreateFunction(opts: SyncTriggerOptions): string {
  const fnName = syncTriggerFunctionName(opts.table, opts.oldColumn, opts.newColumn);
  const body = generateTriggerFunctionBody(opts);

  return `CREATE OR REPLACE FUNCTION ${fnName}()
RETURNS trigger
LANGUAGE plpgsql AS $$
${body}
$$;`;
}

/**
 * Generate the CREATE TRIGGER SQL.
 *
 * Installs a BEFORE INSERT OR UPDATE trigger on the target table.
 * For partitioned tables (PG 14+), installing on the parent table
 * automatically inherits to all partitions (SPEC 5.4, point 3).
 */
export function generateCreateTrigger(opts: SyncTriggerOptions): string {
  const trigName = syncTriggerName(opts.table, opts.oldColumn, opts.newColumn);
  const fnName = syncTriggerFunctionName(opts.table, opts.oldColumn, opts.newColumn);

  return `CREATE TRIGGER ${trigName}
  BEFORE INSERT OR UPDATE ON ${opts.table}
  FOR EACH ROW EXECUTE FUNCTION ${fnName}();`;
}

/**
 * Generate the complete CREATE SQL (function + trigger), optionally
 * wrapped with lock_timeout for safe installation on high-traffic tables.
 *
 * Trigger installation lock (SPEC 5.4, point 5):
 *   CREATE TRIGGER takes AccessExclusiveLock. On high-traffic tables,
 *   this may block behind long-running queries. Setting lock_timeout
 *   causes the statement to fail fast instead of blocking indefinitely.
 *   The caller should implement retry logic.
 */
export function generateCreateSQL(opts: SyncTriggerOptions): string {
  const fnSQL = generateCreateFunction(opts);
  const trigSQL = generateCreateTrigger(opts);

  if (opts.lockTimeout) {
    return `-- Set lock_timeout for safe trigger installation on high-traffic tables
-- CREATE TRIGGER takes AccessExclusiveLock (brief, metadata-only).
-- If the lock cannot be acquired within ${opts.lockTimeout}, the statement
-- will fail and should be retried.
SET lock_timeout = '${opts.lockTimeout}';

${fnSQL}

${trigSQL}

RESET lock_timeout;`;
  }

  return `${fnSQL}

${trigSQL}`;
}

// ---------------------------------------------------------------------------
// SQL generation — DROP statements
// ---------------------------------------------------------------------------

/**
 * Generate the DROP TRIGGER SQL.
 */
export function generateDropTrigger(opts: SyncTriggerOptions): string {
  const trigName = syncTriggerName(opts.table, opts.oldColumn, opts.newColumn);
  return `DROP TRIGGER IF EXISTS ${trigName} ON ${opts.table};`;
}

/**
 * Generate the DROP FUNCTION SQL.
 */
export function generateDropFunction(opts: SyncTriggerOptions): string {
  const fnName = syncTriggerFunctionName(opts.table, opts.oldColumn, opts.newColumn);
  return `DROP FUNCTION IF EXISTS ${fnName}();`;
}

/**
 * Generate the complete DROP SQL (trigger + function).
 * Trigger must be dropped before the function.
 */
export function generateDropSQL(opts: SyncTriggerOptions): string {
  return `${generateDropTrigger(opts)}
${generateDropFunction(opts)}`;
}

// ---------------------------------------------------------------------------
// Combined generation
// ---------------------------------------------------------------------------

/**
 * Generate both CREATE and DROP SQL for a bidirectional sync trigger.
 *
 * This is the primary entry point for trigger generation. Returns a
 * TriggerSQL object containing the complete CREATE and DROP statements,
 * plus the trigger and function names for reference.
 */
export function generateSyncTrigger(opts: SyncTriggerOptions): TriggerSQL {
  const triggerName = syncTriggerName(opts.table, opts.oldColumn, opts.newColumn);
  const functionName = syncTriggerFunctionName(opts.table, opts.oldColumn, opts.newColumn);

  return {
    createSQL: generateCreateSQL(opts),
    dropSQL: generateDropSQL(opts),
    triggerName,
    functionName,
  };
}

// ---------------------------------------------------------------------------
// Conversion from ExpandContractConfig
// ---------------------------------------------------------------------------

/**
 * Convert an ExpandContractConfig (from generator.ts) to SyncTriggerOptions.
 *
 * This bridges the existing generator module with the trigger module,
 * allowing the generator to delegate trigger SQL generation here.
 */
export function configToTriggerOptions(
  config: ExpandContractConfig,
): SyncTriggerOptions {
  return {
    table: config.table,
    oldColumn: config.oldColumn,
    newColumn: config.newColumn,
    oldType: config.oldType,
    newType: config.newType,
    castForward: config.castForward,
    castReverse: config.castReverse,
  };
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/**
 * Validate SyncTriggerOptions. Returns an error message or null.
 */
export function validateTriggerOptions(opts: SyncTriggerOptions): string | null {
  if (!opts.table) {
    return "table is required";
  }
  if (!opts.oldColumn) {
    return "oldColumn is required";
  }
  if (!opts.newColumn) {
    return "newColumn is required";
  }
  if (opts.oldColumn === opts.newColumn) {
    return "oldColumn and newColumn must be different";
  }
  // Validate identifier characters (basic SQL injection prevention)
  const identRe = /^[a-zA-Z_][a-zA-Z0-9_.]*$/;
  if (!identRe.test(opts.table)) {
    return `invalid table name: '${opts.table}'`;
  }
  if (!identRe.test(opts.oldColumn)) {
    return `invalid column name: '${opts.oldColumn}'`;
  }
  if (!identRe.test(opts.newColumn)) {
    return `invalid column name: '${opts.newColumn}'`;
  }
  return null;
}

/**
 * Generate sync trigger SQL with validation.
 *
 * Throws if the options are invalid. Otherwise returns the TriggerSQL.
 */
export function generateSyncTriggerSafe(opts: SyncTriggerOptions): TriggerSQL {
  const err = validateTriggerOptions(opts);
  if (err) {
    throw new Error(`Invalid sync trigger options: ${err}`);
  }
  return generateSyncTrigger(opts);
}
