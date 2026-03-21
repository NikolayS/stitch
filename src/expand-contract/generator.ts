// src/expand-contract/generator.ts — Expand/contract migration pair generator
//
// Generates linked expand + contract migration pairs for zero-downtime
// schema changes. Implements SPEC Section 5.4.
//
// Expand phase (backward-compatible):
//   - Add new column alongside old
//   - Install sync trigger (bidirectional, with recursion guard)
//
// Contract phase (after full app rollout):
//   - Verify all rows backfilled
//   - Drop sync trigger
//   - Drop old column
//
// The two changes are linked in the plan with the contract requiring
// the expand change.

import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { appendChange } from "../plan/writer";
import { computeChangeId, type ChangeIdInput, type Change } from "../plan/types";
import {
  readPlanInfo,
  getPlannerIdentity,
  nowTimestamp,
  type AddOptions,
} from "../commands/add";
import type { MergedConfig } from "../config/index";
import { info, error, verbose } from "../output";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Supported expand/contract operation types. */
export type ExpandOperation = "rename_col" | "change_type";

/** Configuration for an expand/contract migration pair. */
export interface ExpandContractConfig {
  /** Base name for the migration (e.g., "rename_users_name"). */
  name: string;
  /** The operation type. */
  operation: ExpandOperation;
  /** Target table (schema-qualified, e.g., "public.users"). */
  table: string;
  /** Old column name. */
  oldColumn: string;
  /** New column name. */
  newColumn: string;
  /** New column type (for change_type; defaults to same type for rename). */
  newType?: string;
  /** Old column type (used in revert scripts). */
  oldType?: string;
  /** Type cast expression for expand direction (old -> new). */
  castForward?: string;
  /** Type cast expression for contract revert (new -> old). */
  castReverse?: string;
  /** Note for the plan entries. */
  note: string;
  /** Additional requires dependencies. */
  requires: string[];
  /** Conflict dependencies. */
  conflicts: string[];
}

/** Result of generating an expand/contract pair. */
export interface ExpandContractResult {
  /** Name of the expand change. */
  expandName: string;
  /** Name of the contract change. */
  contractName: string;
  /** Paths to all created files. */
  files: string[];
  /** The expand Change object (for plan). */
  expandChange: Change;
  /** The contract Change object (for plan). */
  contractChange: Change;
}

// ---------------------------------------------------------------------------
// Naming conventions
// ---------------------------------------------------------------------------

/**
 * Derive expand and contract change names from a base name.
 *
 * Convention: `<base>_expand` and `<base>_contract`
 */
export function deriveChangeNames(baseName: string): {
  expandName: string;
  contractName: string;
} {
  return {
    expandName: `${baseName}_expand`,
    contractName: `${baseName}_contract`,
  };
}

/**
 * Derive the sync trigger name for a given table and column pair.
 *
 * All sqlever-generated sync triggers use the `sqlever_sync_` prefix
 * per SPEC 5.4 (recursion guard relies on this prefix).
 */
export function syncTriggerName(
  table: string,
  oldColumn: string,
  newColumn: string,
): string {
  // Strip schema prefix for the trigger name
  const tableName = table.includes(".") ? table.split(".").pop()! : table;
  return `sqlever_sync_${tableName}_${oldColumn}_${newColumn}`;
}

/**
 * Derive the sync trigger function name.
 */
export function syncTriggerFunctionName(
  table: string,
  oldColumn: string,
  newColumn: string,
): string {
  const tableName = table.includes(".") ? table.split(".").pop()! : table;
  return `sqlever_sync_fn_${tableName}_${oldColumn}_${newColumn}`;
}

// ---------------------------------------------------------------------------
// SQL Templates — Expand phase
// ---------------------------------------------------------------------------

/**
 * Generate the expand deploy SQL script.
 *
 * Actions:
 *   1. Add new column (same type as old, or specified new type)
 *   2. Create sync trigger function with recursion guard
 *   3. Install BEFORE INSERT OR UPDATE trigger on the table
 */
export function expandDeployTemplate(config: ExpandContractConfig): string {
  const trigName = syncTriggerName(config.table, config.oldColumn, config.newColumn);
  const fnName = syncTriggerFunctionName(config.table, config.oldColumn, config.newColumn);
  const colType = config.newType ?? config.oldType ?? "text";
  const castFwd = config.castForward ? `(${config.castForward})` : `NEW.${config.oldColumn}`;
  const castRev = config.castReverse ? `(${config.castReverse})` : `NEW.${config.newColumn}`;

  return `-- Deploy ${config.name}_expand
-- Expand phase: add new column + sync trigger
-- Table: ${config.table}
-- Operation: ${config.operation} (${config.oldColumn} -> ${config.newColumn})

BEGIN;

-- 1. Add the new column
ALTER TABLE ${config.table} ADD COLUMN ${config.newColumn} ${colType};

-- 2. Create sync trigger function with recursion guard
--    Uses pg_trigger_depth() to prevent infinite recursion between
--    bidirectional sync triggers. See SPEC 5.4 for rationale.
CREATE OR REPLACE FUNCTION ${fnName}()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF pg_trigger_depth() < 2 THEN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
      -- Sync old -> new when old column is modified
      IF NEW.${config.oldColumn} IS DISTINCT FROM OLD.${config.oldColumn}
         OR (TG_OP = 'INSERT') THEN
        NEW.${config.newColumn} := ${castFwd};
      END IF;
      -- Sync new -> old when new column is modified
      IF NEW.${config.newColumn} IS DISTINCT FROM OLD.${config.newColumn}
         OR (TG_OP = 'INSERT' AND NEW.${config.newColumn} IS NOT NULL) THEN
        NEW.${config.oldColumn} := ${castRev};
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

-- 3. Install the sync trigger
CREATE TRIGGER ${trigName}
  BEFORE INSERT OR UPDATE ON ${config.table}
  FOR EACH ROW EXECUTE FUNCTION ${fnName}();

COMMIT;
`;
}

/**
 * Generate the expand revert SQL script.
 *
 * Reverting the expand phase:
 *   1. Drop sync trigger
 *   2. Drop sync trigger function
 *   3. Drop new column
 */
export function expandRevertTemplate(config: ExpandContractConfig): string {
  const trigName = syncTriggerName(config.table, config.oldColumn, config.newColumn);
  const fnName = syncTriggerFunctionName(config.table, config.oldColumn, config.newColumn);

  return `-- Revert ${config.name}_expand
-- Revert expand phase: drop trigger + new column

BEGIN;

DROP TRIGGER IF EXISTS ${trigName} ON ${config.table};
DROP FUNCTION IF EXISTS ${fnName}();
ALTER TABLE ${config.table} DROP COLUMN IF EXISTS ${config.newColumn};

COMMIT;
`;
}

/**
 * Generate the expand verify SQL script.
 */
export function expandVerifyTemplate(config: ExpandContractConfig): string {
  const trigName = syncTriggerName(config.table, config.oldColumn, config.newColumn);

  return `-- Verify ${config.name}_expand
-- Verify: new column exists and sync trigger is installed

BEGIN;

-- Verify the new column exists
SELECT ${config.newColumn} FROM ${config.table} WHERE false;

-- Verify the sync trigger exists
SELECT 1 FROM pg_trigger
WHERE tgname = '${trigName}'
  AND tgrelid = '${config.table}'::regclass;

ROLLBACK;
`;
}

// ---------------------------------------------------------------------------
// SQL Templates — Contract phase
// ---------------------------------------------------------------------------

/**
 * Generate the contract deploy SQL script.
 *
 * Actions:
 *   1. Verify all rows are backfilled (new column has no NULLs where
 *      old column is NOT NULL)
 *   2. Drop sync trigger + function
 *   3. Drop old column
 */
export function contractDeployTemplate(config: ExpandContractConfig): string {
  const trigName = syncTriggerName(config.table, config.oldColumn, config.newColumn);
  const fnName = syncTriggerFunctionName(config.table, config.oldColumn, config.newColumn);

  return `-- Deploy ${config.name}_contract
-- Contract phase: verify backfill, drop trigger + old column
-- Table: ${config.table}
-- Operation: ${config.operation} (${config.oldColumn} -> ${config.newColumn})

BEGIN;

-- 1. Verify backfill is complete: no rows where old column has a value
--    but new column is NULL
DO $$
DECLARE
  unsynced_count bigint;
BEGIN
  SELECT count(*) INTO unsynced_count
  FROM ${config.table}
  WHERE ${config.oldColumn} IS NOT NULL
    AND ${config.newColumn} IS NULL;

  IF unsynced_count > 0 THEN
    RAISE EXCEPTION 'Backfill incomplete: % rows in ${config.table} have ${config.oldColumn} set but ${config.newColumn} is NULL. Run backfill before contracting.', unsynced_count;
  END IF;
END;
$$;

-- 2. Drop sync trigger and function
DROP TRIGGER IF EXISTS ${trigName} ON ${config.table};
DROP FUNCTION IF EXISTS ${fnName}();

-- 3. Drop old column
ALTER TABLE ${config.table} DROP COLUMN ${config.oldColumn};

COMMIT;
`;
}

/**
 * Generate the contract revert SQL script.
 *
 * Reverting the contract phase:
 *   1. Re-add the old column
 *   2. Re-create the sync trigger
 *   3. Backfill old column from new column
 */
export function contractRevertTemplate(config: ExpandContractConfig): string {
  const trigName = syncTriggerName(config.table, config.oldColumn, config.newColumn);
  const fnName = syncTriggerFunctionName(config.table, config.oldColumn, config.newColumn);
  const oldColType = config.oldType ?? config.newType ?? "text";
  const castRev = config.castReverse ? `(${config.castReverse})` : config.newColumn;

  return `-- Revert ${config.name}_contract
-- Revert contract: re-add old column + sync trigger

BEGIN;

-- 1. Re-add old column
ALTER TABLE ${config.table} ADD COLUMN ${config.oldColumn} ${oldColType};

-- 2. Re-create sync trigger function
CREATE OR REPLACE FUNCTION ${fnName}()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF pg_trigger_depth() < 2 THEN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
      IF NEW.${config.oldColumn} IS DISTINCT FROM OLD.${config.oldColumn}
         OR (TG_OP = 'INSERT') THEN
        NEW.${config.newColumn} := NEW.${config.oldColumn};
      END IF;
      IF NEW.${config.newColumn} IS DISTINCT FROM OLD.${config.newColumn}
         OR (TG_OP = 'INSERT' AND NEW.${config.newColumn} IS NOT NULL) THEN
        NEW.${config.oldColumn} := ${castRev};
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

-- 3. Re-install sync trigger
CREATE TRIGGER ${trigName}
  BEFORE INSERT OR UPDATE ON ${config.table}
  FOR EACH ROW EXECUTE FUNCTION ${fnName}();

-- 4. Backfill old column from new column
UPDATE ${config.table} SET ${config.oldColumn} = ${castRev}
WHERE ${config.newColumn} IS NOT NULL;

COMMIT;
`;
}

/**
 * Generate the contract verify SQL script.
 */
export function contractVerifyTemplate(config: ExpandContractConfig): string {
  return `-- Verify ${config.name}_contract
-- Verify: old column is gone, trigger is gone

BEGIN;

-- Verify the old column no longer exists (this should raise an error if it does)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema || '.' || table_name = '${config.table}'
      AND column_name = '${config.oldColumn}'
  ) THEN
    RAISE EXCEPTION 'Old column ${config.oldColumn} still exists on ${config.table}';
  END IF;
END;
$$;

-- Verify the new column exists
SELECT ${config.newColumn} FROM ${config.table} WHERE false;

ROLLBACK;
`;
}

// ---------------------------------------------------------------------------
// Argument parsing for --expand flag
// ---------------------------------------------------------------------------

export interface ExpandAddOptions extends AddOptions {
  /** Whether --expand flag was provided. */
  expand: boolean;
  /** Target table for the expand/contract operation. */
  table: string;
  /** Old column name. */
  oldColumn: string;
  /** New column name. */
  newColumn: string;
  /** New column type (optional, for type changes). */
  newType?: string;
  /** Old column type (optional, for revert scripts). */
  oldType?: string;
  /** Cast expression for old -> new direction. */
  castForward?: string;
  /** Cast expression for new -> old direction. */
  castReverse?: string;
}

/**
 * Parse additional expand/contract flags from CLI args.
 *
 * Expected usage:
 *   sqlever add <name> --expand --table users --old-column name --new-column full_name
 *     [--new-type text] [--old-type varchar] [--cast-forward expr] [--cast-reverse expr]
 */
export function parseExpandArgs(rest: string[]): ExpandAddOptions {
  const opts: ExpandAddOptions = {
    name: "",
    note: "",
    requires: [],
    conflicts: [],
    noVerify: false,
    expand: false,
    table: "",
    oldColumn: "",
    newColumn: "",
  };

  let i = 0;
  while (i < rest.length) {
    const arg = rest[i]!;

    if (arg === "--expand") {
      opts.expand = true;
      i++;
      continue;
    }
    if (arg === "--table") {
      opts.table = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "--old-column") {
      opts.oldColumn = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "--new-column") {
      opts.newColumn = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "--new-type") {
      opts.newType = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "--old-type") {
      opts.oldType = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "--cast-forward") {
      opts.castForward = rest[++i] ?? "";
      i++;
      continue;
    }
    if (arg === "--cast-reverse") {
      opts.castReverse = rest[++i] ?? "";
      i++;
      continue;
    }
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
// Validation
// ---------------------------------------------------------------------------

/**
 * Validate expand/contract options. Returns an error message or null.
 */
export function validateExpandOptions(opts: ExpandAddOptions): string | null {
  if (!opts.name) {
    return "change name is required";
  }
  if (!/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(opts.name)) {
    return `invalid change name '${opts.name}'. Names must start with a letter or underscore and contain only letters, digits, underscores, and hyphens.`;
  }
  if (!opts.table) {
    return "--table is required for --expand migrations";
  }
  if (!opts.oldColumn) {
    return "--old-column is required for --expand migrations";
  }
  if (!opts.newColumn) {
    return "--new-column is required for --expand migrations";
  }
  if (opts.oldColumn === opts.newColumn) {
    return "old column and new column must be different";
  }
  return null;
}

/**
 * Determine the operation type from the options.
 */
export function inferOperation(opts: ExpandAddOptions): ExpandOperation {
  if (opts.newType && opts.oldType && opts.newType !== opts.oldType) {
    return "change_type";
  }
  return "rename_col";
}

// ---------------------------------------------------------------------------
// Core generator
// ---------------------------------------------------------------------------

/**
 * Generate an expand/contract migration pair.
 *
 * Creates:
 *   - deploy/<name>_expand.sql
 *   - revert/<name>_expand.sql
 *   - verify/<name>_expand.sql (unless --no-verify)
 *   - deploy/<name>_contract.sql
 *   - revert/<name>_contract.sql
 *   - verify/<name>_contract.sql (unless --no-verify)
 *
 * And appends both changes to the plan file with the contract
 * depending on the expand change.
 */
export async function generateExpandContract(
  opts: ExpandAddOptions,
  config: MergedConfig,
  env?: Record<string, string | undefined>,
): Promise<ExpandContractResult> {
  const environment = env ?? process.env;

  // Validate
  const validationError = validateExpandOptions(opts);
  if (validationError) {
    throw new Error(validationError);
  }

  const topDir = resolve(opts.topDir ?? config.core.top_dir);
  const deployDir = resolve(topDir, config.core.deploy_dir);
  const revertDir = resolve(topDir, config.core.revert_dir);
  const verifyDir = resolve(topDir, config.core.verify_dir);
  const planPath = resolve(topDir, config.core.plan_file);

  // Ensure plan file exists
  if (!existsSync(planPath)) {
    throw new Error(`plan file not found at ${planPath}. Run 'sqlever init' first.`);
  }

  // Read plan info
  const planInfo = readPlanInfo(planPath);
  const { expandName, contractName } = deriveChangeNames(opts.name);

  // Check for duplicate names
  if (planInfo.existingNames.has(expandName)) {
    throw new Error(`change '${expandName}' already exists in the plan.`);
  }
  if (planInfo.existingNames.has(contractName)) {
    throw new Error(`change '${contractName}' already exists in the plan.`);
  }

  // Get planner identity
  const planner = getPlannerIdentity(environment);

  // Build expand/contract config
  const ecConfig: ExpandContractConfig = {
    name: opts.name,
    operation: inferOperation(opts),
    table: opts.table,
    oldColumn: opts.oldColumn,
    newColumn: opts.newColumn,
    newType: opts.newType,
    oldType: opts.oldType,
    castForward: opts.castForward,
    castReverse: opts.castReverse,
    note: opts.note,
    requires: opts.requires,
    conflicts: opts.conflicts,
  };

  // Create directories
  mkdirSync(deployDir, { recursive: true });
  mkdirSync(revertDir, { recursive: true });
  if (!opts.noVerify) {
    mkdirSync(verifyDir, { recursive: true });
  }

  // Check for existing files
  const files: string[] = [];
  const expandDeployPath = join(deployDir, `${expandName}.sql`);
  const expandRevertPath = join(revertDir, `${expandName}.sql`);
  const expandVerifyPath = join(verifyDir, `${expandName}.sql`);
  const contractDeployPath = join(deployDir, `${contractName}.sql`);
  const contractRevertPath = join(revertDir, `${contractName}.sql`);
  const contractVerifyPath = join(verifyDir, `${contractName}.sql`);

  for (const path of [expandDeployPath, expandRevertPath, contractDeployPath, contractRevertPath]) {
    if (existsSync(path)) {
      throw new Error(`file already exists at ${path}`);
    }
  }
  if (!opts.noVerify) {
    if (existsSync(expandVerifyPath)) {
      throw new Error(`file already exists at ${expandVerifyPath}`);
    }
    if (existsSync(contractVerifyPath)) {
      throw new Error(`file already exists at ${contractVerifyPath}`);
    }
  }

  // Generate SQL files
  writeFileSync(expandDeployPath, expandDeployTemplate(ecConfig), "utf-8");
  files.push(expandDeployPath);
  verbose(`Created ${expandDeployPath}`);

  writeFileSync(expandRevertPath, expandRevertTemplate(ecConfig), "utf-8");
  files.push(expandRevertPath);
  verbose(`Created ${expandRevertPath}`);

  if (!opts.noVerify) {
    writeFileSync(expandVerifyPath, expandVerifyTemplate(ecConfig), "utf-8");
    files.push(expandVerifyPath);
    verbose(`Created ${expandVerifyPath}`);
  }

  writeFileSync(contractDeployPath, contractDeployTemplate(ecConfig), "utf-8");
  files.push(contractDeployPath);
  verbose(`Created ${contractDeployPath}`);

  writeFileSync(contractRevertPath, contractRevertTemplate(ecConfig), "utf-8");
  files.push(contractRevertPath);
  verbose(`Created ${contractRevertPath}`);

  if (!opts.noVerify) {
    writeFileSync(contractVerifyPath, contractVerifyTemplate(ecConfig), "utf-8");
    files.push(contractVerifyPath);
    verbose(`Created ${contractVerifyPath}`);
  }

  // Build expand Change and append to plan
  const expandTimestamp = nowTimestamp();
  const expandRequires = [...opts.requires];
  const expandChangeIdInput: ChangeIdInput = {
    project: planInfo.projectName,
    uri: planInfo.projectUri,
    change: expandName,
    parent: planInfo.lastChangeId,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: expandTimestamp,
    requires: expandRequires,
    conflicts: opts.conflicts,
    note: opts.note ? `[expand] ${opts.note}` : `[expand] ${ecConfig.operation}: ${opts.table}.${opts.oldColumn} -> ${opts.newColumn}`,
  };

  const expandChangeId = computeChangeId(expandChangeIdInput);

  const expandChange: Change = {
    change_id: expandChangeId,
    name: expandName,
    project: planInfo.projectName,
    note: expandChangeIdInput.note,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: expandTimestamp,
    requires: expandRequires,
    conflicts: opts.conflicts,
    parent: planInfo.lastChangeId,
  };

  await appendChange(planPath, expandChange);
  verbose(`Appended expand change to ${planPath}`);

  // Build contract Change and append to plan
  // Contract always requires the expand change
  const contractTimestamp = nowTimestamp();
  const contractRequires = [expandName];
  const contractNote = opts.note
    ? `[contract] ${opts.note}`
    : `[contract] ${ecConfig.operation}: ${opts.table}.${opts.oldColumn} -> ${opts.newColumn}`;

  const contractChangeIdInput: ChangeIdInput = {
    project: planInfo.projectName,
    uri: planInfo.projectUri,
    change: contractName,
    parent: expandChangeId,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: contractTimestamp,
    requires: contractRequires,
    conflicts: [],
    note: contractNote,
  };

  const contractChangeId = computeChangeId(contractChangeIdInput);

  const contractChange: Change = {
    change_id: contractChangeId,
    name: contractName,
    project: planInfo.projectName,
    note: contractNote,
    planner_name: planner.name,
    planner_email: planner.email,
    planned_at: contractTimestamp,
    requires: contractRequires,
    conflicts: [],
    parent: expandChangeId,
  };

  await appendChange(planPath, contractChange);
  verbose(`Appended contract change to ${planPath}`);

  info(`Added expand/contract pair: "${expandName}" + "${contractName}" to ${planPath}`);

  return {
    expandName,
    contractName,
    files,
    expandChange,
    contractChange,
  };
}

// ---------------------------------------------------------------------------
// CLI integration helper
// ---------------------------------------------------------------------------

/**
 * Run the expand/contract generator from CLI args.
 *
 * This is called from the add command when --expand is detected.
 */
export async function runExpandAdd(
  opts: ExpandAddOptions,
  config?: MergedConfig,
  env?: Record<string, string | undefined>,
): Promise<void> {
  const environment = env ?? process.env;

  // Load config if not provided
  const { loadConfig } = await import("../config/index");
  const cfg = config ?? loadConfig(opts.topDir, undefined, environment);

  try {
    await generateExpandContract(opts, cfg, environment);
  } catch (err) {
    error(`Error: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  }
}
