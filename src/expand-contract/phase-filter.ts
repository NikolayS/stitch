// src/expand-contract/phase-filter.ts — Expand/contract phase filtering
//
// Utilities for identifying expand and contract migrations by their naming
// convention (SPEC 5.4). The generator uses `<base>_expand` and `<base>_contract`
// suffixes, and these helpers allow the deploy command to filter migrations
// by phase.

import type { Change } from "../plan/types";

// ---------------------------------------------------------------------------
// Naming convention matchers
// ---------------------------------------------------------------------------

/**
 * Check if a change name follows the expand naming convention.
 *
 * Expand changes are suffixed with `_expand` per the convention
 * established by the generator (deriveChangeNames).
 */
export function isExpandChange(changeName: string): boolean {
  return changeName.endsWith("_expand");
}

/**
 * Check if a change name follows the contract naming convention.
 *
 * Contract changes are suffixed with `_contract` per the convention
 * established by the generator (deriveChangeNames).
 */
export function isContractChange(changeName: string): boolean {
  return changeName.endsWith("_contract");
}

/**
 * Check if a change is part of an expand/contract pair (either phase).
 */
export function isExpandContractChange(changeName: string): boolean {
  return isExpandChange(changeName) || isContractChange(changeName);
}

/**
 * Extract the base name from an expand or contract change name.
 *
 * Given "rename_users_name_expand" returns "rename_users_name".
 * Given "rename_users_name_contract" returns "rename_users_name".
 * Returns null if the name doesn't match either convention.
 */
export function extractBaseName(changeName: string): string | null {
  if (changeName.endsWith("_expand")) {
    return changeName.slice(0, -"_expand".length);
  }
  if (changeName.endsWith("_contract")) {
    return changeName.slice(0, -"_contract".length);
  }
  return null;
}

/**
 * Given a base name, return the corresponding expand change name.
 */
export function expandChangeName(baseName: string): string {
  return `${baseName}_expand`;
}

/**
 * Given a base name, return the corresponding contract change name.
 */
export function contractChangeName(baseName: string): string {
  return `${baseName}_contract`;
}

// ---------------------------------------------------------------------------
// Filtering helpers
// ---------------------------------------------------------------------------

/**
 * Filter a list of changes to only expand migrations.
 */
export function filterExpandChanges(changes: Change[]): Change[] {
  return changes.filter((c) => isExpandChange(c.name));
}

/**
 * Filter a list of changes to only contract migrations.
 */
export function filterContractChanges(changes: Change[]): Change[] {
  return changes.filter((c) => isContractChange(c.name));
}
