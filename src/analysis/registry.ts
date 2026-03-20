// src/analysis/registry.ts — Rule registry for the sqlever analysis engine
//
// Maintains a set of rules, validates uniqueness of rule IDs, and provides
// lookup/iteration. Rules are registered at startup and never change.

import type { Rule } from "./types";

/**
 * Registry of analysis rules.
 *
 * Validates that no two rules share the same ID. Once registered,
 * rules are immutable — the registry does not support removal.
 */
export class RuleRegistry {
  private readonly rules: Map<string, Rule> = new Map();

  /**
   * Register a rule. Throws if a rule with the same ID already exists.
   */
  register(rule: Rule): void {
    if (this.rules.has(rule.id)) {
      throw new Error(
        `Duplicate rule ID "${rule.id}": a rule with this ID is already registered.`,
      );
    }
    this.rules.set(rule.id, rule);
  }

  /**
   * Register multiple rules at once.
   */
  registerAll(rules: Rule[]): void {
    for (const rule of rules) {
      this.register(rule);
    }
  }

  /**
   * Get a rule by ID. Returns undefined if not found.
   */
  get(id: string): Rule | undefined {
    return this.rules.get(id);
  }

  /**
   * Check whether a rule with the given ID exists.
   */
  has(id: string): boolean {
    return this.rules.has(id);
  }

  /**
   * Return all registered rules.
   */
  all(): Rule[] {
    return Array.from(this.rules.values());
  }

  /**
   * Return all registered rule IDs.
   */
  ids(): string[] {
    return Array.from(this.rules.keys());
  }

  /**
   * Number of registered rules.
   */
  get size(): number {
    return this.rules.size;
  }
}

/**
 * The global default rule registry.
 * Rule modules register themselves here at import time.
 */
export const defaultRegistry = new RuleRegistry();
