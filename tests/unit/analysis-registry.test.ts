/**
 * Tests for src/analysis/registry.ts — Rule registry.
 */
import { describe, test, expect } from "bun:test";
import { RuleRegistry } from "../../src/analysis/registry";
import type { Rule, Finding, AnalysisContext } from "../../src/analysis/types";

/** Helper: create a minimal rule for testing. */
function makeRule(id: string, type: "static" | "connected" | "hybrid" = "static"): Rule {
  return {
    id,
    severity: "warn",
    type,
    check(_ctx: AnalysisContext): Finding[] {
      return [];
    },
  };
}

describe("RuleRegistry", () => {
  test("registers and retrieves a rule", () => {
    const reg = new RuleRegistry();
    const rule = makeRule("SA001");
    reg.register(rule);

    expect(reg.get("SA001")).toBe(rule);
    expect(reg.has("SA001")).toBe(true);
    expect(reg.size).toBe(1);
  });

  test("returns undefined for unknown rule ID", () => {
    const reg = new RuleRegistry();
    expect(reg.get("SA999")).toBeUndefined();
    expect(reg.has("SA999")).toBe(false);
  });

  test("throws on duplicate rule ID", () => {
    const reg = new RuleRegistry();
    reg.register(makeRule("SA001"));

    expect(() => reg.register(makeRule("SA001"))).toThrow(
      'Duplicate rule ID "SA001"',
    );
  });

  test("registerAll registers multiple rules", () => {
    const reg = new RuleRegistry();
    reg.registerAll([
      makeRule("SA001"),
      makeRule("SA002"),
      makeRule("SA003"),
    ]);

    expect(reg.size).toBe(3);
    expect(reg.has("SA001")).toBe(true);
    expect(reg.has("SA002")).toBe(true);
    expect(reg.has("SA003")).toBe(true);
  });

  test("registerAll throws on duplicate within batch", () => {
    const reg = new RuleRegistry();
    expect(() =>
      reg.registerAll([makeRule("SA001"), makeRule("SA001")]),
    ).toThrow('Duplicate rule ID "SA001"');
  });

  test("registerAll throws on duplicate with existing rule", () => {
    const reg = new RuleRegistry();
    reg.register(makeRule("SA001"));

    expect(() => reg.registerAll([makeRule("SA002"), makeRule("SA001")])).toThrow(
      'Duplicate rule ID "SA001"',
    );
    // SA002 was registered before the error
    expect(reg.has("SA002")).toBe(true);
  });

  test("all() returns all rules", () => {
    const reg = new RuleRegistry();
    reg.registerAll([makeRule("SA001"), makeRule("SA002")]);

    const all = reg.all();
    expect(all).toHaveLength(2);
    expect(all.map((r) => r.id).sort()).toEqual(["SA001", "SA002"]);
  });

  test("ids() returns all rule IDs", () => {
    const reg = new RuleRegistry();
    reg.registerAll([makeRule("SA003"), makeRule("SA001")]);

    const ids = reg.ids();
    expect(ids).toHaveLength(2);
    expect(ids).toContain("SA001");
    expect(ids).toContain("SA003");
  });

  test("size reflects number of registered rules", () => {
    const reg = new RuleRegistry();
    expect(reg.size).toBe(0);

    reg.register(makeRule("SA001"));
    expect(reg.size).toBe(1);

    reg.register(makeRule("SA002"));
    expect(reg.size).toBe(2);
  });

  test("preserves rule type information", () => {
    const reg = new RuleRegistry();
    reg.register(makeRule("SA001", "static"));
    reg.register(makeRule("SA009", "hybrid"));
    reg.register(makeRule("SA011", "connected"));

    expect(reg.get("SA001")?.type).toBe("static");
    expect(reg.get("SA009")?.type).toBe("hybrid");
    expect(reg.get("SA011")?.type).toBe("connected");
  });
});
