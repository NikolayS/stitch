/**
 * Tests for analysis rules SA001-SA010.
 *
 * Uses libpg-query to parse SQL fixtures and verifies that each rule
 * triggers (or does not trigger) on the appropriate SQL patterns.
 *
 * Test structure per rule:
 * - trigger/ fixtures: must produce at least one finding with the rule ID
 * - no_trigger/ fixtures: must produce zero findings for that rule
 * - Additional inline tests for edge cases and specific behaviors
 */

import { describe, test, expect, beforeAll } from "bun:test";
import { parseSync, loadModule } from "libpg-query";
import { readFileSync, readdirSync } from "fs";
import { join } from "path";

import { SA001 } from "../../src/analysis/rules/SA001.js";
import { SA002 } from "../../src/analysis/rules/SA002.js";
import { SA002b } from "../../src/analysis/rules/SA002b.js";
import { SA003 } from "../../src/analysis/rules/SA003.js";
import { SA004 } from "../../src/analysis/rules/SA004.js";
import { SA005 } from "../../src/analysis/rules/SA005.js";
import { SA006 } from "../../src/analysis/rules/SA006.js";
import { SA007 } from "../../src/analysis/rules/SA007.js";
import { SA008 } from "../../src/analysis/rules/SA008.js";
import { SA009 } from "../../src/analysis/rules/SA009.js";
import { SA010 } from "../../src/analysis/rules/SA010.js";
import { allRules, getRule } from "../../src/analysis/rules/index.js";
import type { AnalysisContext } from "../../src/analysis/types.js";

const FIXTURES_DIR = join(import.meta.dir, "..", "fixtures", "analysis");

/**
 * Build an AnalysisContext from raw SQL text.
 */
function makeContext(
  sql: string,
  overrides: Partial<AnalysisContext> = {},
): AnalysisContext {
  const ast = parseSync(sql);
  return {
    ast,
    rawSql: sql,
    filePath: overrides.filePath ?? "test.sql",
    pgVersion: overrides.pgVersion ?? 17,
    config: overrides.config ?? {},
    isRevertContext: overrides.isRevertContext ?? false,
    ...overrides,
  };
}

/**
 * Load a fixture file and build a context.
 */
function loadFixture(
  ruleId: string,
  category: "trigger" | "no_trigger",
  fileName: string,
  overrides: Partial<AnalysisContext> = {},
): AnalysisContext {
  const filePath = join(FIXTURES_DIR, ruleId, category, fileName);
  const sql = readFileSync(filePath, "utf-8");
  return makeContext(sql, { filePath, ...overrides });
}

/**
 * Get all fixture files in a directory.
 */
function getFixtureFiles(
  ruleId: string,
  category: "trigger" | "no_trigger",
): string[] {
  const dir = join(FIXTURES_DIR, ruleId, category);
  try {
    return readdirSync(dir).filter((f) => f.endsWith(".sql"));
  } catch {
    return [];
  }
}

// Load WASM module before all tests
beforeAll(async () => {
  await loadModule();
});

// ─── Registry ────────────────────────────────────────────────────────

describe("rule registry", () => {
  test("allRules contains 11 rules", () => {
    expect(allRules).toHaveLength(11);
  });

  test("getRule returns rules by ID", () => {
    expect(getRule("SA001")?.id).toBe("SA001");
    expect(getRule("SA002")?.id).toBe("SA002");
    expect(getRule("SA002b")?.id).toBe("SA002b");
    expect(getRule("SA010")?.id).toBe("SA010");
  });

  test("getRule returns undefined for unknown ID", () => {
    expect(getRule("SA999")).toBeUndefined();
  });

  test("all rules have correct interface fields", () => {
    for (const rule of allRules) {
      expect(rule.id).toMatch(/^SA\d{3}b?$/);
      expect(["error", "warn", "info"]).toContain(rule.severity);
      expect(["static", "connected", "hybrid"]).toContain(rule.type);
      expect(typeof rule.check).toBe("function");
    }
  });
});

// ─── SA001: ADD COLUMN NOT NULL without DEFAULT ──────────────────────

describe("SA001: ADD COLUMN NOT NULL without DEFAULT", () => {
  test("metadata", () => {
    expect(SA001.id).toBe("SA001");
    expect(SA001.severity).toBe("error");
    expect(SA001.type).toBe("static");
  });

  // Trigger fixtures
  for (const file of getFixtureFiles("SA001", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA001", "trigger", file);
      const findings = SA001.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA001");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  // No-trigger fixtures
  for (const file of getFixtureFiles("SA001", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA001", "no_trigger", file);
      const findings = SA001.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("finding includes table and column names", () => {
    const ctx = makeContext("ALTER TABLE users ADD COLUMN email text NOT NULL;");
    const findings = SA001.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("users");
  });

  test("finding includes suggestion", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD COLUMN email text NOT NULL;",
    );
    const findings = SA001.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
    expect(findings[0]!.suggestion).toContain("DEFAULT");
  });

  test("finding has valid location", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD COLUMN email text NOT NULL;",
    );
    const findings = SA001.check(ctx);
    expect(findings[0]!.location.line).toBe(1);
    expect(findings[0]!.location.column).toBe(1);
  });

  test("handles multiple ADD COLUMN in one statement", () => {
    const sql = `ALTER TABLE t ADD COLUMN a int NOT NULL, ADD COLUMN b int NOT NULL;`;
    const ctx = makeContext(sql);
    const findings = SA001.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("does not fire on ADD COLUMN with NOT NULL and DEFAULT", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD COLUMN status text NOT NULL DEFAULT 'active';",
    );
    const findings = SA001.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("empty SQL produces no findings", () => {
    const ctx = makeContext("SELECT 1;");
    const findings = SA001.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA002: ADD COLUMN DEFAULT volatile ──────────────────────────────

describe("SA002: ADD COLUMN DEFAULT volatile", () => {
  test("metadata", () => {
    expect(SA002.id).toBe("SA002");
    expect(SA002.severity).toBe("error");
    expect(SA002.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA002", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA002", "trigger", file);
      const findings = SA002.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA002");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA002", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA002", "no_trigger", file);
      const findings = SA002.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("detects gen_random_uuid()", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD COLUMN id uuid DEFAULT gen_random_uuid();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("gen_random_uuid");
  });

  test("detects random()", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN sort_key float DEFAULT random();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("random");
  });

  test("detects clock_timestamp()", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN ts timestamptz DEFAULT clock_timestamp();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("clock_timestamp");
  });

  test("detects txid_current()", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN txid bigint DEFAULT txid_current();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("detects volatile function inside type cast", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN priority int DEFAULT random()::int;",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("random");
  });

  test("does not fire on now() — it is STABLE", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN created_at timestamptz DEFAULT now();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on literal defaults", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN status text DEFAULT 'active';",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on integer defaults", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN count integer DEFAULT 0;",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── SA002b: ADD COLUMN DEFAULT non-volatile on PG < 11 ─────────────

describe("SA002b: ADD COLUMN DEFAULT non-volatile on PG < 11", () => {
  test("metadata", () => {
    expect(SA002b.id).toBe("SA002b");
    expect(SA002b.severity).toBe("warn");
    expect(SA002b.type).toBe("static");
  });

  test("triggers on non-volatile default with pgVersion=10", () => {
    const ctx = loadFixture(
      "SA002b",
      "trigger",
      "add_column_default_literal_pg10.sql",
      { pgVersion: 10 },
    );
    const findings = SA002b.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.ruleId).toBe("SA002b");
    expect(findings[0]!.severity).toBe("warn");
  });

  test("triggers on now() default with pgVersion=10", () => {
    const ctx = loadFixture(
      "SA002b",
      "trigger",
      "add_column_default_now_pg10.sql",
      { pgVersion: 10 },
    );
    const findings = SA002b.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
  });

  test("triggers on integer default with pgVersion=10", () => {
    const ctx = loadFixture(
      "SA002b",
      "trigger",
      "add_column_default_integer_pg10.sql",
      { pgVersion: 10 },
    );
    const findings = SA002b.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
  });

  test("does not trigger on pgVersion=17", () => {
    const ctx = loadFixture(
      "SA002b",
      "no_trigger",
      "add_column_default_literal_pg17.sql",
      { pgVersion: 17 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on pgVersion=14", () => {
    const ctx = loadFixture(
      "SA002b",
      "no_trigger",
      "add_column_default_boolean_pg14.sql",
      { pgVersion: 14 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger when no default is present", () => {
    const ctx = loadFixture("SA002b", "no_trigger", "add_column_no_default.sql", {
      pgVersion: 10,
    });
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on volatile defaults (handled by SA002)", () => {
    const ctx = loadFixture(
      "SA002b",
      "no_trigger",
      "add_column_volatile_default_pg10.sql",
      { pgVersion: 10 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on pgVersion=11 (boundary)", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN status text DEFAULT 'active';",
      { pgVersion: 11 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("finding includes pg version in message", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN status text DEFAULT 'active';",
      { pgVersion: 10 },
    );
    const findings = SA002b.check(ctx);
    expect(findings[0]!.message).toContain("10");
  });
});

// ─── SA003: ALTER COLUMN TYPE non-trivial cast ───────────────────────

describe("SA003: ALTER COLUMN TYPE", () => {
  test("metadata", () => {
    expect(SA003.id).toBe("SA003");
    expect(SA003.severity).toBe("error");
    expect(SA003.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA003", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA003", "trigger", file);
      const findings = SA003.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA003");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA003", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA003", "no_trigger", file);
      const findings = SA003.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on int to bigint", () => {
    const ctx = makeContext("ALTER TABLE t ALTER COLUMN id TYPE bigint;");
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("id");
  });

  test("fires on timestamp to timestamptz", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN ts TYPE timestamptz;",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on USING clause even for trivial types", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN name TYPE text USING name::text;",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("USING");
  });

  test("includes target type in message", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN id TYPE bigint;",
    );
    const findings = SA003.check(ctx);
    expect(findings[0]!.message).toMatch(/bigint|int8/);
  });

  test("includes column name in message", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email TYPE varchar(255);",
    );
    const findings = SA003.check(ctx);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("users");
  });
});

// ─── SA004: CREATE INDEX without CONCURRENTLY ────────────────────────

describe("SA004: CREATE INDEX without CONCURRENTLY", () => {
  test("metadata", () => {
    expect(SA004.id).toBe("SA004");
    expect(SA004.severity).toBe("warn");
    expect(SA004.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA004", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA004", "trigger", file);
      const findings = SA004.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA004");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA004", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA004", "no_trigger", file);
      const findings = SA004.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on CREATE INDEX", () => {
    const ctx = makeContext("CREATE INDEX idx ON users (email);");
    const findings = SA004.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("idx");
    expect(findings[0]!.message).toContain("users");
  });

  test("does not fire on CREATE INDEX CONCURRENTLY", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx ON users (email);",
    );
    const findings = SA004.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on CREATE UNIQUE INDEX without CONCURRENTLY", () => {
    const ctx = makeContext(
      "CREATE UNIQUE INDEX idx ON users (email);",
    );
    const findings = SA004.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("includes suggestion about CONCURRENTLY", () => {
    const ctx = makeContext("CREATE INDEX idx ON users (email);");
    const findings = SA004.check(ctx);
    expect(findings[0]!.suggestion).toContain("CONCURRENTLY");
  });
});

// ─── SA005: DROP INDEX without CONCURRENTLY ──────────────────────────

describe("SA005: DROP INDEX without CONCURRENTLY", () => {
  test("metadata", () => {
    expect(SA005.id).toBe("SA005");
    expect(SA005.severity).toBe("warn");
    expect(SA005.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA005", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA005", "trigger", file);
      const findings = SA005.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA005");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA005", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA005", "no_trigger", file);
      const findings = SA005.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on DROP INDEX", () => {
    const ctx = makeContext("DROP INDEX idx_users_email;");
    const findings = SA005.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("idx_users_email");
  });

  test("does not fire on DROP INDEX CONCURRENTLY", () => {
    const ctx = makeContext("DROP INDEX CONCURRENTLY idx_users_email;");
    const findings = SA005.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP TABLE (different rule)", () => {
    const ctx = makeContext("DROP TABLE users;");
    const findings = SA005.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes AccessExclusiveLock in message", () => {
    const ctx = makeContext("DROP INDEX idx;");
    const findings = SA005.check(ctx);
    expect(findings[0]!.message).toContain("AccessExclusiveLock");
  });

  test("includes suggestion about CONCURRENTLY", () => {
    const ctx = makeContext("DROP INDEX idx;");
    const findings = SA005.check(ctx);
    expect(findings[0]!.suggestion).toContain("CONCURRENTLY");
  });
});

// ─── SA006: DROP COLUMN ──────────────────────────────────────────────

describe("SA006: DROP COLUMN", () => {
  test("metadata", () => {
    expect(SA006.id).toBe("SA006");
    expect(SA006.severity).toBe("warn");
    expect(SA006.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA006", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA006", "trigger", file);
      const findings = SA006.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA006");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA006", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA006", "no_trigger", file);
      const findings = SA006.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on DROP COLUMN", () => {
    const ctx = makeContext("ALTER TABLE users DROP COLUMN email;");
    const findings = SA006.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on DROP COLUMN IF EXISTS", () => {
    const ctx = makeContext(
      "ALTER TABLE users DROP COLUMN IF EXISTS old_field;",
    );
    const findings = SA006.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on DROP COLUMN CASCADE", () => {
    const ctx = makeContext("ALTER TABLE t DROP COLUMN c CASCADE;");
    const findings = SA006.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("does not fire on ADD COLUMN", () => {
    const ctx = makeContext("ALTER TABLE users ADD COLUMN bio text;");
    const findings = SA006.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes data loss in message", () => {
    const ctx = makeContext("ALTER TABLE t DROP COLUMN c;");
    const findings = SA006.check(ctx);
    expect(findings[0]!.message).toContain("data loss");
  });
});

// ─── SA007: DROP TABLE ───────────────────────────────────────────────

describe("SA007: DROP TABLE in non-revert context", () => {
  test("metadata", () => {
    expect(SA007.id).toBe("SA007");
    expect(SA007.severity).toBe("error");
    expect(SA007.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA007", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA007", "trigger", file);
      const findings = SA007.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA007");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  // Special handling for no_trigger: some need isRevertContext
  test("does not trigger in revert context", () => {
    const ctx = loadFixture(
      "SA007",
      "no_trigger",
      "drop_table_in_revert.sql",
      { isRevertContext: true },
    );
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on DROP INDEX", () => {
    const ctx = loadFixture("SA007", "no_trigger", "drop_index.sql");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on CREATE TABLE", () => {
    const ctx = loadFixture("SA007", "no_trigger", "create_table.sql");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on ALTER TABLE", () => {
    const ctx = loadFixture("SA007", "no_trigger", "alter_table.sql");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not trigger on SELECT", () => {
    const ctx = loadFixture("SA007", "no_trigger", "select_statement.sql");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on DROP TABLE", () => {
    const ctx = makeContext("DROP TABLE users;");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on DROP TABLE IF EXISTS", () => {
    const ctx = makeContext("DROP TABLE IF EXISTS users;");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on DROP TABLE CASCADE", () => {
    const ctx = makeContext("DROP TABLE orders CASCADE;");
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("exempt in revert context", () => {
    const ctx = makeContext("DROP TABLE users;", { isRevertContext: true });
    const findings = SA007.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes data loss in message", () => {
    const ctx = makeContext("DROP TABLE t;");
    const findings = SA007.check(ctx);
    expect(findings[0]!.message).toContain("data loss");
  });
});

// ─── SA008: TRUNCATE ─────────────────────────────────────────────────

describe("SA008: TRUNCATE", () => {
  test("metadata", () => {
    expect(SA008.id).toBe("SA008");
    expect(SA008.severity).toBe("warn");
    expect(SA008.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA008", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA008", "trigger", file);
      const findings = SA008.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA008");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA008", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA008", "no_trigger", file);
      const findings = SA008.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on TRUNCATE", () => {
    const ctx = makeContext("TRUNCATE users;");
    const findings = SA008.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on TRUNCATE CASCADE", () => {
    const ctx = makeContext("TRUNCATE orders CASCADE;");
    const findings = SA008.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on TRUNCATE multiple tables", () => {
    const ctx = makeContext("TRUNCATE users, orders;");
    const findings = SA008.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("orders");
  });

  test("excludes TRUNCATE inside CREATE FUNCTION", () => {
    const sql = `
      CREATE FUNCTION reset() RETURNS void AS $$
      BEGIN
        TRUNCATE users;
      END;
      $$ LANGUAGE plpgsql;
    `;
    const ctx = makeContext(sql);
    const findings = SA008.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("excludes TRUNCATE inside DO block", () => {
    const sql = `
      DO $$
      BEGIN
        TRUNCATE old_data;
      END;
      $$;
    `;
    const ctx = makeContext(sql);
    const findings = SA008.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes AccessExclusiveLock in message", () => {
    const ctx = makeContext("TRUNCATE t;");
    const findings = SA008.check(ctx);
    expect(findings[0]!.message).toContain("AccessExclusiveLock");
  });
});

// ─── SA009: ADD FOREIGN KEY without NOT VALID ────────────────────────

describe("SA009: ADD FOREIGN KEY without NOT VALID", () => {
  test("metadata", () => {
    expect(SA009.id).toBe("SA009");
    expect(SA009.severity).toBe("warn");
    expect(SA009.type).toBe("hybrid");
  });

  for (const file of getFixtureFiles("SA009", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA009", "trigger", file);
      const findings = SA009.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA009");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA009", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA009", "no_trigger", file);
      const findings = SA009.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ADD FOREIGN KEY without NOT VALID", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id);",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("fk");
    expect(findings[0]!.message).toContain("user_id");
  });

  test("does not fire on ADD FOREIGN KEY with NOT VALID", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID;",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CHECK constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk CHECK (age > 0);",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on UNIQUE constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT uniq UNIQUE (email);",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about NOT VALID + VALIDATE", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES t2(id);",
    );
    const findings = SA009.check(ctx);
    expect(findings[0]!.suggestion).toContain("NOT VALID");
    expect(findings[0]!.suggestion).toContain("VALIDATE CONSTRAINT");
  });

  test("includes referenced table in message", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);",
    );
    const findings = SA009.check(ctx);
    expect(findings[0]!.message).toContain("users");
  });
});

// ─── SA010: UPDATE/DELETE without WHERE ──────────────────────────────

describe("SA010: UPDATE/DELETE without WHERE", () => {
  test("metadata", () => {
    expect(SA010.id).toBe("SA010");
    expect(SA010.severity).toBe("warn");
    expect(SA010.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA010", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA010", "trigger", file);
      const findings = SA010.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA010");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA010", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA010", "no_trigger", file);
      const findings = SA010.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on UPDATE without WHERE", () => {
    const ctx = makeContext("UPDATE users SET status = 'inactive';");
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("UPDATE");
  });

  test("fires on DELETE without WHERE", () => {
    const ctx = makeContext("DELETE FROM audit_log;");
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("audit_log");
    expect(findings[0]!.message).toContain("DELETE");
  });

  test("does not fire on UPDATE with WHERE", () => {
    const ctx = makeContext("UPDATE users SET status = 'x' WHERE id = 1;");
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DELETE with WHERE", () => {
    const ctx = makeContext("DELETE FROM t WHERE id = 1;");
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("excludes UPDATE inside CREATE FUNCTION", () => {
    const sql = `
      CREATE FUNCTION reset() RETURNS void AS $$
      BEGIN
        UPDATE users SET status = 'inactive';
      END;
      $$ LANGUAGE plpgsql;
    `;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("excludes DELETE inside DO block", () => {
    const sql = `
      DO $$
      BEGIN
        DELETE FROM temp_data;
      END;
      $$;
    `;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on both UPDATE and DELETE in same file", () => {
    const sql = `
      UPDATE users SET status = 'inactive';
      DELETE FROM audit_log;
    `;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("does not fire on INSERT", () => {
    const ctx = makeContext(
      "INSERT INTO users (email) VALUES ('test@example.com');",
    );
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on SELECT", () => {
    const ctx = makeContext("SELECT * FROM users;");
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ─── Cross-cutting tests ─────────────────────────────────────────────

describe("cross-cutting: location and message quality", () => {
  test("stmt_location 0 for first statement maps to line 1", () => {
    // libpg-query's protobuf sets stmt_location=0 for the first statement
    // (protobuf3 zero-value omission), regardless of leading comments.
    // More precise location mapping will be done in analysis/parser.ts.
    const sql = `-- comment line 1
-- comment line 2
ALTER TABLE users ADD COLUMN email text NOT NULL;
`;
    const ctx = makeContext(sql);
    const findings = SA001.check(ctx);
    expect(findings).toHaveLength(1);
    // stmt_location is 0 -> line 1 (parser.ts will refine this later)
    expect(findings[0]!.location.line).toBe(1);
  });

  test("multiple statements in same file are detected", () => {
    const sql = `ALTER TABLE a ADD COLUMN x int NOT NULL;
ALTER TABLE b ADD COLUMN y int NOT NULL;`;
    const ctx = makeContext(sql);
    const findings = SA001.check(ctx);
    expect(findings).toHaveLength(2);
    // Both findings have valid locations
    expect(findings[0]!.location.line).toBeGreaterThanOrEqual(1);
    expect(findings[1]!.location.line).toBeGreaterThanOrEqual(1);
    // Second statement should be at same or later line
    expect(findings[1]!.location.line).toBeGreaterThanOrEqual(
      findings[0]!.location.line,
    );
  });

  test("all findings have required fields", () => {
    const sql = `
ALTER TABLE t ADD COLUMN c int NOT NULL;
ALTER TABLE t ADD COLUMN d uuid DEFAULT gen_random_uuid();
CREATE INDEX idx ON t (c);
DROP INDEX old_idx;
ALTER TABLE t DROP COLUMN old;
DROP TABLE old_table;
TRUNCATE cleanup;
ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES t2(id);
UPDATE t SET c = 1;
DELETE FROM t;
`;
    const ctx = makeContext(sql);

    for (const rule of allRules) {
      // Skip SA002b (needs pgVersion < 11) and SA003 (needs ALTER TYPE)
      if (rule.id === "SA002b") continue;
      const findings = rule.check(ctx);
      for (const f of findings) {
        expect(f.ruleId).toBe(rule.id);
        expect(f.severity).toBeDefined();
        expect(f.message).toBeTruthy();
        expect(f.location).toBeDefined();
        expect(f.location.file).toBeDefined();
        expect(f.location.line).toBeGreaterThan(0);
        expect(f.location.column).toBeGreaterThan(0);
      }
    }
  });
});

describe("cross-cutting: empty/edge-case inputs", () => {
  test("empty SQL produces no findings for any rule", () => {
    const ctx = makeContext("SELECT 1;");
    for (const rule of allRules) {
      const findings = rule.check(ctx);
      expect(findings).toHaveLength(0);
    }
  });

  test("null AST produces no findings", () => {
    const ctx: AnalysisContext = {
      ast: { stmts: [] },
      rawSql: "",
      filePath: "test.sql",
      pgVersion: 17,
      config: {},
    };
    for (const rule of allRules) {
      const findings = rule.check(ctx);
      expect(findings).toHaveLength(0);
    }
  });
});
