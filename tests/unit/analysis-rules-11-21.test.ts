/**
 * Tests for analysis rules SA011-SA021.
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

import { SA011 } from "../../src/analysis/rules/SA011.js";
import { SA012 } from "../../src/analysis/rules/SA012.js";
import { SA013 } from "../../src/analysis/rules/SA013.js";
import { SA014 } from "../../src/analysis/rules/SA014.js";
import { SA015 } from "../../src/analysis/rules/SA015.js";
import { SA016 } from "../../src/analysis/rules/SA016.js";
import { SA017 } from "../../src/analysis/rules/SA017.js";
import { SA018 } from "../../src/analysis/rules/SA018.js";
import { SA019 } from "../../src/analysis/rules/SA019.js";
import { SA020 } from "../../src/analysis/rules/SA020.js";
import { SA021 } from "../../src/analysis/rules/SA021.js";
import { allRules, getRule } from "../../src/analysis/rules/index.js";
import type { AnalysisContext, DatabaseClient } from "../../src/analysis/types.js";

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

/** Stub database client for connected rule tests. */
const stubDb: DatabaseClient = {
  async query() {
    return { rows: [] };
  },
};

// Load WASM module before all tests
beforeAll(async () => {
  await loadModule();
});

// ─── Registry update ──────────────────────────────────────────────────

describe("rule registry (SA011-SA021)", () => {
  test("allRules contains 22 rules (SA001-SA021 plus SA002b)", () => {
    expect(allRules).toHaveLength(22);
  });

  test("getRule returns SA011-SA021 by ID", () => {
    for (let i = 11; i <= 21; i++) {
      const id = `SA0${i}`;
      expect(getRule(id)?.id).toBe(id);
    }
  });

  test("all new rules have correct interface fields", () => {
    const newRules = [
      SA011, SA012, SA013, SA014, SA015,
      SA016, SA017, SA018, SA019, SA020, SA021,
    ];
    for (const rule of newRules) {
      expect(rule.id).toMatch(/^SA\d{3}$/);
      expect(["error", "warn", "info"]).toContain(rule.severity);
      expect(["static", "connected", "hybrid"]).toContain(rule.type);
      expect(typeof rule.check).toBe("function");
    }
  });
});

// ─── SA011: UPDATE/DELETE on large table (connected) ──────────────────

describe("SA011: UPDATE/DELETE on large table (connected)", () => {
  test("metadata", () => {
    expect(SA011.id).toBe("SA011");
    expect(SA011.severity).toBe("warn");
    expect(SA011.type).toBe("connected");
  });

  test("does not fire without db connection", () => {
    const ctx = makeContext("UPDATE users SET status = 'inactive';");
    const findings = SA011.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on UPDATE when db is present", () => {
    const ctx = makeContext(
      "UPDATE users SET status = 'inactive' WHERE last_login < '2020-01-01';",
      { db: stubDb },
    );
    const findings = SA011.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.ruleId).toBe("SA011");
    expect(findings[0]!.severity).toBe("warn");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on DELETE when db is present", () => {
    const ctx = makeContext(
      "DELETE FROM audit_log WHERE created_at < '2020-01-01';",
      { db: stubDb },
    );
    const findings = SA011.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
    expect(findings[0]!.message).toContain("audit_log");
  });

  test("fires on UPDATE without WHERE when db is present", () => {
    const ctx = makeContext("UPDATE orders SET archived = true;", {
      db: stubDb,
    });
    const findings = SA011.check(ctx);
    expect(findings.length).toBeGreaterThanOrEqual(1);
  });

  test("excludes DML inside CREATE FUNCTION", () => {
    const sql = `
      CREATE FUNCTION archive_users() RETURNS void AS $$
      BEGIN
        UPDATE users SET archived = true;
      END;
      $$ LANGUAGE plpgsql;
    `;
    const ctx = makeContext(sql, { db: stubDb });
    const findings = SA011.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("excludes DML inside DO block", () => {
    const sql = `
      DO $$
      BEGIN
        DELETE FROM temp_data;
      END;
      $$;
    `;
    const ctx = makeContext(sql, { db: stubDb });
    const findings = SA011.check(ctx);
    expect(findings).toHaveLength(0);
  });

  // No-trigger fixtures (all without db)
  for (const file of getFixtureFiles("SA011", "no_trigger")) {
    test(`does not trigger on ${file} (no db)`, () => {
      const ctx = loadFixture("SA011", "no_trigger", file);
      const findings = SA011.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("does not fire on INSERT", () => {
    const ctx = makeContext(
      "INSERT INTO users (email) VALUES ('test@example.com');",
      { db: stubDb },
    );
    const findings = SA011.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on SELECT", () => {
    const ctx = makeContext("SELECT * FROM users;", { db: stubDb });
    const findings = SA011.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes threshold in message", () => {
    const ctx = makeContext("UPDATE users SET status = 'x';", {
      db: stubDb,
      config: { maxAffectedRows: 50_000 },
    });
    const findings = SA011.check(ctx);
    expect(findings[0]!.message).toContain("50000");
  });

  test("finding includes suggestion about batching", () => {
    const ctx = makeContext("UPDATE users SET status = 'x';", { db: stubDb });
    const findings = SA011.check(ctx);
    expect(findings[0]!.suggestion).toContain("batch");
  });
});

// ─── SA012: ALTER SEQUENCE RESTART ────────────────────────────────────

describe("SA012: ALTER SEQUENCE RESTART", () => {
  test("metadata", () => {
    expect(SA012.id).toBe("SA012");
    expect(SA012.severity).toBe("info");
    expect(SA012.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA012", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA012", "trigger", file);
      const findings = SA012.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA012");
      expect(findings[0]!.severity).toBe("info");
    });
  }

  for (const file of getFixtureFiles("SA012", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA012", "no_trigger", file);
      const findings = SA012.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ALTER SEQUENCE RESTART WITH value", () => {
    const ctx = makeContext("ALTER SEQUENCE users_id_seq RESTART WITH 1;");
    const findings = SA012.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users_id_seq");
  });

  test("fires on ALTER SEQUENCE RESTART (no value)", () => {
    const ctx = makeContext("ALTER SEQUENCE orders_id_seq RESTART;");
    const findings = SA012.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("does not fire on ALTER SEQUENCE INCREMENT", () => {
    const ctx = makeContext("ALTER SEQUENCE users_id_seq INCREMENT BY 10;");
    const findings = SA012.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CREATE SEQUENCE", () => {
    const ctx = makeContext("CREATE SEQUENCE new_seq START WITH 1;");
    const findings = SA012.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion", () => {
    const ctx = makeContext("ALTER SEQUENCE s RESTART;");
    const findings = SA012.check(ctx);
    expect(findings[0]!.suggestion).toBeDefined();
  });
});

// ─── SA013: SET lock_timeout missing before risky DDL ─────────────────

describe("SA013: SET lock_timeout missing before risky DDL", () => {
  test("metadata", () => {
    expect(SA013.id).toBe("SA013");
    expect(SA013.severity).toBe("warn");
    expect(SA013.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA013", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA013", "trigger", file);
      const findings = SA013.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA013");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA013", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA013", "no_trigger", file);
      const findings = SA013.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ALTER TABLE without SET lock_timeout", () => {
    const ctx = makeContext("ALTER TABLE users ADD COLUMN bio text;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("lock_timeout");
  });

  test("fires on CREATE INDEX without SET lock_timeout", () => {
    const ctx = makeContext("CREATE INDEX idx ON users (email);");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on DROP TABLE without SET lock_timeout", () => {
    const ctx = makeContext("DROP TABLE old_table;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on TRUNCATE without SET lock_timeout", () => {
    const ctx = makeContext("TRUNCATE users;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("fires on REINDEX without SET lock_timeout", () => {
    const ctx = makeContext("REINDEX TABLE users;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("does not fire when SET lock_timeout precedes DDL", () => {
    const sql = `SET lock_timeout = '5s';
ALTER TABLE users ADD COLUMN bio text;`;
    const ctx = makeContext(sql);
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire when SET LOCAL lock_timeout precedes DDL", () => {
    const sql = `SET LOCAL lock_timeout = '5s';
ALTER TABLE users ADD COLUMN bio text;`;
    const ctx = makeContext(sql);
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CREATE INDEX CONCURRENTLY (no heavy lock)", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx ON users (email);",
    );
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP INDEX CONCURRENTLY", () => {
    const ctx = makeContext("DROP INDEX CONCURRENTLY idx;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on REINDEX CONCURRENTLY", () => {
    const ctx = makeContext("REINDEX TABLE CONCURRENTLY users;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on multiple DDL statements after lock_timeout set once", () => {
    const sql = `SET lock_timeout = '5s';
ALTER TABLE users ADD COLUMN a text;
ALTER TABLE users ADD COLUMN b text;`;
    const ctx = makeContext(sql);
    const findings = SA013.check(ctx);
    // lock_timeout was set before both
    expect(findings).toHaveLength(0);
  });

  test("does not fire on regular VACUUM (not risky)", () => {
    const ctx = makeContext("VACUUM users;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("fires on VACUUM FULL without SET lock_timeout", () => {
    const ctx = makeContext("VACUUM FULL users;");
    const findings = SA013.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("includes suggestion about SET lock_timeout", () => {
    const ctx = makeContext("ALTER TABLE t ADD COLUMN c int;");
    const findings = SA013.check(ctx);
    expect(findings[0]!.suggestion).toContain("lock_timeout");
  });
});

// ─── SA014: VACUUM FULL or CLUSTER ────────────────────────────────────

describe("SA014: VACUUM FULL or CLUSTER", () => {
  test("metadata", () => {
    expect(SA014.id).toBe("SA014");
    expect(SA014.severity).toBe("warn");
    expect(SA014.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA014", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA014", "trigger", file);
      const findings = SA014.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA014");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA014", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA014", "no_trigger", file);
      const findings = SA014.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on VACUUM FULL", () => {
    const ctx = makeContext("VACUUM FULL users;");
    const findings = SA014.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("VACUUM FULL");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on CLUSTER", () => {
    const ctx = makeContext("CLUSTER users USING users_pkey;");
    const findings = SA014.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("CLUSTER");
    expect(findings[0]!.message).toContain("users");
  });

  test("does not fire on regular VACUUM", () => {
    const ctx = makeContext("VACUUM users;");
    const findings = SA014.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on VACUUM ANALYZE", () => {
    const ctx = makeContext("VACUUM ANALYZE users;");
    const findings = SA014.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes AccessExclusiveLock in message", () => {
    const ctx = makeContext("VACUUM FULL t;");
    const findings = SA014.check(ctx);
    expect(findings[0]!.message).toContain("AccessExclusiveLock");
  });

  test("includes suggestion about pg_repack", () => {
    const ctx = makeContext("VACUUM FULL t;");
    const findings = SA014.check(ctx);
    expect(findings[0]!.suggestion).toContain("pg_repack");
  });
});

// ─── SA015: ALTER TABLE RENAME ────────────────────────────────────────

describe("SA015: ALTER TABLE RENAME (table or column)", () => {
  test("metadata", () => {
    expect(SA015.id).toBe("SA015");
    expect(SA015.severity).toBe("warn");
    expect(SA015.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA015", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA015", "trigger", file);
      const findings = SA015.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA015");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA015", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA015", "no_trigger", file);
      const findings = SA015.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on table rename", () => {
    const ctx = makeContext("ALTER TABLE users RENAME TO customers;");
    const findings = SA015.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("customers");
  });

  test("fires on column rename", () => {
    const ctx = makeContext(
      "ALTER TABLE users RENAME COLUMN email TO email_address;",
    );
    const findings = SA015.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("email_address");
  });

  test("does not fire on ADD COLUMN", () => {
    const ctx = makeContext("ALTER TABLE users ADD COLUMN bio text;");
    const findings = SA015.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on DROP COLUMN", () => {
    const ctx = makeContext("ALTER TABLE users DROP COLUMN old;");
    const findings = SA015.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about expand/contract", () => {
    const ctx = makeContext("ALTER TABLE t RENAME TO t2;");
    const findings = SA015.check(ctx);
    expect(findings[0]!.suggestion).toContain("expand/contract");
  });
});

// ─── SA016: ADD CONSTRAINT CHECK without NOT VALID ────────────────────

describe("SA016: ADD CONSTRAINT CHECK without NOT VALID", () => {
  test("metadata", () => {
    expect(SA016.id).toBe("SA016");
    expect(SA016.severity).toBe("error");
    expect(SA016.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA016", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA016", "trigger", file);
      const findings = SA016.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA016");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA016", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA016", "no_trigger", file);
      const findings = SA016.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ADD CHECK without NOT VALID", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0);",
    );
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("chk_age");
    expect(findings[0]!.message).toContain("users");
  });

  test("does not fire on ADD CHECK with NOT VALID", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0) NOT VALID;",
    );
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on FOREIGN KEY constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (uid) REFERENCES users(id);",
    );
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on UNIQUE constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT uniq UNIQUE (email);",
    );
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about NOT VALID + VALIDATE", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD CONSTRAINT chk CHECK (x > 0);",
    );
    const findings = SA016.check(ctx);
    expect(findings[0]!.suggestion).toContain("NOT VALID");
    expect(findings[0]!.suggestion).toContain("VALIDATE CONSTRAINT");
  });
});

// ─── SA017: SET NOT NULL on existing column ───────────────────────────

describe("SA017: SET NOT NULL on existing column", () => {
  test("metadata", () => {
    expect(SA017.id).toBe("SA017");
    expect(SA017.severity).toBe("warn");
    expect(SA017.type).toBe("hybrid");
  });

  for (const file of getFixtureFiles("SA017", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA017", "trigger", file);
      const findings = SA017.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA017");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA017", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA017", "no_trigger", file);
      const findings = SA017.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on SET NOT NULL", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email SET NOT NULL;",
    );
    const findings = SA017.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("email");
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on multiple SET NOT NULL in one statement", () => {
    const sql = `ALTER TABLE t ALTER COLUMN a SET NOT NULL, ALTER COLUMN b SET NOT NULL;`;
    const ctx = makeContext(sql);
    const findings = SA017.check(ctx);
    expect(findings).toHaveLength(2);
  });

  test("does not fire on DROP NOT NULL", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;",
    );
    const findings = SA017.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on SET DEFAULT", () => {
    const ctx = makeContext(
      "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';",
    );
    const findings = SA017.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes three-step pattern in suggestion", () => {
    const ctx = makeContext("ALTER TABLE t ALTER COLUMN c SET NOT NULL;");
    const findings = SA017.check(ctx);
    expect(findings[0]!.suggestion).toContain("CHECK");
    expect(findings[0]!.suggestion).toContain("NOT VALID");
    expect(findings[0]!.suggestion).toContain("VALIDATE");
  });
});

// ─── SA018: ADD PRIMARY KEY without pre-existing index ────────────────

describe("SA018: ADD PRIMARY KEY without pre-existing index", () => {
  test("metadata", () => {
    expect(SA018.id).toBe("SA018");
    expect(SA018.severity).toBe("warn");
    expect(SA018.type).toBe("hybrid");
  });

  for (const file of getFixtureFiles("SA018", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA018", "trigger", file);
      const findings = SA018.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA018");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA018", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA018", "no_trigger", file);
      const findings = SA018.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on ADD PRIMARY KEY without USING INDEX", () => {
    const ctx = makeContext("ALTER TABLE users ADD PRIMARY KEY (id);");
    const findings = SA018.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("id");
  });

  test("fires on composite PRIMARY KEY without USING INDEX", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD PRIMARY KEY (a, b);",
    );
    const findings = SA018.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("a, b");
  });

  test("does not fire on ADD PRIMARY KEY USING INDEX", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY USING INDEX users_pkey;",
    );
    const findings = SA018.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on UNIQUE constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT uniq UNIQUE (email);",
    );
    const findings = SA018.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on CHECK constraint", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD CONSTRAINT chk CHECK (x > 0);",
    );
    const findings = SA018.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about creating index concurrently first", () => {
    const ctx = makeContext("ALTER TABLE t ADD PRIMARY KEY (id);");
    const findings = SA018.check(ctx);
    expect(findings[0]!.suggestion).toContain("concurrently");
    expect(findings[0]!.suggestion).toContain("USING INDEX");
  });
});

// ─── SA019: REINDEX without CONCURRENTLY ──────────────────────────────

describe("SA019: REINDEX without CONCURRENTLY", () => {
  test("metadata", () => {
    expect(SA019.id).toBe("SA019");
    expect(SA019.severity).toBe("warn");
    expect(SA019.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA019", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA019", "trigger", file);
      const findings = SA019.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA019");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA019", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA019", "no_trigger", file);
      const findings = SA019.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on REINDEX TABLE", () => {
    const ctx = makeContext("REINDEX TABLE users;");
    const findings = SA019.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on REINDEX INDEX", () => {
    const ctx = makeContext("REINDEX INDEX idx_users_email;");
    const findings = SA019.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("idx_users_email");
  });

  test("fires on REINDEX DATABASE", () => {
    const ctx = makeContext("REINDEX DATABASE mydb;");
    const findings = SA019.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("mydb");
  });

  test("does not fire on REINDEX CONCURRENTLY", () => {
    const ctx = makeContext("REINDEX TABLE CONCURRENTLY users;");
    const findings = SA019.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on REINDEX INDEX CONCURRENTLY", () => {
    const ctx = makeContext("REINDEX INDEX CONCURRENTLY idx;");
    const findings = SA019.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes AccessExclusiveLock in message", () => {
    const ctx = makeContext("REINDEX TABLE t;");
    const findings = SA019.check(ctx);
    expect(findings[0]!.message).toContain("AccessExclusiveLock");
  });

  test("includes suggestion about CONCURRENTLY", () => {
    const ctx = makeContext("REINDEX TABLE t;");
    const findings = SA019.check(ctx);
    expect(findings[0]!.suggestion).toContain("CONCURRENTLY");
  });
});

// ─── SA020: CONCURRENTLY in transactional context ─────────────────────

describe("SA020: CONCURRENTLY in transactional context", () => {
  test("metadata", () => {
    expect(SA020.id).toBe("SA020");
    expect(SA020.severity).toBe("error");
    expect(SA020.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA020", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA020", "trigger", file);
      const findings = SA020.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA020");
      expect(findings[0]!.severity).toBe("error");
    });
  }

  for (const file of getFixtureFiles("SA020", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA020", "no_trigger", file);
      const findings = SA020.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on CREATE INDEX CONCURRENTLY", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx ON users (email);",
    );
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("CREATE INDEX CONCURRENTLY");
  });

  test("fires on DROP INDEX CONCURRENTLY", () => {
    const ctx = makeContext("DROP INDEX CONCURRENTLY idx;");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("DROP INDEX CONCURRENTLY");
  });

  test("fires on REINDEX CONCURRENTLY", () => {
    const ctx = makeContext("REINDEX TABLE CONCURRENTLY users;");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("REINDEX CONCURRENTLY");
  });

  test("does not fire on regular CREATE INDEX", () => {
    const ctx = makeContext("CREATE INDEX idx ON users (email);");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on regular DROP INDEX", () => {
    const ctx = makeContext("DROP INDEX idx;");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on regular REINDEX", () => {
    const ctx = makeContext("REINDEX TABLE users;");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("suppressed by -- sqlever:no-transaction comment", () => {
    const sql = `-- sqlever:no-transaction
CREATE INDEX CONCURRENTLY idx ON users (email);`;
    const ctx = makeContext(sql);
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about non-transactional", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx ON users (email);",
    );
    const findings = SA020.check(ctx);
    expect(findings[0]!.suggestion).toContain("non-transactional");
  });
});

// ─── SA021: Explicit LOCK TABLE ───────────────────────────────────────

describe("SA021: Explicit LOCK TABLE", () => {
  test("metadata", () => {
    expect(SA021.id).toBe("SA021");
    expect(SA021.severity).toBe("warn");
    expect(SA021.type).toBe("static");
  });

  for (const file of getFixtureFiles("SA021", "trigger")) {
    test(`triggers on ${file}`, () => {
      const ctx = loadFixture("SA021", "trigger", file);
      const findings = SA021.check(ctx);
      expect(findings.length).toBeGreaterThanOrEqual(1);
      expect(findings[0]!.ruleId).toBe("SA021");
      expect(findings[0]!.severity).toBe("warn");
    });
  }

  for (const file of getFixtureFiles("SA021", "no_trigger")) {
    test(`does not trigger on ${file}`, () => {
      const ctx = loadFixture("SA021", "no_trigger", file);
      const findings = SA021.check(ctx);
      expect(findings).toHaveLength(0);
    });
  }

  test("fires on LOCK TABLE ACCESS EXCLUSIVE", () => {
    const ctx = makeContext("LOCK TABLE users IN ACCESS EXCLUSIVE MODE;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
    expect(findings[0]!.message).toContain("ACCESS EXCLUSIVE");
  });

  test("fires on LOCK TABLE (default mode)", () => {
    const ctx = makeContext("LOCK TABLE users;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("users");
  });

  test("fires on LOCK TABLE SHARE mode", () => {
    const ctx = makeContext("LOCK TABLE orders IN SHARE MODE;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("SHARE");
  });

  test("fires on LOCK TABLE ACCESS SHARE mode", () => {
    const ctx = makeContext("LOCK TABLE users IN ACCESS SHARE MODE;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("ACCESS SHARE");
  });

  test("fires on LOCK TABLE ROW EXCLUSIVE mode", () => {
    const ctx = makeContext("LOCK TABLE users IN ROW EXCLUSIVE MODE;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("does not fire on SELECT", () => {
    const ctx = makeContext("SELECT * FROM users;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("does not fire on UPDATE", () => {
    const ctx = makeContext("UPDATE users SET status = 'x' WHERE id = 1;");
    const findings = SA021.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("includes suggestion about lock_timeout", () => {
    const ctx = makeContext("LOCK TABLE t;");
    const findings = SA021.check(ctx);
    expect(findings[0]!.suggestion).toContain("lock_timeout");
  });
});

// ─── Cross-cutting tests (SA011-SA021) ────────────────────────────────

describe("cross-cutting: SA011-SA021 empty inputs", () => {
  test("empty SQL produces no findings for any new rule", () => {
    const ctx = makeContext("SELECT 1;");
    const newRules = [
      SA011, SA012, SA013, SA014, SA015,
      SA016, SA017, SA018, SA019, SA020, SA021,
    ];
    for (const rule of newRules) {
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
    const newRules = [
      SA011, SA012, SA013, SA014, SA015,
      SA016, SA017, SA018, SA019, SA020, SA021,
    ];
    for (const rule of newRules) {
      const findings = rule.check(ctx);
      expect(findings).toHaveLength(0);
    }
  });
});
