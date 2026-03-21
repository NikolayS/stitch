/**
 * Deep static analysis tests — expanded rule coverage, PL/pgSQL exclusion,
 * version-aware rules, inline suppression edge cases, analyze command modes,
 * reporter validation, and preprocessor edge cases.
 *
 * Implements GitHub issue #126 (TEST-3).
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { parseSync, loadModule } from "libpg-query";
import { join } from "node:path";
import {
  mkdirSync,
  writeFileSync,
  rmSync,
  existsSync,
} from "node:fs";

import { SA002 } from "../../src/analysis/rules/SA002.js";
import { SA002b } from "../../src/analysis/rules/SA002b.js";
import { SA003 } from "../../src/analysis/rules/SA003.js";
import { SA009 } from "../../src/analysis/rules/SA009.js";
import { SA010 } from "../../src/analysis/rules/SA010.js";
import { SA016 } from "../../src/analysis/rules/SA016.js";
import { SA017 } from "../../src/analysis/rules/SA017.js";
import { SA020 } from "../../src/analysis/rules/SA020.js";
import { Analyzer } from "../../src/analysis/index.js";
import {
  parseSuppressions,
  resolveSuppressionRanges,
  filterFindings,
} from "../../src/analysis/suppression.js";
import { preprocessSql } from "../../src/analysis/preprocessor.js";
import {
  formatText,
  formatJson,
  formatGithubAnnotations,
  formatGitlabCodeQuality,
  computeFingerprint,
  type Finding,
  type ReportMetadata,
} from "../../src/analysis/reporter.js";
import {
  parseAnalyzeArgs,
  runAnalyze,
} from "../../src/commands/analyze.js";
import { allRules } from "../../src/analysis/rules/index.js";
import { defaultRegistry } from "../../src/analysis/registry.js";
import type { AnalysisContext } from "../../src/analysis/types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function makeFinding(
  ruleId: string,
  line: number,
  severity: "error" | "warn" | "info" = "warn",
): Finding {
  return {
    ruleId,
    severity,
    message: `Test finding for ${ruleId}`,
    location: { file: "test.sql", line, column: 1 },
  };
}

// Suppress stdout during test runs
function silenceStdout(): () => void {
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = (() => true) as typeof process.stdout.write;
  return () => {
    process.stdout.write = original;
  };
}

// Capture stdout during test runs
function captureStdout(): { getOutput: () => string; restore: () => void } {
  let output = "";
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk: unknown) => {
    output += typeof chunk === "string" ? chunk : String(chunk);
    return true;
  }) as typeof process.stdout.write;
  return {
    getOutput: () => output,
    restore: () => {
      process.stdout.write = original;
    },
  };
}

// Load WASM module and register rules before all tests
beforeAll(async () => {
  await loadModule();
  // Ensure all rules are registered in the default registry
  for (const rule of allRules) {
    if (!defaultRegistry.has(rule.id)) {
      defaultRegistry.register(rule);
    }
  }
});

// ---------------------------------------------------------------------------
// Temp directory for analyze command tests
// ---------------------------------------------------------------------------

const TMP_DIR = join(import.meta.dir, "..", ".tmp-analysis-deep-tests");

beforeAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
  mkdirSync(TMP_DIR, { recursive: true });
  mkdirSync(join(TMP_DIR, "deploy"), { recursive: true });
  mkdirSync(join(TMP_DIR, "empty-dir"), { recursive: true });
  mkdirSync(join(TMP_DIR, "sql-dir"), { recursive: true });

  // A clean SQL file
  writeFileSync(
    join(TMP_DIR, "deploy", "clean.sql"),
    "CREATE TABLE t (id serial PRIMARY KEY);\n",
  );

  // SA010 trigger file
  writeFileSync(
    join(TMP_DIR, "deploy", "no_where.sql"),
    "UPDATE t SET x = 1;\n",
  );

  // SA004 trigger file
  writeFileSync(
    join(TMP_DIR, "deploy", "index_issue.sql"),
    "CREATE INDEX idx_t_id ON t (id);\n",
  );

  // Parse error file
  writeFileSync(
    join(TMP_DIR, "deploy", "broken.sql"),
    "CREATE TABL oops;\n",
  );

  // Directory scan files
  writeFileSync(
    join(TMP_DIR, "sql-dir", "a.sql"),
    "SELECT 1;\n",
  );
  writeFileSync(
    join(TMP_DIR, "sql-dir", "b.sql"),
    "UPDATE users SET x = 1;\n",
  );
  writeFileSync(
    join(TMP_DIR, "sql-dir", "not_sql.txt"),
    "not sql",
  );

  // sqitch.plan
  writeFileSync(
    join(TMP_DIR, "sqitch.plan"),
    `%project=test
%uri=https://example.com

clean 2024-01-15T10:30:00Z dev <dev@example.com> # clean migration
no_where 2024-01-15T10:31:00Z dev <dev@example.com> # no where clause
`,
  );
});

afterAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
});

// ===========================================================================
// 1. Expanded SA rule coverage (20+ tests)
// ===========================================================================

describe("SA002: volatile function edge cases", () => {
  test("now() is STABLE — must NOT trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN created_at timestamptz DEFAULT now();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("current_timestamp is STABLE — must NOT trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN created_at timestamptz DEFAULT current_timestamp;",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("clock_timestamp() IS volatile — must trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN ts timestamptz DEFAULT clock_timestamp();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA002");
    expect(findings[0]!.message).toContain("clock_timestamp");
  });

  test("random() triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN sort_key float DEFAULT random();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("random");
  });

  test("gen_random_uuid() triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN id uuid DEFAULT gen_random_uuid();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("gen_random_uuid");
  });

  test("nextval() triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN seq_val bigint DEFAULT nextval('my_seq');",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("nextval");
  });

  test("statement_timestamp() triggers (volatile)", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN ts timestamptz DEFAULT statement_timestamp();",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("volatile function nested in COALESCE triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN val int DEFAULT COALESCE(NULL, random()::int);",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("random");
  });

  test("literal string default does NOT trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN status text DEFAULT 'pending';",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("boolean literal default does NOT trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN is_active boolean DEFAULT false;",
    );
    const findings = SA002.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

describe("SA003: ALTER COLUMN TYPE edge cases", () => {
  test("varchar(50) to varchar(100) — static analysis flags conservatively", () => {
    // SA003 in static mode flags ALL type changes (no source type info)
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN name TYPE varchar(100);",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA003");
  });

  test("varchar to text — static analysis flags conservatively", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN name TYPE text;",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA003");
  });

  test("int to bigint — fires (unsafe rewrite)", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN id TYPE bigint;",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("id");
  });

  test("USING clause always triggers even for text to text", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN name TYPE text USING name::text;",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("USING");
  });

  test("USING clause with expression triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE t ALTER COLUMN age TYPE int USING age::int;",
    );
    const findings = SA003.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("USING");
  });
});

describe("SA009: ADD FOREIGN KEY edge cases", () => {
  test("FK with NOT VALID — no trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID;",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("FK without NOT VALID — triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.message).toContain("fk_user");
    expect(findings[0]!.message).toContain("users");
  });

  test("VALIDATE CONSTRAINT does not trigger SA009", () => {
    const ctx = makeContext(
      "ALTER TABLE orders VALIDATE CONSTRAINT fk_user;",
    );
    const findings = SA009.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

describe("SA010: PL/pgSQL body exclusion for UPDATE/DELETE", () => {
  test("top-level UPDATE without WHERE — triggers", () => {
    const ctx = makeContext("UPDATE users SET status = 'inactive';");
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA010");
  });

  test("UPDATE inside CREATE FUNCTION — no trigger", () => {
    const sql = `
CREATE FUNCTION reset_all() RETURNS void AS $$
BEGIN
  UPDATE users SET status = 'inactive';
END;
$$ LANGUAGE plpgsql;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("DELETE inside CREATE FUNCTION — no trigger", () => {
    const sql = `
CREATE FUNCTION cleanup() RETURNS void AS $$
BEGIN
  DELETE FROM temp_data;
END;
$$ LANGUAGE plpgsql;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("UPDATE with WHERE clause — no trigger", () => {
    const ctx = makeContext(
      "UPDATE users SET status = 'inactive' WHERE id = 1;",
    );
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

describe("SA016: ADD CHECK constraint edge cases", () => {
  test("ADD CHECK without NOT VALID — triggers", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0);",
    );
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA016");
    expect(findings[0]!.message).toContain("chk_age");
  });

  test("ADD CHECK with NOT VALID — no trigger", () => {
    const ctx = makeContext(
      "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0) NOT VALID;",
    );
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("ADD CHECK followed by VALIDATE — only fires on first statement", () => {
    const sql = `ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0);
ALTER TABLE users VALIDATE CONSTRAINT chk_age;`;
    const ctx = makeContext(sql);
    const findings = SA016.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA016");
  });
});

describe("SA020: CONCURRENTLY in transactional context", () => {
  test("CIC without no-transaction marker — triggers", () => {
    const ctx = makeContext(
      "CREATE INDEX CONCURRENTLY idx ON users (email);",
    );
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA020");
  });

  test("CIC with -- sqlever:no-transaction — no trigger", () => {
    const sql = `-- sqlever:no-transaction
CREATE INDEX CONCURRENTLY idx ON users (email);`;
    const ctx = makeContext(sql);
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("DROP INDEX CONCURRENTLY without marker — triggers", () => {
    const ctx = makeContext("DROP INDEX CONCURRENTLY old_idx;");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("DROP INDEX CONCURRENTLY with marker — no trigger", () => {
    const sql = `-- sqlever:no-transaction
DROP INDEX CONCURRENTLY old_idx;`;
    const ctx = makeContext(sql);
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("REINDEX CONCURRENTLY without marker — triggers", () => {
    const ctx = makeContext("REINDEX TABLE CONCURRENTLY users;");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(1);
  });

  test("regular CREATE INDEX — no trigger", () => {
    const ctx = makeContext("CREATE INDEX idx ON users (email);");
    const findings = SA020.check(ctx);
    expect(findings).toHaveLength(0);
  });
});

// ===========================================================================
// 2. PL/pgSQL body exclusion (6 tests)
// ===========================================================================

describe("PL/pgSQL body exclusion", () => {
  test("CREATE FUNCTION body excludes UPDATE without WHERE", () => {
    const sql = `
CREATE FUNCTION reset_users() RETURNS void AS $$
BEGIN
  UPDATE users SET status = 'reset';
END;
$$ LANGUAGE plpgsql;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("CREATE PROCEDURE body excludes DELETE without WHERE", () => {
    // CREATE PROCEDURE is parsed as CreateFunctionStmt in libpg-query
    const sql = `
CREATE PROCEDURE cleanup_data() LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM temp_data;
END;
$$;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("DO block body excludes UPDATE without WHERE", () => {
    const sql = `
DO $$
BEGIN
  UPDATE users SET archived = true;
END;
$$;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("nested dollar-quoting in function body — excluded", () => {
    const sql = `
CREATE FUNCTION complex_fn() RETURNS void AS $func$
BEGIN
  UPDATE users SET status = 'x';
END;
$func$ LANGUAGE plpgsql;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("top-level UPDATE fires, function body does not — exactly one finding", () => {
    const sql = `
UPDATE users SET status = 'inactive';

CREATE FUNCTION reset_fn() RETURNS void AS $$
BEGIN
  UPDATE users SET status = 'reset';
END;
$$ LANGUAGE plpgsql;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA010");
  });

  test("multiple top-level DML fires, function body does not", () => {
    const sql = `
UPDATE users SET status = 'a';
DELETE FROM logs;

CREATE FUNCTION noop() RETURNS void AS $$
BEGIN
  UPDATE users SET status = 'b';
  DELETE FROM audit;
END;
$$ LANGUAGE plpgsql;
`;
    const ctx = makeContext(sql);
    const findings = SA010.check(ctx);
    expect(findings).toHaveLength(2);
  });
});

// ===========================================================================
// 3. Version-aware rules (5 tests)
// ===========================================================================

describe("version-aware rules", () => {
  test("SA002b fires on PG 10 with non-volatile default", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN status text DEFAULT 'active';",
      { pgVersion: 10 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA002b");
    expect(findings[0]!.message).toContain("10");
  });

  test("SA002b does NOT fire on PG 11 with same default", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN status text DEFAULT 'active';",
      { pgVersion: 11 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(0);
  });

  test("SA002b fires on PG 9 with now() default (STABLE but PG<11)", () => {
    const ctx = makeContext(
      "ALTER TABLE t ADD COLUMN created_at timestamptz DEFAULT now();",
      { pgVersion: 9 },
    );
    const findings = SA002b.check(ctx);
    expect(findings).toHaveLength(1);
    expect(findings[0]!.ruleId).toBe("SA002b");
  });

  test("SA017 fires regardless of pgVersion (static mode)", () => {
    // SA017 always fires in static mode — the three-step pattern is needed
    const sql = "ALTER TABLE users ALTER COLUMN email SET NOT NULL;";

    const ctxPg11 = makeContext(sql, { pgVersion: 11 });
    const findingsPg11 = SA017.check(ctxPg11);
    expect(findingsPg11).toHaveLength(1);

    const ctxPg12 = makeContext(sql, { pgVersion: 12 });
    const findingsPg12 = SA017.check(ctxPg12);
    expect(findingsPg12).toHaveLength(1);
  });

  test("pgVersion from config is respected by Analyzer.analyzeSql", () => {
    const analyzer = new Analyzer();
    // SA002b fires only when pgVersion < 11
    const sql = "ALTER TABLE t ADD COLUMN c text DEFAULT 'x';";

    const findingsPg10 = analyzer.analyzeSql(sql, "test.sql", {
      pgVersion: 10,
    });
    const sa002b_10 = findingsPg10.filter((f) => f.ruleId === "SA002b");
    expect(sa002b_10.length).toBeGreaterThanOrEqual(1);

    const findingsPg14 = analyzer.analyzeSql(sql, "test.sql", {
      pgVersion: 14,
    });
    const sa002b_14 = findingsPg14.filter((f) => f.ruleId === "SA002b");
    expect(sa002b_14).toHaveLength(0);
  });
});

// ===========================================================================
// 4. Inline suppression edge cases (9 tests)
// ===========================================================================

describe("inline suppression edge cases", () => {
  const knownRules = new Set([
    "SA001", "SA002", "SA003", "SA004", "SA005",
    "SA006", "SA007", "SA008", "SA009", "SA010",
    "SA011", "SA012", "SA013", "SA014", "SA015",
    "SA016", "SA017", "SA018", "SA019", "SA020", "SA021",
  ]);

  test("block form suppresses findings within range", () => {
    const sql = [
      "-- sqlever:disable SA010",
      "UPDATE users SET status = 'x';",
      "-- sqlever:enable SA010",
    ].join("\n");

    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    const findings = [makeFinding("SA010", 2)];
    const { filtered } = filterFindings(findings, ranges, []);
    expect(filtered).toHaveLength(0);
  });

  test("single-line form only suppresses that line", () => {
    const sql = "UPDATE users SET status = 'x'; -- sqlever:disable SA010\nUPDATE orders SET y = 1;";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    // Line 1 suppressed, line 2 not
    const f1 = [makeFinding("SA010", 1)];
    const r1 = filterFindings(f1, ranges, []);
    expect(r1.filtered).toHaveLength(0);

    // Reset range used flags for second test
    for (const r of ranges) r.used = false;
    const f2 = [makeFinding("SA010", 2)];
    const r2 = filterFindings(f2, ranges, []);
    expect(r2.filtered).toHaveLength(1);
  });

  test("comma-separated rule IDs suppress multiple rules", () => {
    const sql = [
      "-- sqlever:disable SA010,SA013",
      "UPDATE users SET status = 'x';",
      "-- sqlever:enable SA010,SA013",
    ].join("\n");

    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    const findings = [makeFinding("SA010", 2), makeFinding("SA013", 2)];
    const { filtered } = filterFindings(findings, ranges, []);
    expect(filtered).toHaveLength(0);
  });

  test("unclosed block extends to EOF and produces warning", () => {
    const sql = "-- sqlever:disable SA010\nUPDATE users SET status = 'x';";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges, warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    expect(ranges).toHaveLength(1);
    expect(ranges[0]!.endLine).toBe(sqlLines.length);
    const unclosedWarnings = warnings.filter((w) =>
      w.message.includes("Unclosed"),
    );
    expect(unclosedWarnings).toHaveLength(1);
  });

  test("'all' keyword is rejected with warning", () => {
    const sql = "-- sqlever:disable all\nSELECT 1;";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    const allWarnings = warnings.filter((w) =>
      w.message.includes('"all"'),
    );
    expect(allWarnings.length).toBeGreaterThan(0);
    // Should have the "not supported" warning for the "all" keyword
    const notSupportedWarning = allWarnings.find((w) =>
      w.message.includes("not supported"),
    );
    expect(notSupportedWarning).toBeDefined();
  });

  test("unknown rule ID produces warning", () => {
    const sql = "-- sqlever:disable SA999\nSELECT 1;\n-- sqlever:enable SA999";
    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { warnings } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    const unknownWarnings = warnings.filter((w) =>
      w.message.includes("Unknown rule ID"),
    );
    expect(unknownWarnings.length).toBeGreaterThan(0);
    expect(unknownWarnings[0]!.message).toContain("SA999");
  });

  test("unused suppression produces warning", () => {
    const findings: Finding[] = [];
    const ranges = [
      {
        ruleId: "SA010",
        startLine: 1,
        endLine: 3,
        directive: { action: "disable" as const, ruleIds: ["SA010"], line: 1 },
        used: false,
      },
    ];

    const { warnings } = filterFindings(findings, ranges, []);
    const unusedWarnings = warnings.filter((w) =>
      w.message.includes("Unused suppression"),
    );
    expect(unusedWarnings).toHaveLength(1);
    expect(unusedWarnings[0]!.message).toContain("SA010");
  });

  test("nested disable/enable — inner enable closes inner range only", () => {
    const sql = [
      "-- sqlever:disable SA010",   // line 1: open SA010
      "-- sqlever:disable SA013",   // line 2: open SA013
      "UPDATE users SET x = 1;",    // line 3: both suppressed
      "-- sqlever:enable SA013",    // line 4: close SA013
      "UPDATE orders SET y = 2;",   // line 5: only SA010 suppressed
      "-- sqlever:enable SA010",    // line 6: close SA010
    ].join("\n");

    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    // SA010 range: 1-6, SA013 range: 2-4
    const sa010Range = ranges.find((r) => r.ruleId === "SA010");
    const sa013Range = ranges.find((r) => r.ruleId === "SA013");
    expect(sa010Range).toBeDefined();
    expect(sa013Range).toBeDefined();
    expect(sa010Range!.startLine).toBe(1);
    expect(sa010Range!.endLine).toBe(6);
    expect(sa013Range!.startLine).toBe(2);
    expect(sa013Range!.endLine).toBe(4);

    // Finding on line 5 for SA013 should NOT be suppressed
    const f = [makeFinding("SA013", 5)];
    const { filtered } = filterFindings(f, ranges, []);
    expect(filtered).toHaveLength(1);
  });

  test("overlapping ranges for same rule — second disable replaces first", () => {
    const sql = [
      "-- sqlever:disable SA010",   // line 1
      "UPDATE users SET x = 1;",    // line 2
      "-- sqlever:disable SA010",   // line 3: re-open (replaces)
      "UPDATE orders SET y = 2;",   // line 4
      "-- sqlever:enable SA010",    // line 5: closes line 3 range
    ].join("\n");

    const directives = parseSuppressions(sql);
    const sqlLines = sql.split("\n");
    const { ranges } = resolveSuppressionRanges(
      directives,
      sqlLines,
      sqlLines.length,
      knownRules,
      "test.sql",
    );

    // The second disable overwrites the first in the openBlocks map,
    // so we get one range: 3-5
    expect(ranges).toHaveLength(1);
    expect(ranges[0]!.startLine).toBe(3);
    expect(ranges[0]!.endLine).toBe(5);
  });
});

// ===========================================================================
// 5. Analyze command modes (7 tests)
// ===========================================================================

describe("analyze command modes", () => {
  test("single file analysis", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.filesAnalyzed).toBe(1);
      expect(result.exitCode).toBe(0);
    } finally {
      restore();
    }
  });

  test("directory analysis scans all .sql files", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "sql-dir")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.filesAnalyzed).toBe(2); // a.sql, b.sql (not not_sql.txt)
    } finally {
      restore();
    }
  });

  test("pending mode uses sqitch.plan", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
        topDir: TMP_DIR,
        planFile: join(TMP_DIR, "sqitch.plan"),
      });
      expect(result.filesAnalyzed).toBe(2); // clean, no_where
    } finally {
      restore();
    }
  });

  test("--all flag analyzes all migrations from sqitch.plan", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [],
        format: "text",
        strict: false,
        all: true,
        changed: false,
        forceRules: [],
        topDir: TMP_DIR,
        planFile: join(TMP_DIR, "sqitch.plan"),
      });
      expect(result.filesAnalyzed).toBe(2);
    } finally {
      restore();
    }
  });

  test("--changed flag is parsed correctly", () => {
    const opts = parseAnalyzeArgs(["--changed"]);
    expect(opts.changed).toBe(true);
  });

  test("--strict promotes warnings to exit code 2", async () => {
    const restore = silenceStdout();
    try {
      // no_where.sql triggers SA010 (warn) and SA013 (warn)
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "text",
        strict: true,
        all: false,
        changed: false,
        forceRules: [],
      });
      const hasWarnings = result.findings.some(
        (f) => f.severity === "warn" || f.severity === "error",
      );
      if (hasWarnings) {
        expect(result.exitCode).toBe(2);
      }
    } finally {
      restore();
    }
  });

  test("--force-rule skips specified rule from findings", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: ["SA010"],
      });
      const sa010 = result.findings.filter((f) => f.ruleId === "SA010");
      expect(sa010).toHaveLength(0);
    } finally {
      restore();
    }
  });
});

// ===========================================================================
// 6. Reporter validation (6 tests)
// ===========================================================================

describe("reporter validation", () => {
  const finding: Finding = {
    ruleId: "SA004",
    severity: "error",
    message: "CREATE INDEX without CONCURRENTLY locks the table",
    location: { file: "deploy/001.sql", line: 5, column: 1 },
    suggestion: "Use CREATE INDEX CONCURRENTLY",
  };

  const metadata: ReportMetadata = {
    files_analyzed: 1,
    rules_checked: 22,
    duration_ms: 15,
  };

  test("text format includes severity, ruleId, and location", () => {
    const output = formatText([finding], "deploy/001.sql", false);
    expect(output).toContain("error");
    expect(output).toContain("SA004");
    expect(output).toContain("deploy/001.sql:5:1");
    expect(output).toContain("suggestion:");
  });

  test("JSON format parses and has required schema fields", () => {
    const output = formatJson([finding], metadata);
    const parsed = JSON.parse(output);
    expect(parsed.version).toBe(1);
    expect(parsed.metadata.files_analyzed).toBe(1);
    expect(parsed.metadata.rules_checked).toBe(22);
    expect(parsed.metadata.duration_ms).toBe(15);
    expect(parsed.findings).toHaveLength(1);
    expect(parsed.findings[0].ruleId).toBe("SA004");
    expect(parsed.findings[0].severity).toBe("error");
    expect(parsed.findings[0].location.file).toBe("deploy/001.sql");
    expect(parsed.findings[0].location.line).toBe(5);
    expect(parsed.findings[0].location.column).toBe(1);
    expect(parsed.summary.errors).toBe(1);
    expect(parsed.summary.warnings).toBe(0);
    expect(parsed.summary.info).toBe(0);
  });

  test("GitHub annotations match expected regex pattern", () => {
    const output = formatGithubAnnotations([finding]);
    const lines = output.trim().split("\n");
    for (const line of lines) {
      expect(line).toMatch(
        /^::(error|warning|notice) file=.+,line=\d+,col=\d+::.+$/,
      );
    }
  });

  test("GitLab Code Quality has required schema fields", () => {
    const output = formatGitlabCodeQuality([finding]);
    const parsed = JSON.parse(output);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed).toHaveLength(1);

    const entry = parsed[0];
    expect(entry.description).toBeDefined();
    expect(entry.check_name).toBe("SA004");
    expect(entry.fingerprint).toMatch(/^[a-f0-9]{40}$/);
    expect(["critical", "major", "minor"]).toContain(entry.severity);
    expect(entry.location.path).toBe("deploy/001.sql");
    expect(entry.location.lines.begin).toBe(5);
  });

  test("fingerprint is stable across calls", () => {
    const fp1 = computeFingerprint("SA004", "deploy/001.sql", 5);
    const fp2 = computeFingerprint("SA004", "deploy/001.sql", 5);
    expect(fp1).toBe(fp2);
    expect(fp1).toMatch(/^[a-f0-9]{40}$/);

    // Different input = different fingerprint
    const fp3 = computeFingerprint("SA005", "deploy/001.sql", 5);
    expect(fp1).not.toBe(fp3);
  });

  test("empty findings produce valid output for all formats", () => {
    // Text
    const textOutput = formatText([], undefined, false);
    expect(textOutput).toContain("No issues found.");

    // JSON
    const jsonOutput = formatJson([], metadata);
    const parsed = JSON.parse(jsonOutput);
    expect(parsed.findings).toEqual([]);
    expect(parsed.summary).toEqual({ errors: 0, warnings: 0, info: 0 });

    // GitHub annotations
    const ghOutput = formatGithubAnnotations([]);
    expect(ghOutput).toBe("");

    // GitLab code quality
    const glOutput = formatGitlabCodeQuality([]);
    expect(JSON.parse(glOutput)).toEqual([]);
  });
});

// ===========================================================================
// 7. Preprocessor edge cases (7 tests)
// ===========================================================================

describe("preprocessor edge cases", () => {
  test("strips \\i include directive", () => {
    const sql = "\\i shared/functions.sql\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
    expect(result.cleanedSql.split("\n")[1]).toBe("SELECT 1;");
  });

  test("strips \\ir relative include directive", () => {
    const sql = "\\ir ../shared/utils.sql\nCREATE TABLE t (id int);";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
    expect(result.cleanedSql.split("\n")[1]).toBe("CREATE TABLE t (id int);");
  });

  test("strips \\set variable assignment", () => {
    const sql = "\\set ON_ERROR_STOP on\nSELECT 1;";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips \\echo directive", () => {
    const sql = "\\echo 'deploying migration'\nCREATE TABLE t (id int);";
    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1]);
  });

  test("strips \\if/\\endif conditional blocks", () => {
    const sql = [
      "\\if :is_production",
      "SELECT 'prod';",
      "\\else",
      "SELECT 'dev';",
      "\\endif",
    ].join("\n");

    const result = preprocessSql(sql);
    expect(result.strippedLines).toEqual([1, 3, 5]);
    const lines = result.cleanedSql.split("\n");
    expect(lines[1]).toBe("SELECT 'prod';");
    expect(lines[3]).toBe("SELECT 'dev';");
  });

  test("preserves line numbers — cleaned line count equals original", () => {
    const sql = "\\set foo bar\n\\echo hello\nSELECT 1;\n\\i file.sql\nSELECT 2;";
    const result = preprocessSql(sql);
    const originalLineCount = sql.split("\n").length;
    const cleanedLineCount = result.cleanedSql.split("\n").length;
    expect(cleanedLineCount).toBe(originalLineCount);
  });

  test("non-metacommand lines are unchanged", () => {
    const sql = "SELECT 1;\nCREATE TABLE t (id int);\nALTER TABLE t ADD COLUMN c text;";
    const result = preprocessSql(sql);
    expect(result.cleanedSql).toBe(sql);
    expect(result.strippedLines).toEqual([]);
  });
});
