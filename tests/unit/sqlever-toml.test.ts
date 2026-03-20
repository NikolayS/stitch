import { describe, it, expect } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import {
  parseSqleverToml,
  getAnalysisConfig,
  serializeSqleverToml,
} from "../../src/config/sqlever-toml";

const FIXTURES = resolve(import.meta.dir, "../fixtures/config");

function loadFixture(name: string): string {
  return readFileSync(resolve(FIXTURES, name), "utf-8");
}

// ---------------------------------------------------------------------------
// Basic TOML parsing
// ---------------------------------------------------------------------------

describe("parseSqleverToml", () => {
  it("parses analysis section", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const analysis = toml.analysis as Record<string, unknown>;
    expect(analysis).toBeDefined();
    expect(analysis.skip).toEqual(["SA001", "SA002"]);
    expect(analysis.error_on_warn).toBe(false);
    expect(analysis.max_affected_rows).toBe(100000);
    expect(analysis.pg_version).toBe("16");
  });

  it("parses per-rule config", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const analysis = toml.analysis as Record<string, unknown>;
    const rules = analysis.rules as Record<string, Record<string, unknown>>;
    expect(rules).toBeDefined();
    expect(rules.SA003).toBeDefined();
    expect(rules.SA003!.max_affected_rows).toBe(50000);
    expect(rules.SA010).toBeDefined();
    expect(rules.SA010!.severity).toBe("warn");
  });

  it("parses per-file overrides with quoted paths", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const analysis = toml.analysis as Record<string, unknown>;
    const overrides = analysis.overrides as Record<string, Record<string, unknown>>;
    expect(overrides).toBeDefined();
    expect(overrides["deploy/backfill_tiers.sql"]).toBeDefined();
    expect(overrides["deploy/backfill_tiers.sql"]!.skip).toEqual(["SA010"]);
    expect(overrides["deploy/large_migration.sql"]).toBeDefined();
    expect(overrides["deploy/large_migration.sql"]!.skip).toEqual(["SA003", "SA010"]);
    expect(overrides["deploy/large_migration.sql"]!.max_affected_rows).toBe(500000);
  });

  it("parses deploy config", () => {
    const toml = parseSqleverToml(loadFixture("deploy.toml"));
    const deploy = toml.deploy as Record<string, unknown>;
    expect(deploy).toBeDefined();
    expect(deploy.lock_retries).toBe(3);
    expect(deploy.lock_timeout).toBe("10s");
    expect(deploy.idle_in_transaction_session_timeout).toBe("15min");
    expect(deploy.search_path).toBe("public,extensions");
    expect(deploy.verify).toBe(false);
    expect(deploy.mode).toBe("tag");
  });

  it("parses batch config", () => {
    const toml = parseSqleverToml(loadFixture("deploy.toml"));
    const batch = toml.batch as Record<string, unknown>;
    expect(batch).toBeDefined();
    expect(batch.max_dead_tuple_ratio).toBe(0.10);
  });

  it("handles empty TOML", () => {
    const toml = parseSqleverToml(loadFixture("empty.toml"));
    expect(Object.keys(toml).length).toBe(0);
  });

  it("handles empty string", () => {
    const toml = parseSqleverToml("");
    expect(Object.keys(toml).length).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// getAnalysisConfig
// ---------------------------------------------------------------------------

describe("getAnalysisConfig", () => {
  it("extracts analysis config from parsed TOML", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const config = getAnalysisConfig(toml);

    expect(config.skip).toEqual(["SA001", "SA002"]);
    expect(config.error_on_warn).toBe(false);
    expect(config.max_affected_rows).toBe(100000);
    expect(config.pg_version).toBe("16");
  });

  it("extracts per-rule config", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const config = getAnalysisConfig(toml);

    expect(config.rules).toBeDefined();
    expect(config.rules!.SA003).toBeDefined();
    expect(config.rules!.SA003!.max_affected_rows).toBe(50000);
  });

  it("extracts per-file overrides", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const config = getAnalysisConfig(toml);

    expect(config.overrides).toBeDefined();
    expect(config.overrides!["deploy/backfill_tiers.sql"]).toBeDefined();
    expect(config.overrides!["deploy/backfill_tiers.sql"]!.skip).toEqual(["SA010"]);
  });

  it("returns empty config when no analysis section", () => {
    const toml = parseSqleverToml(loadFixture("deploy.toml"));
    const config = getAnalysisConfig(toml);
    expect(config).toEqual({});
  });

  it("returns empty config for empty TOML", () => {
    const toml = parseSqleverToml("");
    const config = getAnalysisConfig(toml);
    expect(config).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

describe("TOML value parsing", () => {
  it("parses boolean values", () => {
    const toml = parseSqleverToml("[test]\na = true\nb = false\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.a).toBe(true);
    expect(test.b).toBe(false);
  });

  it("parses integer values", () => {
    const toml = parseSqleverToml("[test]\ncount = 42\nneg = -10\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.count).toBe(42);
    expect(test.neg).toBe(-10);
  });

  it("parses float values", () => {
    const toml = parseSqleverToml("[test]\nratio = 0.75\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.ratio).toBe(0.75);
  });

  it("parses string values", () => {
    const toml = parseSqleverToml('[test]\nname = "hello world"\n');
    const test = toml.test as Record<string, unknown>;
    expect(test.name).toBe("hello world");
  });

  it("parses array values", () => {
    const toml = parseSqleverToml('[test]\nitems = ["a", "b", "c"]\n');
    const test = toml.test as Record<string, unknown>;
    expect(test.items).toEqual(["a", "b", "c"]);
  });

  it("parses empty array", () => {
    const toml = parseSqleverToml("[test]\nitems = []\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.items).toEqual([]);
  });

  it("handles underscores in numbers", () => {
    const toml = parseSqleverToml("[test]\nbig = 1_000_000\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.big).toBe(1000000);
  });

  it("handles escaped characters in strings", () => {
    const toml = parseSqleverToml('[test]\npath = "a\\\\b\\nc"\n');
    const test = toml.test as Record<string, unknown>;
    expect(test.path).toBe("a\\b\nc");
  });
});

// ---------------------------------------------------------------------------
// Inline comments
// ---------------------------------------------------------------------------

describe("TOML inline comments", () => {
  it("strips inline comments from values", () => {
    const toml = parseSqleverToml("[test]\ncount = 42 # this is a comment\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.count).toBe(42);
  });

  it("does not strip hash inside quoted strings", () => {
    const toml = parseSqleverToml('[test]\nname = "hello#world"\n');
    const test = toml.test as Record<string, unknown>;
    expect(test.name).toBe("hello#world");
  });

  it("skips comment-only lines", () => {
    const toml = parseSqleverToml("# comment\n[test]\n# another\nval = 1\n");
    const test = toml.test as Record<string, unknown>;
    expect(test.val).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Nested tables
// ---------------------------------------------------------------------------

describe("nested TOML tables", () => {
  it("creates nested structure from dotted table paths", () => {
    const text = `
[a]
x = 1

[a.b]
y = 2

[a.b.c]
z = 3
`;
    const toml = parseSqleverToml(text);
    const a = toml.a as Record<string, unknown>;
    expect(a.x).toBe(1);
    const b = a.b as Record<string, unknown>;
    expect(b.y).toBe(2);
    const c = b.c as Record<string, unknown>;
    expect(c.z).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

describe("serializeSqleverToml", () => {
  it("round-trips basic config", () => {
    const original = parseSqleverToml(loadFixture("basic.toml"));
    const serialized = serializeSqleverToml(original);
    const reparsed = parseSqleverToml(serialized);

    const config = getAnalysisConfig(reparsed);
    expect(config.skip).toEqual(["SA001", "SA002"]);
    expect(config.error_on_warn).toBe(false);
    expect(config.max_affected_rows).toBe(100000);
    expect(config.pg_version).toBe("16");
  });

  it("handles empty config", () => {
    const toml = parseSqleverToml("");
    const serialized = serializeSqleverToml(toml);
    expect(serialized).toBe("\n");
  });

  it("quotes paths with slashes in table headers", () => {
    const toml = parseSqleverToml(loadFixture("basic.toml"));
    const serialized = serializeSqleverToml(toml);
    // File paths in table names should be quoted
    expect(serialized).toContain('"deploy/backfill_tiers.sql"');
  });
});
