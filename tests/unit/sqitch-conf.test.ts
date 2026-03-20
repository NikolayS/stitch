import { describe, it, expect } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import {
  parseSqitchConf,
  confGet,
  confGetString,
  confGetBool,
  confGetAll,
  confListSubsections,
  confGetSection,
  confSet,
  confUnset,
  serializeSqitchConf,
  toBool,
} from "../../src/config/sqitch-conf";

const FIXTURES = resolve(import.meta.dir, "../fixtures/config");

function loadFixture(name: string): string {
  return readFileSync(resolve(FIXTURES, name), "utf-8");
}

// ---------------------------------------------------------------------------
// toBool
// ---------------------------------------------------------------------------

describe("toBool", () => {
  it("recognizes truthy values", () => {
    expect(toBool("true")).toBe(true);
    expect(toBool("True")).toBe(true);
    expect(toBool("TRUE")).toBe(true);
    expect(toBool("yes")).toBe(true);
    expect(toBool("Yes")).toBe(true);
    expect(toBool("on")).toBe(true);
    expect(toBool("ON")).toBe(true);
    expect(toBool("1")).toBe(true);
  });

  it("recognizes falsy values", () => {
    expect(toBool("false")).toBe(false);
    expect(toBool("False")).toBe(false);
    expect(toBool("no")).toBe(false);
    expect(toBool("No")).toBe(false);
    expect(toBool("off")).toBe(false);
    expect(toBool("OFF")).toBe(false);
    expect(toBool("0")).toBe(false);
  });

  it("returns true for bare key (true literal)", () => {
    expect(toBool(true)).toBe(true);
  });

  it("returns undefined for non-boolean strings", () => {
    expect(toBool("pg")).toBeUndefined();
    expect(toBool("42")).toBeUndefined();
    expect(toBool("")).toBeUndefined();
  });

  it("returns undefined for undefined input", () => {
    expect(toBool(undefined)).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Customer zero — PostgresAI Console sqitch.conf
// ---------------------------------------------------------------------------

describe("customer zero (PostgresAI Console sqitch.conf)", () => {
  const conf = parseSqitchConf(loadFixture("customer-zero.conf"));

  it("parses [core] section", () => {
    expect(confGetString(conf, "core.engine")).toBe("pg");
    expect(confGetString(conf, "core.top_dir")).toBe("./db");
    expect(confGetString(conf, "core.deploy_dir")).toBe("db/deploy");
    expect(confGetString(conf, "core.revert_dir")).toBe("db/revert");
    expect(confGetString(conf, "core.verify_dir")).toBe("db/verify");
  });

  it("handles empty subsection [engine \"pg\"]", () => {
    const subs = confListSubsections(conf, "engine");
    // The empty subsection [engine "pg"] creates no entries,
    // so it won't appear in subsection listing (no keys to list).
    // This is correct — an empty section is valid but has no data.
    expect(subs).toEqual([]);
  });

  it("parses [target \"localtest\"] with URI containing password", () => {
    const subs = confListSubsections(conf, "target");
    expect(subs).toEqual(["localtest"]);
    expect(confGetString(conf, "target.localtest.uri")).toBe(
      "db:pg://postgres:Secret@localhost:5460/postgres_ai",
    );
  });

  it("has the correct number of entries", () => {
    // 5 core entries + 1 target entry = 6
    expect(conf.entries.length).toBe(6);
  });
});

// ---------------------------------------------------------------------------
// parseSqitchConf — basic parsing
// ---------------------------------------------------------------------------

describe("parseSqitchConf", () => {
  it("parses a full config with multiple sections", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    expect(confGetString(conf, "core.engine")).toBe("pg");
    expect(confGetString(conf, "core.top_dir")).toBe("migrations");
    expect(confGetString(conf, "core.plan_file")).toBe("migrations/sqitch.plan");
    expect(confGetString(conf, "engine.pg.target")).toBe("db:pg:mydb");
    expect(confGetString(conf, "engine.pg.client")).toBe("/usr/bin/psql");
    expect(confGetBool(conf, "deploy.verify")).toBe(true);
    expect(confGetString(conf, "deploy.mode")).toBe("change");
    expect(confGetString(conf, "target.production.uri")).toBe("db:pg://user@host/dbname");
    expect(confGetString(conf, "target.staging.uri")).toBe(
      "db:pg://user:pass@staging.example.com:5432/mydb",
    );
  });

  it("handles comments correctly", () => {
    const conf = parseSqitchConf(loadFixture("comments.conf"));
    expect(confGetString(conf, "core.engine")).toBe("pg");
    expect(confGetString(conf, "core.top_dir")).toBe("./src");
    expect(confGetBool(conf, "deploy.verify")).toBe(true); // "yes" => true
    expect(confGetString(conf, "deploy.mode")).toBe("change");
  });

  it("handles empty config", () => {
    const conf = parseSqitchConf(loadFixture("empty.conf"));
    expect(conf.entries).toEqual([]);
  });

  it("handles empty string", () => {
    const conf = parseSqitchConf("");
    expect(conf.entries).toEqual([]);
  });

  it("handles quoted values", () => {
    const conf = parseSqitchConf(loadFixture("quoted-values.conf"));
    expect(confGetString(conf, "core.top_dir")).toBe("path with spaces");
    expect(confGetString(conf, "core.deploy_dir")).toBe("deploy");
    expect(confGetString(conf, "target.my target.uri")).toBe(
      "db:pg://user:pass@host/db",
    );
  });

  it("handles Windows line endings", () => {
    const text = "[core]\r\n\tengine = pg\r\n\ttop_dir = .\r\n";
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.engine")).toBe("pg");
    expect(confGetString(conf, "core.top_dir")).toBe(".");
  });

  it("handles key=value without spaces", () => {
    const text = "[core]\n\tengine=pg\n";
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.engine")).toBe("pg");
  });

  it("handles key = value with extra spaces", () => {
    const text = "[core]\n\tengine   =   pg\n";
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.engine")).toBe("pg");
  });

  it("handles subsection names with spaces", () => {
    const text = '[target "my production"]\n\turi = db:pg://host/db\n';
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "target.my production.uri")).toBe("db:pg://host/db");
  });
});

// ---------------------------------------------------------------------------
// Boolean values
// ---------------------------------------------------------------------------

describe("boolean values", () => {
  const conf = parseSqitchConf(loadFixture("booleans.conf"));

  it("parses deploy.verify as true", () => {
    expect(confGetBool(conf, "deploy.verify")).toBe(true);
  });

  it("parses truthy values", () => {
    expect(confGetBool(conf, "test.truthy.a")).toBe(true); // true
    expect(confGetBool(conf, "test.truthy.b")).toBe(true); // yes
    expect(confGetBool(conf, "test.truthy.c")).toBe(true); // on
    expect(confGetBool(conf, "test.truthy.d")).toBe(true); // 1
  });

  it("parses falsy values", () => {
    expect(confGetBool(conf, "test.falsy.a")).toBe(false); // false
    expect(confGetBool(conf, "test.falsy.b")).toBe(false); // no
    expect(confGetBool(conf, "test.falsy.c")).toBe(false); // off
    expect(confGetBool(conf, "test.falsy.d")).toBe(false); // 0
  });

  it("parses bare keys as boolean true", () => {
    expect(confGet(conf, "test.bare.enabled")).toBe(true);
    expect(confGetBool(conf, "test.bare.enabled")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Multi-valued keys
// ---------------------------------------------------------------------------

describe("multi-valued keys", () => {
  const conf = parseSqitchConf(loadFixture("multi-valued.conf"));

  it("confGet returns last value (last-write-wins)", () => {
    expect(confGetString(conf, "core.engine")).toBe("pg");
  });

  it("confGetAll returns all values in order", () => {
    const values = confGetAll(conf, "core.engine");
    expect(values).toEqual(["pg", "mysql", "pg"]);
  });
});

// ---------------------------------------------------------------------------
// confListSubsections
// ---------------------------------------------------------------------------

describe("confListSubsections", () => {
  it("lists all subsections for a section", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    expect(confListSubsections(conf, "target")).toEqual(["production", "staging"]);
    expect(confListSubsections(conf, "engine")).toEqual(["pg"]);
  });

  it("returns empty array for section with no subsections", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    expect(confListSubsections(conf, "core")).toEqual([]);
    expect(confListSubsections(conf, "deploy")).toEqual([]);
  });

  it("returns empty array for nonexistent section", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    expect(confListSubsections(conf, "nonexistent")).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// confGetSection
// ---------------------------------------------------------------------------

describe("confGetSection", () => {
  it("returns all key-values for a section without subsection", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    const core = confGetSection(conf, "core");
    expect(core).toEqual({
      engine: "pg",
      top_dir: "migrations",
      plan_file: "migrations/sqitch.plan",
    });
  });

  it("returns all key-values for a subsection", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    const pgEngine = confGetSection(conf, "engine", "pg");
    expect(pgEngine).toEqual({
      target: "db:pg:mydb",
      client: "/usr/bin/psql",
    });
  });

  it("returns empty object for empty or nonexistent section", () => {
    const conf = parseSqitchConf(loadFixture("full.conf"));
    expect(confGetSection(conf, "nonexistent")).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// confGet — missing keys
// ---------------------------------------------------------------------------

describe("confGet missing keys", () => {
  const conf = parseSqitchConf(loadFixture("full.conf"));

  it("returns undefined for nonexistent key", () => {
    expect(confGet(conf, "core.nonexistent")).toBeUndefined();
  });

  it("returns undefined for nonexistent section", () => {
    expect(confGet(conf, "nonexistent.key")).toBeUndefined();
  });

  it("is case-insensitive for key lookup", () => {
    expect(confGetString(conf, "CORE.ENGINE")).toBe("pg");
    expect(confGetString(conf, "Core.Engine")).toBe("pg");
  });
});

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

describe("confSet", () => {
  it("updates existing key", () => {
    const conf = parseSqitchConf("[core]\n\tengine = pg\n");
    confSet(conf, "core.engine", "mysql");
    expect(confGetString(conf, "core.engine")).toBe("mysql");
  });

  it("appends new key", () => {
    const conf = parseSqitchConf("[core]\n\tengine = pg\n");
    confSet(conf, "core.top_dir", "./src");
    expect(confGetString(conf, "core.top_dir")).toBe("./src");
  });

  it("updates last occurrence for multi-valued key", () => {
    const conf = parseSqitchConf("[core]\n\tengine = pg\n\tengine = mysql\n");
    confSet(conf, "core.engine", "sqlite");
    expect(confGetString(conf, "core.engine")).toBe("sqlite");
    // Should have updated the last one
    expect(confGetAll(conf, "core.engine")).toEqual(["pg", "sqlite"]);
  });
});

describe("confUnset", () => {
  it("removes all entries for a key", () => {
    const conf = parseSqitchConf("[core]\n\tengine = pg\n\tengine = mysql\n");
    confUnset(conf, "core.engine");
    expect(confGet(conf, "core.engine")).toBeUndefined();
    expect(confGetAll(conf, "core.engine")).toEqual([]);
  });

  it("is a no-op for nonexistent keys", () => {
    const conf = parseSqitchConf("[core]\n\tengine = pg\n");
    confUnset(conf, "core.nonexistent");
    expect(conf.entries.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

describe("serializeSqitchConf", () => {
  it("produces valid INI from parsed config", () => {
    const original = loadFixture("full.conf");
    const conf = parseSqitchConf(original);
    const serialized = serializeSqitchConf(conf);

    // Re-parse the serialized output
    const reparsed = parseSqitchConf(serialized);
    expect(confGetString(reparsed, "core.engine")).toBe("pg");
    expect(confGetString(reparsed, "core.top_dir")).toBe("migrations");
    expect(confGetString(reparsed, "engine.pg.target")).toBe("db:pg:mydb");
    expect(confGetString(reparsed, "target.production.uri")).toBe("db:pg://user@host/dbname");
  });

  it("round-trips set/serialize/parse", () => {
    const conf = parseSqitchConf("");
    confSet(conf, "core.engine", "pg");
    confSet(conf, "core.top_dir", "./migrations");
    confSet(conf, "target.prod.uri", "db:pg://host/db");

    const serialized = serializeSqitchConf(conf);
    const reparsed = parseSqitchConf(serialized);

    expect(confGetString(reparsed, "core.engine")).toBe("pg");
    expect(confGetString(reparsed, "core.top_dir")).toBe("./migrations");
    expect(confGetString(reparsed, "target.prod.uri")).toBe("db:pg://host/db");
  });

  it("handles empty config", () => {
    const conf = parseSqitchConf("");
    const serialized = serializeSqitchConf(conf);
    expect(serialized).toBe("\n");
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe("edge cases", () => {
  it("ignores keys before any section header", () => {
    const text = "orphan = value\n[core]\n\tengine = pg\n";
    const conf = parseSqitchConf(text);
    expect(conf.entries.length).toBe(1);
    expect(confGetString(conf, "core.engine")).toBe("pg");
  });

  it("handles section with trailing comment", () => {
    const text = "[core] # main section\n\tengine = pg\n";
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.engine")).toBe("pg");
  });

  it("handles value with hash that is not a comment", () => {
    // Hash inside a quoted string should not be treated as comment
    const text = '[core]\n\tname = "my#project"\n';
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.name")).toBe("my#project");
  });

  it("handles escaped characters in quoted values", () => {
    const text = '[core]\n\tpath = "a\\\\b"\n';
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.path")).toBe("a\\b");
  });

  it("handles multiple sections of the same name", () => {
    const text = "[core]\n\tengine = pg\n[core]\n\ttop_dir = ./src\n";
    const conf = parseSqitchConf(text);
    expect(confGetString(conf, "core.engine")).toBe("pg");
    expect(confGetString(conf, "core.top_dir")).toBe("./src");
  });
});
