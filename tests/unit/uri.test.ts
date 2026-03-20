import { describe, it, expect } from "bun:test";
import {
  parseUri,
  toStandardUri,
  toSqitchUri,
  sqitchToStandard,
  standardToSqitch,
} from "../../src/db/uri";

// ---------------------------------------------------------------------------
// parseUri()
// ---------------------------------------------------------------------------

describe("parseUri", () => {
  describe("standard postgresql:// scheme", () => {
    it("parses full URI with all components", () => {
      const result = parseUri("postgresql://alice:secret@db.example.com:5433/mydb");
      expect(result).toEqual({
        user: "alice",
        password: "secret",
        host: "db.example.com",
        port: 5433,
        database: "mydb",
        params: {},
      });
    });

    it("parses URI with default port", () => {
      const result = parseUri("postgresql://user:pass@host/db");
      expect(result.port).toBe(5432);
    });

    it("parses URI without password", () => {
      const result = parseUri("postgresql://user@host/db");
      expect(result.user).toBe("user");
      expect(result.password).toBeUndefined();
    });

    it("parses URI without user or password", () => {
      const result = parseUri("postgresql://host/db");
      expect(result.user).toBeUndefined();
      expect(result.password).toBeUndefined();
      expect(result.host).toBe("host");
      expect(result.database).toBe("db");
    });

    it("parses URI with query parameters", () => {
      const result = parseUri("postgresql://user:pass@host/db?sslmode=require&connect_timeout=10");
      expect(result.params).toEqual({
        sslmode: "require",
        connect_timeout: "10",
      });
    });

    it("parses URI without database", () => {
      const result = parseUri("postgresql://user:pass@host:5432/");
      expect(result.database).toBe("");
    });

    it("parses URI with localhost", () => {
      const result = parseUri("postgresql://localhost/testdb");
      expect(result.host).toBe("localhost");
      expect(result.database).toBe("testdb");
    });

    it("parses postgres:// shorthand scheme", () => {
      const result = parseUri("postgres://user:pass@host:5432/db");
      expect(result).toEqual({
        user: "user",
        password: "pass",
        host: "host",
        port: 5432,
        database: "db",
        params: {},
      });
    });

    it("parses URI with IPv4 host", () => {
      const result = parseUri("postgresql://user:pass@192.168.1.1:5432/db");
      expect(result.host).toBe("192.168.1.1");
    });

    it("parses URI with IPv6 host", () => {
      const result = parseUri("postgresql://user:pass@[::1]:5432/db");
      expect(result.host).toBe("::1");
    });

    it("parses URI with encoded special characters in password", () => {
      const result = parseUri("postgresql://user:p%40ss%3Aword@host/db");
      expect(result.password).toBe("p@ss:word");
    });

    it("parses URI with encoded special characters in user", () => {
      const result = parseUri("postgresql://us%40er:pass@host/db");
      expect(result.user).toBe("us@er");
    });
  });

  describe("Sqitch db:pg:// scheme", () => {
    it("parses full db:pg:// URI with all components", () => {
      const result = parseUri("db:pg://alice:secret@db.example.com:5433/mydb");
      expect(result).toEqual({
        user: "alice",
        password: "secret",
        host: "db.example.com",
        port: 5433,
        database: "mydb",
        params: {},
      });
    });

    it("parses db:pg:// URI with default port", () => {
      const result = parseUri("db:pg://user:pass@host/db");
      expect(result.port).toBe(5432);
    });

    it("parses db:pg:// URI without password", () => {
      const result = parseUri("db:pg://user@host/db");
      expect(result.user).toBe("user");
      expect(result.password).toBeUndefined();
    });

    it("parses db:pg:// URI without user or password", () => {
      const result = parseUri("db:pg://host/db");
      expect(result.user).toBeUndefined();
      expect(result.password).toBeUndefined();
      expect(result.host).toBe("host");
    });

    it("parses db:pg:// URI with query parameters", () => {
      const result = parseUri("db:pg://user:pass@host/db?sslmode=require");
      expect(result.params).toEqual({ sslmode: "require" });
    });
  });

  describe("error handling", () => {
    it("rejects unsupported scheme", () => {
      expect(() => parseUri("mysql://host/db")).toThrow("Unsupported URI scheme");
    });

    it("rejects empty string", () => {
      expect(() => parseUri("")).toThrow("Unsupported URI scheme");
    });

    it("rejects bare hostname", () => {
      expect(() => parseUri("localhost")).toThrow("Unsupported URI scheme");
    });

    it("error message does not leak password", () => {
      try {
        // This should parse fine, but if we craft a bad URI that still
        // has a password-like pattern...
        parseUri("db:pg://user:secret@:badport/db");
        // The above might parse or fail depending on URL parser
      } catch (e: unknown) {
        const message = (e as Error).message;
        expect(message).not.toContain("secret");
      }
    });
  });

  describe("equivalence between schemes", () => {
    it("parses identically regardless of scheme", () => {
      const sqitch = parseUri("db:pg://alice:secret@db.example.com:5433/mydb");
      const standard = parseUri("postgresql://alice:secret@db.example.com:5433/mydb");
      expect(sqitch).toEqual(standard);
    });

    it("parses identically for simple URIs", () => {
      const sqitch = parseUri("db:pg://localhost/testdb");
      const standard = parseUri("postgresql://localhost/testdb");
      expect(sqitch).toEqual(standard);
    });
  });
});

// ---------------------------------------------------------------------------
// toStandardUri()
// ---------------------------------------------------------------------------

describe("toStandardUri", () => {
  it("builds full URI with all components", () => {
    const uri = toStandardUri({
      user: "alice",
      password: "secret",
      host: "db.example.com",
      port: 5433,
      database: "mydb",
      params: {},
    });
    expect(uri).toBe("postgresql://alice:secret@db.example.com:5433/mydb");
  });

  it("omits port when default (5432)", () => {
    const uri = toStandardUri({
      user: "alice",
      password: "secret",
      host: "host",
      port: 5432,
      database: "db",
      params: {},
    });
    expect(uri).toBe("postgresql://alice:secret@host/db");
  });

  it("omits password when not set", () => {
    const uri = toStandardUri({
      user: "alice",
      host: "host",
      port: 5432,
      database: "db",
      params: {},
    });
    expect(uri).toBe("postgresql://alice@host/db");
  });

  it("omits user and password when not set", () => {
    const uri = toStandardUri({
      host: "host",
      port: 5432,
      database: "db",
      params: {},
    });
    expect(uri).toBe("postgresql://host/db");
  });

  it("includes query parameters", () => {
    const uri = toStandardUri({
      user: "alice",
      host: "host",
      port: 5432,
      database: "db",
      params: { sslmode: "require" },
    });
    expect(uri).toBe("postgresql://alice@host/db?sslmode=require");
  });

  it("wraps IPv6 host in brackets", () => {
    const uri = toStandardUri({
      user: "user",
      password: "pass",
      host: "::1",
      port: 5432,
      database: "db",
      params: {},
    });
    expect(uri).toBe("postgresql://user:pass@[::1]/db");
  });

  it("wraps full IPv6 host in brackets with non-default port", () => {
    const uri = toStandardUri({
      user: "user",
      password: "pass",
      host: "2001:db8::1",
      port: 5433,
      database: "db",
      params: {},
    });
    expect(uri).toBe("postgresql://user:pass@[2001:db8::1]:5433/db");
  });

  it("encodes special characters in user and password", () => {
    const uri = toStandardUri({
      user: "us@er",
      password: "p@ss:word",
      host: "host",
      port: 5432,
      database: "db",
      params: {},
    });
    expect(uri).toBe("postgresql://us%40er:p%40ss%3Aword@host/db");
  });
});

// ---------------------------------------------------------------------------
// toSqitchUri()
// ---------------------------------------------------------------------------

describe("toSqitchUri", () => {
  it("builds db:pg:// URI", () => {
    const uri = toSqitchUri({
      user: "alice",
      password: "secret",
      host: "db.example.com",
      port: 5433,
      database: "mydb",
      params: {},
    });
    expect(uri).toBe("db:pg://alice:secret@db.example.com:5433/mydb");
  });

  it("omits port when default", () => {
    const uri = toSqitchUri({
      user: "alice",
      host: "host",
      port: 5432,
      database: "db",
      params: {},
    });
    expect(uri).toBe("db:pg://alice@host/db");
  });
});

// ---------------------------------------------------------------------------
// sqitchToStandard() / standardToSqitch()
// ---------------------------------------------------------------------------

describe("sqitchToStandard", () => {
  it("converts db:pg:// to postgresql://", () => {
    expect(sqitchToStandard("db:pg://user:pass@host/db")).toBe(
      "postgresql://user:pass@host/db",
    );
  });

  it("passes through postgresql:// unchanged", () => {
    expect(sqitchToStandard("postgresql://user:pass@host/db")).toBe(
      "postgresql://user:pass@host/db",
    );
  });

  it("passes through postgres:// unchanged", () => {
    expect(sqitchToStandard("postgres://user:pass@host/db")).toBe(
      "postgres://user:pass@host/db",
    );
  });
});

describe("standardToSqitch", () => {
  it("converts postgresql:// to db:pg://", () => {
    expect(standardToSqitch("postgresql://user:pass@host/db")).toBe(
      "db:pg://user:pass@host/db",
    );
  });

  it("converts postgres:// to db:pg://", () => {
    expect(standardToSqitch("postgres://user:pass@host/db")).toBe(
      "db:pg://user:pass@host/db",
    );
  });

  it("passes through db:pg:// unchanged", () => {
    expect(standardToSqitch("db:pg://user:pass@host/db")).toBe(
      "db:pg://user:pass@host/db",
    );
  });
});

// ---------------------------------------------------------------------------
// Round-trip: parseUri -> toStandardUri / toSqitchUri -> parseUri
// ---------------------------------------------------------------------------

describe("round-trip conversion", () => {
  const testUris = [
    "postgresql://alice:secret@db.example.com:5433/mydb",
    "postgresql://user@host/db",
    "postgresql://host/db",
    "postgresql://user:pass@localhost:5432/testdb",
    "postgresql://user:pass@[::1]:5432/db",
    "postgresql://user:pass@[2001:db8::1]:5433/mydb",
  ];

  for (const uri of testUris) {
    it(`round-trips via standard: ${uri}`, () => {
      const parsed = parseUri(uri);
      const rebuilt = toStandardUri(parsed);
      const reparsed = parseUri(rebuilt);
      expect(reparsed).toEqual(parsed);
    });

    it(`round-trips via sqitch: ${uri}`, () => {
      const parsed = parseUri(uri);
      const sqitchUri = toSqitchUri(parsed);
      const reparsed = parseUri(sqitchUri);
      expect(reparsed).toEqual(parsed);
    });
  }
});
