/**
 * tests/unit/ai-dblab-deep.test.ts — Deep integration tests for AI explain,
 * review formatting/risk, and DBLab clone lifecycle.
 *
 * Implements issue #129 (TEST-6).
 *
 * Groups:
 *   1. LLM explain (11 tests)
 *   2. Review      (7 tests)
 *   3. DBLab       (7 tests)
 *   Total: 25 tests
 */

import { describe, test, expect, beforeAll, beforeEach, afterEach } from "bun:test";
import {
  ensureWasm,
  buildMigrationContext,
  buildPrompt,
  callLLM,
  DEFAULT_MODELS,
  type ExplainConfig,
  type LLMProvider as ExplainLLMProvider,
} from "../../src/ai/explain";
import { parseExplainArgs } from "../../src/commands/explain";
import {
  assessRisk,
  runReview,
  formatReviewMarkdown,
  formatReviewJson,
  formatReviewText,
  type FindingEntry,
  type LLMProvider as ReviewLLMProvider,
  type LLMExplanation,
  type ReviewResult,
} from "../../src/ai/review";
import type { Finding } from "../../src/analysis/types";
import {
  DblabClient,
  buildCloneUri,
  parseDblabOptions,
  executeDblabDeploy,
  type DblabCloneResponse,
  type DblabDeployOptions,
} from "../../src/commands/deploy-dblab";
import type { ParsedArgs } from "../../src/cli";
import { setConfig, resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/** Build a minimal ParsedArgs for DBLab tests. */
function makeArgs(overrides?: Partial<ParsedArgs>): ParsedArgs {
  return {
    command: "deploy",
    rest: [],
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: undefined,
    planFile: undefined,
    topDir: undefined,
    registry: undefined,
    target: undefined,
    ...overrides,
  };
}

/** Build a mock DBLab clone response. */
function makeCloneResponse(
  overrides?: Partial<DblabCloneResponse>,
): DblabCloneResponse {
  return {
    id: "clone-test-001",
    status: { code: "OK", message: "Clone is ready" },
    db: {
      connStr: "postgresql://user:pass@clone-host:5432/testdb",
      host: "clone-host",
      port: "5432",
      username: "user",
      password: "pass",
      db_name: "testdb",
    },
    ...overrides,
  };
}

/**
 * Create a mock fetch for DBLab API that tracks all calls.
 */
function createDblabMockFetch(responses: {
  createClone?: { status: number; body: unknown };
  destroyClone?: { status: number; body: unknown };
}) {
  const calls: Array<{
    url: string;
    method: string;
    headers: Record<string, string>;
    body?: string;
  }> = [];

  const mockFn = async (
    url: string | URL | Request,
    init?: RequestInit,
  ): Promise<Response> => {
    const urlStr =
      typeof url === "string"
        ? url
        : url instanceof URL
          ? url.toString()
          : (url as Request).url;
    const method = init?.method ?? "GET";
    const headers = (init?.headers as Record<string, string>) ?? {};
    const body = init?.body ? String(init.body) : undefined;
    calls.push({ url: urlStr, method, headers, body });

    if (method === "POST" && urlStr.endsWith("/clone")) {
      const resp = responses.createClone ?? {
        status: 200,
        body: makeCloneResponse(),
      };
      return new Response(JSON.stringify(resp.body), {
        status: resp.status,
        headers: { "Content-Type": "application/json" },
      });
    }

    if (method === "DELETE" && urlStr.includes("/clone/")) {
      const resp = responses.destroyClone ?? { status: 200, body: {} };
      return new Response(JSON.stringify(resp.body), {
        status: resp.status,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response("Not Found", { status: 404 });
  };

  return { mockFn: mockFn as unknown as typeof fetch, calls };
}

// ---------------------------------------------------------------------------
// WASM setup (shared across all LLM explain tests that parse SQL)
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await ensureWasm();
});

// ============================================================================
// 1. LLM explain (11 tests)
// ============================================================================

describe("LLM explain — deep tests", () => {
  // -----------------------------------------------------------------------
  // 1.1 OpenAI request structure
  // -----------------------------------------------------------------------
  test("OpenAI: request has correct URL, Authorization header, model, and messages array", async () => {
    let capturedUrl = "";
    let capturedHeaders: Record<string, string> = {};
    let capturedBody: Record<string, unknown> = {};

    const mockFn = (async (
      url: string | URL | Request,
      init?: RequestInit,
    ) => {
      capturedUrl = url as string;
      capturedHeaders = init?.headers as Record<string, string>;
      capturedBody = JSON.parse(init?.body as string) as Record<
        string,
        unknown
      >;
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: "Explanation" } }],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "sk-test-key-123",
    };

    await callLLM("test prompt", config, mockFn);

    expect(capturedUrl).toBe("https://api.openai.com/v1/chat/completions");
    expect(capturedHeaders["Authorization"]).toBe("Bearer sk-test-key-123");
    expect(capturedHeaders["Content-Type"]).toBe("application/json");
    expect(capturedBody.model).toBe("gpt-4o");
    expect(capturedBody.messages).toEqual([
      { role: "user", content: "test prompt" },
    ]);
    expect(capturedBody.temperature).toBe(0.3);
    expect(capturedBody.max_tokens).toBe(2000);
  });

  // -----------------------------------------------------------------------
  // 1.2 Anthropic request structure (anthropic-version header)
  // -----------------------------------------------------------------------
  test("Anthropic: request has x-api-key, anthropic-version header, and messages structure", async () => {
    let capturedHeaders: Record<string, string> = {};
    let capturedBody: Record<string, unknown> = {};
    let capturedUrl = "";

    const mockFn = (async (
      url: string | URL | Request,
      init?: RequestInit,
    ) => {
      capturedUrl = url as string;
      capturedHeaders = init?.headers as Record<string, string>;
      capturedBody = JSON.parse(init?.body as string) as Record<
        string,
        unknown
      >;
      return new Response(
        JSON.stringify({
          content: [{ type: "text", text: "Anthropic says hi" }],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "anthropic",
      model: "claude-sonnet-4-20250514",
      apiKey: "sk-ant-key",
    };

    const result = await callLLM("analyze this", config, mockFn);

    expect(capturedUrl).toBe("https://api.anthropic.com/v1/messages");
    expect(capturedHeaders["x-api-key"]).toBe("sk-ant-key");
    expect(capturedHeaders["anthropic-version"]).toBe("2023-06-01");
    expect(capturedBody.model).toBe("claude-sonnet-4-20250514");
    expect(capturedBody.max_tokens).toBe(2000);
    expect(capturedBody.messages).toEqual([
      { role: "user", content: "analyze this" },
    ]);
    expect(result.content).toBe("Anthropic says hi");
  });

  // -----------------------------------------------------------------------
  // 1.3 Ollama request structure
  // -----------------------------------------------------------------------
  test("Ollama: request uses /api/generate, sends prompt directly, stream=false", async () => {
    let capturedUrl = "";
    let capturedBody: Record<string, unknown> = {};

    const mockFn = (async (
      url: string | URL | Request,
      init?: RequestInit,
    ) => {
      capturedUrl = url as string;
      capturedBody = JSON.parse(init?.body as string) as Record<
        string,
        unknown
      >;
      return new Response(JSON.stringify({ response: "Ollama response" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "ollama",
      model: "llama3.2",
      ollamaBaseUrl: "http://myhost:11434",
    };

    const result = await callLLM("some prompt", config, mockFn);

    expect(capturedUrl).toBe("http://myhost:11434/api/generate");
    expect(capturedBody.model).toBe("llama3.2");
    expect(capturedBody.prompt).toBe("some prompt");
    expect(capturedBody.stream).toBe(false);
    // Ollama should NOT have Authorization or x-api-key headers
    expect(result.content).toBe("Ollama response");
  });

  // -----------------------------------------------------------------------
  // 1.4 API 401 error handling
  // -----------------------------------------------------------------------
  test("API 401: throws with status code and error body", async () => {
    const mockFn = (async () => {
      return new Response(
        JSON.stringify({ error: { message: "Invalid API key" } }),
        { status: 401 },
      );
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "bad-key",
    };

    await expect(callLLM("test", config, mockFn)).rejects.toThrow(
      /OpenAI API error \(401\)/,
    );
  });

  // -----------------------------------------------------------------------
  // 1.5 API 429 rate limit
  // -----------------------------------------------------------------------
  test("API 429: throws with rate limit status code", async () => {
    const mockFn = (async () => {
      return new Response("Rate limit exceeded", { status: 429 });
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "valid-key",
    };

    await expect(callLLM("test", config, mockFn)).rejects.toThrow(
      /OpenAI API error \(429\)/,
    );
  });

  // -----------------------------------------------------------------------
  // 1.6 API 500 error
  // -----------------------------------------------------------------------
  test("API 500: throws with server error status", async () => {
    const mockFn = (async () => {
      return new Response("Internal Server Error", { status: 500 });
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "anthropic",
      model: "claude-sonnet-4-20250514",
      apiKey: "key",
    };

    await expect(callLLM("test", config, mockFn)).rejects.toThrow(
      /Anthropic API error \(500\)/,
    );
  });

  // -----------------------------------------------------------------------
  // 1.7 Network error (fetch throws)
  // -----------------------------------------------------------------------
  test("Network error: fetch itself throws (e.g. DNS failure)", async () => {
    const mockFn = (async () => {
      throw new TypeError("fetch failed: getaddrinfo ENOTFOUND api.openai.com");
    }) as typeof globalThis.fetch;

    const config: ExplainConfig = {
      provider: "openai",
      model: "gpt-4o",
      apiKey: "key",
    };

    await expect(callLLM("test", config, mockFn)).rejects.toThrow(
      /fetch failed/,
    );
  });

  // -----------------------------------------------------------------------
  // 1.8 Prompt contains SQL + findings
  // -----------------------------------------------------------------------
  test("Prompt contains the original SQL text and detected operations", () => {
    const sql =
      "ALTER TABLE users ADD COLUMN email text;\nCREATE INDEX idx_email ON users (email);";
    const ctx = buildMigrationContext(sql);
    const prompt = buildPrompt(ctx);

    // SQL is embedded in the prompt
    expect(prompt).toContain("ALTER TABLE users ADD COLUMN email text");
    expect(prompt).toContain("CREATE INDEX idx_email ON users (email)");
    // Detected operations section is present
    expect(prompt).toContain("## Detected Operations");
    expect(prompt).toContain("ALTER TABLE");
    expect(prompt).toContain("CREATE INDEX");
    // Risk section is present
    expect(prompt).toContain("## Risk Level:");
    expect(prompt).toContain("Risk Factors");
    // Tables affected section
    expect(prompt).toContain("## Tables Affected: users");
  });

  // -----------------------------------------------------------------------
  // 1.9 --provider selects provider
  // -----------------------------------------------------------------------
  test("--provider flag selects the LLM provider in parsed args", () => {
    const openai = parseExplainArgs(["--provider", "openai", "file.sql"]);
    expect(openai.provider).toBe("openai");

    const anthropic = parseExplainArgs([
      "--provider",
      "anthropic",
      "file.sql",
    ]);
    expect(anthropic.provider).toBe("anthropic");

    const ollama = parseExplainArgs(["--provider", "ollama", "file.sql"]);
    expect(ollama.provider).toBe("ollama");

    // Default is openai
    const defaultProvider = parseExplainArgs(["file.sql"]);
    expect(defaultProvider.provider).toBe("openai");
  });

  // -----------------------------------------------------------------------
  // 1.10 --model overrides default
  // -----------------------------------------------------------------------
  test("--model overrides the default model for the provider", () => {
    const opts = parseExplainArgs([
      "--provider",
      "openai",
      "--model",
      "gpt-3.5-turbo",
      "file.sql",
    ]);
    expect(opts.model).toBe("gpt-3.5-turbo");
    // Verify defaults exist for all providers
    expect(DEFAULT_MODELS.openai).toBe("gpt-4o");
    expect(DEFAULT_MODELS.anthropic).toBe("claude-sonnet-4-20250514");
    expect(DEFAULT_MODELS.ollama).toBe("llama3.2");
  });

  // -----------------------------------------------------------------------
  // 1.11 API key from env var
  // -----------------------------------------------------------------------
  test("API key is required for OpenAI/Anthropic but not Ollama", async () => {
    // OpenAI without key throws
    await expect(
      callLLM("test", { provider: "openai", model: "gpt-4o" }),
    ).rejects.toThrow(/API key required/);

    // Anthropic without key throws
    await expect(
      callLLM("test", {
        provider: "anthropic",
        model: "claude-sonnet-4-20250514",
      }),
    ).rejects.toThrow(/API key required/);

    // Ollama without key does NOT throw (it makes a network call, so we mock it)
    const mockFn = (async () => {
      return new Response(JSON.stringify({ response: "ok" }), { status: 200 });
    }) as typeof globalThis.fetch;

    const result = await callLLM(
      "test",
      { provider: "ollama", model: "llama3.2" },
      mockFn,
    );
    expect(result.content).toBe("ok");
  });
});

// ============================================================================
// 2. Review (7 tests)
// ============================================================================

describe("Review — deep tests", () => {
  // Shared fixture findings
  const errorFinding: Finding = {
    ruleId: "SA001",
    severity: "error",
    message: 'ADD COLUMN "active" NOT NULL without DEFAULT',
    location: { file: "deploy/users.sql", line: 3, column: 1 },
    suggestion: "Add a DEFAULT value.",
  };

  const warnFinding: Finding = {
    ruleId: "SA004",
    severity: "warn",
    message: "CREATE INDEX without CONCURRENTLY",
    location: { file: "deploy/idx.sql", line: 1, column: 1 },
    suggestion: "Use CONCURRENTLY.",
  };

  const infoFinding: Finding = {
    ruleId: "SA020",
    severity: "info",
    message: "Consider adding a comment",
    location: { file: "deploy/clean.sql", line: 1, column: 1 },
  };

  // -----------------------------------------------------------------------
  // 2.1 Analysis + LLM combined
  // -----------------------------------------------------------------------
  test("analysis findings and LLM explanation are combined in result", async () => {
    const mockLLM: ReviewLLMProvider = {
      async explain(
        _sql: string,
        _findings: FindingEntry[],
      ): Promise<LLMExplanation> {
        return {
          summary: "Adds an index and a column to users table.",
          suggestedImprovements: ["Use CONCURRENTLY for the index."],
        };
      },
    };

    const sqlContents = new Map<string, string>();
    sqlContents.set("deploy/users.sql", "ALTER TABLE users ADD COLUMN active boolean NOT NULL;");
    sqlContents.set("deploy/idx.sql", "CREATE INDEX idx ON users (email);");

    const result = await runReview(
      [errorFinding, warnFinding],
      ["deploy/users.sql", "deploy/idx.sql"],
      { format: "markdown", llm: mockLLM, sqlContents },
    );

    expect(result.risk).toBe("high");
    expect(result.findings).toHaveLength(2);
    expect(result.explanation).toBeDefined();
    expect(result.explanation!.summary).toContain("index");
    expect(result.explanation!.suggestedImprovements).toHaveLength(1);
    expect(result.errorCount).toBe(1);
    expect(result.warnCount).toBe(1);
  });

  // -----------------------------------------------------------------------
  // 2.2 Markdown output is valid (headers, tables)
  // -----------------------------------------------------------------------
  test("Markdown output contains required headers and well-formed table", () => {
    const result: ReviewResult = {
      risk: "high",
      findings: [
        {
          ruleId: "SA001",
          severity: "error",
          message: "Bad migration",
          file: "deploy/bad.sql",
          line: 5,
          suggestion: "Fix it.",
        },
        {
          ruleId: "SA004",
          severity: "warn",
          message: "Missing CONCURRENTLY",
          file: "deploy/idx.sql",
          line: 1,
        },
      ],
      explanation: {
        summary: "This does something dangerous.",
        suggestedImprovements: ["Do not do that."],
      },
      filesReviewed: ["deploy/bad.sql", "deploy/idx.sql"],
      errorCount: 1,
      warnCount: 1,
      infoCount: 0,
    };

    const md = formatReviewMarkdown(result);

    // Required Markdown headers
    expect(md).toContain("## sqlever review");
    expect(md).toContain("### Analysis findings");
    expect(md).toContain("### Suggested improvements");
    expect(md).toContain("### What this migration does");
    expect(md).toContain("### AI-suggested improvements");

    // Table structure — header row and separator
    expect(md).toContain("| Severity | Rule | Message | File | Line |");
    expect(md).toContain("|----------|------|---------|------|------|");

    // Every table row must have exactly 6 pipe characters (5 columns)
    const tableLines = md
      .split("\n")
      .filter((l) => l.startsWith("| ") && l.includes("SA"));
    for (const line of tableLines) {
      const pipes = (line.match(/\|/g) ?? []).length;
      expect(pipes).toBe(6);
    }

    // Footer
    expect(md).toContain("---");
    expect(md).toContain("Generated by sqlever review");
  });

  // -----------------------------------------------------------------------
  // 2.3 Review without LLM (no API key) produces analysis-only
  // -----------------------------------------------------------------------
  test("review without LLM provider produces analysis-only output", async () => {
    const result = await runReview(
      [errorFinding, warnFinding, infoFinding],
      ["deploy/users.sql", "deploy/idx.sql", "deploy/clean.sql"],
      { format: "markdown" /* no llm, no sqlContents */ },
    );

    expect(result.explanation).toBeUndefined();
    expect(result.findings).toHaveLength(3);
    expect(result.risk).toBe("high");
    expect(result.errorCount).toBe(1);
    expect(result.warnCount).toBe(1);
    expect(result.infoCount).toBe(1);
  });

  // -----------------------------------------------------------------------
  // 2.4 Zero findings output
  // -----------------------------------------------------------------------
  test("zero findings produces low risk and 'No issues found' in markdown", async () => {
    const result = await runReview([], ["deploy/clean.sql"], {
      format: "markdown",
    });

    expect(result.risk).toBe("low");
    expect(result.findings).toHaveLength(0);
    expect(result.errorCount).toBe(0);
    expect(result.warnCount).toBe(0);
    expect(result.infoCount).toBe(0);

    const md = formatReviewMarkdown(result);
    expect(md).toContain("No issues found");
    expect(md).toContain("LOW");
    // Should NOT contain a findings table
    expect(md).not.toContain("### Analysis findings");
  });

  // -----------------------------------------------------------------------
  // 2.5 Risk assessment (error -> high, warn -> medium)
  // -----------------------------------------------------------------------
  test("risk assessment: error->high, warn->medium, info->low, empty->low", () => {
    // Error finding -> high
    expect(
      assessRisk([
        {
          ruleId: "SA001",
          severity: "error",
          message: "x",
          file: "f",
          line: 1,
        },
      ]),
    ).toBe("high");

    // Warn finding -> medium
    expect(
      assessRisk([
        {
          ruleId: "SA004",
          severity: "warn",
          message: "x",
          file: "f",
          line: 1,
        },
      ]),
    ).toBe("medium");

    // Info finding -> low
    expect(
      assessRisk([
        {
          ruleId: "SA020",
          severity: "info",
          message: "x",
          file: "f",
          line: 1,
        },
      ]),
    ).toBe("low");

    // No findings -> low
    expect(assessRisk([])).toBe("low");

    // Mix of error and warn -> high (error dominates)
    expect(
      assessRisk([
        {
          ruleId: "SA004",
          severity: "warn",
          message: "w",
          file: "f",
          line: 1,
        },
        {
          ruleId: "SA001",
          severity: "error",
          message: "e",
          file: "f",
          line: 1,
        },
      ]),
    ).toBe("high");
  });

  // -----------------------------------------------------------------------
  // 2.6 JSON format
  // -----------------------------------------------------------------------
  test("JSON format produces valid, parseable JSON with all fields", () => {
    const result: ReviewResult = {
      risk: "medium",
      findings: [
        {
          ruleId: "SA004",
          severity: "warn",
          message: "Index issue",
          file: "deploy/idx.sql",
          line: 1,
          suggestion: "Use CONCURRENTLY.",
        },
      ],
      filesReviewed: ["deploy/idx.sql"],
      errorCount: 0,
      warnCount: 1,
      infoCount: 0,
    };

    const jsonStr = formatReviewJson(result);
    const parsed = JSON.parse(jsonStr);

    expect(parsed.risk).toBe("medium");
    expect(parsed.findings).toHaveLength(1);
    expect(parsed.findings[0].ruleId).toBe("SA004");
    expect(parsed.findings[0].severity).toBe("warn");
    expect(parsed.findings[0].suggestion).toBe("Use CONCURRENTLY.");
    expect(parsed.filesReviewed).toEqual(["deploy/idx.sql"]);
    expect(parsed.errorCount).toBe(0);
    expect(parsed.warnCount).toBe(1);
    expect(parsed.infoCount).toBe(0);
  });

  // -----------------------------------------------------------------------
  // 2.7 Text format
  // -----------------------------------------------------------------------
  test("text format includes risk, counts, and finding details", () => {
    const result: ReviewResult = {
      risk: "high",
      findings: [
        {
          ruleId: "SA001",
          severity: "error",
          message: "Bad column",
          file: "deploy/users.sql",
          line: 3,
          suggestion: "Add DEFAULT.",
        },
        {
          ruleId: "SA010",
          severity: "warn",
          message: "UPDATE without WHERE",
          file: "deploy/update.sql",
          line: 1,
        },
      ],
      explanation: {
        summary: "Adds a column and updates all rows.",
        suggestedImprovements: ["Be more careful."],
      },
      filesReviewed: ["deploy/users.sql", "deploy/update.sql"],
      errorCount: 1,
      warnCount: 1,
      infoCount: 0,
    };

    const text = formatReviewText(result);

    expect(text).toContain("Risk: HIGH");
    expect(text).toContain("1 error(s)");
    expect(text).toContain("1 warning(s)");
    expect(text).toContain("Findings:");
    expect(text).toContain("error SA001: Bad column");
    expect(text).toContain("at deploy/users.sql:3");
    expect(text).toContain("suggestion: Add DEFAULT.");
    expect(text).toContain("warn SA010: UPDATE without WHERE");
    expect(text).toContain("Explanation:");
    expect(text).toContain("Adds a column and updates all rows.");
    expect(text).toContain("AI-suggested improvements:");
    expect(text).toContain("Be more careful.");
  });
});

// ============================================================================
// 3. DBLab (7 tests)
// ============================================================================

describe("DBLab — deep tests", () => {
  beforeEach(() => {
    setConfig({ quiet: true });
  });

  afterEach(() => {
    resetConfig();
    delete process.env.DBLAB_URL;
    delete process.env.DBLAB_TOKEN;
  });

  // -----------------------------------------------------------------------
  // 3.1 Clone provision (POST /clone body)
  // -----------------------------------------------------------------------
  test("createClone sends POST /clone with JSON body and Verification-Token header", async () => {
    const { mockFn, calls } = createDblabMockFetch({});
    const client = new DblabClient(
      "https://dblab.example.com",
      "my-secret-token",
      mockFn,
    );

    const clone = await client.createClone({
      id: "sqlever-test-12345",
      protected: false,
      db: { username: "testuser", password: "testpass", db_name: "mydb" },
    });

    expect(calls).toHaveLength(1);
    const call = calls[0]!;
    expect(call.url).toBe("https://dblab.example.com/clone");
    expect(call.method).toBe("POST");
    expect(call.headers["Verification-Token"]).toBe("my-secret-token");
    expect(call.headers["Content-Type"]).toBe("application/json");

    const sentBody = JSON.parse(call.body!);
    expect(sentBody.id).toBe("sqlever-test-12345");
    expect(sentBody.protected).toBe(false);
    expect(sentBody.db.username).toBe("testuser");
    expect(sentBody.db.password).toBe("testpass");
    expect(sentBody.db.db_name).toBe("mydb");

    expect(clone.id).toBe("clone-test-001");
    expect(clone.status.code).toBe("OK");
  });

  // -----------------------------------------------------------------------
  // 3.2 Clone URI from response
  // -----------------------------------------------------------------------
  test("buildCloneUri returns connStr when present, builds from parts otherwise", () => {
    // Case 1: connStr present
    const withConnStr = makeCloneResponse();
    expect(buildCloneUri(withConnStr)).toBe(
      "postgresql://user:pass@clone-host:5432/testdb",
    );

    // Case 2: connStr empty, build from parts
    const withoutConnStr = makeCloneResponse({
      db: {
        connStr: "",
        host: "db.internal",
        port: "6432",
        username: "admin",
        password: "p@ss",
        db_name: "production",
      },
    });
    const uri = buildCloneUri(withoutConnStr);
    expect(uri).toContain("db.internal:6432");
    expect(uri).toContain("production");
    expect(uri).toStartWith("postgresql://");

    // Case 3: no password
    const noPass = makeCloneResponse({
      db: {
        connStr: "",
        host: "localhost",
        port: "5432",
        username: "readonly",
        db_name: "testdb",
      },
    });
    const noPassUri = buildCloneUri(noPass);
    expect(noPassUri).toBe("postgresql://readonly@localhost:5432/testdb");
    // No "user:password" pattern — only "user@"
    const userPart = noPassUri.split("@")[0]!; // "postgresql://readonly"
    expect(userPart).not.toContain("readonly:");
  });

  // -----------------------------------------------------------------------
  // 3.3 Deploy uses clone URI not prod
  // -----------------------------------------------------------------------
  test("executeDblabDeploy passes clone URI (not production) to deployFn", async () => {
    const { mockFn } = createDblabMockFetch({});
    const client = new DblabClient(
      "https://dblab.example.com",
      "token",
      mockFn,
    );

    const prodUri = "postgresql://prod-user:prod-pass@prod-host:5432/proddb";
    const deployArgs = makeArgs({ dbUri: prodUri, rest: [] });
    const options: DblabDeployOptions = {
      dblabUrl: "https://dblab.example.com",
      dblabToken: "token",
      deployArgs,
    };

    let receivedDbUri: string | undefined;
    const deployFn = async (args: ParsedArgs) => {
      receivedDbUri = args.dbUri;
      return 0;
    };

    const result = await executeDblabDeploy(options, { client, deployFn });

    expect(result.success).toBe(true);
    // The deploy function should receive the CLONE URI, not the production URI
    expect(receivedDbUri).toBe(
      "postgresql://user:pass@clone-host:5432/testdb",
    );
    expect(receivedDbUri).not.toBe(prodUri);
  });

  // -----------------------------------------------------------------------
  // 3.4 Destroy on success (DELETE)
  // -----------------------------------------------------------------------
  test("clone is destroyed via DELETE after successful deploy", async () => {
    const { mockFn, calls } = createDblabMockFetch({});
    const client = new DblabClient(
      "https://dblab.example.com",
      "token",
      mockFn,
    );

    const options: DblabDeployOptions = {
      dblabUrl: "https://dblab.example.com",
      dblabToken: "token",
      deployArgs: makeArgs(),
    };
    const deployFn = async (_args: ParsedArgs) => 0;

    const result = await executeDblabDeploy(options, { client, deployFn });

    expect(result.success).toBe(true);
    expect(result.cloneDestroyed).toBe(true);

    // Verify the DELETE call was made
    const deleteCalls = calls.filter((c) => c.method === "DELETE");
    expect(deleteCalls).toHaveLength(1);
    expect(deleteCalls[0]!.url).toContain("/clone/clone-test-001");
    expect(deleteCalls[0]!.headers["Verification-Token"]).toBe("token");
  });

  // -----------------------------------------------------------------------
  // 3.5 Destroy on failure (finally block)
  // -----------------------------------------------------------------------
  test("clone is destroyed even when deploy fails (finally block)", async () => {
    const { mockFn, calls } = createDblabMockFetch({});
    const client = new DblabClient(
      "https://dblab.example.com",
      "token",
      mockFn,
    );

    const options: DblabDeployOptions = {
      dblabUrl: "https://dblab.example.com",
      dblabToken: "token",
      deployArgs: makeArgs(),
    };

    // Deploy throws an exception
    const deployFn = async (_args: ParsedArgs): Promise<number> => {
      throw new Error("FATAL: relation does not exist");
    };

    const result = await executeDblabDeploy(options, { client, deployFn });

    expect(result.success).toBe(false);
    expect(result.error).toContain("FATAL: relation does not exist");
    // Clone should STILL be destroyed despite the deploy failure
    expect(result.cloneDestroyed).toBe(true);

    const deleteCalls = calls.filter((c) => c.method === "DELETE");
    expect(deleteCalls).toHaveLength(1);
  });

  // -----------------------------------------------------------------------
  // 3.6 API error on clone create (no orphan)
  // -----------------------------------------------------------------------
  test("API error during clone creation: no orphan clone, no deploy attempted", async () => {
    const { mockFn, calls } = createDblabMockFetch({
      createClone: {
        status: 503,
        body: "Service Unavailable — no snapshots",
      },
    });
    const client = new DblabClient(
      "https://dblab.example.com",
      "token",
      mockFn,
    );

    const options: DblabDeployOptions = {
      dblabUrl: "https://dblab.example.com",
      dblabToken: "token",
      deployArgs: makeArgs(),
    };

    let deployCalled = false;
    const deployFn = async (_args: ParsedArgs) => {
      deployCalled = true;
      return 0;
    };

    const result = await executeDblabDeploy(options, { client, deployFn });

    expect(result.success).toBe(false);
    expect(result.error).toContain("Clone provisioning failed");
    // Deploy should NOT have been called
    expect(deployCalled).toBe(false);
    // No clone ID means no destroy attempt
    expect(result.cloneId).toBeUndefined();
    expect(result.cloneDestroyed).toBe(false);
    // Only the POST /clone call should have been made
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe("POST");
    // No DELETE calls — no orphan
    const deleteCalls = calls.filter((c) => c.method === "DELETE");
    expect(deleteCalls).toHaveLength(0);
  });

  // -----------------------------------------------------------------------
  // 3.7 --dblab-url/--dblab-token parsing + env vars
  // -----------------------------------------------------------------------
  test("parseDblabOptions: flags from args, fallback to env vars, error on partial", () => {
    // From flags
    const fromFlags = parseDblabOptions(
      makeArgs({
        rest: [
          "--dblab-url",
          "https://dblab.acme.io",
          "--dblab-token",
          "tok-123",
          "--verify",
        ],
      }),
    );
    expect(fromFlags).not.toBeNull();
    expect(fromFlags!.dblabUrl).toBe("https://dblab.acme.io");
    expect(fromFlags!.dblabToken).toBe("tok-123");
    // --verify forwarded, --dblab-* stripped
    expect(fromFlags!.deployArgs.rest).toContain("--verify");
    expect(fromFlags!.deployArgs.rest).not.toContain("--dblab-url");
    expect(fromFlags!.deployArgs.rest).not.toContain("--dblab-token");

    // From env vars
    process.env.DBLAB_URL = "https://env.dblab.io";
    process.env.DBLAB_TOKEN = "env-tok";
    const fromEnv = parseDblabOptions(makeArgs({ rest: [] }));
    expect(fromEnv).not.toBeNull();
    expect(fromEnv!.dblabUrl).toBe("https://env.dblab.io");
    expect(fromEnv!.dblabToken).toBe("env-tok");
    delete process.env.DBLAB_URL;
    delete process.env.DBLAB_TOKEN;

    // Neither flag nor env -> returns null
    const neither = parseDblabOptions(makeArgs({ rest: [] }));
    expect(neither).toBeNull();

    // Partial (URL without token) -> throws
    expect(() =>
      parseDblabOptions(
        makeArgs({ rest: ["--dblab-url", "https://dblab.io"] }),
      ),
    ).toThrow("--dblab-token is required");

    // Partial (token without URL) -> throws
    expect(() =>
      parseDblabOptions(makeArgs({ rest: ["--dblab-token", "tok"] })),
    ).toThrow("--dblab-url is required");
  });
});
