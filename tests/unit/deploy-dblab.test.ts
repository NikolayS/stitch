import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { resetConfig, setConfig } from "../../src/output";
import type { ParsedArgs } from "../../src/cli";
import {
  DblabClient,
  buildCloneUri,
  parseDblabOptions,
  executeDblabDeploy,
  runDblabDeploy,
  type DblabCloneResponse,
  type DblabDeployOptions,
} from "../../src/commands/deploy-dblab";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal ParsedArgs for testing. */
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
    id: "clone-abc-123",
    status: { code: "OK", message: "Clone is ready" },
    db: {
      connStr: "postgresql://clone_user:clone_pass@clone-host:5432/testdb",
      host: "clone-host",
      port: "5432",
      username: "clone_user",
      password: "clone_pass",
      db_name: "testdb",
    },
    ...overrides,
  };
}

/**
 * Create a mock fetch function for DBLab API.
 *
 * Tracks all requests for assertions.
 */
function createMockFetch(responses: {
  createClone?: { status: number; body: unknown };
  getClone?: { status: number; body: unknown };
  destroyClone?: { status: number; body: unknown };
}) {
  const calls: Array<{ url: string; method: string; headers: Record<string, string>; body?: string }> = [];

  const mockFn = async (url: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const urlStr = typeof url === "string" ? url : url instanceof URL ? url.toString() : (url as Request).url;
    const method = init?.method ?? "GET";
    const headers = init?.headers as Record<string, string> | undefined;
    const body = init?.body ? String(init.body) : undefined;

    calls.push({ url: urlStr, method, headers: headers ?? {}, body });

    // POST /clone
    if (method === "POST" && urlStr.endsWith("/clone")) {
      const resp = responses.createClone ?? { status: 200, body: makeCloneResponse() };
      return new Response(JSON.stringify(resp.body), {
        status: resp.status,
        headers: { "Content-Type": "application/json" },
      });
    }

    // DELETE /clone/<id>
    if (method === "DELETE" && urlStr.includes("/clone/")) {
      const resp = responses.destroyClone ?? { status: 200, body: {} };
      return new Response(JSON.stringify(resp.body), {
        status: resp.status,
        headers: { "Content-Type": "application/json" },
      });
    }

    // GET /clone/<id>
    if (method === "GET" && urlStr.includes("/clone/")) {
      const resp = responses.getClone ?? {
        status: 200,
        body: { id: "clone-abc-123", status: { code: "OK", message: "Ready" } },
      };
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
// Tests
// ---------------------------------------------------------------------------

describe("deploy-dblab", () => {
  beforeEach(() => {
    setConfig({ quiet: true }); // suppress output during tests
  });

  afterEach(() => {
    resetConfig();
    // Clean up env vars that tests may set
    delete process.env.DBLAB_URL;
    delete process.env.DBLAB_TOKEN;
  });

  // =========================================================================
  // parseDblabOptions
  // =========================================================================

  describe("parseDblabOptions", () => {
    it("returns null when no DBLab flags are present", () => {
      const args = makeArgs({ rest: ["--verify", "--no-tui"] });
      expect(parseDblabOptions(args)).toBeNull();
    });

    it("parses --dblab-url and --dblab-token from rest args", () => {
      const args = makeArgs({
        rest: ["--dblab-url", "https://dblab.example.com", "--dblab-token", "secret-token", "--verify"],
      });
      const result = parseDblabOptions(args);
      expect(result).not.toBeNull();
      expect(result!.dblabUrl).toBe("https://dblab.example.com");
      expect(result!.dblabToken).toBe("secret-token");
      // --verify should be forwarded in filteredRest
      expect(result!.deployArgs.rest).toContain("--verify");
      // --dblab-url/--dblab-token should NOT be forwarded
      expect(result!.deployArgs.rest).not.toContain("--dblab-url");
      expect(result!.deployArgs.rest).not.toContain("--dblab-token");
    });

    it("reads from environment variables when flags are not set", () => {
      process.env.DBLAB_URL = "https://env-dblab.example.com";
      process.env.DBLAB_TOKEN = "env-token";
      const args = makeArgs({ rest: [] });
      const result = parseDblabOptions(args);
      expect(result).not.toBeNull();
      expect(result!.dblabUrl).toBe("https://env-dblab.example.com");
      expect(result!.dblabToken).toBe("env-token");
    });

    it("throws when --dblab-url is set without --dblab-token", () => {
      const args = makeArgs({ rest: ["--dblab-url", "https://dblab.example.com"] });
      expect(() => parseDblabOptions(args)).toThrow("--dblab-token is required");
    });

    it("throws when --dblab-token is set without --dblab-url", () => {
      const args = makeArgs({ rest: ["--dblab-token", "token"] });
      expect(() => parseDblabOptions(args)).toThrow("--dblab-url is required");
    });

    it("throws when --dblab-url is missing its value", () => {
      const args = makeArgs({ rest: ["--dblab-url"] });
      expect(() => parseDblabOptions(args)).toThrow("--dblab-url requires a URL value");
    });

    it("throws when --dblab-token is missing its value", () => {
      const args = makeArgs({ rest: ["--dblab-token"] });
      expect(() => parseDblabOptions(args)).toThrow("--dblab-token requires a token value");
    });
  });

  // =========================================================================
  // buildCloneUri
  // =========================================================================

  describe("buildCloneUri", () => {
    it("returns connStr when available", () => {
      const clone = makeCloneResponse();
      expect(buildCloneUri(clone)).toBe(
        "postgresql://clone_user:clone_pass@clone-host:5432/testdb",
      );
    });

    it("builds URI from components when connStr is empty", () => {
      const clone = makeCloneResponse({
        db: {
          connStr: "",
          host: "my-host",
          port: "6432",
          username: "admin",
          password: "s3cret",
          db_name: "mydb",
        },
      });
      expect(buildCloneUri(clone)).toBe(
        "postgresql://admin:s3cret@my-host:6432/mydb",
      );
    });

    it("builds URI without password when not provided", () => {
      const clone = makeCloneResponse({
        db: {
          connStr: "",
          host: "my-host",
          port: "5432",
          username: "user",
          db_name: "db",
        },
      });
      expect(buildCloneUri(clone)).toBe("postgresql://user@my-host:5432/db");
    });
  });

  // =========================================================================
  // DblabClient
  // =========================================================================

  describe("DblabClient", () => {
    it("creates a clone with correct headers and body", async () => {
      const { mockFn, calls } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "my-token", mockFn);

      const clone = await client.createClone({ id: "test-clone" });

      expect(calls).toHaveLength(1);
      expect(calls[0]!.url).toBe("https://dblab.example.com/clone");
      expect(calls[0]!.method).toBe("POST");
      expect(calls[0]!.headers["Verification-Token"]).toBe("my-token");
      expect(calls[0]!.headers["Content-Type"]).toBe("application/json");
      expect(JSON.parse(calls[0]!.body!)).toEqual({ id: "test-clone" });
      expect(clone.id).toBe("clone-abc-123");
    });

    it("throws on non-2xx response from createClone", async () => {
      const { mockFn } = createMockFetch({
        createClone: { status: 500, body: "Internal Server Error" },
      });
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      await expect(client.createClone()).rejects.toThrow("POST /clone returned 500");
    });

    it("destroys a clone with DELETE request", async () => {
      const { mockFn, calls } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com/", "my-token", mockFn);

      await client.destroyClone("clone-xyz");

      expect(calls).toHaveLength(1);
      expect(calls[0]!.url).toBe("https://dblab.example.com/clone/clone-xyz");
      expect(calls[0]!.method).toBe("DELETE");
      expect(calls[0]!.headers["Verification-Token"]).toBe("my-token");
    });

    it("throws on non-2xx response from destroyClone", async () => {
      const { mockFn } = createMockFetch({
        destroyClone: { status: 404, body: "Not Found" },
      });
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      await expect(client.destroyClone("bad-id")).rejects.toThrow(
        "DELETE /clone/bad-id returned 404",
      );
    });

    it("strips trailing slash from base URL", async () => {
      const { mockFn, calls } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com///", "token", mockFn);

      await client.createClone();
      expect(calls[0]!.url).toBe("https://dblab.example.com/clone");
    });

    it("gets clone status with GET request", async () => {
      const { mockFn, calls } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "my-token", mockFn);

      const status = await client.getCloneStatus("clone-abc-123");

      expect(calls).toHaveLength(1);
      expect(calls[0]!.url).toBe("https://dblab.example.com/clone/clone-abc-123");
      expect(calls[0]!.method).toBe("GET");
      expect(status.id).toBe("clone-abc-123");
    });

    it("throws on non-2xx response from getCloneStatus", async () => {
      const { mockFn } = createMockFetch({
        getClone: { status: 403, body: "Forbidden" },
      });
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      await expect(client.getCloneStatus("clone-id")).rejects.toThrow(
        "GET /clone/clone-id returned 403",
      );
    });
  });

  // =========================================================================
  // executeDblabDeploy
  // =========================================================================

  describe("executeDblabDeploy", () => {
    it("full success: provision → deploy → destroy", async () => {
      const { mockFn } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      const deployArgs = makeArgs({ rest: [] });
      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs,
      };

      // Mock deployFn that succeeds
      const deployCalls: ParsedArgs[] = [];
      const deployFn = async (args: ParsedArgs) => {
        deployCalls.push(args);
        return 0;
      };

      const result = await executeDblabDeploy(options, { client, deployFn });

      expect(result.success).toBe(true);
      expect(result.cloneId).toBe("clone-abc-123");
      expect(result.cloneDestroyed).toBe(true);
      expect(result.error).toBeUndefined();
      // deployFn should be called with clone URI
      expect(deployCalls).toHaveLength(1);
      expect(deployCalls[0]!.dbUri).toBe(
        "postgresql://clone_user:clone_pass@clone-host:5432/testdb",
      );
      // --verify and --no-tui should be added
      expect(deployCalls[0]!.rest).toContain("--verify");
      expect(deployCalls[0]!.rest).toContain("--no-tui");
    });

    it("deploy failure: reports error, still destroys clone", async () => {
      const { mockFn } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs: makeArgs(),
      };

      const deployFn = async (_args: ParsedArgs) => 1; // exit code 1 = failure

      const result = await executeDblabDeploy(options, { client, deployFn });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Deploy+verify failed on clone");
      expect(result.error).toContain("Production was NOT touched");
      expect(result.cloneDestroyed).toBe(true);
    });

    it("deploy exception: catches error, destroys clone", async () => {
      const { mockFn } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs: makeArgs(),
      };

      const deployFn = async (_args: ParsedArgs): Promise<number> => {
        throw new Error("Connection refused");
      };

      const result = await executeDblabDeploy(options, { client, deployFn });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Connection refused");
      expect(result.cloneDestroyed).toBe(true);
    });

    it("clone provisioning failure: returns error without deploy", async () => {
      const { mockFn } = createMockFetch({
        createClone: { status: 503, body: "Service Unavailable" },
      });
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

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
      expect(deployCalled).toBe(false);
      // No clone to destroy since provisioning failed
      expect(result.cloneDestroyed).toBe(false);
    });

    it("clone destroy failure: logs warning but still returns result", async () => {
      const { mockFn } = createMockFetch({
        destroyClone: { status: 500, body: "Internal Server Error" },
      });
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs: makeArgs(),
      };

      const deployFn = async (_args: ParsedArgs) => 0;

      const result = await executeDblabDeploy(options, { client, deployFn });

      // Deploy succeeded even though clone destroy failed
      expect(result.success).toBe(true);
      expect(result.cloneDestroyed).toBe(false);
    });

    it("clone with bad status code: returns error without deploy", async () => {
      const { mockFn } = createMockFetch({
        createClone: {
          status: 200,
          body: makeCloneResponse({
            status: { code: "FATAL", message: "No snapshots available" },
          }),
        },
      });
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

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
      expect(result.error).toContain("FATAL");
      expect(result.error).toContain("No snapshots available");
      expect(deployCalled).toBe(false);
      // Clone was created but had bad status — should still try to destroy
      expect(result.cloneDestroyed).toBe(true);
    });

    it("records timing for provisioning and deploy", async () => {
      const { mockFn } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);

      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs: makeArgs(),
      };

      const deployFn = async (_args: ParsedArgs) => 0;

      const result = await executeDblabDeploy(options, { client, deployFn });

      expect(result.cloneProvisionMs).toBeDefined();
      expect(typeof result.cloneProvisionMs).toBe("number");
      expect(result.cloneProvisionMs!).toBeGreaterThanOrEqual(0);
      expect(result.deployMs).toBeDefined();
      expect(typeof result.deployMs).toBe("number");
      expect(result.deployMs!).toBeGreaterThanOrEqual(0);
    });
  });

  // =========================================================================
  // runDblabDeploy
  // =========================================================================

  describe("runDblabDeploy", () => {
    it("returns exit code 0 on success", async () => {
      const { mockFn } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);
      const deployFn = async (_args: ParsedArgs) => 0;

      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs: makeArgs(),
      };

      const exitCode = await runDblabDeploy(options, { client, deployFn });
      expect(exitCode).toBe(0);
    });

    it("returns exit code 1 on failure", async () => {
      const { mockFn } = createMockFetch({});
      const client = new DblabClient("https://dblab.example.com", "token", mockFn);
      const deployFn = async (_args: ParsedArgs) => 1;

      const options: DblabDeployOptions = {
        dblabUrl: "https://dblab.example.com",
        dblabToken: "token",
        deployArgs: makeArgs(),
      };

      const exitCode = await runDblabDeploy(options, { client, deployFn });
      expect(exitCode).toBe(1);
    });
  });
});
