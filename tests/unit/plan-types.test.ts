// tests/unit/plan-types.test.ts — Tests for plan types and ID computation
//
// Validates that computeChangeId, computeTagId, and computeScriptHash
// produce Sqitch-compatible SHA-1 hashes. All expected values are
// manually computed reference hashes.

import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { writeFileSync, unlinkSync, mkdtempSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  buildChangeContent,
  buildTagContent,
  computeChangeId,
  computeTagId,
  computeScriptHash,
  computeScriptHashFromBytes,
} from "../../src/plan/types";

import type {
  Project,
  Change,
  Tag,
  Dependency,
  Plan,
  PlanEntry,
  ChangeIdInput,
  TagIdInput,
} from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Manually compute SHA-1 hex for verification. */
function manualSha1(prefix: string, content: string): string {
  const contentBytes = Buffer.from(content, "utf-8");
  const header = `${prefix} ${contentBytes.length}\0`;
  const hash = createHash("sha1");
  hash.update(Buffer.from(header, "utf-8"));
  hash.update(contentBytes);
  return hash.digest("hex");
}

// ---------------------------------------------------------------------------
// Change ID computation
// ---------------------------------------------------------------------------

describe("computeChangeId", () => {
  it("1: minimal change — first change, no parent, no deps, no note, no uri", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const id = computeChangeId(input);
    expect(id).toBe("1da0ceafe3d2a70b4f870bb4a9ae55fa48b57a83");
  });

  it("2: change with parent (second change in plan)", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "second_change",
      parent: "abc123def456",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const id = computeChangeId(input);
    expect(id).toBe("bd04c81f17776794f4f60b0187d590d8627347b6");
  });

  it("3: change with URI", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      uri: "https://example.com/myproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const id = computeChangeId(input);
    expect(id).toBe("4e7702993536aa012624298de4d2be01e1b1ef79");
  });

  it("4: change with requires dependencies", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "add_users",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["create_schema", "add_roles"],
      conflicts: [],
      note: "",
    };

    const id = computeChangeId(input);
    expect(id).toBe("3ac863adf2afd3ee84e22b733c39e4df60a24fbb");
  });

  it("5: change with conflicts", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "add_users",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: ["old_users"],
      note: "",
    };

    const id = computeChangeId(input);
    expect(id).toBe("0756774c5df829eac58222fdbe91b72f1aaf06a7");
  });

  it("6: change with note", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "add_users",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "Add users table for authentication",
    };

    const id = computeChangeId(input);
    expect(id).toBe("ac389d5803514b747af189a32f6a62f844703ade");
  });

  it("7: full change — URI, parent, requires, conflicts, note", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      uri: "https://example.com/myproject",
      change: "add_users",
      parent: "abc123",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["create_schema", "add_roles"],
      conflicts: ["old_users"],
      note: "Add users table for authentication",
    };

    const id = computeChangeId(input);
    expect(id).toBe("6d194b2a7fe6d9a8655f31332cb350bb1dc72dde");
  });

  it("11: unicode in change name, planner, and note", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "add_ñoño",
      planner_name: "José García",
      planner_email: "jose@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "Añadir tabla de usuarios",
    };

    const id = computeChangeId(input);
    expect(id).toBe("3aa918f9713eb601d944efc41093ac5a2334ce68");
  });

  it("12: requires AND conflicts together", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "add_users",
      parent: "abc123",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["create_schema"],
      conflicts: ["old_users", "legacy_auth"],
      note: "",
    };

    const id = computeChangeId(input);
    expect(id).toBe("b7955f080808fc1dcd3c708d596b2cdf51eb5620");
  });

  it("content uses byte length, not string length for unicode", () => {
    // Unicode characters take more bytes than string length suggests.
    // The envelope must use byte length.
    const input: ChangeIdInput = {
      project: "myproject",
      change: "add_ñoño",
      planner_name: "José García",
      planner_email: "jose@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "Añadir tabla de usuarios",
    };

    const content = buildChangeContent(input);
    const byteLength = Buffer.from(content, "utf-8").length;
    const stringLength = content.length;

    // UTF-8 bytes > string length due to multi-byte chars
    expect(byteLength).toBeGreaterThan(stringLength);
    expect(byteLength).toBe(129);
    expect(stringLength).toBe(124);
  });

  it("empty parent string is treated as no parent", () => {
    const withoutParent: ChangeIdInput = {
      project: "myproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const withEmptyParent: ChangeIdInput = {
      ...withoutParent,
      parent: "",
    };

    expect(computeChangeId(withEmptyParent)).toBe(computeChangeId(withoutParent));
  });

  it("empty URI string is treated as no URI", () => {
    const withoutUri: ChangeIdInput = {
      project: "myproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const withEmptyUri: ChangeIdInput = {
      ...withoutUri,
      uri: "",
    };

    expect(computeChangeId(withEmptyUri)).toBe(computeChangeId(withoutUri));
  });

  it("dependency order matters (not sorted)", () => {
    const input1: ChangeIdInput = {
      project: "myproject",
      change: "add_users",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["alpha", "beta"],
      conflicts: [],
      note: "",
    };

    const input2: ChangeIdInput = {
      ...input1,
      requires: ["beta", "alpha"],
    };

    // Different order = different ID
    expect(computeChangeId(input1)).not.toBe(computeChangeId(input2));
  });
});

// ---------------------------------------------------------------------------
// buildChangeContent — verify exact content format
// ---------------------------------------------------------------------------

describe("buildChangeContent", () => {
  it("minimal content format is correct", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const content = buildChangeContent(input);
    expect(content).toBe(
      "project myproject\n" +
        "change first_change\n" +
        "planner Test User <test@example.com>\n" +
        "date 2024-01-15T12:00:00Z",
    );
  });

  it("full content format includes all sections in correct order", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      uri: "https://example.com/myproject",
      change: "add_users",
      parent: "abc123",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["create_schema", "add_roles"],
      conflicts: ["old_users"],
      note: "Add users table for authentication",
    };

    const content = buildChangeContent(input);
    expect(content).toBe(
      "project myproject\n" +
        "uri https://example.com/myproject\n" +
        "change add_users\n" +
        "parent abc123\n" +
        "planner Test User <test@example.com>\n" +
        "date 2024-01-15T12:00:00Z\n" +
        "requires\n" +
        "  + create_schema\n" +
        "  + add_roles\n" +
        "conflicts\n" +
        "  - old_users\n" +
        "\n" +
        "Add users table for authentication",
    );
  });

  it("note is preceded by blank line separator", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "test",
      planner_name: "User",
      planner_email: "user@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "Some note",
    };

    const content = buildChangeContent(input);
    // Should end with: ...date line\n\nSome note (no trailing \n)
    expect(content).toContain("2024-01-15T12:00:00Z\n\nSome note");
    expect(content).not.toMatch(/Some note\n$/);
  });

  it("no blank line when note is empty", () => {
    const input: ChangeIdInput = {
      project: "myproject",
      change: "test",
      planner_name: "User",
      planner_email: "user@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    };

    const content = buildChangeContent(input);
    // Should NOT have trailing blank line or trailing \n
    expect(content).toBe(
      "project myproject\n" +
        "change test\n" +
        "planner User <user@example.com>\n" +
        "date 2024-01-15T12:00:00Z",
    );
  });

  it("requires section with indented + prefix", () => {
    const input: ChangeIdInput = {
      project: "p",
      change: "c",
      planner_name: "U",
      planner_email: "u@e.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["dep1", "dep2"],
      conflicts: [],
      note: "",
    };

    const content = buildChangeContent(input);
    expect(content).toContain("requires\n  + dep1\n  + dep2");
  });

  it("conflicts section with indented - prefix", () => {
    const input: ChangeIdInput = {
      project: "p",
      change: "c",
      planner_name: "U",
      planner_email: "u@e.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: ["conf1"],
      note: "",
    };

    const content = buildChangeContent(input);
    expect(content).toContain("conflicts\n  - conf1");
  });
});

// ---------------------------------------------------------------------------
// Tag ID computation
// ---------------------------------------------------------------------------

describe("computeTagId", () => {
  it("8: minimal tag — no uri, no note", () => {
    const input: TagIdInput = {
      project: "myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "",
    };

    const id = computeTagId(input);
    expect(id).toBe("c4838045e864db0eb340afcfc035702e13e122e9");
  });

  it("9: tag with URI and note", () => {
    const input: TagIdInput = {
      project: "myproject",
      uri: "https://example.com/myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "First release",
    };

    const id = computeTagId(input);
    expect(id).toBe("9b012177c5b8c2a53af608f6e78e20cdffafead4");
  });

  it("tag name includes @ prefix in content", () => {
    const input: TagIdInput = {
      project: "myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "",
    };

    const content = buildTagContent(input);
    expect(content).toContain("tag @v1.0\n");
    // Must NOT have "tag v1.0" (without @)
    expect(content).not.toMatch(/^tag v1\.0$/m);
  });

  it("tag envelope uses 'tag' prefix not 'change'", () => {
    const input: TagIdInput = {
      project: "myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "",
    };

    // Manually verify the envelope format
    const content = buildTagContent(input);

    // Verify the ID matches what we'd get with "tag" prefix
    const expected = manualSha1("tag", content);
    expect(computeTagId(input)).toBe(expected);

    // Verify it does NOT match "change" prefix
    const wrongPrefix = manualSha1("change", content);
    expect(computeTagId(input)).not.toBe(wrongPrefix);
  });

  it("empty URI string is treated as no URI", () => {
    const withoutUri: TagIdInput = {
      project: "myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "",
    };

    const withEmptyUri: TagIdInput = {
      ...withoutUri,
      uri: "",
    };

    expect(computeTagId(withEmptyUri)).toBe(computeTagId(withoutUri));
  });
});

// ---------------------------------------------------------------------------
// buildTagContent — verify exact content format
// ---------------------------------------------------------------------------

describe("buildTagContent", () => {
  it("minimal tag content format is correct", () => {
    const input: TagIdInput = {
      project: "myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "",
    };

    const content = buildTagContent(input);
    expect(content).toBe(
      "project myproject\n" +
        "tag @v1.0\n" +
        "change deadbeef\n" +
        "planner Test User <test@example.com>\n" +
        "date 2024-01-15T12:00:00Z",
    );
  });

  it("full tag content includes URI and note with blank line", () => {
    const input: TagIdInput = {
      project: "myproject",
      uri: "https://example.com/myproject",
      tag: "v1.0",
      change_id: "deadbeef",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      note: "First release",
    };

    const content = buildTagContent(input);
    expect(content).toBe(
      "project myproject\n" +
        "uri https://example.com/myproject\n" +
        "tag @v1.0\n" +
        "change deadbeef\n" +
        "planner Test User <test@example.com>\n" +
        "date 2024-01-15T12:00:00Z\n" +
        "\n" +
        "First release",
    );
  });
});

// ---------------------------------------------------------------------------
// script_hash computation
// ---------------------------------------------------------------------------

describe("computeScriptHash", () => {
  it("10: plain SHA-1 of raw file bytes (no blob prefix)", () => {
    const dir = mkdtempSync(join(tmpdir(), "sqlever-test-"));
    const filePath = join(dir, "deploy.sql");
    const content = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n";
    writeFileSync(filePath, content, "utf-8");

    try {
      const hash = computeScriptHash(filePath);
      expect(hash).toBe("ace3589c10ea2a2ec813b87299be20aaefd4f52d");

      // Verify this is NOT the git-style "blob <size>\0" hash
      const gitHash = createHash("sha1")
        .update(`blob ${Buffer.from(content).length}\0`)
        .update(content)
        .digest("hex");
      expect(hash).not.toBe(gitHash);
    } finally {
      unlinkSync(filePath);
    }
  });

  it("computeScriptHashFromBytes matches computeScriptHash", () => {
    const dir = mkdtempSync(join(tmpdir(), "sqlever-test-"));
    const filePath = join(dir, "deploy.sql");
    const content = "SELECT 1;\n";
    writeFileSync(filePath, content, "utf-8");

    try {
      const fromFile = computeScriptHash(filePath);
      const fromBytes = computeScriptHashFromBytes(Buffer.from(content, "utf-8"));
      expect(fromFile).toBe(fromBytes);
    } finally {
      unlinkSync(filePath);
    }
  });

  it("binary content is hashed without line-ending normalization", () => {
    const dir = mkdtempSync(join(tmpdir(), "sqlever-test-"));
    const filePath = join(dir, "deploy.sql");

    // Write content with explicit \r\n line endings
    const content = Buffer.from("SELECT 1;\r\nSELECT 2;\r\n", "utf-8");
    writeFileSync(filePath, content);

    try {
      const hash = computeScriptHash(filePath);
      // Should hash the raw bytes including \r\n, not normalize to \n
      const expected = createHash("sha1").update(content).digest("hex");
      expect(hash).toBe(expected);
    } finally {
      unlinkSync(filePath);
    }
  });

  it("empty file produces consistent hash", () => {
    const dir = mkdtempSync(join(tmpdir(), "sqlever-test-"));
    const filePath = join(dir, "empty.sql");
    writeFileSync(filePath, "", "utf-8");

    try {
      const hash = computeScriptHash(filePath);
      // SHA-1 of empty input
      const expected = createHash("sha1").update(Buffer.alloc(0)).digest("hex");
      expect(hash).toBe(expected);
    } finally {
      unlinkSync(filePath);
    }
  });
});

// ---------------------------------------------------------------------------
// Type structure tests (compile-time + runtime shape)
// ---------------------------------------------------------------------------

describe("type structure", () => {
  it("Change interface has all required fields", () => {
    const change: Change = {
      change_id: "abc123",
      name: "add_users",
      project: "myproject",
      note: "Add users table",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: ["create_schema"],
      conflicts: [],
    };

    expect(change.change_id).toBe("abc123");
    expect(change.name).toBe("add_users");
    expect(change.requires).toEqual(["create_schema"]);
    expect(change.parent).toBeUndefined();
  });

  it("Change supports optional parent", () => {
    const change: Change = {
      change_id: "abc123",
      name: "add_users",
      project: "myproject",
      note: "",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      parent: "def456",
    };

    expect(change.parent).toBe("def456");
  });

  it("Tag interface has all required fields", () => {
    const tag: Tag = {
      tag_id: "abc123",
      name: "v1.0",
      project: "myproject",
      change_id: "def456",
      note: "First release",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
    };

    expect(tag.tag_id).toBe("abc123");
    expect(tag.name).toBe("v1.0");
    expect(tag.change_id).toBe("def456");
  });

  it("Dependency interface supports both types", () => {
    const req: Dependency = {
      type: "require",
      name: "create_schema",
    };

    const conflict: Dependency = {
      type: "conflict",
      name: "old_users",
      project: "other_project",
    };

    expect(req.type).toBe("require");
    expect(req.project).toBeUndefined();
    expect(conflict.type).toBe("conflict");
    expect(conflict.project).toBe("other_project");
  });

  it("Project interface supports optional URI", () => {
    const withUri: Project = { name: "myproject", uri: "https://example.com" };
    const withoutUri: Project = { name: "myproject" };

    expect(withUri.uri).toBe("https://example.com");
    expect(withoutUri.uri).toBeUndefined();
  });

  it("Plan interface has all fields", () => {
    const plan: Plan = {
      project: { name: "myproject" },
      pragmas: new Map([["project", "myproject"]]),
      changes: [],
      tags: [],
    };

    expect(plan.project.name).toBe("myproject");
    expect(plan.pragmas.get("project")).toBe("myproject");
    expect(plan.changes).toEqual([]);
    expect(plan.tags).toEqual([]);
  });

  it("PlanEntry discriminated union works", () => {
    const changeEntry: PlanEntry = {
      type: "change",
      value: {
        change_id: "abc",
        name: "test",
        project: "p",
        note: "",
        planner_name: "U",
        planner_email: "u@e.com",
        planned_at: "2024-01-15T12:00:00Z",
        requires: [],
        conflicts: [],
      },
    };

    const tagEntry: PlanEntry = {
      type: "tag",
      value: {
        tag_id: "def",
        name: "v1",
        project: "p",
        change_id: "abc",
        note: "",
        planner_name: "U",
        planner_email: "u@e.com",
        planned_at: "2024-01-15T12:00:00Z",
      },
    };

    expect(changeEntry.type).toBe("change");
    expect(tagEntry.type).toBe("tag");

    // TypeScript narrows correctly
    if (changeEntry.type === "change") {
      expect(changeEntry.value.change_id).toBe("abc");
    }
    if (tagEntry.type === "tag") {
      expect(tagEntry.value.tag_id).toBe("def");
    }
  });
});

// ---------------------------------------------------------------------------
// Chain test: computing IDs for a sequence of changes
// ---------------------------------------------------------------------------

describe("chained change IDs", () => {
  it("each change uses previous change_id as parent", () => {
    const project = "myproject";
    const planner_name = "Test User";
    const planner_email = "test@example.com";
    const planned_at = "2024-01-15T12:00:00Z";

    // First change: no parent
    const id1 = computeChangeId({
      project,
      change: "create_schema",
      planner_name,
      planner_email,
      planned_at,
      requires: [],
      conflicts: [],
      note: "",
    });

    // Second change: parent is id1
    const id2 = computeChangeId({
      project,
      change: "add_users",
      parent: id1,
      planner_name,
      planner_email,
      planned_at,
      requires: ["create_schema"],
      conflicts: [],
      note: "",
    });

    // Third change: parent is id2
    const id3 = computeChangeId({
      project,
      change: "add_posts",
      parent: id2,
      planner_name,
      planner_email,
      planned_at,
      requires: ["add_users"],
      conflicts: [],
      note: "",
    });

    // All IDs must be unique
    expect(id1).not.toBe(id2);
    expect(id2).not.toBe(id3);
    expect(id1).not.toBe(id3);

    // All IDs are 40-char hex strings
    const hexPattern = /^[0-9a-f]{40}$/;
    expect(id1).toMatch(hexPattern);
    expect(id2).toMatch(hexPattern);
    expect(id3).toMatch(hexPattern);

    // Verify determinism: same inputs = same output
    const id1Again = computeChangeId({
      project,
      change: "create_schema",
      planner_name,
      planner_email,
      planned_at,
      requires: [],
      conflicts: [],
      note: "",
    });
    expect(id1Again).toBe(id1);
  });
});
