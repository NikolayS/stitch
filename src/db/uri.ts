// src/db/uri.ts — Database connection URI parsing for sqlever
//
// Supports two URI schemes:
//   - db:pg://user:pass@host:port/dbname   (Sqitch URI scheme)
//   - postgresql://user:pass@host:port/dbname (standard PostgreSQL)
//
// Both are normalized to a common ParsedUri structure for consumption by
// DatabaseClient and for conversion back to either format.

/** Parsed components of a PostgreSQL connection URI. */
export interface ParsedUri {
  user?: string;
  password?: string;
  host: string;
  port: number;
  database: string;
  params: Record<string, string>;
}

/** Default PostgreSQL port. */
const DEFAULT_PORT = 5432;

/** Schemes we recognize. */
const SQITCH_PREFIX = "db:pg://";
const STANDARD_PREFIXES = ["postgresql://", "postgres://"];

/**
 * Parse a database connection URI into its components.
 *
 * Accepts:
 *   - db:pg://user:pass@host:port/dbname?param=val
 *   - postgresql://user:pass@host:port/dbname?param=val
 *   - postgres://user:pass@host:port/dbname?param=val
 *
 * Throws on unrecognized schemes or malformed URIs.
 */
export function parseUri(uri: string): ParsedUri {
  let normalized: string;

  if (uri.startsWith(SQITCH_PREFIX)) {
    // Convert db:pg:// to postgresql:// for URL parsing
    normalized = "postgresql://" + uri.slice(SQITCH_PREFIX.length);
  } else if (STANDARD_PREFIXES.some((p) => uri.startsWith(p))) {
    normalized = uri;
  } else {
    throw new Error(
      `Unsupported URI scheme: expected "db:pg://", "postgresql://", or "postgres://". Got: "${uri.split("://")[0]}://"`,
    );
  }

  let url: URL;
  try {
    url = new URL(normalized);
  } catch {
    throw new Error(`Malformed database URI: unable to parse "${maskUriForError(uri)}"`);
  }

  // URL parser keeps brackets around IPv6 addresses (e.g., "[::1]"),
  // strip them so the host is a plain address.
  let host = decodeURIComponent(url.hostname) || "localhost";
  if (host.startsWith("[") && host.endsWith("]")) {
    host = host.slice(1, -1);
  }
  const portStr = url.port;
  const port = portStr ? parseInt(portStr, 10) : DEFAULT_PORT;

  if (isNaN(port) || port < 1 || port > 65535) {
    throw new Error(`Invalid port in database URI: ${portStr}`);
  }

  // Database name is the pathname minus the leading slash
  const pathname = decodeURIComponent(url.pathname.slice(1));
  const database = pathname || "";

  const user = url.username ? decodeURIComponent(url.username) : undefined;
  const password = url.password ? decodeURIComponent(url.password) : undefined;

  // Collect query parameters
  const params: Record<string, string> = {};
  url.searchParams.forEach((value, key) => {
    params[key] = value;
  });

  return { user, password, host, port, database, params };
}

/**
 * Convert a ParsedUri back to a standard PostgreSQL connection URI.
 *
 * Format: postgresql://[user[:password]@]host[:port]/database[?params]
 */
export function toStandardUri(parsed: ParsedUri): string {
  return buildUri("postgresql://", parsed);
}

/**
 * Convert a ParsedUri back to a Sqitch-style connection URI.
 *
 * Format: db:pg://[user[:password]@]host[:port]/database[?params]
 */
export function toSqitchUri(parsed: ParsedUri): string {
  return buildUri("db:pg://", parsed);
}

/**
 * Convert a `db:pg://` URI to `postgresql://` format (or pass through if
 * already standard).
 */
export function sqitchToStandard(uri: string): string {
  if (uri.startsWith(SQITCH_PREFIX)) {
    return "postgresql://" + uri.slice(SQITCH_PREFIX.length);
  }
  return uri;
}

/**
 * Convert a `postgresql://` or `postgres://` URI to `db:pg://` format
 * (or pass through if already Sqitch-style).
 */
export function standardToSqitch(uri: string): string {
  for (const prefix of STANDARD_PREFIXES) {
    if (uri.startsWith(prefix)) {
      return SQITCH_PREFIX + uri.slice(prefix.length);
    }
  }
  return uri;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function buildUri(scheme: string, parsed: ParsedUri): string {
  let uri = scheme;

  if (parsed.user) {
    uri += encodeURIComponent(parsed.user);
    if (parsed.password) {
      uri += ":" + encodeURIComponent(parsed.password);
    }
    uri += "@";
  }

  uri += parsed.host;

  if (parsed.port !== DEFAULT_PORT) {
    uri += ":" + parsed.port;
  }

  uri += "/" + encodeURIComponent(parsed.database);

  const paramEntries = Object.entries(parsed.params);
  if (paramEntries.length > 0) {
    const search = new URLSearchParams(paramEntries);
    uri += "?" + search.toString();
  }

  return uri;
}

/**
 * Mask password in a URI for safe inclusion in error messages.
 * Simpler than the output.ts version — just for error strings here.
 */
function maskUriForError(uri: string): string {
  return uri.replace(
    /^([a-zA-Z][a-zA-Z0-9+.:~-]*:\/\/)([^:@/]+):(.+)@(?=[^@]*$)/,
    "$1$2:***@",
  );
}
