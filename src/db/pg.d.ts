// Type declarations for pg/lib/client (node-postgres internal module).
//
// We import pg/lib/client directly rather than the top-level "pg" module
// to get the Client constructor without the pool/types baggage. This
// declaration provides the minimal surface we use.

declare module "pg/lib/client" {
  interface QueryResult {
    rows: unknown[];
    rowCount: number | null;
    command: string;
  }

  interface ClientConfig {
    connectionString?: string;
    host?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;
    ssl?: boolean | object;
  }

  class Client {
    constructor(config?: ClientConfig | string);
    connect(): Promise<void>;
    end(): Promise<void>;
    query(text: string, values?: unknown[]): Promise<QueryResult>;
  }

  export default Client;
}
