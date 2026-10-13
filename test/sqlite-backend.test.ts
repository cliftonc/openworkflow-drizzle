import { BackendDrizzleSqlite, createSqliteTables } from "../src/index.js";
import { createSqliteSchema } from "./support.js";
import { createClient } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { randomUUID } from "node:crypto";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { testBackend } from "openworkflow/testing";
import { afterAll, describe } from "vitest";

// libsql serves a :memory: database from one connection, so it also checks
// that no query runs beside an open transaction
describe.each([
  { prefix: "", storage: "file" },
  { prefix: "ow_", storage: ":memory:" },
])(
  'BackendDrizzleSqlite (libsql $storage, prefix: "$prefix")',
  ({ prefix, storage }) => {
    const dbPath = path.join(
      tmpdir(),
      `openworkflow-drizzle-${randomUUID()}.db`,
    );
    const client = createClient({
      url: storage === "file" ? `file:${dbPath}` : storage,
    });
    const db = drizzle({ client });
    const tables = createSqliteTables({ prefix });
    const schemaReady = createSqliteSchema(db, tables);

    afterAll(() => {
      client.close();
      for (const suffix of ["", "-wal", "-shm", "-journal"]) {
        rmSync(`${dbPath}${suffix}`, { force: true });
      }
    });

    testBackend({
      setup: async () => {
        await schemaReady;
        return new BackendDrizzleSqlite({
          db,
          tables,
          namespaceId: randomUUID(),
        });
      },
      teardown: async (backend) => {
        await backend.stop();
      },
    });
  },
);
