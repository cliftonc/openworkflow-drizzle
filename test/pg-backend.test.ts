import { BackendDrizzlePg, createPgTables } from "../src/index.js";
import { createPgSchema, ignoreNotice, POSTGRES_URL } from "./support.js";
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { randomUUID } from "node:crypto";
import { testBackend } from "openworkflow/testing";
import postgres from "postgres";
import { afterAll, describe } from "vitest";

describe.each(["", "ow_"])(
  'BackendDrizzlePg (postgres.js, prefix: "%s")',
  (prefix) => {
    const schema = `openworkflow_drizzle_${randomUUID().replaceAll("-", "")}`;
    const client = postgres(POSTGRES_URL, { onnotice: ignoreNotice });
    const db = drizzle({ client });
    const tables = createPgTables({ prefix, schema });
    const schemaReady = createPgSchema(db, tables);

    afterAll(async () => {
      await db.execute(sql.raw(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`));
      await client.end();
    });

    testBackend({
      setup: async () => {
        await schemaReady;
        return new BackendDrizzlePg({
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
