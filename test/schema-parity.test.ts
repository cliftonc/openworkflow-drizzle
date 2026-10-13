import { createPgTables, createSqliteTables } from "../src/index.js";
import {
  createPgSchema,
  createSqliteSchema,
  ignoreNotice,
  POSTGRES_URL,
} from "./support.js";
import { createClient, type Client, type Value } from "@libsql/client";
import { drizzle as drizzleLibsql } from "drizzle-orm/libsql";
import { drizzle as drizzlePg } from "drizzle-orm/postgres-js";
import { randomUUID } from "node:crypto";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { BackendPostgres } from "openworkflow/postgres";
import { BackendSqlite } from "openworkflow/sqlite";
import postgres from "postgres";
import { afterAll, describe, expect, test } from "vitest";

const TABLES = [
  "workflow_runs",
  "step_attempts",
  "workflow_signals",
  "workflow_schedules",
];

// The reference databases come from OpenWorkflow's own backends, which run
// their migrations on connect.

describe("Drizzle tables match the SQLite backend migrations", () => {
  test.each(["", "ow_"])('prefix: "%s"', async (prefix) => {
    const upstreamPath = path.join(
      tmpdir(),
      `openworkflow-parity-${randomUUID()}.db`,
    );
    await BackendSqlite.connect(upstreamPath).stop();
    const upstream = createClient({ url: `file:${upstreamPath}` });

    const client = createClient({ url: ":memory:" });
    await createSqliteSchema(
      drizzleLibsql({ client }),
      createSqliteTables({ prefix }),
    );

    const expected = await describeSqlite(upstream, "");
    const actual = await describeSqlite(client, prefix);

    // drizzle-kit 0.31 can't emit json_extract(...) in an index, so the
    // signal-wait index uses the equivalent `->>` expression
    const signalWait = "step_attempts_signal_wait_idx";
    expect(expected.expressions[signalWait]).toBe(
      `namespace_id,json_extract(context,'$.signal')`,
    );
    expect(actual.expressions[signalWait]).toBe(
      `namespace_id,context->>'$.signal'`,
    );
    expected.expressions[signalWait] = actual.expressions[signalWait] ?? "";

    expect(actual).toEqual(expected);
    upstream.close();
    client.close();
    for (const suffix of ["", "-wal", "-shm"]) {
      rmSync(`${upstreamPath}${suffix}`, { force: true });
    }
  });
});

describe("Drizzle tables match the Postgres backend migrations", () => {
  const pg = postgres(POSTGRES_URL, { onnotice: ignoreNotice });
  const schemas: string[] = [];

  afterAll(async () => {
    for (const schema of schemas) {
      await pg.unsafe(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    }
    await pg.end();
  });

  test.each(["", "ow_"])('prefix: "%s"', async (prefix) => {
    const suffix = randomUUID().replaceAll("-", "");
    const upstreamSchema = `openworkflow_parity_${suffix}`;
    const drizzleSchema = `openworkflow_drizzle_parity_${suffix}`;
    schemas.push(upstreamSchema, drizzleSchema);

    const backend = await BackendPostgres.connect(POSTGRES_URL, {
      schema: upstreamSchema,
    });
    await backend.stop();
    await createPgSchema(
      drizzlePg({ client: pg }),
      createPgTables({ prefix, schema: drizzleSchema }),
    );

    const expected = await describePg(pg, upstreamSchema, "");
    const actual = await describePg(pg, drizzleSchema, prefix);
    expect(actual).toEqual(expected);
  });
});

interface SqliteSchemaDescription {
  columns: Record<string, unknown[]>;
  foreignKeys: Record<string, unknown[]>;
  indexes: Record<string, unknown[]>;
  expressions: Record<string, string>;
}

/**
 * Describe the OpenWorkflow tables in a SQLite database, with the prefix
 * stripped from table and index names.
 * @param db - libsql client
 * @param prefix - Table name prefix
 * @returns Schema description
 */
async function describeSqlite(
  db: Client,
  prefix: string,
): Promise<SqliteSchemaDescription> {
  const strip = (value: string) =>
    value.startsWith(prefix) ? value.slice(prefix.length) : value;
  const description: SqliteSchemaDescription = {
    columns: {},
    foreignKeys: {},
    indexes: {},
    expressions: {},
  };

  for (const table of TABLES) {
    const name = `${prefix}${table}`;
    const columns = await db.execute(`PRAGMA table_info("${name}")`);
    description.columns[table] = columns.rows.map((row) => [
      row["cid"],
      row["name"],
      text(row["type"]).toUpperCase(),
      row["notnull"],
      row["dflt_value"],
      row["pk"],
    ]);

    const foreignKeys = await db.execute(`PRAGMA foreign_key_list("${name}")`);
    description.foreignKeys[table] = foreignKeys.rows
      .map((row) => [
        strip(text(row["table"])),
        row["from"],
        row["to"],
        row["on_delete"],
        row["seq"],
      ])
      .toSorted(compareJson);

    const indexes = await db.execute(`PRAGMA index_list("${name}")`);
    const described = [];
    for (const index of indexes.rows.filter((row) => row["origin"] === "c")) {
      const indexName = text(index["name"]);
      const info = await db.execute(`PRAGMA index_xinfo("${indexName}")`);
      const ddl = await db.execute({
        sql: "SELECT sql FROM sqlite_master WHERE name = ?",
        args: [indexName],
      });
      const statement = normalizeSql(text(ddl.rows[0]?.["sql"]), name);
      const where = statement.includes("where")
        ? statement.slice(statement.indexOf("where"))
        : "";
      described.push([
        strip(indexName),
        index["unique"],
        index["partial"],
        info.rows
          .filter((row) => row["key"])
          .map((row) => [row["name"], row["desc"]]),
        where,
      ]);
      if (info.rows.some((row) => row["key"] && row["name"] === null)) {
        description.expressions[strip(indexName)] =
          /\((.*)\)/s.exec(statement.replace(/where.*/s, ""))?.[1] ?? "";
      }
    }
    description.indexes[table] = described.toSorted(compareJson);
  }

  return description;
}

/**
 * Describe the OpenWorkflow tables in a Postgres schema, with the schema and
 * prefix stripped from every name.
 * @param pg - postgres.js client
 * @param schema - Schema name
 * @param prefix - Table name prefix
 * @returns Lines describing columns, indexes, and constraints
 */
async function describePg(
  pg: postgres.Sql,
  schema: string,
  prefix: string,
): Promise<string[]> {
  const tables = TABLES.map((table) => `${prefix}${table}`);
  const clean = (value: string) =>
    value
      .replaceAll(`"${schema}".`, "")
      .replaceAll(`${schema}.`, "")
      .replaceAll(new RegExp(`\\b${prefix}(?=(workflow_|step_))`, "g"), "")
      .replaceAll('"', "");

  const columns = await pg<
    {
      table_name: string;
      column_name: string;
      data_type: string;
      is_nullable: string;
      column_default: string | null;
      ordinal_position: number;
    }[]
  >`
    SELECT table_name, column_name, data_type, is_nullable, column_default, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = ${schema} AND table_name IN ${pg(tables)}
  `;
  const indexes = await pg<{ indexdef: string }[]>`
    SELECT indexdef FROM pg_indexes
    WHERE schemaname = ${schema} AND tablename IN ${pg(tables)}
  `;
  const constraints = await pg<{ name: string; definition: string }[]>`
    SELECT c.conname AS name, pg_get_constraintdef(c.oid) AS definition
    FROM pg_constraint c
    JOIN pg_namespace n ON n.oid = c.connamespace
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE n.nspname = ${schema} AND t.relname IN ${pg(tables)} AND c.contype IN ('p', 'f', 'u')
  `;

  return [
    ...columns.map((column) =>
      clean(
        `column ${column.table_name}.${column.column_name} ${column.data_type} nullable=${column.is_nullable} default=${column.column_default ?? ""} position=${String(column.ordinal_position)}`,
      ),
    ),
    ...indexes.map((index) => clean(`index ${index.indexdef}`)),
    ...constraints.map((constraint) =>
      clean(`constraint ${constraint.name} ${constraint.definition}`),
    ),
  ].toSorted(compareJson);
}

/**
 * Normalize index DDL for comparison: drop quotes, whitespace, and table
 * qualifiers, and lowercase it.
 * @param statement - CREATE INDEX statement
 * @param table - Indexed table name
 * @returns Normalized statement
 */
function normalizeSql(statement: string, table: string): string {
  return statement
    .replaceAll(`"${table}".`, "")
    .replaceAll(/["`\s]/g, "")
    .toLowerCase();
}

/**
 * Read a PRAGMA or sqlite_master column as text.
 * @param value - libsql column value
 * @returns Text
 */
function text(value: Value | undefined): string {
  if (value === undefined || value === null) return "";
  return String(value);
}

/**
 * Order values by their JSON encoding, for stable comparisons.
 * @param a - First value
 * @param b - Second value
 * @returns Sort order
 */
function compareJson<Item>(a: Item, b: Item): number {
  return JSON.stringify(a).localeCompare(JSON.stringify(b));
}
