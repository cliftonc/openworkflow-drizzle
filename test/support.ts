import type {
  createPgTables,
  DrizzlePgDatabase,
  DrizzleSqliteDatabase,
  DrizzleSqliteTables,
} from "../src/index.js";
import { sql } from "drizzle-orm";

/** Postgres for the tests; CI runs a postgres service on the default port. */
export const POSTGRES_URL =
  process.env["OPENWORKFLOW_POSTGRES_URL"] ??
  "postgresql://postgres:postgres@localhost:5432/postgres";

/** Drop server notices, e.g. from DROP SCHEMA ... CASCADE. */
export function ignoreNotice(): void {
  // notices are expected during cleanup
}

type PgTables = ReturnType<typeof createPgTables>;

/** Tables passed to drizzle-kit; `{}` is the empty starting schema. */
type SchemaImports = DrizzleSqliteTables | PgTables | Record<string, never>;

declare const snapshotBrand: unique symbol;

/** A drizzle-kit schema snapshot, passed straight back to drizzle-kit. */
interface Snapshot {
  readonly [snapshotBrand]: true;
}

/** The part of drizzle-kit's programmatic API these helpers use. */
interface DrizzleKitApi {
  generateDrizzleJson(imports: SchemaImports): Snapshot | Promise<Snapshot>;
  generateMigration(prev: Snapshot, cur: Snapshot): Promise<string[]>;
}

/**
 * Load drizzle-kit's generate API for a dialect. drizzle-kit 1.x has one
 * module per dialect; 0.x has a single `drizzle-kit/api` module.
 * @param dialect - SQL dialect
 * @returns Generate functions
 */
async function loadDrizzleKit(
  dialect: "sqlite" | "postgres",
): Promise<DrizzleKitApi> {
  // variable specifiers: only one of these modules exists per version
  const v1 = `drizzle-kit/payload/${dialect}`;
  const v0 = "drizzle-kit/api";
  try {
    // safety: drizzle-kit 1.x payload modules export these functions.
    return (await import(v1)) as DrizzleKitApi;
  } catch {
    // safety: drizzle-kit 0.x exports these functions from drizzle-kit/api.
    const api = (await import(v0)) as {
      generateDrizzleJson: DrizzleKitApi["generateDrizzleJson"];
      generateMigration: DrizzleKitApi["generateMigration"];
      generateSQLiteDrizzleJson: DrizzleKitApi["generateDrizzleJson"];
      generateSQLiteMigration: DrizzleKitApi["generateMigration"];
    };
    return dialect === "sqlite"
      ? {
          generateDrizzleJson: api.generateSQLiteDrizzleJson,
          generateMigration: api.generateSQLiteMigration,
        }
      : api;
  }
}

/**
 * Generate the DDL drizzle-kit would put in an app's first migration for
 * these tables, the same way `drizzle-kit generate` does.
 * @param dialect - SQL dialect
 * @param tables - Drizzle tables (and pg schema) to create
 * @returns SQL statements
 */
async function generateDdl(
  dialect: "sqlite" | "postgres",
  tables: DrizzleSqliteTables | PgTables,
): Promise<string[]> {
  const kit = await loadDrizzleKit(dialect);
  return await kit.generateMigration(
    await kit.generateDrizzleJson({}),
    await kit.generateDrizzleJson(tables),
  );
}

/**
 * Create tables on a SQLite database from the drizzle-kit generated DDL.
 * @param db - Drizzle SQLite database
 * @param tables - Drizzle tables to create
 */
export async function createSqliteSchema(
  db: DrizzleSqliteDatabase,
  tables: DrizzleSqliteTables,
): Promise<void> {
  for (const statement of await generateDdl("sqlite", tables)) {
    await db.all(sql.raw(statement));
  }
}

/**
 * Create tables on a Postgres database from the drizzle-kit generated DDL.
 * @param db - Drizzle Postgres database
 * @param tables - Drizzle tables (and pg schema) to create
 */
export async function createPgSchema(
  db: DrizzlePgDatabase,
  tables: PgTables,
): Promise<void> {
  for (const statement of await generateDdl("postgres", tables)) {
    await db.execute(sql.raw(statement));
  }
}
