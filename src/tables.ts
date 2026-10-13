// Adapted from OpenWorkflow (https://github.com/openworkflowdev/openworkflow),
// Apache-2.0, see LICENSE-APACHE. Modified to run on Drizzle ORM.
import { sql } from "drizzle-orm";
import {
  foreignKey as pgForeignKey,
  index as pgIndex,
  integer as pgInteger,
  jsonb,
  pgSchema,
  pgTable,
  primaryKey as pgPrimaryKey,
  text as pgText,
  timestamp,
  uniqueIndex as pgUniqueIndex,
  type PgSchema,
  type PgTableExtraConfigValue,
} from "drizzle-orm/pg-core";
import {
  foreignKey,
  index,
  integer,
  primaryKey,
  sqliteTable,
  text,
  uniqueIndex,
  type SQLiteTableExtraConfigValue,
} from "drizzle-orm/sqlite-core";

/**
 * Options for the Drizzle table factories.
 */
export interface DrizzleTableOptions {
  /**
   * Prefix for every table, index, and constraint name, e.g. `"ow_"`. Use one
   * when the database already has tables named `workflow_runs` etc.
   */
  prefix?: string | undefined;
}

/**
 * Options for {@link createPgTables}.
 */
export interface DrizzlePgTableOptions extends DrizzleTableOptions {
  /**
   * Postgres schema for the tables. Defaults to the connection's search path
   * (usually `public`). When set, also export the returned `schema` from your
   * Drizzle schema, so drizzle-kit creates it instead of dropping it.
   */
  schema?: string | undefined;
}

/**
 * Create the OpenWorkflow SQLite tables. Export them from your Drizzle schema
 * so drizzle-kit includes them in your migrations, then pass them to
 * `BackendDrizzleSqlite`. The DDL matches the `openworkflow/sqlite` backend's
 * migrations.
 * @param options - Table options
 * @returns Drizzle tables
 */
export function createSqliteTables(options: DrizzleTableOptions = {}) {
  const name = prefixName(options.prefix);

  const workflowRuns = sqliteTable(
    name("workflow_runs"),
    {
      namespaceId: text("namespace_id").notNull(),
      id: text("id").notNull(),
      workflowName: text("workflow_name").notNull(),
      version: text("version"),
      status: text("status").notNull(),
      idempotencyKey: text("idempotency_key"),
      config: text("config").notNull(),
      context: text("context"),
      input: text("input"),
      output: text("output"),
      error: text("error"),
      attempts: integer("attempts").notNull(),
      parentStepAttemptNamespaceId: text("parent_step_attempt_namespace_id"),
      parentStepAttemptId: text("parent_step_attempt_id"),
      workerId: text("worker_id"),
      availableAt: text("available_at"),
      deadlineAt: text("deadline_at"),
      startedAt: text("started_at"),
      finishedAt: text("finished_at"),
      createdAt: text("created_at").notNull(),
      updatedAt: text("updated_at").notNull(),
      // added by a later migration, so it comes last
      workflowScheduleId: text("workflow_schedule_id"),
    },
    // the explicit return type breaks the circular foreign key inference
    (t): SQLiteTableExtraConfigValue[] => [
      primaryKey({ columns: [t.namespaceId, t.id] }),
      foreignKey({
        columns: [t.parentStepAttemptNamespaceId, t.parentStepAttemptId],
        // extra config runs lazily, once both tables exist
        foreignColumns: [stepAttempts.namespaceId, stepAttempts.id],
      }).onDelete("set null"),
      index(name("workflow_runs_status_available_at_created_at_idx")).on(
        t.namespaceId,
        t.status,
        t.availableAt,
        t.createdAt,
      ),
      index(
        name("workflow_runs_workflow_name_idempotency_key_created_at_idx"),
      ).on(t.namespaceId, t.workflowName, t.idempotencyKey, t.createdAt),
      index(name("workflow_runs_parent_step_idx"))
        .on(t.parentStepAttemptNamespaceId, t.parentStepAttemptId)
        .where(
          sql`${t.parentStepAttemptNamespaceId} IS NOT NULL AND ${t.parentStepAttemptId} IS NOT NULL`,
        ),
      index(name("workflow_runs_created_at_desc_idx")).on(
        t.namespaceId,
        sql`${t.createdAt} DESC`,
      ),
      index(name("workflow_runs_status_created_at_desc_idx")).on(
        t.namespaceId,
        t.status,
        sql`${t.createdAt} DESC`,
      ),
      index(name("workflow_runs_workflow_name_status_created_at_desc_idx")).on(
        t.namespaceId,
        t.workflowName,
        t.status,
        sql`${t.createdAt} DESC`,
      ),
    ],
  );

  const stepAttempts = sqliteTable(
    name("step_attempts"),
    {
      namespaceId: text("namespace_id").notNull(),
      id: text("id").notNull(),
      workflowRunId: text("workflow_run_id").notNull(),
      stepName: text("step_name").notNull(),
      kind: text("kind").notNull(),
      status: text("status").notNull(),
      config: text("config").notNull(),
      context: text("context"),
      output: text("output"),
      error: text("error"),
      childWorkflowRunNamespaceId: text("child_workflow_run_namespace_id"),
      childWorkflowRunId: text("child_workflow_run_id"),
      startedAt: text("started_at"),
      finishedAt: text("finished_at"),
      createdAt: text("created_at").notNull(),
      updatedAt: text("updated_at").notNull(),
      stepIndex: integer("step_index"),
    },
    (t): SQLiteTableExtraConfigValue[] => [
      primaryKey({ columns: [t.namespaceId, t.id] }),
      foreignKey({
        columns: [t.namespaceId, t.workflowRunId],
        foreignColumns: [workflowRuns.namespaceId, workflowRuns.id],
      }).onDelete("cascade"),
      foreignKey({
        columns: [t.childWorkflowRunNamespaceId, t.childWorkflowRunId],
        foreignColumns: [workflowRuns.namespaceId, workflowRuns.id],
      }).onDelete("set null"),
      index(name("step_attempts_workflow_run_created_at_idx")).on(
        t.namespaceId,
        t.workflowRunId,
        t.createdAt,
      ),
      index(name("step_attempts_workflow_run_step_name_created_at_idx")).on(
        t.namespaceId,
        t.workflowRunId,
        t.stepName,
        t.createdAt,
      ),
      index(name("step_attempts_child_workflow_run_idx"))
        .on(t.childWorkflowRunNamespaceId, t.childWorkflowRunId)
        .where(
          sql`${t.childWorkflowRunNamespaceId} IS NOT NULL AND ${t.childWorkflowRunId} IS NOT NULL`,
        ),
      // `->>` rather than `json_extract(...)`: drizzle-kit 0.31 splits index
      // expressions on commas. Queries must use the same expression to hit it.
      index(name("step_attempts_signal_wait_idx"))
        .on(t.namespaceId, sql`${t.context} ->> '$.signal'`)
        .where(sql`${t.kind} = 'signal-wait' AND ${t.status} = 'running'`),
    ],
  );

  const workflowSignals = sqliteTable(
    name("workflow_signals"),
    {
      namespaceId: text("namespace_id").notNull(),
      id: text("id").notNull(),
      signal: text("signal").notNull(),
      data: text("data"),
      senderIdempotencyKey: text("sender_idempotency_key"),
      workflowRunId: text("workflow_run_id").notNull(),
      stepAttemptId: text("step_attempt_id").notNull(),
      createdAt: text("created_at").notNull(),
    },
    (t): SQLiteTableExtraConfigValue[] => [
      primaryKey({ columns: [t.namespaceId, t.id] }),
      uniqueIndex(name("workflow_signals_step_attempt_idx")).on(
        t.namespaceId,
        t.stepAttemptId,
      ),
      index(name("workflow_signals_idempotency_idx"))
        .on(t.namespaceId, t.signal, t.senderIdempotencyKey)
        .where(sql`${t.senderIdempotencyKey} IS NOT NULL`),
    ],
  );

  const workflowSchedules = sqliteTable(
    name("workflow_schedules"),
    {
      namespaceId: text("namespace_id").notNull(),
      id: text("id").notNull(),
      workflowName: text("workflow_name").notNull(),
      input: text("input"),
      cron: text("cron").notNull(),
      timezone: text("timezone").notNull(),
      nextRunAt: text("next_run_at"),
      createdAt: text("created_at").notNull(),
      updatedAt: text("updated_at").notNull(),
    },
    (t): SQLiteTableExtraConfigValue[] => [
      primaryKey({ columns: [t.namespaceId, t.id] }),
      index(name("workflow_schedules_due_idx"))
        .on(t.namespaceId, t.nextRunAt, t.id)
        .where(sql`${t.nextRunAt} IS NOT NULL`),
      index(name("workflow_schedules_created_at_idx")).on(
        t.namespaceId,
        t.createdAt,
        t.id,
      ),
    ],
  );

  return { workflowRuns, stepAttempts, workflowSignals, workflowSchedules };
}

/**
 * Create the OpenWorkflow Postgres tables. Export them (and `schema`, when
 * set) from your Drizzle schema so drizzle-kit includes them in your
 * migrations, then pass them to `BackendDrizzlePg`. The DDL matches the
 * `openworkflow/postgres` backend's migrations.
 * @param options - Table options
 * @returns Drizzle tables, plus the Postgres schema when one is set
 */
export function createPgTables(options: DrizzlePgTableOptions = {}) {
  const name = prefixName(options.prefix);
  const schema: PgSchema | undefined =
    options.schema === undefined ? undefined : pgSchema(options.schema);
  // safety: PgSchema#table has the same call signature as pgTable.
  const table = (
    schema ? schema.table.bind(schema) : pgTable
  ) as typeof pgTable;

  const workflowRuns = table(
    name("workflow_runs"),
    {
      namespaceId: pgText("namespace_id").notNull(),
      id: pgText("id").notNull(),
      workflowName: pgText("workflow_name").notNull(),
      version: pgText("version"),
      status: pgText("status").notNull(),
      idempotencyKey: pgText("idempotency_key"),
      config: jsonb("config").notNull(),
      context: jsonb("context"),
      input: jsonb("input"),
      output: jsonb("output"),
      error: jsonb("error"),
      attempts: pgInteger("attempts").notNull(),
      parentStepAttemptNamespaceId: pgText("parent_step_attempt_namespace_id"),
      parentStepAttemptId: pgText("parent_step_attempt_id"),
      workerId: pgText("worker_id"),
      availableAt: timestamptz("available_at"),
      deadlineAt: timestamptz("deadline_at"),
      startedAt: timestamptz("started_at"),
      finishedAt: timestamptz("finished_at"),
      createdAt: timestamptz("created_at").notNull(),
      updatedAt: timestamptz("updated_at").notNull(),
      // added by a later migration, so it comes last
      workflowScheduleId: pgText("workflow_schedule_id"),
    },
    // the explicit return type breaks the circular foreign key inference
    (t): PgTableExtraConfigValue[] => [
      pgPrimaryKey({
        name: name("workflow_runs_pkey"),
        columns: [t.namespaceId, t.id],
      }),
      pgForeignKey({
        name: name("workflow_runs_parent_step_attempt_fk"),
        columns: [t.parentStepAttemptNamespaceId, t.parentStepAttemptId],
        // extra config runs lazily, once both tables exist
        foreignColumns: [stepAttempts.namespaceId, stepAttempts.id],
      }).onDelete("set null"),
      pgIndex(name("workflow_runs_status_available_at_created_at_idx")).on(
        t.namespaceId,
        t.status,
        t.availableAt,
        t.createdAt,
      ),
      pgIndex(
        name("workflow_runs_workflow_name_idempotency_key_created_at_idx"),
      ).on(t.namespaceId, t.workflowName, t.idempotencyKey, t.createdAt),
      pgIndex(name("workflow_runs_parent_step_idx"))
        .on(t.parentStepAttemptNamespaceId, t.parentStepAttemptId)
        .where(
          sql`${t.parentStepAttemptNamespaceId} IS NOT NULL AND ${t.parentStepAttemptId} IS NOT NULL`,
        ),
      // plain DESC means NULLS FIRST in Postgres; drizzle's .desc() alone
      // emits NULLS LAST
      pgIndex(name("workflow_runs_created_at_desc_idx")).on(
        t.namespaceId,
        t.createdAt.desc().nullsFirst(),
      ),
      pgIndex(name("workflow_runs_status_created_at_desc_idx")).on(
        t.namespaceId,
        t.status,
        t.createdAt.desc().nullsFirst(),
      ),
      pgIndex(
        name("workflow_runs_workflow_name_status_created_at_desc_idx"),
      ).on(
        t.namespaceId,
        t.workflowName,
        t.status,
        t.createdAt.desc().nullsFirst(),
      ),
    ],
  );

  const stepAttempts = table(
    name("step_attempts"),
    {
      namespaceId: pgText("namespace_id").notNull(),
      id: pgText("id").notNull(),
      workflowRunId: pgText("workflow_run_id").notNull(),
      stepName: pgText("step_name").notNull(),
      kind: pgText("kind").notNull(),
      status: pgText("status").notNull(),
      config: jsonb("config").notNull(),
      context: jsonb("context"),
      output: jsonb("output"),
      error: jsonb("error"),
      childWorkflowRunNamespaceId: pgText("child_workflow_run_namespace_id"),
      childWorkflowRunId: pgText("child_workflow_run_id"),
      startedAt: timestamptz("started_at"),
      finishedAt: timestamptz("finished_at"),
      createdAt: timestamptz("created_at").notNull(),
      updatedAt: timestamptz("updated_at").notNull(),
      stepIndex: pgInteger("step_index"),
    },
    (t): PgTableExtraConfigValue[] => [
      pgPrimaryKey({
        name: name("step_attempts_pkey"),
        columns: [t.namespaceId, t.id],
      }),
      pgForeignKey({
        name: name("step_attempts_workflow_run_fk"),
        columns: [t.namespaceId, t.workflowRunId],
        foreignColumns: [workflowRuns.namespaceId, workflowRuns.id],
      }).onDelete("cascade"),
      pgForeignKey({
        name: name("step_attempts_child_workflow_run_fk"),
        columns: [t.childWorkflowRunNamespaceId, t.childWorkflowRunId],
        foreignColumns: [workflowRuns.namespaceId, workflowRuns.id],
      }).onDelete("set null"),
      pgIndex(name("step_attempts_workflow_run_created_at_idx")).on(
        t.namespaceId,
        t.workflowRunId,
        t.createdAt,
      ),
      pgIndex(name("step_attempts_workflow_run_step_name_created_at_idx")).on(
        t.namespaceId,
        t.workflowRunId,
        t.stepName,
        t.createdAt,
      ),
      pgIndex(name("step_attempts_child_workflow_run_idx"))
        .on(t.childWorkflowRunNamespaceId, t.childWorkflowRunId)
        .where(
          sql`${t.childWorkflowRunNamespaceId} IS NOT NULL AND ${t.childWorkflowRunId} IS NOT NULL`,
        ),
      pgIndex(name("step_attempts_signal_wait_idx"))
        .on(t.namespaceId, sql`(${t.context}->>'signal')`)
        .where(sql`${t.kind} = 'signal-wait' AND ${t.status} = 'running'`),
    ],
  );

  const workflowSignals = table(
    name("workflow_signals"),
    {
      namespaceId: pgText("namespace_id").notNull(),
      id: pgText("id").notNull(),
      signal: pgText("signal").notNull(),
      data: jsonb("data"),
      senderIdempotencyKey: pgText("sender_idempotency_key"),
      workflowRunId: pgText("workflow_run_id").notNull(),
      stepAttemptId: pgText("step_attempt_id").notNull(),
      createdAt: timestamptz("created_at").notNull().defaultNow(),
    },
    (t): PgTableExtraConfigValue[] => [
      pgPrimaryKey({
        name: name("workflow_signals_pkey"),
        columns: [t.namespaceId, t.id],
      }),
      pgUniqueIndex(name("workflow_signals_step_attempt_idx")).on(
        t.namespaceId,
        t.stepAttemptId,
      ),
      pgIndex(name("workflow_signals_idempotency_idx"))
        .on(t.namespaceId, t.signal, t.senderIdempotencyKey)
        .where(sql`${t.senderIdempotencyKey} IS NOT NULL`),
    ],
  );

  const workflowSchedules = table(
    name("workflow_schedules"),
    {
      namespaceId: pgText("namespace_id").notNull(),
      id: pgText("id").notNull(),
      workflowName: pgText("workflow_name").notNull(),
      input: jsonb("input"),
      cron: pgText("cron").notNull(),
      timezone: pgText("timezone").notNull(),
      nextRunAt: timestamptz("next_run_at"),
      createdAt: timestamptz("created_at").notNull(),
      updatedAt: timestamptz("updated_at").notNull(),
    },
    (t): PgTableExtraConfigValue[] => [
      pgPrimaryKey({
        name: name("workflow_schedules_pkey"),
        columns: [t.namespaceId, t.id],
      }),
      pgIndex(name("workflow_schedules_due_idx"))
        .on(t.namespaceId, t.nextRunAt, t.id)
        .where(sql`${t.nextRunAt} IS NOT NULL`),
      pgIndex(name("workflow_schedules_created_at_idx")).on(
        t.namespaceId,
        t.createdAt,
        t.id,
      ),
    ],
  );

  return {
    workflowRuns,
    stepAttempts,
    workflowSignals,
    workflowSchedules,
    schema,
  };
}

/**
 * Tables used by `BackendDrizzleSqlite`.
 */
export type DrizzleSqliteTables = ReturnType<typeof createSqliteTables>;

/**
 * Tables used by `BackendDrizzlePg`.
 */
export type DrizzlePgTables = Omit<ReturnType<typeof createPgTables>, "schema">;

/**
 * A `timestamptz` column, as the Postgres backend's migrations define them.
 * @param column - Column name
 * @returns Column builder
 */
function timestamptz(column: string) {
  return timestamp(column, { withTimezone: true, mode: "date" });
}

/**
 * Return a function that applies the table prefix to a name.
 * @param prefix - Optional prefix
 * @returns Function that prefixes a name
 */
function prefixName(prefix: string | undefined): (name: string) => string {
  return (name) => `${prefix ?? ""}${name}`;
}
