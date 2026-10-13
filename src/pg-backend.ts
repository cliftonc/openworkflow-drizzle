// Adapted from OpenWorkflow (https://github.com/openworkflowdev/openworkflow),
// Apache-2.0, see LICENSE-APACHE. Modified to run on Drizzle ORM.
import {
  fromJSON,
  pgSelectList,
  rowToStepAttempt,
  rowToWorkflowRun,
  rowToWorkflowSchedule,
  STEP_ATTEMPT_COLUMNS,
  toJSON,
  WORKFLOW_RUN_COLUMNS,
  WORKFLOW_SCHEDULE_COLUMNS,
  type StepAttemptRow,
  type WorkflowRunRow,
  type WorkflowScheduleRow,
} from "./rows.js";
import type { DrizzlePgTables } from "./tables.js";
import { sql, type SQL } from "drizzle-orm";
import {
  buildPaginatedResponse,
  computeFailedWorkflowRunUpdate,
  DEFAULT_NAMESPACE_ID,
  DEFAULT_PAGINATION_PAGE_SIZE,
  DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS,
  decodeListCursor,
  nextScheduleOccurrence,
  prepareWorkflowRerun,
  requireRow,
  resolveCancelWorkflowRunConflict,
  toWorkflowRunCounts,
  type Backend,
  type CancelWorkflowRunParams,
  type ClaimWorkflowRunParams,
  type CompleteStepAttemptParams,
  type CompleteWorkflowRunParams,
  type CreateStepAttemptParams,
  type CreateWorkflowRunParams,
  type Cursor,
  type EnqueueScheduledRunsParams,
  type ExtendWorkflowRunLeaseParams,
  type FailStepAttemptParams,
  type FailWorkflowRunParams,
  type GetSignalDeliveryParams,
  type GetStepAttemptParams,
  type GetWorkflowRunParams,
  type JsonValue,
  type ListStepAttemptsParams,
  type ListWorkflowRunsParams,
  type PaginatedResponse,
  type PaginationOptions,
  type RerunWorkflowRunParams,
  type RescheduleWorkflowRunAfterFailedStepAttemptParams,
  type SendSignalParams,
  type SendSignalResult,
  type SetStepAttemptChildWorkflowRunParams,
  type SleepWorkflowRunParams,
  type StepAttempt,
  type WorkflowRun,
  type WorkflowRunCounts,
  type WorkflowSchedule,
  type WorkflowScheduleDefinition,
  type WorkflowScheduleIdParams,
} from "openworkflow/backend";

/**
 * A Drizzle Postgres database on any driver (postgres.js, node-postgres, …).
 * Only the methods the backend calls are listed, so any Drizzle version
 * matches.
 */
export interface DrizzlePgDatabase {
  execute(query: SQL): PromiseLike<DrizzlePgResult>;
  transaction<Result>(
    transaction: (tx: DrizzlePgDatabase) => Promise<Result>,
    config: { isolationLevel: "read committed" },
  ): Promise<Result>;
}

/**
 * A query result: the rows on postgres.js, or `{ rows }` on node-postgres
 * and Neon.
 */
type DrizzlePgResult = readonly object[] | { readonly rows: readonly object[] };

/**
 * Options for {@link BackendDrizzlePg}.
 */
export interface BackendDrizzlePgOptions {
  /** Drizzle database. The backend never opens or closes connections. */
  db: DrizzlePgDatabase;
  /** Tables from `createPgTables()`, as exported from your schema. */
  tables: DrizzlePgTables;
  namespaceId?: string | undefined;
}

type Executor = Pick<DrizzlePgDatabase, "execute">;

const WORKFLOW_RUN_SELECT = sql.raw(pgSelectList(WORKFLOW_RUN_COLUMNS));
const WR_WORKFLOW_RUN_SELECT = sql.raw(
  pgSelectList(WORKFLOW_RUN_COLUMNS, "wr"),
);
const STEP_ATTEMPT_SELECT = sql.raw(pgSelectList(STEP_ATTEMPT_COLUMNS));
const SA_STEP_ATTEMPT_SELECT = sql.raw(
  pgSelectList(STEP_ATTEMPT_COLUMNS, "sa"),
);
const WORKFLOW_SCHEDULE_SELECT = sql.raw(
  pgSelectList(WORKFLOW_SCHEDULE_COLUMNS),
);

/**
 * Workflow storage on an existing Drizzle Postgres database. The queries
 * mirror `BackendPostgres`.
 */
export class BackendDrizzlePg implements Backend {
  private readonly db: DrizzlePgDatabase;
  private readonly tables: DrizzlePgTables;
  private readonly namespaceId: string;

  constructor(options: BackendDrizzlePgOptions) {
    this.db = options.db;
    this.tables = options.tables;
    this.namespaceId = options.namespaceId ?? DEFAULT_NAMESPACE_ID;
  }

  /**
   * Does nothing: the database belongs to the caller.
   * @returns Resolved promise
   */
  stop(): Promise<void> {
    return Promise.resolve();
  }

  async upsertWorkflowSchedule(
    params: WorkflowScheduleDefinition,
  ): Promise<WorkflowSchedule> {
    const now = new Date();
    const [row] = await this.rows<WorkflowScheduleRow>(sql`
      INSERT INTO ${this.tables.workflowSchedules} AS s
        ("namespace_id", "id", "workflow_name", "input", "cron", "timezone", "next_run_at", "created_at", "updated_at")
      VALUES (
        ${this.namespaceId}, ${params.id}, ${params.workflowName}, ${jsonb(params.input)},
        ${params.cron}, ${params.timezone}, ${timestamptz(nextScheduleOccurrence(params, now))},
        ${timestamptz(now)}, ${timestamptz(now)}
      )
      ON CONFLICT ("namespace_id", "id") DO UPDATE SET
        "workflow_name" = excluded."workflow_name", "input" = excluded."input",
        "cron" = excluded."cron", "timezone" = excluded."timezone", "updated_at" = excluded."updated_at",
        "next_run_at" = CASE WHEN s."next_run_at" IS NOT NULL
          AND (s."cron" <> excluded."cron" OR s."timezone" <> excluded."timezone")
          THEN excluded."next_run_at" ELSE s."next_run_at" END
      RETURNING ${WORKFLOW_SCHEDULE_SELECT}
    `);
    requireRow(row, "register schedule");
    return rowToWorkflowSchedule(row);
  }

  async getWorkflowSchedule(
    params: WorkflowScheduleIdParams,
  ): Promise<WorkflowSchedule | null> {
    const [row] = await this.rows<WorkflowScheduleRow>(sql`
      SELECT ${WORKFLOW_SCHEDULE_SELECT} FROM ${this.tables.workflowSchedules}
      WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
    `);
    return row ? rowToWorkflowSchedule(row) : null;
  }

  async listWorkflowSchedules(
    params: PaginationOptions,
  ): Promise<PaginatedResponse<WorkflowSchedule>> {
    const cursor = decodeListCursor(params);
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const where = cursor
      ? sql`AND ("created_at", "id") ${sql.raw(params.before ? "<" : ">")} (${timestamptz(cursor.createdAt)}, ${cursor.id})`
      : sql``;
    const order = sql.raw(params.before ? "DESC" : "ASC");
    const rows = await this.rows<WorkflowScheduleRow>(sql`
      SELECT ${WORKFLOW_SCHEDULE_SELECT} FROM ${this.tables.workflowSchedules}
      WHERE "namespace_id" = ${this.namespaceId} ${where}
      ORDER BY "created_at" ${order}, "id" ${order} LIMIT ${limit + 1}
    `);
    return buildPaginatedResponse(
      rows.map((row) => rowToWorkflowSchedule(row)),
      limit,
      !!params.after,
      !!params.before,
    );
  }

  async pauseWorkflowSchedule(
    params: WorkflowScheduleIdParams & { paused: boolean },
  ): Promise<WorkflowSchedule> {
    const schedules = this.tables.workflowSchedules;

    return await this.transaction(async (tx) => {
      const [row] = await this.rows<WorkflowScheduleRow>(
        sql`
        SELECT ${WORKFLOW_SCHEDULE_SELECT} FROM ${schedules}
        WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
        FOR UPDATE
      `,
        tx,
      );
      requireRow(row, "change schedule pause state");
      const schedule = rowToWorkflowSchedule(row);
      if ((schedule.nextRunAt === null) === params.paused) return schedule;
      const now = new Date();
      const next = params.paused ? null : nextScheduleOccurrence(schedule, now);
      const [updated] = await this.rows<WorkflowScheduleRow>(
        sql`
        UPDATE ${schedules}
        SET "next_run_at" = ${timestamptz(next)}, "updated_at" = ${timestamptz(now)}
        WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
        RETURNING ${WORKFLOW_SCHEDULE_SELECT}
      `,
        tx,
      );
      requireRow(updated, "read updated schedule");
      return rowToWorkflowSchedule(updated);
    });
  }

  async deleteWorkflowSchedule(
    params: WorkflowScheduleIdParams,
  ): Promise<void> {
    await this.rows(sql`
      DELETE FROM ${this.tables.workflowSchedules}
      WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
    `);
  }

  async enqueueScheduledRuns(
    params: EnqueueScheduledRunsParams,
  ): Promise<number> {
    const { workflowRuns, workflowSchedules } = this.tables;

    return await this.transaction(async (tx) => {
      const schedules = await this.rows<
        Pick<WorkflowScheduleRow, "id" | "cron" | "timezone">
      >(
        sql`
        SELECT "id", "cron", "timezone" FROM ${workflowSchedules}
        WHERE "namespace_id" = ${this.namespaceId} AND "next_run_at" <= ${timestamptz(params.before)}
        ORDER BY "next_run_at", "id" LIMIT ${params.limit} FOR UPDATE SKIP LOCKED
      `,
        tx,
      );
      if (schedules.length === 0) return 0;
      const advances = schedules.map((schedule) => ({
        id: schedule.id,
        next_run_at: nextScheduleOccurrence(
          schedule,
          params.before,
        ).toISOString(),
      }));
      await this.rows(
        sql`
        WITH advances AS (
          SELECT * FROM jsonb_to_recordset(${jsonb(advances)})
            AS a(id TEXT, next_run_at TIMESTAMPTZ)
        ), advanced AS (
          UPDATE ${workflowSchedules} s
          SET "next_run_at" = a.next_run_at, "updated_at" = ${timestamptz(params.before)}
          FROM advances a WHERE s."namespace_id" = ${this.namespaceId} AND s."id" = a.id
          RETURNING s."namespace_id", s."id", s."workflow_name", s."input"
        )
        INSERT INTO ${workflowRuns}
          ("namespace_id", "id", "workflow_name", "status", "config", "input", "attempts",
           "workflow_schedule_id", "available_at", "created_at", "updated_at")
        SELECT "namespace_id", gen_random_uuid(), "workflow_name", 'pending', '{}'::jsonb, "input", 0,
          "id", ${timestamptz(params.before)}, date_trunc('milliseconds', clock_timestamp()), clock_timestamp()
        FROM advanced
      `,
        tx,
      );
      return schedules.length;
    });
  }

  async createWorkflowRun(
    params: CreateWorkflowRunParams,
  ): Promise<WorkflowRun> {
    if (params.idempotencyKey === null) {
      return await this.insertWorkflowRun(this.db, params);
    }

    const { workflowName, idempotencyKey } = params;
    const lockScope = JSON.stringify({
      namespaceId: this.namespaceId,
      workflowName,
      idempotencyKey,
    });

    return await this.transaction(async (tx) => {
      await this.rows(
        sql`SELECT pg_advisory_xact_lock(hashtextextended(${lockScope}::text, 0::bigint))`,
        tx,
      );

      const existing = await this.getWorkflowRunByIdempotencyKey(
        tx,
        workflowName,
        idempotencyKey,
        new Date(Date.now() - DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS),
      );
      if (existing) {
        return existing;
      }

      return await this.insertWorkflowRun(tx, params);
    });
  }

  async rerunWorkflowRun(
    request: RerunWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const { workflowRuns, stepAttempts } = this.tables;

    return await this.transaction(async (tx) => {
      const [source] = await this.rows<WorkflowRunRow>(
        sql`
        SELECT ${WORKFLOW_RUN_SELECT} FROM ${workflowRuns}
        WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${request.workflowRunId}
        FOR UPDATE
      `,
        tx,
      );
      const steps =
        request.fromStep === null
          ? []
          : await this.rows<
              Pick<StepAttemptRow, "step_name" | "step_index" | "status">
            >(
              sql`
        SELECT "step_name", "step_index", "status" FROM ${stepAttempts}
        WHERE "namespace_id" = ${this.namespaceId} AND "workflow_run_id" = ${request.workflowRunId}
      `,
              tx,
            );
      const { params, stepIndex } = prepareWorkflowRerun(
        source ? rowToWorkflowRun(source) : null,
        request,
        steps.map((step) => ({
          stepName: step.step_name,
          stepIndex: step.step_index,
          // safety: backend transitions write domain status values.
          status: step.status as StepAttempt["status"],
        })),
      );
      const run = await this.insertWorkflowRun(tx, params);
      if (stepIndex !== null) {
        await this.rows(
          sql`
          INSERT INTO ${stepAttempts} (
            "namespace_id", "id", "workflow_run_id", "step_name", "step_index", "kind", "status",
            "config", "context", "output", "child_workflow_run_namespace_id", "child_workflow_run_id",
            "started_at", "finished_at", "created_at", "updated_at"
          )
          SELECT "namespace_id", gen_random_uuid(), ${run.id}::text, "step_name", "step_index", "kind", "status",
            "config", "context", "output", "child_workflow_run_namespace_id", "child_workflow_run_id",
            "started_at", "finished_at", "created_at", "updated_at"
          FROM ${stepAttempts}
          WHERE "namespace_id" = ${this.namespaceId} AND "workflow_run_id" = ${request.workflowRunId}
            AND "status" IN ('completed', 'succeeded')
            AND "step_index" < ${stepIndex}::integer
        `,
          tx,
        );
      }
      return run;
    });
  }

  private async insertWorkflowRun(
    executor: Executor,
    params: CreateWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const [workflowRun] = await this.rows<WorkflowRunRow>(
      sql`
      INSERT INTO ${this.tables.workflowRuns} (
        "namespace_id",
        "id",
        "workflow_name",
        "version",
        "status",
        "idempotency_key",
        "config",
        "context",
        "input",
        "attempts",
        "parent_step_attempt_namespace_id",
        "parent_step_attempt_id",
        "available_at",
        "deadline_at",
        "created_at",
        "updated_at"
      )
      VALUES (
        ${this.namespaceId},
        gen_random_uuid(),
        ${params.workflowName},
        ${params.version},
        'pending',
        ${params.idempotencyKey},
        ${jsonb(params.config)},
        ${jsonb(params.context)},
        ${jsonb(params.input)},
        0,
        ${params.parentStepAttemptNamespaceId ?? null},
        ${params.parentStepAttemptId ?? null},
        ${params.availableAt ? timestamptz(params.availableAt) : sql`NOW()`},
        ${timestamptz(params.deadlineAt)},
        date_trunc('milliseconds', NOW()),
        NOW()
      )
      RETURNING ${WORKFLOW_RUN_SELECT}
    `,
      executor,
    );

    requireRow(workflowRun, "create workflow run");

    return rowToWorkflowRun(workflowRun);
  }

  private async getWorkflowRunByIdempotencyKey(
    executor: Executor,
    workflowName: string,
    idempotencyKey: string,
    createdAt: Date,
  ): Promise<WorkflowRun | null> {
    const [workflowRun] = await this.rows<WorkflowRunRow>(
      sql`
      SELECT ${WORKFLOW_RUN_SELECT}
      FROM ${this.tables.workflowRuns}
      WHERE "namespace_id" = ${this.namespaceId}
        AND "workflow_name" = ${workflowName}
        AND "idempotency_key" = ${idempotencyKey}
        AND "created_at" >= ${timestamptz(createdAt)}
      ORDER BY "created_at" ASC, "id" ASC
      LIMIT 1
    `,
      executor,
    );

    return workflowRun ? rowToWorkflowRun(workflowRun) : null;
  }

  async getWorkflowRun(
    params: GetWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    const [workflowRun] = await this.rows<WorkflowRunRow>(sql`
      SELECT ${WORKFLOW_RUN_SELECT}
      FROM ${this.tables.workflowRuns}
      WHERE "namespace_id" = ${this.namespaceId}
      AND "id" = ${params.workflowRunId}
      LIMIT 1
    `);

    return workflowRun ? rowToWorkflowRun(workflowRun) : null;
  }

  async sendSignal(params: SendSignalParams): Promise<SendSignalResult> {
    const { workflowRuns, stepAttempts, workflowSignals } = this.tables;

    return await this.transaction(async (tx) => {
      if (params.idempotencyKey !== null) {
        const existing = await this.rows<{ workflow_run_id: string }>(
          sql`
            SELECT DISTINCT "workflow_run_id"
            FROM ${workflowSignals}
            WHERE "namespace_id" = ${this.namespaceId}
              AND "signal" = ${params.signal}
              AND "sender_idempotency_key" = ${params.idempotencyKey}
          `,
          tx,
        );
        if (existing.length > 0) {
          return { workflowRunIds: existing.map((r) => r.workflow_run_id) };
        }
      }

      const waiters = await this.rows<{ id: string; workflow_run_id: string }>(
        sql`
          SELECT sa."id", sa."workflow_run_id"
          FROM ${workflowRuns} wr
          JOIN ${stepAttempts} sa
            ON sa."namespace_id" = wr."namespace_id"
            AND sa."workflow_run_id" = wr."id"
          WHERE sa."namespace_id" = ${this.namespaceId}
            AND sa."kind" = 'signal-wait'
            AND sa."status" = 'running'
            AND sa."context"->>'signal' = ${params.signal}
            AND wr."status" IN ('pending', 'running', 'sleeping')
          FOR UPDATE OF wr, sa
        `,
        tx,
      );

      if (waiters.length === 0) {
        return { workflowRunIds: [] };
      }

      const deliveredRunIds = new Set<string>();
      for (const w of waiters) {
        const inserted = await this.rows(
          sql`
            INSERT INTO ${workflowSignals} (
              "namespace_id", "id", "signal", "data",
              "sender_idempotency_key", "workflow_run_id",
              "step_attempt_id", "created_at"
            )
            VALUES (
              ${this.namespaceId},
              gen_random_uuid(),
              ${params.signal},
              ${jsonb(params.data)},
              ${params.idempotencyKey},
              ${w.workflow_run_id},
              ${w.id},
              NOW()
            )
            ON CONFLICT ("namespace_id", "step_attempt_id") DO NOTHING
            RETURNING "workflow_run_id"
          `,
          tx,
        );
        if (inserted.length > 0) {
          deliveredRunIds.add(w.workflow_run_id);
        }
      }

      if (deliveredRunIds.size === 0) {
        return { workflowRunIds: [] };
      }

      // wake each waiting workflow run
      const runIds = [...deliveredRunIds];
      await this.rows(
        sql`
          UPDATE ${workflowRuns}
          SET
            "available_at" = CASE
              WHEN "available_at" IS NULL OR "available_at" > NOW()
                THEN NOW()
              ELSE "available_at"
            END,
            "updated_at" = NOW()
          WHERE "namespace_id" = ${this.namespaceId}
            AND "id" IN (${sql.join(
              runIds.map((id) => sql`${id}`),
              sql`, `,
            )})
            AND "status" IN ('pending', 'running', 'sleeping')
            AND "worker_id" IS NULL
        `,
        tx,
      );

      return { workflowRunIds: runIds };
    });
  }

  async getSignalDelivery(
    params: GetSignalDeliveryParams,
  ): Promise<JsonValue | undefined> {
    const [row] = await this.rows<{ data: string | null }>(sql`
      SELECT "data"::text AS "data"
      FROM ${this.tables.workflowSignals}
      WHERE "namespace_id" = ${this.namespaceId}
        AND "step_attempt_id" = ${params.stepAttemptId}
      LIMIT 1
    `);

    return row ? fromJSON(row.data) : undefined;
  }

  async listWorkflowRuns(
    params: ListWorkflowRunsParams,
  ): Promise<PaginatedResponse<WorkflowRun>> {
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const { after, before } = params;
    const cursor = decodeListCursor(params);

    const whereClause = this.buildListWorkflowRunsWhere(params, cursor);
    const order = before
      ? sql`ORDER BY "created_at" ASC, "id" ASC`
      : sql`ORDER BY "created_at" DESC, "id" DESC`;

    const rows = await this.rows<WorkflowRunRow>(sql`
      SELECT ${WORKFLOW_RUN_SELECT}
      FROM ${this.tables.workflowRuns}
      WHERE ${whereClause}
      ${order}
      LIMIT ${limit + 1}
    `);

    return buildPaginatedResponse(
      rows.map((row) => rowToWorkflowRun(row)),
      limit,
      !!after,
      !!before,
    );
  }

  private buildListWorkflowRunsWhere(
    params: ListWorkflowRunsParams,
    cursor: Cursor | null,
  ): SQL {
    const { after } = params;
    const conditions = [sql`"namespace_id" = ${this.namespaceId}`];

    if (params.status) {
      conditions.push(sql`"status" = ${params.status}`);
    }

    if (params.workflowName) {
      conditions.push(sql`"workflow_name" = ${params.workflowName}`);
    }

    if (cursor) {
      const op = sql.raw(after ? "<" : ">");
      conditions.push(
        sql`("created_at", "id") ${op} (${timestamptz(cursor.createdAt)}, ${cursor.id}::text)`,
      );
    }

    return sql.join(conditions, sql` AND `);
  }

  async countWorkflowRuns(): Promise<WorkflowRunCounts> {
    const rows = await this.rows<{ status: string; count: string }>(sql`
      SELECT "status", COUNT(*) AS "count"
      FROM ${this.tables.workflowRuns}
      WHERE "namespace_id" = ${this.namespaceId}
      GROUP BY "status"
    `);

    return toWorkflowRunCounts(rows);
  }

  async claimWorkflowRun(
    params: ClaimWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    // 1. mark any deadline-expired workflow runs as failed
    // 2. find an available workflow run to claim
    // 3. claim the workflow run
    const { workflowRuns } = this.tables;

    const [claimed] = await this.rows<WorkflowRunRow>(sql`
      WITH expired AS (
        UPDATE ${workflowRuns}
        SET
          "status" = 'failed',
          "error" = ${jsonb({ message: "Workflow run deadline exceeded" })},
          "worker_id" = NULL,
          "available_at" = NULL,
          "finished_at" = NOW(),
          "updated_at" = NOW()
        WHERE "namespace_id" = ${this.namespaceId}
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "deadline_at" IS NOT NULL
          AND "deadline_at" <= NOW()
        RETURNING "id"
      ),
      candidate AS (
        SELECT "id"
        FROM ${workflowRuns}
        WHERE "namespace_id" = ${this.namespaceId}
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "available_at" <= NOW()
          AND ("deadline_at" IS NULL OR "deadline_at" > NOW())
        ORDER BY
          CASE WHEN "status" = 'pending' THEN 0 ELSE 1 END,
          "available_at",
          "created_at"
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE ${workflowRuns} AS wr
      SET
        "status" = 'running',
        "attempts" = "attempts" + 1,
        "worker_id" = ${params.workerId},
        "available_at" = ${leaseExpiry(params.leaseDurationMs)},
        "started_at" = COALESCE("started_at", NOW()),
        "updated_at" = NOW()
      FROM candidate
      WHERE wr."id" = candidate."id"
        AND wr."namespace_id" = ${this.namespaceId}
      RETURNING ${WR_WORKFLOW_RUN_SELECT};
    `);

    return claimed ? rowToWorkflowRun(claimed) : null;
  }

  async extendWorkflowRunLease(
    params: ExtendWorkflowRunLeaseParams,
  ): Promise<WorkflowRun> {
    const [updated] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "available_at" = ${leaseExpiry(params.leaseDurationMs)},
        "updated_at" = NOW()
      WHERE ${this.runningWorkflowRunOwnedByWorkerWhere(params)}
      RETURNING ${WORKFLOW_RUN_SELECT}
    `);

    requireRow(updated, "extend lease for workflow run");

    return rowToWorkflowRun(updated);
  }

  async sleepWorkflowRun(params: SleepWorkflowRunParams): Promise<WorkflowRun> {
    // 'sleeping' and 'succeeded' statuses are deprecated
    const [updated] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "status" = 'running',
        "available_at" = ${timestamptz(params.availableAt)},
        "worker_id" = NULL,
        "updated_at" = NOW()
      WHERE "namespace_id" = ${this.namespaceId}
      AND "id" = ${params.workflowRunId}
      AND "status" != 'succeeded'
      AND "status" != 'completed'
      AND "status" != 'failed'
      AND "status" != 'canceled'
      AND "worker_id" = ${params.workerId}
      RETURNING ${WORKFLOW_RUN_SELECT}
    `);

    requireRow(updated, "sleep workflow run");

    const reconciled = await this.reconcileWorkflowSleepWakeUp(
      params.workflowRunId,
    );
    return reconciled ?? rowToWorkflowRun(updated);
  }

  /**
   * Reconcile a just-parked parent run that is waiting on workflow replay. If the
   * child already reached a terminal state before the parent cleared workerId,
   * the normal child-completion wake-up can be missed. This forces an immediate
   * wake-up for that case.
   * @param workflowRunId - Parent workflow run id
   * @returns Updated run when reconciliation changed availability, otherwise null
   */
  private async reconcileWorkflowSleepWakeUp(
    workflowRunId: string,
  ): Promise<WorkflowRun | null> {
    const { workflowRuns, stepAttempts, workflowSignals } = this.tables;

    const [updated] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${workflowRuns} wr
      SET
        "available_at" = CASE
          WHEN wr."available_at" IS NULL OR wr."available_at" > NOW()
            THEN NOW()
          ELSE wr."available_at"
        END,
        "updated_at" = NOW()
      WHERE wr."namespace_id" = ${this.namespaceId}
      AND wr."id" = ${workflowRunId}
      AND wr."status" = 'running'
      AND wr."worker_id" IS NULL
      AND (
        EXISTS (
          SELECT 1
          FROM ${stepAttempts} sa
          JOIN ${workflowRuns} child
            ON child."namespace_id" = sa."child_workflow_run_namespace_id"
            AND child."id" = sa."child_workflow_run_id"
          WHERE sa."namespace_id" = wr."namespace_id"
          AND sa."workflow_run_id" = wr."id"
          AND sa."kind" = 'workflow'
          AND sa."status" = 'running'
          AND child."status" IN ('completed', 'succeeded', 'failed', 'canceled')
        )
        OR EXISTS (
          SELECT 1
          FROM ${stepAttempts} sa
          JOIN ${workflowSignals} ws
            ON ws."namespace_id" = sa."namespace_id"
            AND ws."step_attempt_id" = sa."id"
          WHERE sa."namespace_id" = wr."namespace_id"
          AND sa."workflow_run_id" = wr."id"
          AND sa."kind" = 'signal-wait'
          AND sa."status" = 'running'
        )
      )
      RETURNING ${WR_WORKFLOW_RUN_SELECT}
    `);

    return updated ? rowToWorkflowRun(updated) : null;
  }

  async completeWorkflowRun(
    params: CompleteWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const [row] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "status" = 'completed',
        "output" = ${jsonb(params.output)},
        "error" = NULL,
        "worker_id" = ${params.workerId},
        "available_at" = NULL,
        "finished_at" = NOW(),
        "updated_at" = NOW()
      WHERE ${this.runningWorkflowRunOwnedByWorkerWhere(params)}
      RETURNING ${WORKFLOW_RUN_SELECT}
    `);

    requireRow(row, "mark workflow run completed");

    const updated = rowToWorkflowRun(row);
    await this.wakeParentWorkflowRun(updated);

    return updated;
  }

  async failWorkflowRun(params: FailWorkflowRunParams): Promise<WorkflowRun> {
    const { workflowRunId, error } = params;
    const currentTime = new Date();
    let attempts = params.attempts;
    let deadlineAt = params.deadlineAt;

    // Backward-compatible fallback for external callers that don't pass state.
    if (attempts === undefined || deadlineAt === undefined) {
      const workflowRun = await this.getWorkflowRun({ workflowRunId });
      if (!workflowRun) throw new Error("Workflow run not found");
      attempts = workflowRun.attempts;
      deadlineAt = workflowRun.deadlineAt;
    }

    const failureUpdate = computeFailedWorkflowRunUpdate(
      params.retryPolicy,
      attempts,
      deadlineAt,
      error,
      currentTime,
    );

    const [row] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "status" = ${failureUpdate.status},
        "available_at" = ${timestamptz(failureUpdate.availableAt)},
        "finished_at" = ${timestamptz(failureUpdate.finishedAt)},
        "error" = ${jsonb(failureUpdate.error)},
        "worker_id" = NULL,
        "started_at" = NULL,
        "updated_at" = NOW()
      WHERE ${this.runningWorkflowRunOwnedByWorkerWhere(params)}
      RETURNING ${WORKFLOW_RUN_SELECT}
    `);

    requireRow(row, "mark workflow run failed");

    const updated = rowToWorkflowRun(row);
    if (updated.status === "failed") {
      await this.wakeParentWorkflowRun(updated);
    }

    return updated;
  }

  async rescheduleWorkflowRunAfterFailedStepAttempt(
    params: RescheduleWorkflowRunAfterFailedStepAttemptParams,
  ): Promise<WorkflowRun> {
    const [updated] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "status" = 'pending',
        "available_at" = ${timestamptz(params.availableAt)},
        "finished_at" = NULL,
        "error" = ${jsonb(params.error)},
        "worker_id" = NULL,
        "started_at" = NULL,
        "updated_at" = NOW()
      WHERE ${this.runningWorkflowRunOwnedByWorkerWhere(params)}
      RETURNING ${WORKFLOW_RUN_SELECT}
    `);

    requireRow(updated, "reschedule workflow run after failed step attempt");

    return rowToWorkflowRun(updated);
  }

  async cancelWorkflowRun(
    params: CancelWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const [row] = await this.rows<WorkflowRunRow>(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "status" = 'canceled',
        "worker_id" = NULL,
        "available_at" = NULL,
        "finished_at" = NOW(),
        "updated_at" = NOW()
      WHERE "namespace_id" = ${this.namespaceId}
      AND "id" = ${params.workflowRunId}
      AND "status" IN ('pending', 'running', 'sleeping')
      RETURNING ${WORKFLOW_RUN_SELECT}
    `);

    if (!row) {
      // workflow may already be in a terminal state
      const existing = await this.getWorkflowRun({
        workflowRunId: params.workflowRunId,
      });
      return resolveCancelWorkflowRunConflict(params.workflowRunId, existing);
    }

    const updated = rowToWorkflowRun(row);
    await this.wakeParentWorkflowRun(updated);

    return updated;
  }

  private async wakeParentWorkflowRun(
    childWorkflowRun: Readonly<WorkflowRun>,
  ): Promise<void> {
    if (
      !childWorkflowRun.parentStepAttemptNamespaceId ||
      !childWorkflowRun.parentStepAttemptId
    ) {
      return;
    }

    await this.rows(sql`
      UPDATE ${this.tables.workflowRuns} wr
      SET
        "available_at" = CASE
          WHEN wr."available_at" IS NULL OR wr."available_at" > NOW()
            THEN NOW()
          ELSE wr."available_at"
        END,
        "updated_at" = NOW()
      FROM ${this.tables.stepAttempts} sa
      WHERE sa."namespace_id" = ${childWorkflowRun.parentStepAttemptNamespaceId}
      AND sa."id" = ${childWorkflowRun.parentStepAttemptId}
      AND sa."kind" = 'workflow'
      AND sa."status" = 'running'
      AND sa."child_workflow_run_namespace_id" = ${childWorkflowRun.namespaceId}
      AND sa."child_workflow_run_id" = ${childWorkflowRun.id}
      AND wr."namespace_id" = sa."namespace_id"
      AND wr."id" = sa."workflow_run_id"
      AND (
        wr."status" = 'sleeping'
        OR (wr."status" = 'running' AND wr."worker_id" IS NULL)
      )
    `);
  }

  async createStepAttempt(
    params: CreateStepAttemptParams,
  ): Promise<StepAttempt> {
    // parameters in an INSERT ... SELECT list need explicit types
    const [stepAttempt] = await this.rows<StepAttemptRow>(sql`
      WITH owned_workflow_run AS (
        ${this.runningWorkflowRunOwnedByWorkerForUpdate(params)}
      )
      INSERT INTO ${this.tables.stepAttempts} (
        "namespace_id",
        "id",
        "workflow_run_id",
        "step_name",
        "step_index",
        "kind",
        "status",
        "config",
        "context",
        "started_at",
        "created_at",
        "updated_at"
      )
      SELECT
        ${this.namespaceId}::text,
        gen_random_uuid(),
        ${params.workflowRunId}::text,
        ${params.stepName}::text,
        ${params.stepIndex ?? null}::integer,
        ${params.kind}::text,
        'running',
        ${jsonb(params.config)},
        ${jsonb(params.context)},
        NOW(),
        date_trunc('milliseconds', NOW()),
        NOW()
      FROM owned_workflow_run
      RETURNING ${STEP_ATTEMPT_SELECT}
    `);

    requireRow(stepAttempt, "create step attempt");

    return rowToStepAttempt(stepAttempt);
  }

  async setStepAttemptChildWorkflowRun(
    params: SetStepAttemptChildWorkflowRunParams,
  ): Promise<StepAttempt> {
    const [updated] = await this.rows<StepAttemptRow>(sql`
      WITH owned_workflow_run AS (
        ${this.runningWorkflowRunOwnedByWorkerForUpdate(params)}
      )
      UPDATE ${this.tables.stepAttempts} sa
      SET
        "child_workflow_run_namespace_id" = ${params.childWorkflowRunNamespaceId},
        "child_workflow_run_id" = ${params.childWorkflowRunId},
        "updated_at" = NOW()
      FROM owned_workflow_run
      WHERE sa."status" = 'running'
      AND ${this.stepAttemptByIdWhere(params)}
      RETURNING ${SA_STEP_ATTEMPT_SELECT}
    `);

    requireRow(updated, "set step attempt child workflow run");

    return rowToStepAttempt(updated);
  }

  async getStepAttempt(
    params: GetStepAttemptParams,
  ): Promise<StepAttempt | null> {
    const [stepAttempt] = await this.rows<StepAttemptRow>(sql`
      SELECT ${STEP_ATTEMPT_SELECT}
      FROM ${this.tables.stepAttempts}
      WHERE "namespace_id" = ${this.namespaceId}
      AND "id" = ${params.stepAttemptId}
      LIMIT 1
    `);
    return stepAttempt ? rowToStepAttempt(stepAttempt) : null;
  }

  async listStepAttempts(
    params: ListStepAttemptsParams,
  ): Promise<PaginatedResponse<StepAttempt>> {
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const { after, before } = params;
    const cursor = decodeListCursor(params);

    const conditions = [
      sql`"namespace_id" = ${this.namespaceId}`,
      sql`"workflow_run_id" = ${params.workflowRunId}`,
    ];
    if (cursor) {
      const op = sql.raw(after ? ">" : "<");
      conditions.push(
        sql`("created_at", COALESCE("step_index", -1), "id") ${op} (${timestamptz(cursor.createdAt)}, ${cursor.stepIndex ?? -1}::integer, ${cursor.id}::text)`,
      );
    }
    const order = before
      ? sql`ORDER BY "created_at" DESC, "step_index" DESC NULLS LAST, "id" DESC`
      : sql`ORDER BY "created_at" ASC, "step_index" ASC NULLS FIRST, "id" ASC`;

    const rows = await this.rows<StepAttemptRow>(sql`
      SELECT ${STEP_ATTEMPT_SELECT}
      FROM ${this.tables.stepAttempts}
      WHERE ${sql.join(conditions, sql` AND `)}
      ${order}
      LIMIT ${limit + 1}
    `);

    return buildPaginatedResponse(
      rows.map((row) => rowToStepAttempt(row)),
      limit,
      !!after,
      !!before,
    );
  }

  /**
   * Match a running workflow run currently owned by the given worker. Shared
   * by the workflow-run mutation queries which all fence on this same
   * condition.
   * @param params - Identifiers for the workflow run and owning worker
   * @returns Combined WHERE fragment for the unaliased workflow runs table
   */
  private runningWorkflowRunOwnedByWorkerWhere(
    params: Readonly<{ workflowRunId: string; workerId: string }>,
  ): SQL {
    return sql`
      "namespace_id" = ${this.namespaceId}
      AND "id" = ${params.workflowRunId}
      AND "status" = 'running'
      AND "worker_id" = ${params.workerId}
    `;
  }

  /**
   * Select and lock a running workflow run currently owned by the given
   * worker. Step-attempt mutations use this before touching their target row so
   * lease transfers serialize with all durable step writes.
   * @param params - Identifiers for the workflow run and owning worker
   * @returns Locking SELECT fragment for an owned workflow run
   */
  private runningWorkflowRunOwnedByWorkerForUpdate(
    params: Readonly<{ workflowRunId: string; workerId: string }>,
  ): SQL {
    return sql`
      SELECT 1
      FROM ${this.tables.workflowRuns}
      WHERE ${this.runningWorkflowRunOwnedByWorkerWhere(params)}
      FOR NO KEY UPDATE
    `;
  }

  /**
   * Match a step attempt by namespace, workflow run, and attempt id. The
   * caller is responsible for locking and validating the workflow lease.
   * @param params - Identifiers for the step attempt
   * @returns Combined WHERE fragment using the `sa` alias
   */
  private stepAttemptByIdWhere(
    params: Readonly<{
      workflowRunId: string;
      stepAttemptId: string;
    }>,
  ): SQL {
    return sql`
      sa."namespace_id" = ${this.namespaceId}
      AND sa."workflow_run_id" = ${params.workflowRunId}
      AND sa."id" = ${params.stepAttemptId}
    `;
  }

  async completeStepAttempt(
    params: CompleteStepAttemptParams,
  ): Promise<StepAttempt> {
    const [updated] = await this.rows<StepAttemptRow>(sql`
      WITH owned_workflow_run AS (
        ${this.runningWorkflowRunOwnedByWorkerForUpdate(params)}
      )
      UPDATE ${this.tables.stepAttempts} sa
      SET
        "status" = 'completed',
        "output" = CASE
          WHEN sa."status" = 'running' THEN ${jsonb(params.output)}
          ELSE sa."output"
        END,
        "error" = NULL,
        "finished_at" = COALESCE(sa."finished_at", NOW()),
        "updated_at" = CASE
          WHEN sa."status" = 'running' THEN NOW()
          ELSE sa."updated_at"
        END
      FROM owned_workflow_run
      WHERE sa."status" IN ('running', 'completed')
      AND ${this.stepAttemptByIdWhere(params)}
      RETURNING ${SA_STEP_ATTEMPT_SELECT}
    `);

    requireRow(updated, "mark step attempt completed");

    return rowToStepAttempt(updated);
  }

  async failStepAttempt(params: FailStepAttemptParams): Promise<StepAttempt> {
    const [updated] = await this.rows<StepAttemptRow>(sql`
      WITH owned_workflow_run AS (
        ${this.runningWorkflowRunOwnedByWorkerForUpdate(params)}
      )
      UPDATE ${this.tables.stepAttempts} sa
      SET
        "status" = 'failed',
        "output" = NULL,
        "error" = CASE
          WHEN sa."status" = 'running' THEN ${jsonb(params.error)}
          ELSE sa."error"
        END,
        "finished_at" = COALESCE(sa."finished_at", NOW()),
        "updated_at" = CASE
          WHEN sa."status" = 'running' THEN NOW()
          ELSE sa."updated_at"
        END
      FROM owned_workflow_run
      WHERE sa."status" IN ('running', 'failed')
      AND ${this.stepAttemptByIdWhere(params)}
      RETURNING ${SA_STEP_ATTEMPT_SELECT}
    `);

    requireRow(updated, "mark step attempt failed");

    return rowToStepAttempt(updated);
  }

  /**
   * Run a query and return its rows, whichever driver the database uses.
   * @param query - Query
   * @param executor - Database or transaction to run it on
   * @returns Rows
   */
  private async rows<Row>(query: SQL, executor?: Executor): Promise<Row[]> {
    const target = executor ?? this.db;
    const result = await target.execute(query);
    const rows = "rows" in result ? result.rows : result;
    // safety: callers name the columns their query selects or returns.
    return rows as Row[];
  }

  /**
   * Run a callback in a READ COMMITTED transaction. The callback must use the
   * transaction it is given, not `this.db`.
   * @param task - Work to run in the transaction
   * @returns The callback's result once the transaction commits
   */
  private async transaction<Result>(
    task: (tx: Executor) => Promise<Result>,
  ): Promise<Result> {
    return await this.db.transaction((tx) => task(tx), {
      isolationLevel: "read committed",
    });
  }
}

/**
 * A JSON value as a jsonb parameter, keeping null and undefined as SQL NULL.
 * @param value - Value to store
 * @returns SQL fragment
 */
function jsonb(value: unknown): SQL {
  return sql`${toJSON(value)}::jsonb`;
}

/**
 * A Date as a timestamptz parameter, keeping null as SQL NULL.
 * @param date - Date or null
 * @returns SQL fragment
 */
function timestamptz(date: Date | null): SQL {
  return sql`${date ? date.toISOString() : null}::timestamptz`;
}

/**
 * The lease expiry for a claim or heartbeat, computed by the database clock.
 * @param leaseDurationMs - Lease duration in milliseconds
 * @returns SQL fragment
 */
function leaseExpiry(leaseDurationMs: number): SQL {
  return sql`NOW() + ${leaseDurationMs}::float8 * INTERVAL '1 millisecond'`;
}
