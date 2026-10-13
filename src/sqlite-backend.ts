// Adapted from OpenWorkflow (https://github.com/openworkflowdev/openworkflow),
// Apache-2.0, see LICENSE-APACHE. Modified to run on Drizzle ORM.
import {
  fromJSON,
  Mutex,
  rowToStepAttempt,
  rowToWorkflowRun,
  rowToWorkflowSchedule,
  toJSON,
  type StepAttemptRow,
  type WorkflowRunRow,
  type WorkflowScheduleRow,
} from "./rows.js";
import type { DrizzleSqliteTables } from "./tables.js";
import { sql, type SQL } from "drizzle-orm";
import { randomUUID } from "node:crypto";
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
 * An async Drizzle SQLite database, such as `drizzle-orm/libsql`. Only the
 * methods the backend calls are listed, so any Drizzle version matches.
 */
export interface DrizzleSqliteDatabase {
  all<Row>(query: SQL): Promise<Row[]>;
  transaction<Result>(
    transaction: (tx: DrizzleSqliteDatabase) => Promise<Result>,
    config: { behavior: "immediate" },
  ): Promise<Result>;
}

/**
 * Options for {@link BackendDrizzleSqlite}.
 */
export interface BackendDrizzleSqliteOptions {
  /** Drizzle database. The backend never opens or closes connections. */
  db: DrizzleSqliteDatabase;
  /** Tables from `createSqliteTables()`, as exported from your schema. */
  tables: DrizzleSqliteTables;
  namespaceId?: string | undefined;
}

type Executor = Pick<DrizzleSqliteDatabase, "all">;

/**
 * Workflow storage on an existing Drizzle SQLite database.
 *
 * The queries mirror `BackendSqlite`. That backend relies on `node:sqlite`
 * being synchronous: within a process, its operations never interleave. Async
 * drivers don't give that guarantee, so every method here runs under an
 * in-process lock, and multi-statement writes that `BackendSqlite` runs in
 * `BEGIN IMMEDIATE` run in an immediate transaction. If other code writes to
 * the same database through other connections, serialize those writes at the
 * client.
 */
export class BackendDrizzleSqlite implements Backend {
  private readonly db: DrizzleSqliteDatabase;
  private readonly tables: DrizzleSqliteTables;
  private readonly namespaceId: string;
  private readonly lock = new Mutex();

  constructor(options: BackendDrizzleSqliteOptions) {
    this.db = options.db;
    this.tables = options.tables;
    this.namespaceId = options.namespaceId ?? DEFAULT_NAMESPACE_ID;
  }

  /**
   * Wait for in-flight writes. The database belongs to the caller, so the
   * connection stays open.
   * @returns Promise resolved once queued writes finish
   */
  async stop(): Promise<void> {
    await this.lock.run(() => Promise.resolve());
  }

  async upsertWorkflowSchedule(
    params: WorkflowScheduleDefinition,
  ): Promise<WorkflowSchedule> {
    const schedules = this.tables.workflowSchedules;
    const currentDate = new Date();

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowScheduleRow>(sql`
        INSERT INTO ${schedules}
          ("namespace_id", "id", "workflow_name", "input", "cron", "timezone", "next_run_at", "created_at", "updated_at")
        VALUES (
          ${this.namespaceId}, ${params.id}, ${params.workflowName}, ${toJSON(params.input)},
          ${params.cron}, ${params.timezone}, ${nextScheduleOccurrence(params, currentDate).toISOString()},
          ${currentDate.toISOString()}, ${currentDate.toISOString()}
        )
        ON CONFLICT ("namespace_id", "id") DO UPDATE SET
          "workflow_name" = excluded."workflow_name", "input" = excluded."input",
          "cron" = excluded."cron", "timezone" = excluded."timezone", "updated_at" = excluded."updated_at",
          "next_run_at" = CASE WHEN ${schedules}."next_run_at" IS NOT NULL
            AND (${schedules}."cron" <> excluded."cron" OR ${schedules}."timezone" <> excluded."timezone")
            THEN excluded."next_run_at" ELSE ${schedules}."next_run_at" END
        RETURNING *
      `);
      requireRow(row, "register schedule");
      return rowToWorkflowSchedule(row);
    });
  }

  async getWorkflowSchedule(
    params: WorkflowScheduleIdParams,
  ): Promise<WorkflowSchedule | null> {
    const [row] = await this.serialize(() =>
      this.rows<WorkflowScheduleRow>(sql`
      SELECT * FROM ${this.tables.workflowSchedules}
      WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
    `),
    );
    return row ? rowToWorkflowSchedule(row) : null;
  }

  async listWorkflowSchedules(
    params: PaginationOptions,
  ): Promise<PaginatedResponse<WorkflowSchedule>> {
    const cursor = decodeListCursor(params);
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const conditions = [sql`"namespace_id" = ${this.namespaceId}`];
    if (cursor) {
      const op = sql.raw(params.before ? "<" : ">");
      conditions.push(
        sql`("created_at", "id") ${op} (${cursor.createdAt.toISOString()}, ${cursor.id})`,
      );
    }
    const order = sql.raw(params.before ? "DESC" : "ASC");

    const rows = await this.serialize(() =>
      this.rows<WorkflowScheduleRow>(sql`
      SELECT * FROM ${this.tables.workflowSchedules}
      WHERE ${sql.join(conditions, sql` AND `)}
      ORDER BY "created_at" ${order}, "id" ${order}
      LIMIT ${limit + 1}
    `),
    );
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
      let [row] = await this.rows<WorkflowScheduleRow>(
        sql`
        SELECT * FROM ${schedules}
        WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
      `,
        tx,
      );
      requireRow(row, "change schedule pause state");
      if ((row.next_run_at === null) !== params.paused) {
        const currentDate = new Date();
        const next = params.paused
          ? null
          : nextScheduleOccurrence(row, currentDate);
        [row] = await this.rows<WorkflowScheduleRow>(
          sql`
          UPDATE ${schedules} SET "next_run_at" = ${toISO(next)}, "updated_at" = ${currentDate.toISOString()}
          WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
          RETURNING *
        `,
          tx,
        );
        requireRow(row, "read updated schedule");
      }
      return rowToWorkflowSchedule(row);
    });
  }

  async deleteWorkflowSchedule(
    params: WorkflowScheduleIdParams,
  ): Promise<void> {
    await this.serialize(() =>
      this.rows(sql`
      DELETE FROM ${this.tables.workflowSchedules}
      WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.workflowScheduleId}
    `),
    );
  }

  async enqueueScheduledRuns(
    params: EnqueueScheduledRunsParams,
  ): Promise<number> {
    const { workflowRuns, workflowSchedules } = this.tables;
    const before = params.before.toISOString();

    return await this.transaction(async (tx) => {
      const rows = await this.rows<
        Pick<
          WorkflowScheduleRow,
          "id" | "workflow_name" | "input" | "cron" | "timezone"
        >
      >(
        sql`
        SELECT "id", "workflow_name", "input", "cron", "timezone" FROM ${workflowSchedules}
        WHERE "namespace_id" = ${this.namespaceId} AND "next_run_at" <= ${before}
        ORDER BY "next_run_at", "id" LIMIT ${params.limit}
      `,
        tx,
      );
      for (const row of rows) {
        await this.rows(
          sql`
          UPDATE ${workflowSchedules}
          SET "next_run_at" = ${nextScheduleOccurrence(row, params.before).toISOString()}, "updated_at" = ${before}
          WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${row.id}
        `,
          tx,
        );
        const currentTime = now();
        await this.rows(
          sql`
          INSERT INTO ${workflowRuns}
            ("namespace_id", "id", "workflow_name", "status", "config", "input", "attempts",
             "workflow_schedule_id", "available_at", "created_at", "updated_at")
          VALUES (
            ${this.namespaceId}, ${randomUUID()}, ${row.workflow_name}, 'pending', '{}', ${row.input}, 0,
            ${row.id}, ${before}, ${currentTime}, ${currentTime}
          )
        `,
          tx,
        );
      }
      return rows.length;
    });
  }

  async createWorkflowRun(
    params: CreateWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const { workflowName, idempotencyKey } = params;

    if (idempotencyKey === null) {
      return await this.serialize(() =>
        this.insertWorkflowRun(this.db, params),
      );
    }

    return await this.transaction(async (tx) => {
      const existing = await this.getWorkflowRunByIdempotencyKey(
        tx,
        workflowName,
        idempotencyKey,
        new Date(Date.now() - DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS),
      );
      return existing ?? (await this.insertWorkflowRun(tx, params));
    });
  }

  async rerunWorkflowRun(
    request: RerunWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const { workflowRuns, stepAttempts } = this.tables;

    return await this.transaction(async (tx) => {
      const [source] = await this.rows<WorkflowRunRow>(
        sql`
          SELECT * FROM ${workflowRuns} WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${request.workflowRunId}
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
          SELECT "namespace_id",
            lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
              substr(hex(randomblob(2)), 2) || '-' || substr('89ab', (random() & 3) + 1, 1) ||
              substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))),
            ${run.id}, "step_name", "step_index", "kind", "status",
            "config", "context", "output", "child_workflow_run_namespace_id", "child_workflow_run_id",
            "started_at", "finished_at", "created_at", "updated_at"
          FROM ${stepAttempts}
          WHERE "namespace_id" = ${this.namespaceId} AND "workflow_run_id" = ${request.workflowRunId}
            AND "status" IN ('completed', 'succeeded')
            AND "step_index" < ${stepIndex}
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
    const currentTime = now();
    const availableAt = params.availableAt
      ? params.availableAt.toISOString()
      : currentTime;

    const [row] = await this.rows<WorkflowRunRow>(
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
        ${randomUUID()},
        ${params.workflowName},
        ${params.version},
        'pending',
        ${params.idempotencyKey},
        ${toJSON(params.config)},
        ${toJSON(params.context)},
        ${toJSON(params.input)},
        0,
        ${params.parentStepAttemptNamespaceId ?? null},
        ${params.parentStepAttemptId ?? null},
        ${availableAt},
        ${toISO(params.deadlineAt)},
        ${currentTime},
        ${currentTime}
      )
      RETURNING *
    `,
      executor,
    );
    requireRow(row, "create workflow run");

    return rowToWorkflowRun(row);
  }

  private async getWorkflowRunByIdempotencyKey(
    executor: Executor,
    workflowName: string,
    idempotencyKey: string,
    createdAt: Date,
  ): Promise<WorkflowRun | null> {
    const [row] = await this.rows<WorkflowRunRow>(
      sql`
      SELECT *
      FROM ${this.tables.workflowRuns}
      WHERE "namespace_id" = ${this.namespaceId}
        AND "workflow_name" = ${workflowName}
        AND "idempotency_key" = ${idempotencyKey}
        AND "created_at" >= ${createdAt.toISOString()}
      ORDER BY "created_at" ASC, "id" ASC
      LIMIT 1
    `,
      executor,
    );
    return row ? rowToWorkflowRun(row) : null;
  }

  async getWorkflowRun(
    params: GetWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    return await this.serialize(() =>
      this.findWorkflowRun(params.workflowRunId),
    );
  }

  private async findWorkflowRun(
    workflowRunId: string,
  ): Promise<WorkflowRun | null> {
    const [row] = await this.rows<WorkflowRunRow>(sql`
      SELECT *
      FROM ${this.tables.workflowRuns}
      WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${workflowRunId}
      LIMIT 1
    `);
    return row ? rowToWorkflowRun(row) : null;
  }

  async sendSignal(params: SendSignalParams): Promise<SendSignalResult> {
    const { workflowRuns, stepAttempts, workflowSignals } = this.tables;
    const currentTime = now();

    return await this.transaction(async (tx) => {
      if (params.idempotencyKey !== null) {
        const existing = await this.rows<{ workflow_run_id: string }>(
          sql`
          SELECT DISTINCT "workflow_run_id"
          FROM ${workflowSignals}
          WHERE "namespace_id" = ${this.namespaceId} AND "signal" = ${params.signal} AND "sender_idempotency_key" = ${params.idempotencyKey}
        `,
          tx,
        );
        if (existing.length > 0) {
          return { workflowRunIds: existing.map((r) => r.workflow_run_id) };
        }
      }

      // `->>` matches the step_attempts_signal_wait_idx expression
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
          AND sa."context" ->> '$.signal' = ${params.signal}
          AND wr."status" IN ('pending', 'running', 'sleeping')
      `,
        tx,
      );

      const deliveredRunIds = new Set<string>();
      for (const w of waiters) {
        const inserted = await this.rows(
          sql`
          INSERT OR IGNORE INTO ${workflowSignals} (
            "namespace_id", "id", "signal", "data",
            "sender_idempotency_key", "workflow_run_id",
            "step_attempt_id", "created_at"
          )
          VALUES (
            ${this.namespaceId}, ${randomUUID()}, ${params.signal}, ${toJSON(params.data)},
            ${params.idempotencyKey}, ${w.workflow_run_id}, ${w.id}, ${currentTime}
          )
          RETURNING "workflow_run_id"
        `,
          tx,
        );
        if (inserted.length > 0) {
          deliveredRunIds.add(w.workflow_run_id);
        }
      }

      // wake each waiting workflow run
      const runIds = [...deliveredRunIds];
      for (const runId of runIds) {
        await this.rows(
          sql`
          UPDATE ${workflowRuns}
          SET
            "available_at" = CASE
              WHEN "available_at" IS NULL OR "available_at" > ${currentTime} THEN ${currentTime}
              ELSE "available_at"
            END,
            "updated_at" = ${currentTime}
          WHERE "namespace_id" = ${this.namespaceId}
            AND "id" = ${runId}
            AND "status" IN ('pending', 'running', 'sleeping')
            AND "worker_id" IS NULL
        `,
          tx,
        );
      }

      return { workflowRunIds: runIds };
    });
  }

  async getSignalDelivery(
    params: GetSignalDeliveryParams,
  ): Promise<JsonValue | undefined> {
    const [row] = await this.serialize(() =>
      this.rows<{ data: string | null }>(sql`
      SELECT "data"
      FROM ${this.tables.workflowSignals}
      WHERE "namespace_id" = ${this.namespaceId} AND "step_attempt_id" = ${params.stepAttemptId}
      LIMIT 1
    `),
    );

    return row ? fromJSON(row.data) : undefined;
  }

  async claimWorkflowRun(
    params: ClaimWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    const { workflowRuns } = this.tables;
    const currentTime = now();
    const newAvailableAt = new Date(
      Date.parse(currentTime) + params.leaseDurationMs,
    ).toISOString();

    // SQLite has no SKIP LOCKED, so claims run in an immediate transaction
    return await this.transaction(async (tx) => {
      // 1. mark any deadline-expired workflow runs as failed
      await this.rows(
        sql`
        UPDATE ${workflowRuns}
        SET
          "status" = 'failed',
          "error" = ${toJSON({ message: "Workflow run deadline exceeded" })},
          "worker_id" = NULL,
          "available_at" = NULL,
          "finished_at" = ${currentTime},
          "updated_at" = ${currentTime}
        WHERE "namespace_id" = ${this.namespaceId}
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "deadline_at" IS NOT NULL
          AND "deadline_at" <= ${currentTime}
      `,
        tx,
      );

      // 2. find an available workflow run to claim
      const [candidate] = await this.rows<{ id: string }>(
        sql`
        SELECT "id"
        FROM ${workflowRuns}
        WHERE "namespace_id" = ${this.namespaceId}
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "available_at" <= ${currentTime}
          AND ("deadline_at" IS NULL OR "deadline_at" > ${currentTime})
        ORDER BY
          CASE WHEN "status" = 'pending' THEN 0 ELSE 1 END,
          "available_at",
          "created_at"
        LIMIT 1
      `,
        tx,
      );
      if (!candidate) return null;

      // 3. claim the workflow run
      const [row] = await this.rows<WorkflowRunRow>(
        sql`
        UPDATE ${workflowRuns}
        SET
          "status" = 'running',
          "attempts" = "attempts" + 1,
          "worker_id" = ${params.workerId},
          "available_at" = ${newAvailableAt},
          "started_at" = COALESCE("started_at", ${currentTime}),
          "updated_at" = ${currentTime}
        WHERE "id" = ${candidate.id}
          AND "namespace_id" = ${this.namespaceId}
        RETURNING *
      `,
        tx,
      );
      return row ? rowToWorkflowRun(row) : null;
    });
  }

  async extendWorkflowRunLease(
    params: ExtendWorkflowRunLeaseParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();
    const newAvailableAt = new Date(
      Date.parse(currentTime) + params.leaseDurationMs,
    ).toISOString();

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowRunRow>(sql`
        UPDATE ${this.tables.workflowRuns}
        SET
          "available_at" = ${newAvailableAt},
          "updated_at" = ${currentTime}
        WHERE ${this.runningWorkflowRunOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "extend lease for workflow run");
      return rowToWorkflowRun(row);
    });
  }

  async sleepWorkflowRun(params: SleepWorkflowRunParams): Promise<WorkflowRun> {
    const { workflowRuns, stepAttempts, workflowSignals } = this.tables;
    const currentTime = now();
    const resumeAt = toISO(params.availableAt);

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowRunRow>(sql`
        UPDATE ${workflowRuns}
        SET
          "status" = 'running',
          "available_at" = CASE
            WHEN (
              EXISTS (
                SELECT 1
                FROM ${stepAttempts} sa
                JOIN ${workflowRuns} child
                  ON child."namespace_id" = sa."child_workflow_run_namespace_id"
                  AND child."id" = sa."child_workflow_run_id"
                WHERE sa."namespace_id" = ${workflowRuns}."namespace_id"
                AND sa."workflow_run_id" = ${workflowRuns}."id"
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
                WHERE sa."namespace_id" = ${workflowRuns}."namespace_id"
                AND sa."workflow_run_id" = ${workflowRuns}."id"
                AND sa."kind" = 'signal-wait'
                AND sa."status" = 'running'
              )
            ) AND ${resumeAt} > ${currentTime} THEN ${currentTime}
            ELSE ${resumeAt}
          END,
          "worker_id" = NULL,
          "updated_at" = ${currentTime}
        WHERE "namespace_id" = ${this.namespaceId}
        AND "id" = ${params.workflowRunId}
        AND "status" NOT IN ('succeeded', 'completed', 'failed', 'canceled')
        AND "worker_id" = ${params.workerId}
        RETURNING *
      `);
      requireRow(row, "sleep workflow run");
      return rowToWorkflowRun(row);
    });
  }

  async completeWorkflowRun(
    params: CompleteWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowRunRow>(sql`
        UPDATE ${this.tables.workflowRuns}
        SET
          "status" = 'completed',
          "output" = ${toJSON(params.output)},
          "error" = NULL,
          "worker_id" = ${params.workerId},
          "available_at" = NULL,
          "finished_at" = ${currentTime},
          "updated_at" = ${currentTime}
        WHERE ${this.runningWorkflowRunOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "mark workflow run completed");

      const updated = rowToWorkflowRun(row);
      await this.wakeParentWorkflowRun(updated);
      return updated;
    });
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

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowRunRow>(sql`
        UPDATE ${this.tables.workflowRuns}
        SET
          "status" = ${failureUpdate.status},
          "available_at" = ${toISO(failureUpdate.availableAt)},
          "finished_at" = ${toISO(failureUpdate.finishedAt)},
          "error" = ${toJSON(failureUpdate.error)},
          "worker_id" = NULL,
          "started_at" = NULL,
          "updated_at" = ${currentTime.toISOString()}
        WHERE ${this.runningWorkflowRunOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "mark workflow run failed");

      const updated = rowToWorkflowRun(row);
      if (updated.status === "failed") {
        await this.wakeParentWorkflowRun(updated);
      }
      return updated;
    });
  }

  async rescheduleWorkflowRunAfterFailedStepAttempt(
    params: RescheduleWorkflowRunAfterFailedStepAttemptParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowRunRow>(sql`
        UPDATE ${this.tables.workflowRuns}
        SET
          "status" = 'pending',
          "available_at" = ${toISO(params.availableAt)},
          "finished_at" = NULL,
          "error" = ${toJSON(params.error)},
          "worker_id" = NULL,
          "started_at" = NULL,
          "updated_at" = ${currentTime}
        WHERE ${this.runningWorkflowRunOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "reschedule workflow run after failed step attempt");
      return rowToWorkflowRun(row);
    });
  }

  async cancelWorkflowRun(
    params: CancelWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<WorkflowRunRow>(sql`
        UPDATE ${this.tables.workflowRuns}
        SET
          "status" = 'canceled',
          "worker_id" = NULL,
          "available_at" = NULL,
          "finished_at" = ${currentTime},
          "updated_at" = ${currentTime}
        WHERE "namespace_id" = ${this.namespaceId}
        AND "id" = ${params.workflowRunId}
        AND "status" IN ('pending', 'running', 'sleeping')
        RETURNING *
      `);

      if (!row) {
        // workflow may already be in a terminal state
        const existing = await this.findWorkflowRun(params.workflowRunId);
        return resolveCancelWorkflowRunConflict(params.workflowRunId, existing);
      }

      const updated = rowToWorkflowRun(row);
      await this.wakeParentWorkflowRun(updated);
      return updated;
    });
  }

  /**
   * Match a running workflow run currently owned by the given worker.
   * @param params - Workflow run identity params
   * @returns WHERE fragment for the unaliased workflow runs table
   */
  private runningWorkflowRunOwnedWhere(
    params: Readonly<{ workflowRunId: string; workerId: string }>,
  ): SQL {
    return sql`
      "namespace_id" = ${this.namespaceId}
      AND "id" = ${params.workflowRunId}
      AND "status" = 'running'
      AND "worker_id" = ${params.workerId}`;
  }

  /**
   * Match a step attempt whose parent workflow run is running and held by the
   * given worker.
   * @param params - Step attempt identity params
   * @returns WHERE fragment for the unaliased step attempts table
   */
  private stepAttemptOwnedWhere(
    params: Readonly<{
      workflowRunId: string;
      stepAttemptId: string;
      workerId: string;
    }>,
  ): SQL {
    return sql`
      "namespace_id" = ${this.namespaceId}
      AND "workflow_run_id" = ${params.workflowRunId}
      AND "id" = ${params.stepAttemptId}
      AND EXISTS (
        SELECT 1
        FROM ${this.tables.workflowRuns} wr
        WHERE wr."namespace_id" = ${this.namespaceId}
        AND wr."id" = ${params.workflowRunId}
        AND wr."status" = 'running'
        AND wr."worker_id" = ${params.workerId}
      )`;
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

    const currentTime = now();
    await this.rows(sql`
      UPDATE ${this.tables.workflowRuns}
      SET
        "available_at" = CASE
          WHEN "available_at" IS NULL OR "available_at" > ${currentTime} THEN ${currentTime}
          ELSE "available_at"
        END,
        "updated_at" = ${currentTime}
      WHERE "namespace_id" = ${childWorkflowRun.parentStepAttemptNamespaceId}
      AND "id" = (
        SELECT "workflow_run_id"
        FROM ${this.tables.stepAttempts}
        WHERE "namespace_id" = ${childWorkflowRun.parentStepAttemptNamespaceId}
        AND "id" = ${childWorkflowRun.parentStepAttemptId}
        AND "kind" = 'workflow'
        AND "status" = 'running'
        AND "child_workflow_run_namespace_id" = ${childWorkflowRun.namespaceId}
        AND "child_workflow_run_id" = ${childWorkflowRun.id}
        LIMIT 1
      )
      AND (
        "status" = 'sleeping'
        OR ("status" = 'running' AND "worker_id" IS NULL)
      )
    `);
  }

  async countWorkflowRuns(): Promise<WorkflowRunCounts> {
    const rows = await this.serialize(() =>
      this.rows<{ status: string; count: number }>(sql`
      SELECT "status", COUNT(*) AS "count"
      FROM ${this.tables.workflowRuns}
      WHERE "namespace_id" = ${this.namespaceId}
      GROUP BY "status"
    `),
    );
    return toWorkflowRunCounts(rows);
  }

  async listWorkflowRuns(
    params: ListWorkflowRunsParams,
  ): Promise<PaginatedResponse<WorkflowRun>> {
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const { after, before } = params;
    const cursor = decodeListCursor(params);

    const conditions = [sql`"namespace_id" = ${this.namespaceId}`];
    if (params.status) {
      conditions.push(sql`"status" = ${params.status}`);
    }
    if (params.workflowName) {
      conditions.push(sql`"workflow_name" = ${params.workflowName}`);
    }
    if (cursor) {
      const op = sql.raw(before ? ">" : "<");
      conditions.push(
        sql`("created_at", "id") ${op} (${cursor.createdAt.toISOString()}, ${cursor.id})`,
      );
    }

    const order = sql.raw(before ? "ASC" : "DESC");
    const rows = await this.serialize(() =>
      this.rows<WorkflowRunRow>(sql`
      SELECT * FROM ${this.tables.workflowRuns}
      WHERE ${sql.join(conditions, sql` AND `)}
      ORDER BY "created_at" ${order}, "id" ${order}
      LIMIT ${limit + 1}
    `),
    );
    return buildPaginatedResponse(
      rows.map((row) => rowToWorkflowRun(row)),
      limit,
      !!after,
      !!before,
    );
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
      const op = sql.raw(before ? "<" : ">");
      conditions.push(
        sql`("created_at", COALESCE("step_index", -1), "id") ${op} (${cursor.createdAt.toISOString()}, ${cursor.stepIndex ?? -1}, ${cursor.id})`,
      );
    }

    const order = sql.raw(before ? "DESC" : "ASC");
    const rows = await this.serialize(() =>
      this.rows<StepAttemptRow>(sql`
      SELECT * FROM ${this.tables.stepAttempts}
      WHERE ${sql.join(conditions, sql` AND `)}
      ORDER BY "created_at" ${order}, "step_index" ${order}, "id" ${order}
      LIMIT ${limit + 1}
    `),
    );
    return buildPaginatedResponse(
      rows.map((row) => rowToStepAttempt(row)),
      limit,
      !!after,
      !!before,
    );
  }

  async createStepAttempt(
    params: CreateStepAttemptParams,
  ): Promise<StepAttempt> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<StepAttemptRow>(sql`
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
          ${this.namespaceId},
          ${randomUUID()},
          ${params.workflowRunId},
          ${params.stepName},
          ${params.stepIndex ?? null},
          ${params.kind},
          'running',
          ${toJSON(params.config)},
          ${toJSON(params.context)},
          ${currentTime},
          ${currentTime},
          ${currentTime}
        FROM ${this.tables.workflowRuns}
        WHERE ${this.runningWorkflowRunOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "create step attempt");
      return rowToStepAttempt(row);
    });
  }

  async setStepAttemptChildWorkflowRun(
    params: SetStepAttemptChildWorkflowRunParams,
  ): Promise<StepAttempt> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<StepAttemptRow>(sql`
        UPDATE ${this.tables.stepAttempts}
        SET
          "child_workflow_run_namespace_id" = ${params.childWorkflowRunNamespaceId},
          "child_workflow_run_id" = ${params.childWorkflowRunId},
          "updated_at" = ${currentTime}
        WHERE "status" = 'running'
        AND ${this.stepAttemptOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "set step attempt child workflow run");
      return rowToStepAttempt(row);
    });
  }

  async getStepAttempt(
    params: GetStepAttemptParams,
  ): Promise<StepAttempt | null> {
    const [row] = await this.serialize(() =>
      this.rows<StepAttemptRow>(sql`
      SELECT *
      FROM ${this.tables.stepAttempts}
      WHERE "namespace_id" = ${this.namespaceId} AND "id" = ${params.stepAttemptId}
      LIMIT 1
    `),
    );
    return row ? rowToStepAttempt(row) : null;
  }

  async completeStepAttempt(
    params: CompleteStepAttemptParams,
  ): Promise<StepAttempt> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<StepAttemptRow>(sql`
        UPDATE ${this.tables.stepAttempts}
        SET
          "status" = 'completed',
          "output" = CASE
            WHEN "status" = 'running' THEN ${toJSON(params.output)}
            ELSE "output"
          END,
          "error" = NULL,
          "finished_at" = COALESCE("finished_at", ${currentTime}),
          "updated_at" = CASE
            WHEN "status" = 'running' THEN ${currentTime}
            ELSE "updated_at"
          END
        WHERE "status" IN ('running', 'completed')
        AND ${this.stepAttemptOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "mark step attempt completed");
      return rowToStepAttempt(row);
    });
  }

  async failStepAttempt(params: FailStepAttemptParams): Promise<StepAttempt> {
    const currentTime = now();

    return await this.serialize(async () => {
      const [row] = await this.rows<StepAttemptRow>(sql`
        UPDATE ${this.tables.stepAttempts}
        SET
          "status" = 'failed',
          "output" = NULL,
          "error" = CASE
            WHEN "status" = 'running' THEN ${toJSON(params.error)}
            ELSE "error"
          END,
          "finished_at" = COALESCE("finished_at", ${currentTime}),
          "updated_at" = CASE
            WHEN "status" = 'running' THEN ${currentTime}
            ELSE "updated_at"
          END
        WHERE "status" IN ('running', 'failed')
        AND ${this.stepAttemptOwnedWhere(params)}
        RETURNING *
      `);
      requireRow(row, "mark step attempt failed");
      return rowToStepAttempt(row);
    });
  }

  /**
   * Run a query and return its rows.
   * @param query - Query
   * @param executor - Database or transaction to run it on
   * @returns Rows
   */
  private async rows<Row>(query: SQL, executor?: Executor): Promise<Row[]> {
    const target = executor ?? this.db;
    return await target.all<Row>(query);
  }

  /**
   * Run an operation under the in-process lock. Inside the task, call only
   * unlocked helpers; a nested `serialize()` would wait on itself.
   * @param task - Operation to run
   * @returns The operation's result
   */
  private async serialize<Result>(
    task: () => Promise<Result>,
  ): Promise<Result> {
    return await this.lock.run(task);
  }

  /**
   * Run a callback in an immediate transaction under the in-process lock. The
   * callback must use the transaction it is given, not `this.db`.
   * @param task - Work to run in the transaction
   * @returns The callback's result once the transaction commits
   */
  private async transaction<Result>(
    task: (tx: Executor) => Promise<Result>,
  ): Promise<Result> {
    return await this.lock.run(() =>
      this.db.transaction((tx) => task(tx), { behavior: "immediate" }),
    );
  }
}

/**
 * Current time as an ISO 8601 string, matching how `BackendSqlite` stores
 * timestamps.
 * @returns ISO 8601 timestamp
 */
function now(): string {
  return new Date().toISOString();
}

/**
 * Convert a nullable Date to an ISO 8601 string.
 * @param date - Date or null
 * @returns ISO 8601 string or null
 */
function toISO(date: Date | null): string | null {
  return date ? date.toISOString() : null;
}
