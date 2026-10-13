import type {
  JsonValue,
  StepAttempt,
  WorkflowRun,
  WorkflowSchedule,
} from "openworkflow/backend";

/**
 * A timestamp as selected by the Drizzle backends: an ISO 8601 string. SQLite
 * stores them that way; Postgres selects them that way because drivers
 * disagree on how they return `timestamptz`.
 */
type TimestampValue = string;

/**
 * A workflow_runs row as selected by the Drizzle backends. JSON columns are
 * selected as text so every driver returns them the same way.
 */
export interface WorkflowRunRow {
  namespace_id: string;
  id: string;
  workflow_name: string;
  version: string | null;
  status: string;
  idempotency_key: string | null;
  config: string;
  context: string | null;
  input: string | null;
  output: string | null;
  error: string | null;
  attempts: number;
  parent_step_attempt_namespace_id: string | null;
  parent_step_attempt_id: string | null;
  worker_id: string | null;
  available_at: TimestampValue | null;
  deadline_at: TimestampValue | null;
  started_at: TimestampValue | null;
  finished_at: TimestampValue | null;
  created_at: TimestampValue;
  updated_at: TimestampValue;
  workflow_schedule_id: string | null;
}

/**
 * A step_attempts row as selected by the Drizzle backends.
 */
export interface StepAttemptRow {
  namespace_id: string;
  id: string;
  workflow_run_id: string;
  step_name: string;
  step_index: number | null;
  kind: string;
  status: string;
  config: string;
  context: string | null;
  output: string | null;
  error: string | null;
  child_workflow_run_namespace_id: string | null;
  child_workflow_run_id: string | null;
  started_at: TimestampValue | null;
  finished_at: TimestampValue | null;
  created_at: TimestampValue;
  updated_at: TimestampValue;
}

/**
 * A workflow_schedules row as selected by the Drizzle backends.
 */
export interface WorkflowScheduleRow {
  namespace_id: string;
  id: string;
  workflow_name: string;
  input: string | null;
  cron: string;
  timezone: string;
  next_run_at: TimestampValue | null;
  created_at: TimestampValue;
  updated_at: TimestampValue;
}

export const WORKFLOW_RUN_COLUMNS = [
  "namespace_id",
  "id",
  "workflow_name",
  "version",
  "status",
  "idempotency_key",
  "config",
  "context",
  "input",
  "output",
  "error",
  "attempts",
  "parent_step_attempt_namespace_id",
  "parent_step_attempt_id",
  "worker_id",
  "available_at",
  "deadline_at",
  "started_at",
  "finished_at",
  "created_at",
  "updated_at",
  "workflow_schedule_id",
] as const;

export const STEP_ATTEMPT_COLUMNS = [
  "namespace_id",
  "id",
  "workflow_run_id",
  "step_name",
  "step_index",
  "kind",
  "status",
  "config",
  "context",
  "output",
  "error",
  "child_workflow_run_namespace_id",
  "child_workflow_run_id",
  "started_at",
  "finished_at",
  "created_at",
  "updated_at",
] as const;

export const WORKFLOW_SCHEDULE_COLUMNS = [
  "namespace_id",
  "id",
  "workflow_name",
  "input",
  "cron",
  "timezone",
  "next_run_at",
  "created_at",
  "updated_at",
] as const;

const JSON_COLUMNS = new Set(["config", "context", "input", "output", "error"]);
const TIMESTAMP_COLUMNS = new Set([
  "available_at",
  "deadline_at",
  "started_at",
  "finished_at",
  "next_run_at",
  "created_at",
  "updated_at",
]);

/**
 * Build a Postgres select list that returns JSON columns as text and
 * timestamps as UTC ISO 8601 strings (truncated to milliseconds, like the
 * drivers do), so rows decode the same way on every driver.
 * @param columns - Column names
 * @param alias - Optional table alias to qualify the columns with
 * @returns Select list SQL text (column names only, no user input)
 */
export function pgSelectList(
  columns: readonly string[],
  alias?: string,
): string {
  const qualifier = alias ? `${alias}.` : "";
  return columns
    .map((column) => {
      const ref = `${qualifier}"${column}"`;
      if (JSON_COLUMNS.has(column)) return `${ref}::text AS "${column}"`;
      if (TIMESTAMP_COLUMNS.has(column)) {
        return `to_char(${ref} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "${column}"`;
      }
      return ref;
    })
    .join(", ");
}

/**
 * Convert a selected row to a WorkflowRun.
 * @param row - Workflow run row
 * @returns Workflow run
 * @throws {Error} If required fields are missing
 */
export function rowToWorkflowRun(row: WorkflowRunRow): WorkflowRun {
  const { createdAt, updatedAt, config } = parseRequiredRowFields(row);

  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowName: row.workflow_name,
    workflowScheduleId: row.workflow_schedule_id,
    version: row.version,
    // safety: the status column is written by backend transitions using the domain status values.
    status: row.status as WorkflowRun["status"],
    idempotencyKey: row.idempotency_key,
    config,
    context: fromJSON(row.context),
    input: fromJSON(row.input),
    output: fromJSON(row.output),
    // safety: workflow failures are persisted from the serialized error contract.
    error: fromJSON(row.error) as WorkflowRun["error"],
    attempts: row.attempts,
    parentStepAttemptNamespaceId: row.parent_step_attempt_namespace_id,
    parentStepAttemptId: row.parent_step_attempt_id,
    workerId: row.worker_id,
    availableAt: toDate(row.available_at),
    deadlineAt: toDate(row.deadline_at),
    startedAt: toDate(row.started_at),
    finishedAt: toDate(row.finished_at),
    createdAt,
    updatedAt,
  };
}

/**
 * Convert a selected row to a StepAttempt.
 * @param row - Step attempt row
 * @returns Step attempt
 * @throws {Error} If required fields are missing
 */
export function rowToStepAttempt(row: StepAttemptRow): StepAttempt {
  const { createdAt, updatedAt, config } = parseRequiredRowFields(row);

  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowRunId: row.workflow_run_id,
    stepName: row.step_name,
    stepIndex: row.step_index,
    // safety: the kind column is written from CreateStepAttemptParams.kind.
    kind: row.kind as StepAttempt["kind"],
    // safety: the status column is written by backend transitions using the domain status values.
    status: row.status as StepAttempt["status"],
    config,
    // safety: step context is persisted from StepAttemptContext and decoded here with the same contract.
    context: fromJSON(row.context) as StepAttempt["context"],
    output: fromJSON(row.output),
    error: fromJSON(row.error),
    childWorkflowRunNamespaceId: row.child_workflow_run_namespace_id,
    childWorkflowRunId: row.child_workflow_run_id,
    startedAt: toDate(row.started_at),
    finishedAt: toDate(row.finished_at),
    createdAt,
    updatedAt,
  };
}

/**
 * Convert a selected row to a WorkflowSchedule.
 * @param row - Workflow schedule row
 * @returns Workflow schedule
 */
export function rowToWorkflowSchedule(
  row: WorkflowScheduleRow,
): WorkflowSchedule {
  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowName: row.workflow_name,
    input: fromJSON(row.input),
    cron: row.cron,
    timezone: row.timezone,
    nextRunAt: toDate(row.next_run_at),
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}

/**
 * Serialize a value as JSON text, keeping null and undefined as SQL NULL.
 * @param value - Value to store
 * @returns JSON text or null
 */
export function toJSON(value: unknown): string | null {
  return value === null || value === undefined ? null : JSON.stringify(value);
}

/**
 * Parse JSON text read from the database.
 * @param value - JSON text or null
 * @returns Parsed value
 */
export function fromJSON(value: string | null): JsonValue {
  // safety: JSON.parse returns only JSON values when no reviver is supplied.
  return value === null ? null : (JSON.parse(value) as JsonValue);
}

/**
 * Decode a timestamp selected by either Drizzle backend.
 * @param value - ISO 8601 string or null
 * @returns Date or null
 */
function toDate(value: TimestampValue | null): Date | null {
  return value === null ? null : new Date(value);
}

interface ParsedRowFields {
  createdAt: Date;
  updatedAt: Date;
  config: Exclude<JsonValue, null>;
}

/**
 * Parse and validate the `created_at`, `updated_at`, and `config` fields
 * shared by workflow run and step attempt rows.
 * @param row - Row with timestamp and config columns
 * @returns Parsed createdAt/updatedAt dates and decoded config
 * @throws {Error} If any required field is missing
 */
function parseRequiredRowFields(
  row: Readonly<{
    created_at: TimestampValue;
    updated_at: TimestampValue;
    config: string;
  }>,
): ParsedRowFields {
  const createdAt = toDate(row.created_at);
  const updatedAt = toDate(row.updated_at);
  const config = fromJSON(row.config);

  if (!createdAt) throw new Error("createdAt is required");
  if (!updatedAt) throw new Error("updatedAt is required");
  if (config === null) throw new Error("config is required");

  return { createdAt, updatedAt, config };
}

/**
 * Runs async tasks one at a time, in call order.
 */
export class Mutex {
  private tail: Promise<void> = Promise.resolve();

  /**
   * Run a task once every previously queued task has settled.
   * @param task - Task to run
   * @returns The task's result
   */
  async run<Result>(task: () => Promise<Result>): Promise<Result> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await task();
    } finally {
      release();
    }
  }
}
