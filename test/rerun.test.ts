import {
  BackendDrizzlePg,
  BackendDrizzleSqlite,
  createPgTables,
  createSqliteTables,
} from "../src/index.js";
import {
  createPgSchema,
  createSqliteSchema,
  ignoreNotice,
  POSTGRES_URL,
} from "./support.js";
import { createClient } from "@libsql/client";
import { sql } from "drizzle-orm";
import { drizzle as drizzleLibsql } from "drizzle-orm/libsql";
import { drizzle as drizzlePg } from "drizzle-orm/postgres-js";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { OpenWorkflow } from "openworkflow";
import {
  isTerminalStatus,
  type Backend,
  type WorkflowRun,
} from "openworkflow/backend";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

describe.each(["sqlite", "postgres"])("Drizzle reruns (%s)", (database) => {
  let createBackend: () => Backend;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    if (database === "sqlite") {
      const client = createClient({ url: ":memory:" });
      const db = drizzleLibsql({ client });
      const tables = createSqliteTables({ prefix: "ow_" });
      await createSqliteSchema(db, tables);
      createBackend = () =>
        new BackendDrizzleSqlite({ db, tables, namespaceId: randomUUID() });
      cleanup = () => {
        client.close();
        return Promise.resolve();
      };
    } else {
      const client = postgres(POSTGRES_URL, { onnotice: ignoreNotice });
      const db = drizzlePg({ client });
      const schema = `openworkflow_drizzle_rerun_${randomUUID().replaceAll("-", "")}`;
      const tables = createPgTables({ prefix: "ow_", schema });
      await createPgSchema(db, tables);
      createBackend = () =>
        new BackendDrizzlePg({ db, tables, namespaceId: randomUUID() });
      cleanup = async () => {
        await db.execute(sql.raw(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`));
        await client.end();
      };
    }
  });

  afterAll(async () => {
    await cleanup();
  });

  test("reruns from a step, copying earlier successful steps", async () => {
    const backend = createBackend();
    const client = new OpenWorkflow({ backend });
    const calls = { read: 0, write: 0 };
    let broken = true;
    const workflow = client.defineWorkflow<{ value: number }, number>(
      { name: "recover", version: "v1" },
      async ({ step, input }) => {
        const value = await step.run({ name: "read" }, () => {
          calls.read++;
          return input.value;
        });
        return await step.run(
          {
            name: "write",
            retryPolicy: { maximumAttempts: 2, initialInterval: "1ms" },
          },
          () => {
            calls.write++;
            if (broken) throw new Error("offline");
            return value;
          },
        );
      },
    );

    const original = await workflow.run({ value: 42 });
    const source = await finish(client, backend, original.workflowRun.id);
    expect(source.status).toBe("failed");
    const sourceHistory = await history(backend, source.id);
    const read = sourceHistory.find((step) => step.stepName === "read");
    assert.ok(read);

    broken = false;
    const rerun = await client.rerunWorkflowRun(source.id, {
      fromStep: "write",
    });
    expect(rerun).toMatchObject({
      input: { value: 42 },
      version: "v1",
      status: "pending",
      attempts: 0,
    });
    const copied = await history(backend, rerun.id);
    expect(copied).toEqual([
      { ...read, id: copied[0]?.id, workflowRunId: rerun.id },
    ]);
    expect(copied[0]?.id).not.toBe(read.id);

    await expect(finish(client, backend, rerun.id)).resolves.toMatchObject({
      status: "completed",
      output: 42,
    });
    expect(calls.read).toBe(1);
    expect(await history(backend, source.id)).toEqual(sourceHistory);
  });
});

/**
 * Tick a worker until the run reaches a terminal status.
 * @param client - OpenWorkflow client
 * @param backend - Backend
 * @param workflowRunId - Run to finish
 * @returns The finished run
 */
async function finish(
  client: OpenWorkflow,
  backend: Backend,
  workflowRunId: string,
): Promise<WorkflowRun> {
  const worker = client.newWorker();
  const deadline = performance.now() + 10_000;
  try {
    while (performance.now() < deadline) {
      await worker.tick();
      const run = await backend.getWorkflowRun({ workflowRunId });
      if (run && isTerminalStatus(run.status)) return run;
      await new Promise((resolve) => {
        setTimeout(resolve, 5);
      });
    }
    throw new Error(`Workflow run ${workflowRunId} did not finish`);
  } finally {
    await worker.stop();
  }
}

/**
 * List a run's step attempts.
 * @param backend - Backend
 * @param workflowRunId - Run id
 * @returns Step attempts
 */
async function history(backend: Backend, workflowRunId: string) {
  const result = await backend.listStepAttempts({ workflowRunId, limit: 1000 });
  return result.data;
}
