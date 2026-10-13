# openworkflow-drizzle

A [Drizzle ORM](https://orm.drizzle.team) backend for
[OpenWorkflow](https://openworkflow.dev). It stores workflows in a database your
application already manages with Drizzle, so it uses your connection and
driver, and the tables go into your own migrations. You don't need a second
connection pool or migration system.

It supports SQLite through async drivers such as libsql, and PostgreSQL through
any Drizzle driver (postgres.js, node-postgres, Neon, …).

> [!NOTE]
> The OpenWorkflow backend API is experimental before 1.0 and can change in a
> minor release. Each version of this package supports a narrow range of
> OpenWorkflow versions; see [Supported Versions](#supported-versions).

## Install

```bash
npm install openworkflow openworkflow-drizzle drizzle-orm
```

## Add the Tables to Your Schema

Create the tables with `createSqliteTables()` or `createPgTables()` and export
them from your Drizzle schema. drizzle-kit then includes them in the migrations
it generates for your app:

```ts
// db/schema.ts
import { createSqliteTables } from "openworkflow-drizzle";

export const {
  workflowRuns: owWorkflowRuns,
  stepAttempts: owStepAttempts,
  workflowSignals: owWorkflowSignals,
  workflowSchedules: owWorkflowSchedules,
} = createSqliteTables({ prefix: "ow_" });
```

Then generate and apply a migration as usual:

```bash
npx drizzle-kit generate
npx drizzle-kit migrate
```

The tables match OpenWorkflow's [SQLite](https://openworkflow.dev/docs/sqlite)
and [PostgreSQL](https://openworkflow.dev/docs/postgres) backends column for
column, so an existing OpenWorkflow database can move to this backend without a
data migration. The backend never creates or migrates tables itself.

## Setup

```ts
import * as schema from "./db/schema";
import { createClient } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { OpenWorkflow } from "openworkflow";
import { BackendDrizzleSqlite } from "openworkflow-drizzle";

const db = drizzle({ client: createClient({ url: "file:./app.db" }), schema });

const backend = new BackendDrizzleSqlite({
  db,
  tables: {
    workflowRuns: schema.owWorkflowRuns,
    stepAttempts: schema.owStepAttempts,
    workflowSignals: schema.owWorkflowSignals,
    workflowSchedules: schema.owWorkflowSchedules,
  },
});
const ow = new OpenWorkflow({ backend });
```

For PostgreSQL, use `createPgTables()` and `BackendDrizzlePg`:

```ts
// db/schema.ts
import { createPgTables } from "openworkflow-drizzle";

export const {
  workflowRuns: owWorkflowRuns,
  stepAttempts: owStepAttempts,
  workflowSignals: owWorkflowSignals,
  workflowSchedules: owWorkflowSchedules,
} = createPgTables({ prefix: "ow_" });
```

```ts
import * as schema from "./db/schema";
import { drizzle } from "drizzle-orm/node-postgres";
import { BackendDrizzlePg } from "openworkflow-drizzle";

const backend = new BackendDrizzlePg({
  db: drizzle(process.env.DATABASE_URL!),
  tables: {
    workflowRuns: schema.owWorkflowRuns,
    stepAttempts: schema.owStepAttempts,
    workflowSignals: schema.owWorkflowSignals,
    workflowSchedules: schema.owWorkflowSchedules,
  },
});
```

## Configuration Options

```ts
createSqliteTables({
  // Prefix for table, index, and constraint names (default: none)
  prefix: "ow_",
});

createPgTables({
  prefix: "ow_",
  // Postgres schema for the tables (default: the connection's search path)
  schema: "workflows",
});

new BackendDrizzleSqlite({
  db,
  tables,
  // Namespace for multi-tenant isolation (default: "default")
  namespaceId: "production",
});
```

Use a prefix when your database already has tables named `workflow_runs`,
`step_attempts`, `workflow_signals`, or `workflow_schedules`.

When you set `schema`, also export the returned `schema` object from your
Drizzle schema, so drizzle-kit creates the Postgres schema instead of dropping
it:

```ts
export const {
  workflowRuns: owWorkflowRuns,
  stepAttempts: owStepAttempts,
  workflowSignals: owWorkflowSignals,
  workflowSchedules: owWorkflowSchedules,
  schema: owSchema,
} = createPgTables({ schema: "workflows" });
```

## Connections

The backend uses the database you pass in and never opens or closes
connections. `backend.stop()` leaves your connection open; close it yourself
when your app shuts down.

## SQLite Concurrency

Within a process, the SQLite backend runs its operations one at a time, using
immediate transactions where several statements must apply together. This
matches OpenWorkflow's SQLite backend, whose driver is synchronous. If other
code in your app writes to the same database file through other connections,
serialize those writes in your app too.

## Indexes on Large Tables

OpenWorkflow's PostgreSQL backend builds indexes with
`CREATE INDEX CONCURRENTLY`. drizzle-kit runs migrations in a transaction, which
doesn't allow that, so the generated migration builds them normally. If you're
adding the tables to a database that's already under heavy load, you can edit
the generated migration.

## Known Differences

With drizzle-kit 0.31, the SQLite `step_attempts_signal_wait_idx` index uses
`"context" ->> '$.signal'` instead of `json_extract("context", '$.signal')`,
because drizzle-kit 0.31 can't generate an index on `json_extract`. The two
expressions return the same value, and the backend queries with the one its
index uses.

## Checking Your Driver

CI runs OpenWorkflow's backend test suite on libsql and postgres.js. To check
another driver, run the same suite in your own tests with
`openworkflow/testing` (requires [Vitest](https://vitest.dev)):

```ts
import { db, tables } from "./db";
import { randomUUID } from "node:crypto";
import { BackendDrizzlePg } from "openworkflow-drizzle";
import { testBackend } from "openworkflow/testing";

testBackend({
  setup: async () =>
    new BackendDrizzlePg({ db, tables, namespaceId: randomUUID() }),
  teardown: async (backend) => {
    await backend.stop();
  },
});
```

Each `setup()` should return a backend on its own namespace, so tests don't see
each other's runs.

## Supported Versions

| openworkflow-drizzle | openworkflow                      | drizzle-orm   |
| -------------------- | --------------------------------- | ------------- |
| 0.1.x                | the first release with `/backend` | `>=0.45.0 <2` |

## Development

Tests need PostgreSQL; set `OPENWORKFLOW_POSTGRES_URL` if it isn't on
`localhost:5432`.

```bash
npm ci
npm run link:openworkflow   # see below
npm run ci                  # format, lint, typecheck, build, test
```

Until an OpenWorkflow release includes `openworkflow/backend` and
`openworkflow/testing`, `npm run link:openworkflow` builds OpenWorkflow from
`../openworkflow` (or clones the `backend-api` branch of
[cliftonc/openworkflow](https://github.com/cliftonc/openworkflow)) and installs
it in place of the npm version. To test on the Drizzle 1.0 release candidate,
pass it the packages to install alongside:
`sh scripts/link-openworkflow.sh drizzle-orm@rc drizzle-kit@rc`.

## License

MIT, except for code adapted from OpenWorkflow (see below).

### Third-party code

The table definitions in `src/tables.ts`, the queries in
`src/sqlite-backend.ts` and `src/pg-backend.ts`, and the row mapping in
`src/rows.ts` are adapted from
[OpenWorkflow](https://github.com/openworkflowdev/openworkflow)'s SQLite and
PostgreSQL backends, which are licensed under the Apache License 2.0. They are
modified to run on Drizzle ORM. A copy of that license is in
[`LICENSE-APACHE`](LICENSE-APACHE).
