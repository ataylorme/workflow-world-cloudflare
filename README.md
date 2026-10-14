# @ataylorme/workflow-world-cloudflare

A [Workflow DevKit](https://useworkflow.dev) world implementation for Cloudflare Workers, using **Durable Objects** for per-run state, **D1** for cross-run queries, and **Cloudflare Queues** for message dispatch.

## Architecture

- **Durable Objects**: Each workflow run maps to a single Durable Object instance. All run state (events, steps, hooks, waits, stream chunks) lives in the DO's transactional storage.
- **D1 (SQLite)**: A lightweight global index enables cross-run queries like `runs.list()` and `hooks.getByToken()`, and its unique constraint on hook tokens keeps a token owned by one run at a time.
- **Cloudflare Queues**: Workflow invocations (including queued step executions) are dispatched through Cloudflare Queues for at-least-once delivery.

This package implements the Workflow SDK v5 World contract and declares spec version 8 (`mintedSpecVersion()` from `@workflow/world` 5.x). It requires `workflow` 5.x.

Each run's event log lives in its Durable Object, which assigns slot-numbered event IDs (`evnt_00000000000000000000000001`, ...) in the same atomic write that stores the event, so the log is dense and ordered. Concurrent writers to one run are serialized by its Durable Object.

## Setup

### 1. Install

```bash
npm add @ataylorme/workflow-world-cloudflare
```

### 2. Configure bindings

Copy `wrangler.example.toml` and configure your D1 database ID:

```toml
[durable_objects]
bindings = [{ name = "WORKFLOW_RUNS", class_name = "WorkflowRunDO" }]

[[d1_databases]]
binding = "WORKFLOW_DB"
database_name = "workflow"
database_id = "<your-d1-database-id>"

[[queues.producers]]
binding = "WORKFLOW_QUEUE"
queue = "workflow-jobs"

[[queues.consumers]]
queue = "workflow-jobs"
max_batch_size = 10
max_retries = 3
```

### 3. Run D1 migration

```ts
import { migrate } from '@ataylorme/workflow-world-cloudflare/d1';

// Run once during setup or in a migration script
await migrate(env.WORKFLOW_DB);
```

### 4. Create the world

```ts
import { createWorld, WorkflowRunDO } from '@ataylorme/workflow-world-cloudflare';
import { setWorld } from 'workflow/runtime';

// Re-export the DO class so Cloudflare can instantiate it
export { WorkflowRunDO };

function world(env) {
  const w = createWorld({
    db: env.WORKFLOW_DB,
    runs: env.WORKFLOW_RUNS,
    queue: env.WORKFLOW_QUEUE,
    env, // lets WORKFLOW_SEALED_LOG be read without nodejs_compat
  });
  setWorld(w);
  return w;
}

const worker = {
  async fetch(request, env, ctx) {
    world(env);
    // Route requests, including /.well-known/workflow/v1/*, to the workflow runtime...
  },

  async queue(batch, env, ctx) {
    await world(env).handleQueueBatch(batch, {
      // Hands each message to the flow route (/.well-known/workflow/v1/flow).
      deliver: (req) => worker.fetch(req, env, ctx),
    });
  },
};

export default worker;
```

`deliver` hands each message to the flow route. Without it, the consumer calls the queue handler the runtime registered in the same isolate, which the runtime only does when that route first runs, so pass `deliver` unless the consumer isolate is known to have served the route.

## Upgrading from 1.0.0-beta1 (Workflow SDK v4)

Runs created by the v4 version of this World store ULID event IDs, which the v5 runtime cannot replay; the Durable Object refuses them with `RunNotSupportedError`. A single Worker deployment serves every run, so let in-flight runs finish on the old version before deploying this one. The `port` option is gone: the queue consumer no longer goes through `@workflow/world-local`.

## Limitations

- Optional World features not implemented: `capabilities` (hook token retention, hook resume dedup flag, hook force-claim, in-band fence, dynamic workflow code), `events.createBatch()`, `streams.createWriteSession()`, `runs.waitForTerminalStatus()` (`run.returnValue` polls), `analytics`, `getRuntimeDeadline()`. The runtime falls back for each. `createHook({ experimental_force: true })` fails at registration, since force-claim is not advertised.
- Cloudflare limits apply to payloads: a Durable Object storage value (128 KiB on KV-backed, 2 MB on SQLite-backed Durable Objects) holds each event, and a queue message body is capped at 128 KB.
- Cloudflare Queues has no deduplication, so `idempotencyKey` is not enforced; delivery is at least once and the runtime tolerates duplicates.
- `runs.list()` reads the D1 index, which the Durable Object updates after each run transition, so it can briefly lag.

## License

Apache-2.0
