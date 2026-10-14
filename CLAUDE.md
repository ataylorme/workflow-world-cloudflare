# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run build        # Bundle with ncc + emit .d.ts (scripts/build.sh)
npm run typecheck    # Type-check without emitting
npm run check        # Biome lint + format check
npm run check:fix    # Biome auto-fix
npm run test         # Unit and World tests (vitest)
npm run test:conformance  # @workflow/world-testing suite against in-memory bindings (test/harness)
npm run dev          # Watch mode compilation
```

Run a single test file: `npx vitest run src/d1.test.ts`

## Architecture

This is a [Workflow DevKit](https://useworkflow.dev) world implementation for Cloudflare Workers, published as `@ataylorme/workflow-world-cloudflare`. It implements the `World` interface from `@workflow/world` (combining `Storage`, `Queue`, and `Streamer`) using three Cloudflare primitives:

- **Durable Objects** — `WorkflowRunDO` in `src/durable-object.ts` is the core. Each workflow run maps 1:1 to a DO instance. All per-run state (events, steps, hooks, waits, stream chunks) lives in DO transactional storage. The DO exposes an internal HTTP API (POST /events, GET /run, GET /steps, etc.) consumed by the storage and streamer layers, encoded with `src/codec.ts` so `Uint8Array` payloads, `Date`s and `@workflow/errors` classes survive the hop. Mutations run one at a time per DO; each event takes slot `seq + 1` (`slotToEventId`) in the same atomic write as the event and the entities it changes.

- **D1 (SQLite)** — `src/d1.ts` maintains a lightweight global index. D1 enables cross-run queries (`runs.list`, `hooks.getByToken`) that can't be served by a single DO, and its `UNIQUE` token column arbitrates hook tokens across runs (`claimHookToken`, called by the DO before it commits a `hook_created`). Run index updates are fire-and-forget after mutations.

- **Cloudflare Queues** — `src/queue.ts` sends message envelopes to Cloudflare Queues. `handleQueueBatch` delivers each one to the app's flow route through the caller's `deliver` function, or to the handler the runtime registered via `createQueueHandler`, and honors `{ timeoutSeconds }` answers and delays longer than Cloudflare's 12-hour cap.

**Request routing pattern**: `src/storage.ts` and `src/streamer.ts` are thin routing layers. They resolve which DO instance to talk to (via `getRunStub` from `src/util.ts`), then delegate via `doFetch`. For list operations without a runId, they query D1 first, then fan out to DOs.

The package has three export paths: `.` (main entry with `createWorld`), `./durable-object` (the DO class for re-export in wrangler config), and `./d1` (migration helper).

## Key Constraints

- The World implements the Workflow SDK v5 contract and declares `mintedSpecVersion()`. Event IDs must stay slot-numbered and dense; `test/world.test.ts` races concurrent appends and the conformance suite's `numbers events by position` checks it end to end.

## Tooling

- TypeScript strict mode, ES2024 target, NodeNext module resolution
- Biome v2 for linting/formatting (tab indentation)
- Vitest for tests
- Cloudflare Workers types via `@cloudflare/workers-types`
- `@vercel/ncc` bundles all runtime deps into single JS files per entry point; all deps are devDependencies
