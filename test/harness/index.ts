/**
 * A `WORKFLOW_TARGET_WORLD` entry for `@workflow/world-testing`: the real
 * World, Durable Object class and queue consumer, wired to the in-memory
 * bindings in `./fakes.ts`.
 */
import { type CloudflareWorld, createWorld as createCloudflareWorld, migrate, WorkflowRunDO } from "../../src/index.js";
import {
	FakeD1Database,
	FakeDurableObjectNamespace,
	FakeQueue,
} from "./fakes.js";

const KEY = Symbol.for("@ataylorme/workflow-world-cloudflare//test-harness");

export async function createWorld(): Promise<CloudflareWorld> {
	const g = globalThis as { [KEY]?: Promise<CloudflareWorld> };
	g[KEY] ??= (async () => {
		const db = new FakeD1Database();
		await migrate(db as unknown as D1Database);
		const runs = new FakeDurableObjectNamespace(
			WorkflowRunDO as never,
			{ WORKFLOW_DB: db },
		);
		const queue = new FakeQueue();
		const world = createCloudflareWorld({
			db: db as unknown as D1Database,
			runs: runs as unknown as DurableObjectNamespace,
			queue: queue as unknown as globalThis.Queue,
		});
		// Like a Worker's queue consumer forwarding to its own fetch handler:
		// the deliveries go to the test server's flow route.
		queue.consumer = (batch) =>
			world.handleQueueBatch(batch as never, {
				deliver: async (req) =>
					fetch(`http://localhost:${process.env.PORT}${new URL(req.url).pathname}`, {
						method: req.method,
						headers: req.headers,
						body: await req.text(),
					}),
			});
		return world;
	})();
	return g[KEY];
}
