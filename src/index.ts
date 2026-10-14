import { mintedSpecVersion, type World } from "@workflow/world";
import type { CloudflareWorldConfig } from "./config.js";
import { type CloudflareQueue, createQueue } from "./queue.js";
import { createStorage } from "./storage.js";
import { createStreamer } from "./streamer.js";

export type CloudflareWorld = World & {
	/** Process a batch of messages from the Cloudflare Queue consumer */
	handleQueueBatch: CloudflareQueue["handleQueueBatch"];
};

/**
 * Creates a Cloudflare Workers world implementation using:
 * - Durable Objects for per-run state (events, steps, hooks, waits, streams)
 * - D1 for cross-run index queries (runs.list, hooks.getByToken) and hook
 *   token uniqueness
 * - Cloudflare Queues for message dispatch
 */
export function createWorld(config: CloudflareWorldConfig): CloudflareWorld {
	const storage = createStorage(config);
	const queue = createQueue(config);
	const streamer = createStreamer(config);

	return {
		// Read per call, so `WORKFLOW_SEALED_LOG=0` takes effect without a
		// rebuild. A Worker has no `process.env` unless `nodejs_compat` is on.
		specVersion: mintedSpecVersion(
			config.env ??
				(globalThis as { process?: { env?: Record<string, string> } }).process
					?.env ??
				{},
		),
		...storage,
		...queue,
		...streamer,
	};
}

export type { CloudflareWorldConfig } from "./config.js";
export { migrate } from "./d1.js";
export { WorkflowRunDO } from "./durable-object.js";
export type {
	CloudflareQueue,
	HandleQueueBatchOptions,
	QueueMessageEnvelope,
} from "./queue.js";
