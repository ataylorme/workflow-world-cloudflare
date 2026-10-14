export interface CloudflareWorldConfig {
	/** D1 database binding for the global index */
	db: D1Database;
	/** Durable Object namespace binding for WorkflowRunDO */
	runs: DurableObjectNamespace;
	/** Cloudflare Queue binding for message dispatch */
	queue: globalThis.Queue;
	/**
	 * Environment variables the World reads (currently `WORKFLOW_SEALED_LOG`).
	 * Defaults to `process.env`, which a Worker only has with the
	 * `nodejs_compat` flag; pass the Worker's `env` to avoid relying on it.
	 */
	env?: Record<string, string | undefined>;
}
