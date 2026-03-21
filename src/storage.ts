import { HookNotFoundError, WorkflowAPIError } from "@workflow/errors";
import type {
	CreateEventParams,
	CreateEventRequest,
	Event,
	EventResult,
	GetEventParams,
	GetHookParams,
	GetStepParams,
	GetWorkflowRunParams,
	Hook,
	ListEventsByCorrelationIdParams,
	ListEventsParams,
	ListHooksParams,
	ListWorkflowRunStepsParams,
	ListWorkflowRunsParams,
	PaginatedResponse,
	RunCreatedEventRequest,
	Step,
	StepWithoutData,
	Storage,
	WorkflowRun,
	WorkflowRunWithoutData,
} from "@workflow/world";
import { monotonicFactory } from "ulid";
import type { CloudflareWorldConfig } from "./config.js";
import { doFetch, getRunStub } from "./util.js";

const ulid = monotonicFactory();

/**
 * Creates the Storage implementation that routes requests to Durable Objects
 * and D1 for cross-run queries.
 */
export function createStorage(config: CloudflareWorldConfig): Storage {
	return {
		runs: createRunsStorage(config),
		events: createEventsStorage(config),
		hooks: createHooksStorage(config),
		steps: createStepsStorage(config),
	};
}

function createRunsStorage(config: CloudflareWorldConfig): Storage["runs"] {
	return {
		get: (async (id: string, params?: GetWorkflowRunParams) => {
			const stub = getRunStub(config.runs, id);
			const resolveData = params?.resolveData ?? "all";
			return doFetch<WorkflowRun | WorkflowRunWithoutData>(
				stub,
				`/run?resolveData=${resolveData}`,
			);
		}) as Storage["runs"]["get"],

		list: (async (params?: ListWorkflowRunsParams) => {
			const limit = params?.pagination?.limit ?? 20;
			const cursor = params?.pagination?.cursor;
			const resolveData = params?.resolveData ?? "all";

			// Query D1 index for the list
			let sql = "SELECT * FROM workflow_runs_index WHERE 1=1";
			const bindings: unknown[] = [];

			if (params?.workflowName) {
				sql += " AND workflow_name = ?";
				bindings.push(params.workflowName);
			}
			if (params?.status) {
				sql += " AND status = ?";
				bindings.push(params.status);
			}
			if (cursor) {
				sql += " AND run_id < ?";
				bindings.push(cursor);
			}
			sql += " ORDER BY run_id DESC";
			sql += ` LIMIT ${limit + 1}`;

			const result = await config.db
				.prepare(sql)
				.bind(...bindings)
				.all();

			const rows = result.results.slice(0, limit) as Record<string, unknown>[];
			const hasMore = result.results.length > limit;

			if (resolveData === "none") {
				const data = rows.map((row) => ({
					runId: row.run_id as string,
					workflowName: row.workflow_name as string,
					status: row.status as WorkflowRun["status"],
					deploymentId: row.deployment_id as string,
					specVersion: row.spec_version as number,
					input: undefined,
					output: undefined,
					error: undefined,
					createdAt: new Date(row.created_at as string),
					updatedAt: new Date(row.updated_at as string),
					startedAt: row.started_at
						? new Date(row.started_at as string)
						: undefined,
					completedAt: row.completed_at
						? new Date(row.completed_at as string)
						: undefined,
					expiredAt: row.expired_at
						? new Date(row.expired_at as string)
						: undefined,
				}));
				return {
					data,
					cursor: (rows.at(-1)?.run_id as string) ?? null,
					hasMore,
				};
			}

			// For resolveData='all', fan out to DOs for full data
			const data = await Promise.all(
				rows.map(async (row) => {
					const stub = getRunStub(config.runs, row.run_id as string);
					try {
						return await doFetch<WorkflowRun>(
							stub,
							`/run?resolveData=${resolveData}`,
						);
					} catch {
						// If DO is unavailable, fall back to index data
						return {
							runId: row.run_id as string,
							workflowName: row.workflow_name as string,
							status: row.status as string,
							deploymentId: row.deployment_id as string,
							specVersion: row.spec_version as number,
							input: undefined,
							output: undefined,
							createdAt: new Date(row.created_at as string),
							updatedAt: new Date(row.updated_at as string),
							startedAt: row.started_at
								? new Date(row.started_at as string)
								: undefined,
							completedAt: row.completed_at
								? new Date(row.completed_at as string)
								: undefined,
							expiredAt: row.expired_at
								? new Date(row.expired_at as string)
								: undefined,
						} as WorkflowRun;
					}
				}),
			);

			return {
				data,
				cursor: (rows.at(-1)?.run_id as string) ?? null,
				hasMore,
			};
		}) as Storage["runs"]["list"],
	};
}

function createEventsStorage(config: CloudflareWorldConfig): Storage["events"] {
	return {
		async create(
			runId: string | null,
			data: RunCreatedEventRequest | CreateEventRequest,
			params?: CreateEventParams,
		): Promise<EventResult> {
			// For run_created with null runId, generate the runId client-side so
			// the routing key and the stored runId map to the same DO instance.
			let effectiveRunId = runId;
			if (data.eventType === "run_created" && !runId) {
				effectiveRunId = `wrun_${ulid()}`;
			}

			const stub = getRunStub(config.runs, effectiveRunId as string);
			return doFetch<EventResult>(stub, "/events", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					runId: effectiveRunId,
					data,
					params,
				}),
			});
		},

		async get(
			runId: string,
			eventId: string,
			params?: GetEventParams,
		): Promise<Event> {
			const stub = getRunStub(config.runs, runId);
			const resolveData = params?.resolveData ?? "all";
			return doFetch<Event>(
				stub,
				`/events/${encodeURIComponent(eventId)}?resolveData=${resolveData}`,
			);
		},

		async list(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
			const stub = getRunStub(config.runs, params.runId);
			const qs = new URLSearchParams({
				limit: String(params.pagination?.limit ?? 100),
				sortOrder: params.pagination?.sortOrder ?? "asc",
				resolveData: params.resolveData ?? "all",
			});
			if (params.pagination?.cursor) {
				qs.set("cursor", params.pagination.cursor);
			}
			return doFetch<PaginatedResponse<Event>>(stub, `/events?${qs}`);
		},

		async listByCorrelationId(
			_params: ListEventsByCorrelationIdParams,
		): Promise<PaginatedResponse<Event>> {
			throw new WorkflowAPIError(
				"listByCorrelationId requires routing context. Use events.list with runId instead.",
				{ status: 501 },
			);
		},
	};
}

function createHooksStorage(config: CloudflareWorldConfig): Storage["hooks"] {
	return {
		async get(hookId: string, params?: GetHookParams): Promise<Hook> {
			const result = await config.db
				.prepare("SELECT run_id FROM workflow_hooks_index WHERE hook_id = ?")
				.bind(hookId)
				.first<{ run_id: string }>();

			if (!result) {
				throw new HookNotFoundError(hookId);
			}

			const stub = getRunStub(config.runs, result.run_id);
			const resolveData = params?.resolveData ?? "all";
			return doFetch<Hook>(
				stub,
				`/hooks/${encodeURIComponent(hookId)}?resolveData=${resolveData}`,
			);
		},

		async getByToken(token: string, params?: GetHookParams): Promise<Hook> {
			const result = await config.db
				.prepare(
					"SELECT hook_id, run_id FROM workflow_hooks_index WHERE token = ?",
				)
				.bind(token)
				.first<{ hook_id: string; run_id: string }>();

			if (!result) {
				throw new HookNotFoundError(token);
			}

			const stub = getRunStub(config.runs, result.run_id);
			const resolveData = params?.resolveData ?? "all";
			return doFetch<Hook>(
				stub,
				`/hooks/${encodeURIComponent(result.hook_id)}?resolveData=${resolveData}`,
			);
		},

		async list(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
			if (params.runId) {
				const stub = getRunStub(config.runs, params.runId);
				const qs = new URLSearchParams({
					limit: String(params.pagination?.limit ?? 100),
					sortOrder: params.pagination?.sortOrder ?? "asc",
					resolveData: params.resolveData ?? "all",
				});
				if (params.pagination?.cursor) {
					qs.set("cursor", params.pagination.cursor);
				}
				return doFetch<PaginatedResponse<Hook>>(stub, `/hooks?${qs}`);
			}

			// Without runId, query D1 index then fan out to DOs
			const limit = params.pagination?.limit ?? 100;
			const cursor = params.pagination?.cursor;
			const sortOrder = params.pagination?.sortOrder ?? "asc";

			let sql = "SELECT * FROM workflow_hooks_index WHERE 1=1";
			const bindings: unknown[] = [];
			if (cursor) {
				sql += sortOrder === "asc" ? " AND hook_id > ?" : " AND hook_id < ?";
				bindings.push(cursor);
			}
			sql += ` ORDER BY hook_id ${sortOrder === "asc" ? "ASC" : "DESC"}`;
			sql += ` LIMIT ${limit + 1}`;

			const result = await config.db
				.prepare(sql)
				.bind(...bindings)
				.all();

			const rows = result.results.slice(0, limit) as Record<string, unknown>[];
			const hasMore = result.results.length > limit;

			const resolveData = params.resolveData ?? "all";
			const data = await Promise.all(
				rows.map(async (row) => {
					const stub = getRunStub(config.runs, row.run_id as string);
					try {
						return await doFetch<Hook>(
							stub,
							`/hooks/${encodeURIComponent(row.hook_id as string)}?resolveData=${resolveData}`,
						);
					} catch {
						return {
							hookId: row.hook_id as string,
							runId: row.run_id as string,
							token: row.token as string,
							ownerId: row.owner_id as string,
							projectId: row.project_id as string,
							environment: row.environment as string,
							createdAt: new Date(row.created_at as string),
							isWebhook: row.is_webhook === 1,
							specVersion: row.spec_version as number,
						} as Hook;
					}
				}),
			);

			return {
				data,
				cursor: (rows.at(-1)?.hook_id as string) ?? null,
				hasMore,
			};
		},
	};
}

function createStepsStorage(config: CloudflareWorldConfig): Storage["steps"] {
	return {
		get: (async (
			runId: string | undefined,
			stepId: string,
			params?: GetStepParams,
		) => {
			if (!runId) {
				throw new WorkflowAPIError(
					"runId is required for Cloudflare world step lookups",
					{ status: 400 },
				);
			}
			const stub = getRunStub(config.runs, runId);
			const resolveData = params?.resolveData ?? "all";
			return doFetch<Step | StepWithoutData>(
				stub,
				`/steps/${encodeURIComponent(stepId)}?resolveData=${resolveData}`,
			);
		}) as Storage["steps"]["get"],

		list: (async (params: ListWorkflowRunStepsParams) => {
			const stub = getRunStub(config.runs, params.runId);
			const qs = new URLSearchParams({
				limit: String(params.pagination?.limit ?? 20),
				resolveData: params.resolveData ?? "all",
			});
			if (params.pagination?.cursor) {
				qs.set("cursor", params.pagination.cursor);
			}
			return doFetch<PaginatedResponse<Step | StepWithoutData>>(
				stub,
				`/steps?${qs}`,
			);
		}) as Storage["steps"]["list"],
	};
}
