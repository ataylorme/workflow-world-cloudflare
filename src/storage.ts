import { HookNotFoundError, WorkflowWorldError } from "@workflow/errors";
import { globalSingleton } from "@workflow/utils";
import type {
	CreateEventParams,
	CreateEventRequest,
	Event,
	EventListResponse,
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

const ids = globalSingleton(
	"@ataylorme/workflow-world-cloudflare//storage-ids",
	1,
	() => ({ ulid: monotonicFactory() }),
);

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

function indexRowToRun(row: Record<string, unknown>): WorkflowRunWithoutData {
	return {
		runId: row.run_id as string,
		workflowName: row.workflow_name as string,
		status: row.status as WorkflowRun["status"],
		deploymentId: row.deployment_id as string,
		specVersion: (row.spec_version as number | null) ?? undefined,
		attributes: {},
		input: undefined,
		output: undefined,
		error: undefined,
		createdAt: new Date(row.created_at as string),
		updatedAt: new Date(row.updated_at as string),
		startedAt: row.started_at ? new Date(row.started_at as string) : undefined,
		completedAt: row.completed_at
			? new Date(row.completed_at as string)
			: undefined,
		expiredAt: row.expired_at ? new Date(row.expired_at as string) : undefined,
	} as WorkflowRunWithoutData;
}

function createRunsStorage(config: CloudflareWorldConfig): Storage["runs"] {
	return {
		get: (async (id: string, params?: GetWorkflowRunParams) => {
			const stub = getRunStub(config.runs, id);
			const qs = new URLSearchParams({
				runId: id,
				resolveData: params?.resolveData ?? "all",
			});
			return doFetch<WorkflowRun | WorkflowRunWithoutData>(stub, `/run?${qs}`);
		}) as Storage["runs"]["get"],

		list: (async (params?: ListWorkflowRunsParams) => {
			const limit = params?.pagination?.limit ?? 20;
			const cursor = params?.pagination?.cursor;
			const resolveData = params?.resolveData ?? "all";

			let sql = "SELECT * FROM workflow_runs_index WHERE 1=1";
			const bindings: unknown[] = [];

			if (params?.workflowName) {
				sql += " AND workflow_name = ?";
				bindings.push(params.workflowName);
			}
			if (params?.status !== undefined) {
				const statuses = Array.isArray(params.status)
					? params.status
					: [params.status];
				if (statuses.length === 0) {
					// `status: []` matches nothing, like SQL `IN ()`.
					return { data: [], cursor: null, hasMore: false };
				}
				sql += ` AND status IN (${statuses.map(() => "?").join(", ")})`;
				bindings.push(...statuses);
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
			const nextCursor = (rows.at(-1)?.run_id as string) ?? null;

			if (resolveData === "none") {
				return { data: rows.map(indexRowToRun), cursor: nextCursor, hasMore };
			}

			// For resolveData='all', fan out to DOs for full data
			const data = await Promise.all(
				rows.map(async (row) => {
					const runId = row.run_id as string;
					try {
						return await doFetch<WorkflowRun>(
							getRunStub(config.runs, runId),
							`/run?${new URLSearchParams({ runId, resolveData })}`,
						);
					} catch {
						// If DO is unavailable, fall back to index data
						return indexRowToRun(row) as WorkflowRun;
					}
				}),
			);

			return { data, cursor: nextCursor, hasMore };
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
			const effectiveRunId =
				data.eventType === "run_created" && !runId
					? `wrun_${ids.ulid()}`
					: runId;
			if (!effectiveRunId) {
				throw new WorkflowWorldError(
					"runId is required for non-run_created events",
					{ status: 400 },
				);
			}
			// The observer is an in-process callback and cannot cross into the
			// Durable Object. It is optional for a World to call it.
			const { replayEventObserver: _observer, ...wireParams } = params ?? {};
			return doFetch<EventResult>(
				getRunStub(config.runs, effectiveRunId),
				"/events",
				{ body: { runId: effectiveRunId, data, params: wireParams } },
			);
		},

		async get(
			runId: string,
			eventId: string,
			params?: GetEventParams,
		): Promise<Event> {
			const resolveData = params?.resolveData ?? "all";
			return doFetch<Event>(
				getRunStub(config.runs, runId),
				`/events/${encodeURIComponent(eventId)}?resolveData=${resolveData}`,
			);
		},

		async list(params: ListEventsParams): Promise<EventListResponse> {
			const qs = new URLSearchParams({
				sortOrder: params.pagination?.sortOrder ?? "asc",
				resolveData: params.resolveData ?? "all",
			});
			if (params.pagination?.limit) {
				qs.set("limit", String(params.pagination.limit));
			}
			if (params.pagination?.cursor) {
				qs.set("cursor", params.pagination.cursor);
			}
			return doFetch<EventListResponse>(
				getRunStub(config.runs, params.runId),
				`/events?${qs}`,
			);
		},

		async listByCorrelationId(
			params: ListEventsByCorrelationIdParams,
		): Promise<PaginatedResponse<Event>> {
			const qs = new URLSearchParams({
				correlationId: params.correlationId,
				limit: String(params.pagination?.limit ?? 100),
				sortOrder: params.pagination?.sortOrder ?? "asc",
				resolveData: params.resolveData ?? "all",
			});
			if (params.pagination?.cursor) {
				qs.set("cursor", params.pagination.cursor);
			}
			return doFetch<PaginatedResponse<Event>>(
				getRunStub(config.runs, params.runId),
				`/events?${qs}`,
			);
		},
	};
}

function createHooksStorage(config: CloudflareWorldConfig): Storage["hooks"] {
	const fetchHook = (runId: string, hookId: string, params?: GetHookParams) =>
		doFetch<Hook>(
			getRunStub(config.runs, runId),
			`/hooks/${encodeURIComponent(hookId)}?resolveData=${params?.resolveData ?? "all"}`,
		);

	return {
		async get(hookId: string, params?: GetHookParams): Promise<Hook> {
			const result = await config.db
				.prepare("SELECT run_id FROM workflow_hooks_index WHERE hook_id = ?")
				.bind(hookId)
				.first<{ run_id: string }>();
			if (!result) {
				throw new HookNotFoundError(hookId);
			}
			return fetchHook(result.run_id, hookId, params);
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
			return fetchHook(result.run_id, result.hook_id, params);
		},

		async list(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
			if (params.runId) {
				const qs = new URLSearchParams({
					limit: String(params.pagination?.limit ?? 100),
					sortOrder: params.pagination?.sortOrder ?? "asc",
					resolveData: params.resolveData ?? "all",
				});
				if (params.pagination?.cursor) {
					qs.set("cursor", params.pagination.cursor);
				}
				return doFetch<PaginatedResponse<Hook>>(
					getRunStub(config.runs, params.runId),
					`/hooks?${qs}`,
				);
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

			const data = await Promise.all(
				rows.map(async (row) => {
					try {
						return await fetchHook(
							row.run_id as string,
							row.hook_id as string,
							params,
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
		get: (async (runId: string, stepId: string, params?: GetStepParams) => {
			const resolveData = params?.resolveData ?? "all";
			return doFetch<Step | StepWithoutData>(
				getRunStub(config.runs, runId),
				`/steps/${encodeURIComponent(stepId)}?resolveData=${resolveData}`,
			);
		}) as Storage["steps"]["get"],

		list: (async (params: ListWorkflowRunStepsParams) => {
			const qs = new URLSearchParams({
				limit: String(params.pagination?.limit ?? 20),
				resolveData: params.resolveData ?? "all",
			});
			if (params.pagination?.cursor) {
				qs.set("cursor", params.pagination.cursor);
			}
			return doFetch<PaginatedResponse<Step | StepWithoutData>>(
				getRunStub(config.runs, params.runId),
				`/steps?${qs}`,
			);
		}) as Storage["steps"]["list"],
	};
}
