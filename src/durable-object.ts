import {
	EntityConflictError,
	HookNotFoundError,
	RunExpiredError,
	RunNotSupportedError,
	TooEarlyError,
	WorkflowRunNotFoundError,
	WorkflowWorldError,
} from "@workflow/errors";
import type {
	CreateEventParams,
	Event,
	EventResult,
	EventsResolveData,
	Hook,
	PaginatedResponse,
	ResolveData,
	Step,
	StepWithoutData,
	StreamChunksResponse,
	StreamInfoResponse,
	Wait,
	WorkflowRun,
	WorkflowRunWithoutData,
} from "@workflow/world";
import {
	applyAttributeChanges,
	EventSchema,
	FIRST_EVENT_SLOT,
	HookSchema,
	isChildEntityCreationEvent,
	isHookEventRequiringExistence,
	isLegacySpecVersion,
	isStepEventType,
	isTerminalRunEventType,
	isTerminalStepStatus,
	isTerminalWorkflowRunStatus,
	requiresNewerWorld,
	SPEC_VERSION_CURRENT,
	StepSchema,
	slotToEventId,
	stripEventDataRefs,
	validateAttributeChanges,
	validateUlidTimestamp,
	WorkflowRunSchema,
} from "@workflow/world";
import { decode, encode, statusForError, toErrorEnvelope } from "./codec.js";
import {
	claimHookToken,
	deleteHookIndex,
	deleteHooksForRunIndex,
	type HookTokenClaim,
	upsertRunIndex,
} from "./d1.js";
import { toISOOrUndef } from "./util.js";

// ============================================================
// Data filtering helpers (strip data for resolveData='none')
// ============================================================

function filterRunData(
	run: WorkflowRun,
	resolveData: ResolveData,
): WorkflowRun | WorkflowRunWithoutData {
	if (resolveData === "none") {
		const { input: _, output: __, ...rest } = run;
		return { input: undefined, output: undefined, ...rest };
	}
	return run;
}

function filterStepData(
	step: Step,
	resolveData: ResolveData,
): Step | StepWithoutData {
	if (resolveData === "none") {
		const { input: _, output: __, ...rest } = step;
		return { input: undefined, output: undefined, ...rest };
	}
	return step;
}

function filterHookData(hook: Hook, resolveData: ResolveData): Hook {
	if (resolveData === "none" && "metadata" in hook) {
		const { metadata: _, ...rest } = hook;
		return { metadata: undefined, ...rest };
	}
	return hook;
}

/** `skip-step-inputs` is a read hint; anything other than 'none' returns data. */
function entityResolve(
	resolveData: EventsResolveData | undefined,
): ResolveData {
	return resolveData === "none" ? "none" : "all";
}

// ============================================================
// DO storage keys
// ============================================================

/**
 * Highest event slot committed in this run's log. Event `n` lives under
 * `KEY.event(slotToEventId(n))`; `slotToEventId` pads to a fixed width, so a
 * prefix listing returns events in slot order.
 */
const SEQ_KEY = "seq";

const KEY = {
	run: "run",
	event: (eventId: string) => `evt:${eventId}`,
	step: (id: string) => `step:${id}`,
	hook: (id: string) => `hook:${id}`,
	/** The eventId of a hook's `hook_created`, so a retry converges on it. */
	hookCreated: (id: string) => `hook_evt:${id}`,
	wait: (id: string) => `wait:${id}`,
	/** `(runId, resumeId)` claim for `hook_received` redelivery convergence. */
	resume: (resumeId: string) => `resume:${resumeId}`,
	/** Dedup for workflow-written `attr_set` events, keyed by correlation id. */
	attr: (correlationId: string) => `attr:${correlationId}`,
	streamChunk: (name: string, index: number) =>
		`strm:${name}:chnk:${String(index).padStart(12, "0")}`,
	streamMeta: (name: string) => `strm:${name}:meta`,
	streamIndex: "strm_idx",
} as const;

const DEFAULT_EVENTS_PAGE = 1000;
const MAX_EVENTS_PAGE = 10_000;

interface StreamMeta {
	count: number;
	done: boolean;
}

interface ResumeClaim {
	hookId: string;
	eventId: string;
	payloadDigest?: string;
}

/**
 * Writes made while processing one event, committed together.
 *
 * Every `put` and `delete` is issued in one synchronous burst at `commit()`
 * with no `await` between them, which a Durable Object commits as a single
 * atomic write (write coalescing). The event row, the slot counter and every
 * entity it changes therefore become durable together or not at all, and
 * reads through `get`/`list` see the staged state first.
 */
class Staged {
	private puts = new Map<string, unknown>();
	private dels = new Set<string>();

	constructor(private storage: DurableObjectStorage) {}

	async get<T>(key: string): Promise<T | undefined> {
		if (this.dels.has(key)) return undefined;
		if (this.puts.has(key)) return this.puts.get(key) as T;
		return this.storage.get<T>(key);
	}

	put(key: string, value: unknown): void {
		this.dels.delete(key);
		this.puts.set(key, value);
	}

	delete(key: string): void {
		this.puts.delete(key);
		this.dels.add(key);
	}

	async list<T>(prefix: string): Promise<Map<string, T>> {
		const stored = await this.storage.list<T>({ prefix });
		const merged = new Map<string, T>();
		for (const [k, v] of stored) {
			if (!this.dels.has(k)) merged.set(k, v);
		}
		for (const [k, v] of this.puts) {
			if (k.startsWith(prefix)) merged.set(k, v as T);
		}
		return merged;
	}

	async commit(): Promise<void> {
		const ops: Promise<unknown>[] = [];
		const puts = [...this.puts.entries()];
		for (let i = 0; i < puts.length; i += 128) {
			ops.push(this.storage.put(Object.fromEntries(puts.slice(i, i + 128))));
		}
		const dels = [...this.dels];
		for (let i = 0; i < dels.length; i += 128) {
			ops.push(this.storage.delete(dels.slice(i, i + 128)));
		}
		await Promise.all(ops);
	}
}

// ============================================================
// WorkflowRunDO — one instance per workflow run
// ============================================================

interface Env {
	WORKFLOW_DB: D1Database;
}

interface CreateEventBody {
	runId: string;
	data: Record<string, unknown> & { eventType: string };
	params?: Omit<CreateEventParams, "replayEventObserver">;
}

export class WorkflowRunDO implements DurableObject {
	private storage: DurableObjectStorage;
	private db: D1Database;
	/**
	 * Tail of the mutation chain. This instance is the only writer of its
	 * run's storage anywhere, so ordering mutations here orders every writer
	 * of the run: one `events.create` finishes (slot drawn and committed)
	 * before the next starts reading.
	 */
	private chain: Promise<unknown> = Promise.resolve();
	/** Readers tailing a stream, woken on every write and close. */
	private streamWaiters = new Map<string, Set<() => void>>();

	constructor(state: DurableObjectState, env: Env) {
		this.storage = state.storage;
		this.db = env.WORKFLOW_DB;
	}

	private serialize<T>(fn: () => Promise<T>): Promise<T> {
		const next = this.chain.then(fn, fn);
		this.chain = next.catch(() => {});
		return next;
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const method = request.method;
		const path = url.pathname;
		const q = url.searchParams;
		const body = async <T>() => decode<T>(await request.text());
		const json = (value: unknown) =>
			new Response(encode(value), {
				headers: { "content-type": "application/json" },
			});

		try {
			if (method === "POST" && path === "/events") {
				const { runId, data, params } = await body<CreateEventBody>();
				return json(
					await this.serialize(() => this.createEvent(runId, data, params)),
				);
			}

			if (method === "GET" && path === "/run") {
				return json(
					await this.getRun(
						q.get("runId") ?? "",
						(q.get("resolveData") as ResolveData) ?? "all",
					),
				);
			}

			if (method === "GET" && path === "/events") {
				return json(
					await this.listEvents({
						limit: Number.parseInt(q.get("limit") ?? "", 10) || undefined,
						cursor: q.get("cursor") ?? undefined,
						sortOrder: (q.get("sortOrder") as "asc" | "desc") ?? "asc",
						resolveData: (q.get("resolveData") as EventsResolveData) ?? "all",
						correlationId: q.get("correlationId") ?? undefined,
					}),
				);
			}

			if (method === "GET" && path.startsWith("/events/")) {
				const eventId = decodeURIComponent(path.slice("/events/".length));
				return json(
					await this.getEvent(
						eventId,
						(q.get("resolveData") as ResolveData) ?? "all",
					),
				);
			}

			if (method === "GET" && path === "/steps") {
				return json(
					await this.listSteps({
						limit: Number.parseInt(q.get("limit") ?? "20", 10),
						cursor: q.get("cursor") ?? undefined,
						resolveData: (q.get("resolveData") as ResolveData) ?? "all",
					}),
				);
			}

			if (method === "GET" && path.startsWith("/steps/")) {
				const stepId = decodeURIComponent(path.slice("/steps/".length));
				return json(
					await this.getStep(
						stepId,
						(q.get("resolveData") as ResolveData) ?? "all",
					),
				);
			}

			if (method === "GET" && path === "/hooks") {
				return json(
					await this.listHooks({
						limit: Number.parseInt(q.get("limit") ?? "100", 10),
						cursor: q.get("cursor") ?? undefined,
						sortOrder: (q.get("sortOrder") as "asc" | "desc") ?? "asc",
						resolveData: (q.get("resolveData") as ResolveData) ?? "all",
					}),
				);
			}

			if (method === "GET" && path.startsWith("/hooks/")) {
				const hookId = decodeURIComponent(path.slice("/hooks/".length));
				return json(
					await this.getHook(
						hookId,
						(q.get("resolveData") as ResolveData) ?? "all",
					),
				);
			}

			const streamMatch = path.match(/^\/streams\/([^/]+)\/([a-z-]+)$/);
			if (streamMatch) {
				const name = decodeURIComponent(streamMatch[1]);
				const action = streamMatch[2];
				if (method === "POST" && action === "write") {
					const { chunks } = await body<{ chunks: Uint8Array[] }>();
					await this.serialize(() => this.writeStreamChunks(name, chunks));
					return new Response(null, { status: 204 });
				}
				if (method === "POST" && action === "close") {
					await this.serialize(() => this.closeStream(name));
					return new Response(null, { status: 204 });
				}
				if (method === "GET" && action === "read") {
					const stream = await this.readStream(
						name,
						Number.parseInt(q.get("startIndex") ?? "0", 10) || 0,
					);
					return new Response(stream, {
						headers: { "content-type": "application/octet-stream" },
					});
				}
				if (method === "GET" && action === "chunks") {
					return json(
						await this.getStreamChunks(name, {
							limit: Number.parseInt(q.get("limit") ?? "100", 10),
							cursor: q.get("cursor") ?? undefined,
						}),
					);
				}
				if (method === "GET" && action === "info") {
					return json(await this.getStreamInfo(name));
				}
			}

			if (method === "GET" && path === "/streams") {
				return json(await this.listStreams());
			}

			return new Response("Not Found", { status: 404 });
		} catch (err: unknown) {
			const envelope = toErrorEnvelope(err);
			return new Response(encode({ error: envelope }), {
				status: statusForError(envelope),
				headers: { "content-type": "application/json" },
			});
		}
	}

	// ============================================================
	// Run reads
	// ============================================================

	private async getRun(
		runId: string,
		resolveData: ResolveData,
	): Promise<WorkflowRun | WorkflowRunWithoutData> {
		const run = await this.storage.get<WorkflowRun>(KEY.run);
		if (!run) {
			throw new WorkflowRunNotFoundError(runId);
		}
		return filterRunData(WorkflowRunSchema.parse(run), resolveData);
	}

	// ============================================================
	// Event processing
	// ============================================================

	/**
	 * Append one event and apply it to the run's entities.
	 *
	 * Runs inside `serialize()`, so the slot drawn here is `seq + 1` with no
	 * competing writer, and it is committed in the same atomic write as the
	 * event (see `Staged`). That makes the log dense by construction: a
	 * rejected write commits nothing, and nothing can be appended behind a
	 * slot a reader has already passed.
	 */
	private async createEvent(
		runId: string,
		data: CreateEventBody["data"],
		params?: CreateEventBody["params"],
	): Promise<EventResult> {
		const tx = new Staged(this.storage);
		const now = new Date();
		const resolveData = params?.resolveData ?? "all";
		const effectiveSpecVersion =
			(data.specVersion as number | undefined) ?? SPEC_VERSION_CURRENT;
		const correlationId = data.correlationId as string | undefined;

		if (data.eventType === "run_created") {
			const validationError = validateUlidTimestamp(runId, "wrun_");
			if (validationError) {
				throw new WorkflowWorldError(validationError, { status: 400 });
			}
		}

		let seq = (await tx.get<number>(SEQ_KEY)) ?? 0;
		/** Append an event at the next slot. */
		const append = (event: Omit<Event, "eventId">): Event => {
			seq += 1;
			const stored = EventSchema.parse({
				...event,
				eventId: slotToEventId(seq),
			});
			tx.put(KEY.event(stored.eventId), stored);
			tx.put(SEQ_KEY, seq);
			return stored;
		};

		let currentRun =
			data.eventType === "run_created"
				? undefined
				: await tx.get<WorkflowRun>(KEY.run);

		if (currentRun && (await this.storage.get(SEQ_KEY)) === undefined) {
			// A run written by the v4 version of this World: its events are
			// ULID-numbered, which the v5 runtime cannot replay. See README.
			throw new RunNotSupportedError(
				currentRun.specVersion ?? 0,
				SPEC_VERSION_CURRENT,
			);
		}

		// Resilient start: a run_started for a run whose run_created never
		// landed carries the creation data, so the run is created here, with
		// its run_created taking the earlier slot so it replays first.
		if (data.eventType === "run_started" && !currentRun && data.eventData) {
			const input = data.eventData as {
				deploymentId?: string;
				workflowName?: string;
				input?: unknown;
				executionContext?: Record<string, unknown>;
				attributes?: Record<string, string>;
				allowReservedAttributes?: true;
				encryptionPublicKey?: string;
			};
			if (
				input.deploymentId &&
				input.workflowName &&
				input.input !== undefined
			) {
				validateAttributeChanges(
					Object.entries(input.attributes ?? {}).map(([key, value]) => ({
						key,
						value,
					})),
					{ allowReservedAttributes: input.allowReservedAttributes === true },
				);
				currentRun = WorkflowRunSchema.parse({
					runId,
					deploymentId: input.deploymentId,
					workflowName: input.workflowName,
					specVersion: effectiveSpecVersion,
					executionContext: input.executionContext,
					input: input.input,
					attributes: input.attributes ?? {},
					encryptionPublicKey: input.encryptionPublicKey,
					status: "pending",
					createdAt: now,
					updatedAt: now,
				});
				tx.put(KEY.run, currentRun);
				append({
					eventType: "run_created",
					runId,
					createdAt: now,
					specVersion: effectiveSpecVersion,
					eventData: {
						deploymentId: input.deploymentId,
						workflowName: input.workflowName,
						input: input.input,
						executionContext: input.executionContext,
						attributes: input.attributes,
						allowReservedAttributes: input.allowReservedAttributes,
						encryptionPublicKey: input.encryptionPublicKey,
					},
				} as Omit<Event, "eventId">);
			}
		}

		if (data.eventType !== "run_created" && !currentRun) {
			throw new WorkflowRunNotFoundError(runId);
		}

		if (currentRun) {
			if (requiresNewerWorld(currentRun.specVersion)) {
				throw new RunNotSupportedError(
					currentRun.specVersion ?? 0,
					SPEC_VERSION_CURRENT,
				);
			}
			if (isLegacySpecVersion(currentRun.specVersion)) {
				return this.handleLegacyEvent(tx, runId, data, currentRun, append);
			}
		}

		// ============================================================
		// VALIDATION
		// ============================================================

		const createsChildEntity = isChildEntityCreationEvent(data as never);
		const lazyStepStart =
			createsChildEntity && data.eventType === "step_started";

		if (currentRun && isTerminalWorkflowRunStatus(currentRun.status)) {
			if (
				data.eventType === "run_cancelled" &&
				currentRun.status === "cancelled"
			) {
				const event = append({
					...data,
					runId,
					createdAt: now,
					specVersion: effectiveSpecVersion,
				} as Omit<Event, "eventId">);
				await tx.commit();
				return this.finish(runId, event, { run: currentRun }, params, seq);
			}
			if (data.eventType === "run_started") {
				throw new RunExpiredError(
					`Workflow run "${runId}" is already in terminal state "${currentRun.status}"`,
					runId,
					currentRun.status,
				);
			}
			if (isTerminalRunEventType(data.eventType)) {
				throw new EntityConflictError(
					`Cannot transition run from terminal state "${currentRun.status}"`,
				);
			}
			if (createsChildEntity) {
				throw new EntityConflictError(
					`Cannot create new entities on run in terminal state "${currentRun.status}"`,
				);
			}
			if (data.eventType === "attr_set") {
				throw new EntityConflictError(
					`Cannot set attributes on run in terminal state "${currentRun.status}"`,
				);
			}
		}

		let validatedStep: Step | undefined;
		if (
			isStepEventType(data.eventType) &&
			data.eventType !== "step_created" &&
			correlationId
		) {
			validatedStep = await tx.get<Step>(KEY.step(correlationId));
			if (!validatedStep && !lazyStepStart) {
				throw new WorkflowWorldError(`Step "${correlationId}" not found`, {
					status: 404,
				});
			}
			if (lazyStepStart && validatedStep) {
				// Another handler already created this step: it owns the body.
				throw new EntityConflictError(
					`Step "${correlationId}" already created`,
				);
			}
			if (validatedStep) {
				if (isTerminalStepStatus(validatedStep.status)) {
					throw new EntityConflictError(
						`Cannot modify step in terminal state "${validatedStep.status}"`,
					);
				}
				// A step_started on a finished run is a redelivery of a start some
				// earlier delivery already claimed: refuse it even when the step
				// still reads `running`, so the body does not run a second time
				// for an outcome nothing will read.
				if (
					currentRun &&
					isTerminalWorkflowRunStatus(currentRun.status) &&
					(validatedStep.status !== "running" ||
						data.eventType === "step_started")
				) {
					throw new RunExpiredError(
						`Cannot ${data.eventType === "step_started" ? "start" : "modify non-running"} step on run in terminal state "${currentRun.status}"`,
						runId,
						currentRun.status,
					);
				}
			}
		}

		if (isHookEventRequiringExistence(data.eventType) && correlationId) {
			if (data.eventType === "hook_received" && params?.resumeId) {
				const claim = await tx.get<ResumeClaim>(KEY.resume(params.resumeId));
				if (claim) {
					if (claim.hookId !== correlationId) {
						throw new EntityConflictError(
							`hook_received resumeId "${params.resumeId}" already recorded for a different hook`,
						);
					}
					if (
						params.resumePayloadDigest &&
						claim.payloadDigest &&
						claim.payloadDigest !== params.resumePayloadDigest
					) {
						throw new EntityConflictError(
							`hook_received resumeId "${params.resumeId}" already recorded with a different payload`,
						);
					}
					const committed = await tx.get<Event>(KEY.event(claim.eventId));
					if (committed) {
						return { event: stripEventDataRefs(committed, resolveData) };
					}
				}
			}
			const existingHook = await tx.get<Hook>(KEY.hook(correlationId));
			if (!existingHook) {
				throw new HookNotFoundError(correlationId);
			}
		}

		// ============================================================
		// Entity creation/updates
		// ============================================================

		let run: WorkflowRun | undefined;
		let step: Step | undefined;
		let hook: Hook | undefined;
		let wait: Wait | undefined;
		let stepCreatedLazily = false;
		let releaseClaim: HookTokenClaim | undefined;
		const terminalCleanup = async () => {
			await this.disposeAllHooksAndWaits(tx);
		};

		const eventRow = {
			...data,
			runId,
			createdAt: now,
			specVersion: effectiveSpecVersion,
			...(data.eventType === "hook_received" && params?.resumeId
				? { resumeId: params.resumeId }
				: {}),
		} as Record<string, unknown>;

		switch (data.eventType) {
			case "run_created": {
				if (await tx.get(KEY.run)) {
					throw new EntityConflictError(
						`Workflow run "${runId}" already exists`,
					);
				}
				const runData = data.eventData as {
					deploymentId: string;
					workflowName: string;
					input: unknown;
					executionContext?: Record<string, unknown>;
					attributes?: Record<string, string>;
					allowReservedAttributes?: true;
					encryptionPublicKey?: string;
					dynamicWorkflowCode?: unknown;
				};
				validateAttributeChanges(
					Object.entries(runData.attributes ?? {}).map(([key, value]) => ({
						key,
						value,
					})),
					{ allowReservedAttributes: runData.allowReservedAttributes === true },
				);
				run = WorkflowRunSchema.parse({
					runId,
					deploymentId: runData.deploymentId,
					workflowName: runData.workflowName,
					specVersion: effectiveSpecVersion,
					executionContext: runData.executionContext,
					input: runData.input,
					attributes: runData.attributes ?? {},
					encryptionPublicKey: runData.encryptionPublicKey,
					status: "pending",
					createdAt: now,
					updatedAt: now,
				});
				tx.put(KEY.run, run);
				const {
					dynamicWorkflowCode: _code,
					dynamicWorkflowCodeRef: _ref,
					...eventData
				} = data.eventData as Record<string, unknown>;
				eventRow.eventData = eventData;
				break;
			}

			case "run_started": {
				const existing = currentRun as WorkflowRun;
				if (existing.status === "running") {
					// Idempotent for concurrent invocations: no second event.
					const preload = params?.skipPreload
						? undefined
						: await this.readEvents({ limit: DEFAULT_EVENTS_PAGE }, tx);
					await tx.commit();
					if (!preload) return { run: existing };
					return {
						run: existing,
						events: preload.data.map((e) => stripEventDataRefs(e, resolveData)),
						cursor: preload.cursor,
						hasMore: preload.hasMore,
					};
				}
				run = WorkflowRunSchema.parse({
					...existing,
					status: "running",
					startedAt: existing.startedAt ?? now,
					updatedAt: now,
				});
				tx.put(KEY.run, run);
				delete eventRow.eventData;
				break;
			}

			case "run_completed": {
				const eventData = data.eventData as { output?: unknown };
				run = WorkflowRunSchema.parse({
					...currentRun,
					status: "completed",
					output: eventData.output,
					completedAt: now,
					updatedAt: now,
				});
				tx.put(KEY.run, run);
				await terminalCleanup();
				break;
			}

			case "run_failed": {
				const eventData = data.eventData as {
					error: unknown;
					errorCode?: string;
				};
				run = WorkflowRunSchema.parse({
					...currentRun,
					status: "failed",
					error: eventData.error,
					errorCode: eventData.errorCode,
					completedAt: now,
					updatedAt: now,
				});
				tx.put(KEY.run, run);
				await terminalCleanup();
				break;
			}

			case "run_cancelled": {
				run = WorkflowRunSchema.parse({
					...currentRun,
					status: "cancelled",
					completedAt: now,
					updatedAt: now,
				});
				tx.put(KEY.run, run);
				await terminalCleanup();
				break;
			}

			case "attr_set": {
				const eventData = data.eventData as {
					changes: Parameters<typeof applyAttributeChanges>[1];
					writer: { type: string };
					allowReservedAttributes?: true;
				};
				const fresh = currentRun as WorkflowRun;
				validateAttributeChanges(eventData.changes, {
					existingKeys: Object.keys(fresh.attributes ?? {}),
					allowReservedAttributes: eventData.allowReservedAttributes === true,
				});
				if (correlationId && eventData.writer.type === "workflow") {
					if (await tx.get(KEY.attr(correlationId))) {
						throw new EntityConflictError(
							`Attribute event "${correlationId}" already exists`,
						);
					}
					tx.put(KEY.attr(correlationId), true);
				}
				run = WorkflowRunSchema.parse({
					...fresh,
					attributes: applyAttributeChanges(
						fresh.attributes ?? {},
						eventData.changes,
					),
					updatedAt: now,
				});
				tx.put(KEY.run, run);
				break;
			}

			case "step_created": {
				const id = correlationId as string;
				if (await tx.get(KEY.step(id))) {
					throw new EntityConflictError(`Step "${id}" already created`);
				}
				const eventData = data.eventData as {
					stepName: string;
					input: unknown;
				};
				step = StepSchema.parse({
					runId,
					stepId: id,
					stepName: eventData.stepName,
					input: eventData.input,
					status: "pending",
					attempt: 0,
					specVersion: effectiveSpecVersion,
					createdAt: now,
					updatedAt: now,
				});
				tx.put(KEY.step(id), step);
				break;
			}

			case "step_started": {
				const id = correlationId as string;
				if (!validatedStep && lazyStepStart) {
					// Lazy start: create the step, and a synthetic step_created at
					// the slot below this step_started, so replay observes it.
					const lazy = data.eventData as { stepName: string; input: unknown };
					validatedStep = StepSchema.parse({
						runId,
						stepId: id,
						stepName: lazy.stepName,
						input: lazy.input,
						status: "pending",
						attempt: 0,
						specVersion: effectiveSpecVersion,
						createdAt: now,
						updatedAt: now,
					});
					append({
						eventType: "step_created",
						runId,
						correlationId: id,
						createdAt: now,
						specVersion: effectiveSpecVersion,
						eventData: { stepName: lazy.stepName, input: lazy.input },
					} as Omit<Event, "eventId">);
					stepCreatedLazily = true;
					const { input: _input, ...rest } = data.eventData as Record<
						string,
						unknown
					>;
					eventRow.eventData = rest;
				}
				const current = validatedStep as Step;
				if (current.retryAfter && current.retryAfter.getTime() > Date.now()) {
					throw new TooEarlyError(
						`Cannot start step "${id}": retryAfter timestamp has not been reached yet`,
						{
							retryAfter: Math.ceil(
								(current.retryAfter.getTime() - Date.now()) / 1000,
							),
						},
					);
				}
				step = StepSchema.parse({
					...current,
					status: "running",
					attempt: current.attempt + 1,
					startedAt: current.startedAt ?? now,
					retryAfter: undefined,
					updatedAt: now,
				});
				tx.put(KEY.step(id), step);
				break;
			}

			case "step_completed": {
				const eventData = data.eventData as { result?: unknown };
				step = StepSchema.parse({
					...validatedStep,
					status: "completed",
					output: eventData.result,
					completedAt: now,
					updatedAt: now,
				});
				tx.put(KEY.step(correlationId as string), step);
				break;
			}

			case "step_failed": {
				const eventData = data.eventData as { error: unknown };
				step = StepSchema.parse({
					...validatedStep,
					status: "failed",
					error: eventData.error,
					completedAt: now,
					updatedAt: now,
				});
				tx.put(KEY.step(correlationId as string), step);
				break;
			}

			case "step_retrying": {
				const eventData = data.eventData as {
					error: unknown;
					retryAfter?: Date;
				};
				step = StepSchema.parse({
					...validatedStep,
					status: "pending",
					error: eventData.error,
					retryAfter: eventData.retryAfter,
					updatedAt: now,
				});
				tx.put(KEY.step(correlationId as string), step);
				break;
			}

			case "hook_created": {
				const id = correlationId as string;
				const eventData = data.eventData as {
					token: string;
					metadata?: unknown;
					isWebhook?: boolean;
					isSystem?: boolean;
				};
				if (await tx.get(KEY.hook(id))) {
					throw new EntityConflictError(`Hook "${id}" already created`);
				}
				// Tokens are unique across runs, so the claim is settled by the
				// D1 unique constraint rather than inside this run's storage.
				const claim = await claimHookToken(this.db, {
					hookId: id,
					runId,
					token: eventData.token,
					createdAt: now.toISOString(),
					isWebhook: eventData.isWebhook ?? false,
					specVersion: effectiveSpecVersion,
				});
				if (claim.runId !== runId || claim.hookId !== id) {
					const conflict = append({
						eventType: "hook_conflict",
						runId,
						correlationId: id,
						createdAt: now,
						specVersion: effectiveSpecVersion,
						eventData: {
							token: eventData.token,
							conflictingRunId: claim.runId,
						},
					} as Omit<Event, "eventId">);
					await tx.commit();
					return this.finish(runId, conflict, {}, params, seq);
				}
				releaseClaim = claim;
				hook = HookSchema.parse({
					runId,
					hookId: id,
					token: eventData.token,
					metadata: eventData.metadata,
					ownerId: "",
					projectId: "",
					environment: "",
					specVersion: effectiveSpecVersion,
					isWebhook: eventData.isWebhook ?? false,
					isSystem: eventData.isSystem ?? false,
					createdAt: now,
				});
				tx.put(KEY.hook(id), hook);
				break;
			}

			case "hook_disposed": {
				const id = correlationId as string;
				tx.delete(KEY.hook(id));
				tx.delete(KEY.hookCreated(id));
				break;
			}

			case "hook_received":
				// Stored in the log; the hook entity has no payload field.
				break;

			case "wait_created": {
				const id = correlationId as string;
				if (await tx.get(KEY.wait(id))) {
					throw new EntityConflictError(`Wait "${id}" already exists`);
				}
				const eventData = data.eventData as { resumeAt?: Date };
				wait = {
					waitId: `${runId}-${id}`,
					runId,
					status: "waiting",
					resumeAt: eventData.resumeAt,
					createdAt: now,
					updatedAt: now,
					specVersion: effectiveSpecVersion,
				};
				tx.put(KEY.wait(id), wait);
				break;
			}

			case "wait_completed": {
				const id = correlationId as string;
				const existingWait = await tx.get<Wait>(KEY.wait(id));
				if (!existingWait) {
					throw new WorkflowWorldError(`Wait "${id}" not found`, {
						status: 404,
					});
				}
				if (existingWait.status === "completed") {
					throw new EntityConflictError(`Wait "${id}" already completed`);
				}
				wait = {
					...existingWait,
					status: "completed",
					completedAt: now,
					updatedAt: now,
				};
				tx.put(KEY.wait(id), wait);
				break;
			}

			default:
				throw new WorkflowWorldError(
					`Unsupported event type "${data.eventType}"`,
					{ status: 400 },
				);
		}

		const event = append(eventRow as Omit<Event, "eventId">);
		if (data.eventType === "hook_created") {
			tx.put(KEY.hookCreated(correlationId as string), event.eventId);
		}
		if (data.eventType === "hook_received" && params?.resumeId) {
			tx.put(KEY.resume(params.resumeId), {
				hookId: correlationId as string,
				eventId: event.eventId,
				payloadDigest: params.resumePayloadDigest,
			} satisfies ResumeClaim);
		}

		try {
			await tx.commit();
		} catch (err) {
			if (releaseClaim) {
				await deleteHookIndex(this.db, releaseClaim.hookId).catch(() => {});
			}
			throw err;
		}

		// D1 index side effects, after the commit they describe. Token releases
		// are awaited so the token is free for another run by the time this
		// response arrives. A failed release is logged and not retried, so the
		// stale index row keeps the token claimed by this run.
		const logIndexError = (err: unknown) =>
			console.error(
				"[workflow-world-cloudflare] hook index update failed",
				err,
			);
		if (data.eventType === "hook_disposed") {
			await deleteHookIndex(this.db, correlationId as string).catch(
				logIndexError,
			);
		}
		if (run && isTerminalWorkflowRunStatus(run.status)) {
			await deleteHooksForRunIndex(this.db, runId).catch(logIndexError);
		}
		if (run) this.updateRunIndex(run);

		const preload =
			data.eventType === "run_started" && !params?.skipPreload
				? await this.readEvents({ limit: DEFAULT_EVENTS_PAGE })
				: undefined;

		return this.finish(
			runId,
			event,
			{
				run: run
					? (filterRunData(run, entityResolve(resolveData)) as WorkflowRun)
					: undefined,
				step,
				hook,
				wait,
				...(stepCreatedLazily ? { stepCreated: true as const } : {}),
			},
			params,
			seq,
			preload,
		);
	}

	/**
	 * Attach the read-back halves of an `events.create` response: the
	 * `sinceCursor` delta, the `run_started` preload, or the bump-and-report
	 * span, in that order of precedence (they share `events`/`cursor`/
	 * `hasMore`, and each later one is a subset of the earlier).
	 */
	private async finish(
		_runId: string,
		event: Event,
		entities: Omit<EventResult, "event">,
		params: CreateEventBody["params"],
		committedSlot: number,
		preload?: PaginatedResponse<Event>,
	): Promise<EventResult> {
		const resolveData = params?.resolveData ?? "all";
		const strip = (events: Event[]) =>
			events.map((e) => stripEventDataRefs(e, resolveData));
		const result = {
			event: stripEventDataRefs(event, resolveData),
			...entities,
		} as EventResult;

		if (typeof params?.sinceCursor === "string") {
			const delta = await this.readEvents({
				cursor: params.sinceCursor,
				limit: DEFAULT_EVENTS_PAGE,
			});
			return {
				...result,
				events: strip(delta.data),
				cursor: delta.cursor,
				hasMore: delta.hasMore,
			};
		}
		if (preload) {
			return {
				...result,
				events: strip(preload.data),
				cursor: preload.cursor,
				hasMore: preload.hasMore,
			};
		}

		// Bump and report: the writer expected slot `eventCount + 1`. Every
		// event between that slot and the one this write landed on is one the
		// writer has not seen, so hand them back instead of rejecting.
		const askedFor = params?.eventCount;
		if (
			typeof askedFor === "number" &&
			askedFor >= FIRST_EVENT_SLOT - 1 &&
			committedSlot > askedFor + 1
		) {
			const skipped = await this.readEvents({
				cursor:
					askedFor >= FIRST_EVENT_SLOT ? slotToEventId(askedFor) : undefined,
				limit: committedSlot - askedFor - 1,
			});
			const events = skipped.data.filter((e) => e.eventId < event.eventId);
			return {
				...result,
				events: strip(events),
				// Not a page the writer has read to the end of, so it must not
				// advance the writer's read position.
				cursor: null,
				hasMore: false,
			};
		}
		return result;
	}

	private async handleLegacyEvent(
		tx: Staged,
		runId: string,
		data: CreateEventBody["data"],
		currentRun: WorkflowRun,
		append: (event: Omit<Event, "eventId">) => Event,
	): Promise<EventResult> {
		const now = new Date();
		switch (data.eventType) {
			case "run_cancelled": {
				const updated = WorkflowRunSchema.parse({
					...currentRun,
					status: "cancelled",
					completedAt: now,
					updatedAt: now,
				});
				tx.put(KEY.run, updated);
				await this.disposeAllHooksAndWaits(tx);
				await tx.commit();
				this.updateRunIndex(updated);
				return { run: updated };
			}
			case "wait_completed":
			case "hook_received": {
				const event = append({
					...data,
					runId,
					createdAt: now,
					specVersion: SPEC_VERSION_CURRENT,
				} as Omit<Event, "eventId">);
				await tx.commit();
				return { event };
			}
			default:
				throw new Error(
					`Event type '${data.eventType}' not supported for legacy runs ` +
						`(specVersion: ${currentRun.specVersion || "undefined"}). ` +
						"Please upgrade @workflow packages.",
				);
		}
	}

	private async disposeAllHooksAndWaits(tx: Staged): Promise<void> {
		for (const prefix of ["hook:", "hook_evt:", "wait:"]) {
			for (const key of (await tx.list(prefix)).keys()) {
				tx.delete(key);
			}
		}
	}

	private updateRunIndex(run: WorkflowRun): void {
		upsertRunIndex(this.db, {
			runId: run.runId,
			workflowName: run.workflowName,
			status: run.status,
			deploymentId: run.deploymentId,
			specVersion: run.specVersion,
			createdAt: run.createdAt.toISOString(),
			updatedAt: run.updatedAt.toISOString(),
			startedAt: toISOOrUndef(run.startedAt),
			completedAt: toISOOrUndef(run.completedAt),
			expiredAt: toISOOrUndef(run.expiredAt),
		}).catch((err) => {
			console.error("[workflow-world-cloudflare] run index update failed", err);
		});
	}

	// ============================================================
	// Event reads
	// ============================================================

	private async getEvent(
		eventId: string,
		resolveData: ResolveData,
	): Promise<Event> {
		const event = await this.storage.get<Event>(KEY.event(eventId));
		if (!event) {
			throw new WorkflowWorldError(`Event not found: ${eventId}`, {
				status: 404,
			});
		}
		return stripEventDataRefs(event, resolveData);
	}

	/**
	 * One page of the log in slot order. The cursor is the last eventId the
	 * caller has, and is exclusive.
	 */
	private async readEvents(
		opts: { cursor?: string; limit?: number; sortOrder?: "asc" | "desc" },
		tx?: Staged,
	): Promise<PaginatedResponse<Event>> {
		const limit = Math.min(opts.limit ?? DEFAULT_EVENTS_PAGE, MAX_EVENTS_PAGE);
		const desc = opts.sortOrder === "desc";
		const listed = tx
			? await tx.list<Event>("evt:")
			: await this.storage.list<Event>({
					prefix: "evt:",
					...(opts.cursor
						? desc
							? { end: KEY.event(opts.cursor) }
							: { startAfter: KEY.event(opts.cursor) }
						: {}),
					reverse: desc,
					limit: limit + 1,
				});
		let entries = [...listed.entries()]
			.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
			.map(([, v]) => v);
		if (desc) entries.reverse();
		if (tx && opts.cursor) {
			const c = opts.cursor;
			entries = entries.filter((e) => (desc ? e.eventId < c : e.eventId > c));
		}
		const data = entries.slice(0, limit);
		const hasMore = entries.length > limit;
		return { data, cursor: data.at(-1)?.eventId ?? null, hasMore };
	}

	private async listEvents(params: {
		limit?: number;
		cursor?: string;
		sortOrder: "asc" | "desc";
		resolveData: EventsResolveData;
		correlationId?: string;
	}): Promise<PaginatedResponse<Event>> {
		if (params.correlationId) {
			const all = await this.storage.list<Event>({ prefix: "evt:" });
			let entries = [...all.values()].filter(
				(e) => e.correlationId === params.correlationId,
			);
			if (params.sortOrder === "desc") entries.reverse();
			if (params.cursor) {
				const c = params.cursor;
				entries = entries.filter((e) =>
					params.sortOrder === "desc" ? e.eventId < c : e.eventId > c,
				);
			}
			const limit = params.limit ?? 100;
			const data = entries.slice(0, limit);
			return {
				data: data.map((e) => stripEventDataRefs(e, params.resolveData)),
				cursor: data.at(-1)?.eventId ?? null,
				hasMore: entries.length > limit,
			};
		}
		const page = await this.readEvents(params);
		return {
			...page,
			data: page.data.map((e) => stripEventDataRefs(e, params.resolveData)),
		};
	}

	// ============================================================
	// Step reads
	// ============================================================

	private async getStep(
		stepId: string,
		resolveData: ResolveData,
	): Promise<Step | StepWithoutData> {
		const step = await this.storage.get<Step>(KEY.step(stepId));
		if (!step) {
			throw new WorkflowWorldError(`Step not found: ${stepId}`, {
				status: 404,
			});
		}
		return filterStepData(StepSchema.parse(step), resolveData);
	}

	private async listSteps(params: {
		limit: number;
		cursor?: string;
		resolveData: ResolveData;
	}): Promise<PaginatedResponse<Step | StepWithoutData>> {
		const all = await this.storage.list<Step>({ prefix: "step:" });
		let entries = [...all.values()].sort((a, b) =>
			b.stepId > a.stepId ? 1 : -1,
		);
		if (params.cursor) {
			const c = params.cursor;
			entries = entries.filter((e) => e.stepId < c);
		}
		const data = entries.slice(0, params.limit);
		return {
			data: data.map((s) =>
				filterStepData(StepSchema.parse(s), params.resolveData),
			),
			cursor: data.at(-1)?.stepId ?? null,
			hasMore: entries.length > params.limit,
		};
	}

	// ============================================================
	// Hook reads
	// ============================================================

	private async getHook(
		hookId: string,
		resolveData: ResolveData,
	): Promise<Hook> {
		const hook = await this.storage.get<Hook>(KEY.hook(hookId));
		if (!hook) {
			throw new HookNotFoundError(hookId);
		}
		return filterHookData(HookSchema.parse(hook), resolveData);
	}

	private async listHooks(params: {
		limit: number;
		cursor?: string;
		sortOrder: "asc" | "desc";
		resolveData: ResolveData;
	}): Promise<PaginatedResponse<Hook>> {
		const all = await this.storage.list<Hook>({ prefix: "hook:" });
		let entries = [...all.values()];
		if (params.sortOrder === "desc") entries.reverse();
		if (params.cursor) {
			const idx = entries.findIndex((e) => e.hookId === params.cursor);
			if (idx >= 0) entries = entries.slice(idx + 1);
		}
		const data = entries.slice(0, params.limit);
		return {
			data: data.map((h) =>
				filterHookData(HookSchema.parse(h), params.resolveData),
			),
			cursor: data.at(-1)?.hookId ?? null,
			hasMore: entries.length > params.limit,
		};
	}

	// ============================================================
	// Streams
	// ============================================================

	private notifyStream(name: string): void {
		const waiters = this.streamWaiters.get(name);
		if (!waiters) return;
		this.streamWaiters.delete(name);
		for (const wake of waiters) wake();
	}

	private async writeStreamChunks(
		name: string,
		chunks: Uint8Array[],
	): Promise<void> {
		if (chunks.length === 0) return;
		const meta = (await this.storage.get<StreamMeta>(KEY.streamMeta(name))) ?? {
			count: 0,
			done: false,
		};
		const entries: Record<string, unknown> = {};
		for (const chunk of chunks) {
			entries[KEY.streamChunk(name, meta.count)] = chunk;
			meta.count += 1;
		}
		entries[KEY.streamMeta(name)] = meta;
		const streams = (await this.storage.get<string[]>(KEY.streamIndex)) ?? [];
		if (!streams.includes(name)) {
			entries[KEY.streamIndex] = [...streams, name];
		}
		for (let i = 0, keys = Object.keys(entries); i < keys.length; i += 128) {
			const slice = keys.slice(i, i + 128);
			await this.storage.put(
				Object.fromEntries(slice.map((k) => [k, entries[k]])),
			);
		}
		this.notifyStream(name);
	}

	private async closeStream(name: string): Promise<void> {
		const meta = (await this.storage.get<StreamMeta>(KEY.streamMeta(name))) ?? {
			count: 0,
			done: false,
		};
		const streams = (await this.storage.get<string[]>(KEY.streamIndex)) ?? [];
		await this.storage.put({
			[KEY.streamMeta(name)]: { ...meta, done: true },
			...(streams.includes(name)
				? {}
				: { [KEY.streamIndex]: [...streams, name] }),
		});
		this.notifyStream(name);
	}

	/**
	 * A live tail of the stream from `startIndex` (negative counts back from
	 * the current end). It delivers what is already written, then waits for
	 * further writes, and ends when the stream is closed.
	 */
	private async readStream(
		name: string,
		startIndex: number,
	): Promise<ReadableStream<Uint8Array>> {
		const initial = (await this.storage.get<StreamMeta>(
			KEY.streamMeta(name),
		)) ?? { count: 0, done: false };
		let next =
			startIndex < 0 ? Math.max(0, initial.count + startIndex) : startIndex;
		const waitForChange = () =>
			new Promise<void>((resolve) => {
				let waiters = this.streamWaiters.get(name);
				if (!waiters) {
					waiters = new Set();
					this.streamWaiters.set(name, waiters);
				}
				const wake = () => {
					clearTimeout(timer);
					resolve();
				};
				// Bounded so a missed wake-up costs a second, not a hang.
				const timer = setTimeout(() => {
					waiters?.delete(wake);
					resolve();
				}, 1000);
				waiters.add(wake);
			});

		return new ReadableStream<Uint8Array>({
			pull: async (controller) => {
				for (;;) {
					const meta = (await this.storage.get<StreamMeta>(
						KEY.streamMeta(name),
					)) ?? { count: 0, done: false };
					if (next < meta.count) {
						const end = Math.min(meta.count, next + 128);
						const listed = await this.storage.list<Uint8Array>({
							start: KEY.streamChunk(name, next),
							end: KEY.streamChunk(name, end),
						});
						for (const chunk of listed.values()) {
							controller.enqueue(chunk);
						}
						next = end;
						return;
					}
					if (meta.done) {
						controller.close();
						return;
					}
					await waitForChange();
				}
			},
		});
	}

	private async getStreamChunks(
		name: string,
		options: { limit: number; cursor?: string },
	): Promise<StreamChunksResponse> {
		const meta = (await this.storage.get<StreamMeta>(KEY.streamMeta(name))) ?? {
			count: 0,
			done: false,
		};
		const start = options.cursor ? Number.parseInt(options.cursor, 10) || 0 : 0;
		const end = Math.min(meta.count, start + Math.min(options.limit, 1000));
		const listed =
			end > start
				? await this.storage.list<Uint8Array>({
						start: KEY.streamChunk(name, start),
						end: KEY.streamChunk(name, end),
					})
				: new Map<string, Uint8Array>();
		const data = [...listed.values()].map((chunk, i) => ({
			index: start + i,
			data: chunk,
		}));
		const hasMore = end < meta.count;
		return {
			data,
			cursor: hasMore ? String(end) : null,
			hasMore,
			done: meta.done && !hasMore,
		};
	}

	private async getStreamInfo(name: string): Promise<StreamInfoResponse> {
		const meta = (await this.storage.get<StreamMeta>(KEY.streamMeta(name))) ?? {
			count: 0,
			done: false,
		};
		return { tailIndex: meta.count - 1, done: meta.done };
	}

	private async listStreams(): Promise<string[]> {
		return (await this.storage.get<string[]>(KEY.streamIndex)) ?? [];
	}
}
