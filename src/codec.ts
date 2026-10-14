import {
	EntityConflictError,
	HookConflictError,
	HookNotFoundError,
	RunExpiredError,
	RunNotSupportedError,
	TooEarlyError,
	WorkflowRunNotFoundError,
	WorkflowWorldError,
} from "@workflow/errors";

/**
 * JSON encoding for requests and responses between the Worker and a run's
 * Durable Object.
 *
 * Payloads (`input`, `output`, `result`, `error`, `metadata`, `payload`) are
 * `Uint8Array`s in the v5 contract, and entity timestamps are `Date`s. Plain
 * `JSON.stringify` turns the former into `{"0":1,...}` objects and the latter
 * into strings, so both are tagged on the way out and restored on the way in.
 */
const U8_TAG = "__u8";
const DATE_TAG = "__date";

function bytesToBase64(bytes: Uint8Array): string {
	let binary = "";
	for (let i = 0; i < bytes.length; i += 0x8000) {
		binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
	}
	return btoa(binary);
}

function base64ToBytes(b64: string): Uint8Array {
	const binary = atob(b64);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return bytes;
}

function tag(value: unknown): unknown {
	if (value instanceof Uint8Array) return { [U8_TAG]: bytesToBase64(value) };
	if (value instanceof Date) return { [DATE_TAG]: value.toISOString() };
	if (Array.isArray(value)) return value.map(tag);
	if (value && typeof value === "object") {
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(value)) {
			if (v !== undefined) out[k] = tag(v);
		}
		return out;
	}
	return value;
}

function untag(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(untag);
	if (value && typeof value === "object") {
		const obj = value as Record<string, unknown>;
		const keys = Object.keys(obj);
		if (keys.length === 1 && typeof obj[U8_TAG] === "string") {
			return base64ToBytes(obj[U8_TAG] as string);
		}
		if (keys.length === 1 && typeof obj[DATE_TAG] === "string") {
			return new Date(obj[DATE_TAG] as string);
		}
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(obj)) out[k] = untag(v);
		return out;
	}
	return value;
}

export function encode(value: unknown): string {
	return JSON.stringify(tag(value));
}

export function decode<T>(text: string): T {
	return untag(JSON.parse(text)) as T;
}

/**
 * The fields of a thrown error that survive the hop from the Durable Object
 * back to the Worker. The runtime tells errors apart by `name` (every
 * `@workflow/errors` class has a static `is()` keyed on it), so the name and
 * the few fields the runtime reads are what has to cross.
 */
export interface ErrorEnvelope {
	name: string;
	message: string;
	status?: number;
	code?: string;
	retryAfter?: number;
	runId?: string;
	runStatus?: string;
	token?: string;
	conflictingRunId?: string;
	runSpecVersion?: number;
	worldSpecVersion?: number;
}

export function toErrorEnvelope(err: unknown): ErrorEnvelope {
	const e = (err ?? {}) as Record<string, unknown>;
	const pick = <K extends keyof ErrorEnvelope>(k: K) =>
		e[k] === undefined ? {} : { [k]: e[k] };
	return {
		name: typeof e.name === "string" ? e.name : "Error",
		message: typeof e.message === "string" ? e.message : String(err),
		...pick("status"),
		...pick("code"),
		...pick("retryAfter"),
		...pick("runId"),
		...pick("runStatus"),
		...pick("token"),
		...pick("conflictingRunId"),
		...pick("runSpecVersion"),
		...pick("worldSpecVersion"),
	};
}

export function statusForError(env: ErrorEnvelope): number {
	if (typeof env.status === "number") return env.status;
	switch (env.name) {
		case "WorkflowRunNotFoundError":
		case "HookNotFoundError":
			return 404;
		case "EntityConflictError":
		case "HookConflictError":
			return 409;
		case "RunExpiredError":
			return 410;
		case "TooEarlyError":
			return 425;
		default:
			return 500;
	}
}

/** Rebuild the `@workflow/errors` class the Durable Object threw. */
export function fromErrorEnvelope(env: ErrorEnvelope): Error {
	switch (env.name) {
		case "WorkflowRunNotFoundError":
			return new WorkflowRunNotFoundError(env.runId ?? "");
		case "HookNotFoundError":
			return new HookNotFoundError(env.token ?? "");
		case "HookConflictError":
			return new HookConflictError(env.token ?? "", env.conflictingRunId);
		case "EntityConflictError":
			return new EntityConflictError(env.message);
		case "RunExpiredError":
			return new RunExpiredError(
				env.message,
				env.runId,
				env.runStatus as never,
			);
		case "TooEarlyError":
			return new TooEarlyError(env.message, { retryAfter: env.retryAfter });
		case "RunNotSupportedError":
			return new RunNotSupportedError(
				env.runSpecVersion ?? 0,
				env.worldSpecVersion ?? 0,
			);
		default: {
			const error = new WorkflowWorldError(env.message, {
				status: env.status,
				code: env.code,
				retryAfter: env.retryAfter,
			});
			// Keep the original name so name-keyed checks (e.g.
			// `AttributeValidationError`) still match on this side.
			error.name = env.name;
			return error;
		}
	}
}
