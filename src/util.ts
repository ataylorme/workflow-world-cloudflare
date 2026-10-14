import {
	decode,
	type ErrorEnvelope,
	encode,
	fromErrorEnvelope,
} from "./codec.js";

/**
 * Get a Durable Object stub for a given runId.
 */
export function getRunStub(
	namespace: DurableObjectNamespace,
	runId: string,
): DurableObjectStub {
	const id = namespace.idFromName(runId);
	return namespace.get(id);
}

/**
 * Make a request to a run's Durable Object and decode the response. Errors
 * the Durable Object threw are rebuilt as the `@workflow/errors` class they
 * started as, because the runtime branches on that class.
 */
export async function doFetch<T>(
	stub: DurableObjectStub,
	path: string,
	init?: { method?: string; body?: unknown },
): Promise<T> {
	const res = await stub.fetch(`http://do${path}`, {
		method: init?.method ?? (init?.body === undefined ? "GET" : "POST"),
		headers: { "content-type": "application/json" },
		body: init?.body === undefined ? undefined : encode(init.body),
	});
	const text = await res.text();
	if (!res.ok) {
		let envelope: ErrorEnvelope | undefined;
		try {
			envelope = decode<{ error: ErrorEnvelope }>(text).error;
		} catch {
			// Not one of ours (e.g. a platform error page).
		}
		throw fromErrorEnvelope(
			envelope ?? {
				name: "WorkflowWorldError",
				message: `Durable Object request failed (${res.status}): ${path} - ${text}`,
				status: res.status,
			},
		);
	}
	return (text === "" ? undefined : decode<T>(text)) as T;
}

/**
 * Make a request to a run's Durable Object that returns a byte stream.
 */
export async function doFetchStream(
	stub: DurableObjectStub,
	path: string,
): Promise<ReadableStream<Uint8Array>> {
	const res = await stub.fetch(`http://do${path}`);
	if (!res.ok) {
		const text = await res.text();
		let envelope: ErrorEnvelope | undefined;
		try {
			envelope = decode<{ error: ErrorEnvelope }>(text).error;
		} catch {}
		throw fromErrorEnvelope(
			envelope ?? {
				name: "WorkflowWorldError",
				message: `Durable Object stream request failed (${res.status}): ${path} - ${text}`,
				status: res.status,
			},
		);
	}
	if (!res.body) {
		throw new Error(`Durable Object stream request returned no body: ${path}`);
	}
	return res.body;
}

/** ISO string or undefined */
export function toISOOrUndef(d: Date | undefined | null): string | undefined {
	return d ? d.toISOString() : undefined;
}
