import type { QueuePayload, QueuePrefix } from "@workflow/world";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	createQueue,
	MAX_DELAY_SECONDS,
	type QueueMessageEnvelope,
} from "./queue.js";

function makeMessage(body: QueueMessageEnvelope, attempts = 1) {
	return {
		id: "m1",
		timestamp: new Date(),
		body,
		attempts,
		ack: vi.fn(),
		retry: vi.fn(),
	};
}

describe("cloudflare queue", () => {
	const queueSend = vi.fn();
	const queueSendBatch = vi.fn();
	const config = {
		db: {} as D1Database,
		runs: {} as DurableObjectNamespace,
		queue: {
			send: queueSend,
			sendBatch: queueSendBatch,
		} as unknown as globalThis.Queue,
	};
	const prefix = "__wkf_workflow_" as QueuePrefix;

	beforeEach(() => {
		vi.clearAllMocks();
	});

	it("sends an envelope, with the delay, to the cloudflare queue", async () => {
		const queue = createQueue(config);
		const message = { runId: "wrun_01ABC" } satisfies QueuePayload;

		const result = await queue.queue("__wkf_workflow_test-flow", message, {
			headers: { traceparent: "trace-parent" },
			delaySeconds: 5,
		});

		expect(result.messageId).toMatch(/^msg_/);
		expect(queueSend).toHaveBeenCalledWith(
			expect.objectContaining({
				queueName: "__wkf_workflow_test-flow",
				payload: message,
				messageId: result.messageId,
				headers: { traceparent: "trace-parent" },
			}),
			{ delaySeconds: 5 },
		);
	});

	it("caps a long delay at the platform maximum and re-sends until due", async () => {
		const queue = createQueue(config);
		await queue.queue(
			"__wkf_workflow_wf",
			{ runId: "wrun_1" },
			{
				delaySeconds: MAX_DELAY_SECONDS * 2,
			},
		);
		expect(queueSend.mock.calls[0][1]).toEqual({
			delaySeconds: MAX_DELAY_SECONDS,
		});

		// Delivered after the first hop: not due yet, so it goes back unhandled.
		const handler = vi.fn();
		queue.createQueueHandler(prefix, handler);
		const msg = makeMessage(queueSend.mock.calls[0][0]);
		await queue.handleQueueBatch({ messages: [msg] } as never);
		expect(handler).not.toHaveBeenCalled();
		expect(msg.ack).toHaveBeenCalled();
		expect(queueSend).toHaveBeenCalledTimes(2);
		expect(queueSend.mock.calls[1][0].messageId).toBe(
			queueSend.mock.calls[0][0].messageId,
		);
	});

	it("delivers to the registered handler with a stable messageId and the attempt", async () => {
		const queue = createQueue(config);
		const handler = vi.fn(async () => undefined);
		queue.createQueueHandler(prefix, handler);
		await queue.queue("__wkf_workflow_wf", { runId: "wrun_1" });
		const envelope = queueSend.mock.calls[0][0] as QueueMessageEnvelope;

		const msg = makeMessage(envelope, 3);
		await queue.handleQueueBatch({ messages: [msg] } as never);

		expect(handler).toHaveBeenCalledWith(
			{ runId: "wrun_1" },
			expect.objectContaining({
				attempt: 3,
				messageId: envelope.messageId,
				queueName: "__wkf_workflow_wf",
			}),
		);
		expect(msg.ack).toHaveBeenCalled();
		expect(msg.retry).not.toHaveBeenCalled();
	});

	it("re-sends the same message when the handler asks for a timeout", async () => {
		const queue = createQueue(config);
		queue.createQueueHandler(prefix, async () => ({ timeoutSeconds: 30 }));
		await queue.queue("__wkf_workflow_wf", { runId: "wrun_1" });
		const envelope = queueSend.mock.calls[0][0] as QueueMessageEnvelope;

		const msg = makeMessage(envelope);
		await queue.handleQueueBatch({ messages: [msg] } as never);

		expect(msg.ack).toHaveBeenCalled();
		expect(queueSend).toHaveBeenCalledTimes(2);
		expect(queueSend.mock.calls[1][0].messageId).toBe(envelope.messageId);
		expect(queueSend.mock.calls[1][1]).toEqual({ delaySeconds: 30 });
	});

	it("retries the message when the handler throws", async () => {
		const queue = createQueue(config);
		queue.createQueueHandler(prefix, async () => {
			throw new Error("boom");
		});
		await queue.queue("__wkf_workflow_wf", { runId: "wrun_1" });
		const msg = makeMessage(queueSend.mock.calls[0][0]);
		vi.spyOn(console, "error").mockImplementation(() => {});

		await queue.handleQueueBatch({ messages: [msg] } as never);

		expect(msg.retry).toHaveBeenCalled();
		expect(msg.ack).not.toHaveBeenCalled();
	});

	it("publishes a batch in one sendBatch and reports one result per message", async () => {
		const queue = createQueue(config);
		const results = await queue.queueBatch?.("__wkf_workflow_wf", [
			{ message: { runId: "wrun_1" } },
			{ message: { runId: "wrun_1" }, opts: { delaySeconds: 2 } },
		]);
		expect(results).toHaveLength(2);
		expect(results?.every((r) => r.error === undefined)).toBe(true);
		expect(queueSendBatch).toHaveBeenCalledTimes(1);
		expect(queueSendBatch.mock.calls[0][0][1]).toMatchObject({
			delaySeconds: 2,
		});
	});

	it("returns cloudflare as deployment id", async () => {
		const queue = createQueue(config);
		expect(await queue.getDeploymentId()).toBe("cloudflare");
	});
});
