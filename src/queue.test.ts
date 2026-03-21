import type { QueuePayload } from "@workflow/world";
import { createLocalWorld } from "@workflow/world-local";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createQueue } from "./queue.js";

vi.mock("@workflow/world-local", () => ({
	createLocalWorld: vi.fn(),
}));

describe("cloudflare queue", () => {
	const wrappedHandler = vi.fn(async () => Response.json({ ok: true }));
	const localWorldClose = vi.fn();
	const mockCreateQueueHandler = vi.fn(() => wrappedHandler);
	const mockRegisterHandler = vi.fn();
	const queueSend = vi.fn();

	const mockConfig = {
		db: {} as D1Database,
		runs: {} as DurableObjectNamespace,
		queue: { send: queueSend } as unknown as globalThis.Queue,
	};

	beforeEach(() => {
		vi.clearAllMocks();

		vi.mocked(createLocalWorld).mockReturnValue({
			createQueueHandler: mockCreateQueueHandler,
			registerHandler: mockRegisterHandler,
			close: localWorldClose,
		} as ReturnType<typeof createLocalWorld>);
	});

	it("registers queue handlers with the local world", () => {
		const queue = createQueue(mockConfig);
		const handler = vi.fn(async () => undefined);

		const wrapped = queue.createQueueHandler("__wkf_step_", handler);

		expect(mockCreateQueueHandler).toHaveBeenCalledWith("__wkf_step_", handler);
		expect(mockRegisterHandler).toHaveBeenCalledWith(
			"__wkf_step_",
			wrappedHandler,
		);
		expect(wrapped).toBe(wrappedHandler);
	});

	it("sends messages to the cloudflare queue", async () => {
		const queue = createQueue(mockConfig);

		const message = {
			runId: "run_01ABC",
		} satisfies QueuePayload;

		const result = await queue.queue("__wkf_workflow_test-flow", message, {
			headers: { traceparent: "trace-parent" },
			idempotencyKey: "idem-key",
			delaySeconds: 5,
		});

		expect(result.messageId).toBeTruthy();
		expect(queueSend).toHaveBeenCalledWith(
			expect.objectContaining({
				queueName: "__wkf_workflow_test-flow",
				payload: message,
				attempt: 1,
				idempotencyKey: "idem-key",
				headers: { traceparent: "trace-parent" },
			}),
			expect.objectContaining({
				delaySeconds: 5,
			}),
		);
	});

	it("sends messages without delay when delaySeconds not specified", async () => {
		const queue = createQueue(mockConfig);

		await queue.queue("__wkf_step_test-step", {
			workflowName: "test-workflow",
			workflowRunId: "run_01ABC",
			workflowStartedAt: Date.now(),
			stepId: "step_01ABC",
		});

		expect(queueSend).toHaveBeenCalledWith(
			expect.objectContaining({
				queueName: "__wkf_step_test-step",
				attempt: 1,
			}),
			expect.not.objectContaining({
				delaySeconds: expect.anything(),
			}),
		);
	});

	it("returns cloudflare as deployment id", async () => {
		const queue = createQueue(mockConfig);
		const deploymentId = await queue.getDeploymentId();
		expect(deploymentId).toBe("cloudflare");
	});
});
