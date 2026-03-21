import type {
	Queue as QueueInterface,
	QueuePayload,
	QueuePrefix,
	ValidQueueName,
} from "@workflow/world";
import { MessageId as MessageIdSchema } from "@workflow/world";
import { createLocalWorld, type LocalWorld } from "@workflow/world-local";
import { monotonicFactory } from "ulid";
import type { CloudflareWorldConfig } from "./config.js";

/** Message envelope stored in Cloudflare Queues */
interface QueueMessageEnvelope {
	queueName: ValidQueueName;
	payload: QueuePayload;
	messageId: string;
	attempt: number;
	idempotencyKey?: string;
	headers?: Record<string, string>;
}

export interface CloudflareQueue extends QueueInterface {
	/** Process a batch of messages from the Cloudflare Queue consumer */
	handleQueueBatch(batch: MessageBatch<QueueMessageEnvelope>): Promise<void>;
}

function getQueuePrefix(queueName: string): QueuePrefix | null {
	if (queueName.startsWith("__wkf_step_")) return "__wkf_step_";
	if (queueName.startsWith("__wkf_workflow_")) return "__wkf_workflow_";
	return null;
}

export function createQueue(config: CloudflareWorldConfig): CloudflareQueue {
	const port = config.port;
	const localWorld: LocalWorld = createLocalWorld({
		dataDir: undefined,
		port,
	});
	const generateMessageId = monotonicFactory();

	const createQueueHandler: QueueInterface["createQueueHandler"] = (
		prefix,
		handler,
	) => {
		const wrappedHandler = localWorld.createQueueHandler(prefix, handler);
		localWorld.registerHandler(prefix, wrappedHandler);
		return wrappedHandler;
	};

	const getDeploymentId: QueueInterface["getDeploymentId"] = async () => {
		return "cloudflare";
	};

	const queue: QueueInterface["queue"] = async (queueName, message, opts) => {
		const messageId = MessageIdSchema.parse(`msg_${generateMessageId()}`);

		const envelope: QueueMessageEnvelope = {
			queueName,
			payload: message,
			messageId,
			attempt: 1,
			idempotencyKey: opts?.idempotencyKey,
			headers: opts?.headers,
		};

		await config.queue.send(envelope, {
			...(opts?.delaySeconds ? { delaySeconds: opts.delaySeconds } : {}),
		});

		return { messageId };
	};

	const handleQueueBatch = async (
		batch: MessageBatch<QueueMessageEnvelope>,
	): Promise<void> => {
		for (const msg of batch.messages) {
			const envelope = msg.body;
			try {
				const prefix = getQueuePrefix(envelope.queueName);
				if (!prefix) {
					msg.retry();
					continue;
				}

				const pathname = prefix === "__wkf_step_" ? "step" : "flow";
				const request = new Request(
					`http://localhost/.well-known/workflow/v1/${pathname}`,
					{
						method: "POST",
						headers: {
							"content-type": "application/json",
							"x-vqs-queue-name": envelope.queueName,
							"x-vqs-message-id": envelope.messageId,
							"x-vqs-message-attempt": String(envelope.attempt),
							...(envelope.headers ?? {}),
						},
						body: JSON.stringify(envelope.payload),
					},
				);

				// Dispatch to the registered handler via localWorld
				// We re-queue through localWorld.queue which handles direct dispatch
				// But for batch processing, we can call the handler directly
				const handler = localWorld.createQueueHandler(
					prefix,
					async () => undefined,
				);
				// Actually use the local world's queue to dispatch
				const response = await handler(request);

				if (response.ok) {
					const text = await response.text();
					try {
						const parsed = JSON.parse(text);
						if (
							typeof parsed.timeoutSeconds === "number" &&
							parsed.timeoutSeconds > 0
						) {
							// Reschedule with delay
							const rescheduledEnvelope: QueueMessageEnvelope = {
								...envelope,
								attempt: envelope.attempt + 1,
							};
							await config.queue.send(rescheduledEnvelope, {
								delaySeconds: parsed.timeoutSeconds,
							});
						}
					} catch {
						// No timeout, just ack
					}
					msg.ack();
				} else {
					msg.retry();
				}
			} catch {
				msg.retry();
			}
		}
	};

	return {
		createQueueHandler,
		getDeploymentId,
		queue,
		handleQueueBatch,
	};
}
