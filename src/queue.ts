import { globalSingleton } from "@workflow/utils";
import type {
	QueueBatchResult,
	Queue as QueueInterface,
	QueueOptions,
	QueuePayload,
	ValidQueueName,
} from "@workflow/world";
import {
	MessageId as MessageIdSchema,
	WORKFLOW_FLOW_ROUTE,
} from "@workflow/world";
import { monotonicFactory } from "ulid";
import { decode, encode } from "./codec.js";
import type { CloudflareWorldConfig } from "./config.js";

/** Message envelope stored in Cloudflare Queues */
export interface QueueMessageEnvelope {
	queueName: ValidQueueName;
	payload: QueuePayload;
	/** Stable for the life of the message, across redeliveries and delays. */
	messageId: string;
	/** ISO timestamp of the original `queue()` call. */
	createdAt: string;
	/**
	 * Epoch ms before which the message must not run. Cloudflare caps a
	 * single delay at 12 hours, so a longer one is re-sent until it is due.
	 */
	notBefore?: number;
	headers?: Record<string, string>;
}

type RawHandler = Parameters<QueueInterface["createQueueHandler"]>[1];

export interface HandleQueueBatchOptions {
	/**
	 * Sends one delivery to the app's workflow flow route
	 * (`/.well-known/workflow/v1/flow`) and returns its response, e.g.
	 * `(req) => worker.fetch(req, env, ctx)`. Without it, messages go to the
	 * handler the runtime registered in this isolate, which it only does when
	 * that route first runs.
	 */
	deliver?: (request: Request) => Promise<Response>;
}

export interface CloudflareQueue extends QueueInterface {
	/** Process a batch of messages from the Cloudflare Queue consumer */
	handleQueueBatch(
		batch: MessageBatch<QueueMessageEnvelope>,
		options?: HandleQueueBatchOptions,
	): Promise<void>;
}

/** Cloudflare Queues' maximum `delaySeconds` for one send. */
export const MAX_DELAY_SECONDS = 12 * 60 * 60;
/** Cloudflare Queues' maximum messages per `sendBatch`. */
const MAX_BATCH_SIZE = 100;

// The runtime registers its handlers through whichever copy of this module
// its bundle holds, and the queue consumer may run through another, so the
// registry is process-wide.
const state = globalSingleton(
	"@ataylorme/workflow-world-cloudflare//queue",
	1,
	() => ({
		handlers: new Map<string, RawHandler>(),
		generateMessageId: monotonicFactory(),
	}),
);

function findHandler(queueName: string): RawHandler | undefined {
	let best: [string, RawHandler] | undefined;
	for (const entry of state.handlers) {
		if (
			queueName.startsWith(entry[0]) &&
			(!best || entry[0].length > best[0].length)
		) {
			best = entry;
		}
	}
	return best?.[1];
}

function delayFor(envelope: QueueMessageEnvelope): number | undefined {
	if (!envelope.notBefore) return undefined;
	const remaining = Math.ceil((envelope.notBefore - Date.now()) / 1000);
	return remaining > 0 ? Math.min(remaining, MAX_DELAY_SECONDS) : undefined;
}

export function createQueue(config: CloudflareWorldConfig): CloudflareQueue {
	const envelopeFor = (
		queueName: ValidQueueName,
		message: QueuePayload,
		opts?: QueueOptions,
	): QueueMessageEnvelope => ({
		queueName,
		payload: message,
		messageId: MessageIdSchema.parse(`msg_${state.generateMessageId()}`),
		createdAt: new Date().toISOString(),
		...(opts?.delaySeconds && opts.delaySeconds > 0
			? { notBefore: Date.now() + opts.delaySeconds * 1000 }
			: {}),
		...(opts?.headers ? { headers: opts.headers } : {}),
	});

	const send = (envelope: QueueMessageEnvelope) => {
		const delaySeconds = delayFor(envelope);
		return config.queue.send(envelope, delaySeconds ? { delaySeconds } : {});
	};

	/**
	 * Run one delivery through the runtime's handler. A `{ timeoutSeconds }`
	 * answer asks for the same message again after that delay (0 means now),
	 * which is sent as a fresh Cloudflare message carrying the same messageId.
	 */
	const deliver = async (
		handler: RawHandler,
		envelope: QueueMessageEnvelope,
		attempt: number,
	): Promise<unknown> => {
		const result = await handler(envelope.payload, {
			attempt,
			queueName: envelope.queueName,
			messageId: MessageIdSchema.parse(envelope.messageId),
			createdAt: new Date(envelope.createdAt),
		});
		const timeoutSeconds = Number(
			(result as { timeoutSeconds?: unknown } | undefined)?.timeoutSeconds,
		);
		if (Number.isFinite(timeoutSeconds) && timeoutSeconds >= 0) {
			await send({
				...envelope,
				notBefore: Date.now() + timeoutSeconds * 1000,
			});
		}
		return result;
	};

	const createQueueHandler: QueueInterface["createQueueHandler"] = (
		prefix,
		handler,
	) => {
		state.handlers.set(prefix, handler);
		// Over HTTP the body is an encoded envelope; this is what a consumer
		// that forwards messages to the app's flow route sends.
		return async (req: Request) => {
			let envelope: QueueMessageEnvelope;
			try {
				envelope = decode<QueueMessageEnvelope>(await req.text());
			} catch {
				return Response.json(
					{ error: "Invalid message body" },
					{ status: 400 },
				);
			}
			if (!envelope.queueName?.startsWith(prefix)) {
				return Response.json(
					{
						error: "Unhandled queue",
						detail: `This route handles queue prefix "${prefix}", not "${envelope.queueName}".`,
					},
					{ status: 400 },
				);
			}
			const attempt = Number(req.headers.get("x-workflow-attempt") ?? "1");
			try {
				await deliver(handler, envelope, attempt);
				return Response.json({ ok: true });
			} catch (err) {
				return Response.json({ error: String(err) }, { status: 500 });
			}
		};
	};

	const getDeploymentId: QueueInterface["getDeploymentId"] = async () => {
		return "cloudflare";
	};

	const queue: QueueInterface["queue"] = async (queueName, message, opts) => {
		const envelope = envelopeFor(queueName, message, opts);
		await send(envelope);
		return { messageId: MessageIdSchema.parse(envelope.messageId) };
	};

	const queueBatch: NonNullable<QueueInterface["queueBatch"]> = async (
		queueName,
		messages,
	) => {
		const envelopes = messages.map(({ message, opts }) =>
			envelopeFor(queueName, message, opts),
		);
		// A rejected sendBatch is a request-level failure: nothing reports
		// which messages landed, so it is thrown rather than reported.
		for (let i = 0; i < envelopes.length; i += MAX_BATCH_SIZE) {
			await config.queue.sendBatch(
				envelopes.slice(i, i + MAX_BATCH_SIZE).map((body) => {
					const delaySeconds = delayFor(body);
					return delaySeconds ? { body, delaySeconds } : { body };
				}),
			);
		}
		return envelopes.map(
			(e): QueueBatchResult => ({
				messageId: MessageIdSchema.parse(e.messageId),
			}),
		);
	};

	/**
	 * Run one message: through the app's flow route when the consumer can
	 * reach it, else through the handler the runtime registered in this
	 * isolate (it registers on the route's first request).
	 */
	const dispatch = async (
		envelope: QueueMessageEnvelope,
		attempt: number,
		options?: HandleQueueBatchOptions,
	): Promise<void> => {
		if (!options?.deliver) {
			const handler = findHandler(envelope.queueName);
			if (!handler) {
				throw new Error(
					`No handler registered for queue "${envelope.queueName}" and no \`deliver\` option was passed to handleQueueBatch()`,
				);
			}
			await deliver(handler, envelope, attempt);
			return;
		}
		const response = await options.deliver(
			new Request(new URL(WORKFLOW_FLOW_ROUTE, "http://workflow.internal"), {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-workflow-attempt": String(attempt),
				},
				body: encode(envelope),
			}),
		);
		if (!response.ok) {
			throw new Error(
				`Flow route answered ${response.status}: ${await response.text()}`,
			);
		}
	};

	const handleQueueBatch = async (
		batch: MessageBatch<QueueMessageEnvelope>,
		options?: HandleQueueBatchOptions,
	): Promise<void> => {
		await Promise.all(
			batch.messages.map(async (msg) => {
				const envelope = msg.body;
				try {
					if (envelope.notBefore && envelope.notBefore > Date.now() + 1000) {
						// Not due yet: the delay was longer than one send allows.
						await send(envelope);
						msg.ack();
						return;
					}
					await dispatch(envelope, msg.attempts, options);
					msg.ack();
				} catch (err) {
					console.error(
						`[workflow-world-cloudflare] Queue message ${envelope.messageId} failed (attempt ${msg.attempts})`,
						err,
					);
					msg.retry();
				}
			}),
		);
	};

	return {
		createQueueHandler,
		getDeploymentId,
		queue,
		queueBatch,
		handleQueueBatch,
	};
}
