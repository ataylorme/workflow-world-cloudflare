import type {
	GetChunksOptions,
	StreamChunksResponse,
	Streamer,
	StreamInfoResponse,
} from "@workflow/world";
import type { CloudflareWorldConfig } from "./config.js";
import { doFetch, doFetchStream, getRunStub } from "./util.js";

const encoder = new TextEncoder();
const toBytes = (chunk: string | Uint8Array) =>
	typeof chunk === "string" ? encoder.encode(chunk) : chunk;

/**
 * Creates a Streamer implementation backed by Durable Objects.
 * Stream chunks are stored in the run's DO storage.
 */
export function createStreamer(config: CloudflareWorldConfig): Streamer {
	const path = (name: string, action: string) =>
		`/streams/${encodeURIComponent(name)}/${action}`;

	const writeMulti = async (
		runId: string,
		name: string,
		chunks: (string | Uint8Array)[],
	): Promise<void> => {
		if (chunks.length === 0) return;
		await doFetch<void>(getRunStub(config.runs, runId), path(name, "write"), {
			body: { chunks: chunks.map(toBytes) },
		});
	};

	return {
		streams: {
			write: (runId, name, chunk) => writeMulti(runId, name, [chunk]),

			writeMulti,

			async close(runId: string, name: string): Promise<void> {
				await doFetch<void>(
					getRunStub(config.runs, runId),
					path(name, "close"),
					{ body: {} },
				);
			},

			async get(
				runId: string,
				name: string,
				startIndex?: number,
			): Promise<ReadableStream<Uint8Array>> {
				const qs = startIndex ? `?startIndex=${startIndex}` : "";
				return doFetchStream(
					getRunStub(config.runs, runId),
					`${path(name, "read")}${qs}`,
				);
			},

			async list(runId: string): Promise<string[]> {
				return doFetch<string[]>(getRunStub(config.runs, runId), "/streams");
			},

			async getChunks(
				runId: string,
				name: string,
				options?: GetChunksOptions,
			): Promise<StreamChunksResponse> {
				const qs = new URLSearchParams({
					limit: String(options?.limit ?? 100),
				});
				if (options?.cursor) qs.set("cursor", options.cursor);
				return doFetch<StreamChunksResponse>(
					getRunStub(config.runs, runId),
					`${path(name, "chunks")}?${qs}`,
				);
			},

			async getInfo(runId: string, name: string): Promise<StreamInfoResponse> {
				return doFetch<StreamInfoResponse>(
					getRunStub(config.runs, runId),
					path(name, "info"),
				);
			},
		},
	};
}
