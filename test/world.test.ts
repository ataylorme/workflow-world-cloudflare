import { eventIdToSlot, SPEC_VERSION_CURRENT } from "@workflow/world";
import { describe, expect, it } from "vitest";
import { createWorld, migrate, WorkflowRunDO } from "../src/index.js";
import {
	FakeD1Database,
	FakeDurableObjectNamespace,
	FakeQueue,
} from "./harness/fakes.js";

async function setup() {
	const db = new FakeD1Database();
	await migrate(db as unknown as D1Database);
	const runs = new FakeDurableObjectNamespace(WorkflowRunDO as never, {
		WORKFLOW_DB: db,
	});
	return createWorld({
		db: db as unknown as D1Database,
		runs: runs as unknown as DurableObjectNamespace,
		queue: new FakeQueue() as unknown as globalThis.Queue,
		env: {},
	});
}

const input = new Uint8Array([1, 2, 3]);

async function startRun(world: Awaited<ReturnType<typeof setup>>) {
	const { run } = await world.events.create(null, {
		eventType: "run_created",
		specVersion: SPEC_VERSION_CURRENT,
		eventData: { deploymentId: "d", workflowName: "wf", input },
	});
	await world.events.create(run.runId, {
		eventType: "run_started",
		specVersion: SPEC_VERSION_CURRENT,
	});
	return run.runId;
}

describe("cloudflare world", () => {
	it("declares the spec version the SDK mints", async () => {
		expect((await setup()).specVersion).toBe(SPEC_VERSION_CURRENT);
		expect(
			createWorld({
				db: {} as D1Database,
				runs: {} as DurableObjectNamespace,
				queue: {} as globalThis.Queue,
				env: { WORKFLOW_SEALED_LOG: "0" },
			}).specVersion,
		).toBeLessThan(SPEC_VERSION_CURRENT);
	});

	it("numbers concurrent appends densely and reports what each writer skipped", async () => {
		const world = await setup();
		const runId = await startRun(world);
		const writers = 25;

		// Every writer replayed the same two-event log, so all expect slot 3.
		const results = await Promise.all(
			Array.from({ length: writers }, (_, i) =>
				world.events.create(
					runId,
					{
						eventType: "step_created",
						specVersion: SPEC_VERSION_CURRENT,
						correlationId: `step_${i}`,
						eventData: { stepName: "s", input },
					},
					{ eventCount: 2 },
				),
			),
		);

		const slots = results.map((r) => eventIdToSlot(r.event?.eventId ?? ""));
		expect(new Set(slots).size).toBe(writers);
		expect([...slots].sort((a, b) => (a ?? 0) - (b ?? 0))).toEqual(
			Array.from({ length: writers }, (_, i) => i + 3),
		);
		for (const r of results) {
			const slot = eventIdToSlot(r.event?.eventId ?? "") as number;
			// Bumped writers are handed exactly the slots between 3 and theirs.
			expect((r.events ?? []).map((e) => eventIdToSlot(e.eventId))).toEqual(
				Array.from({ length: slot - 3 }, (_, i) => i + 3),
			);
		}

		const { data } = await world.events.list({ runId });
		expect(data.map((e) => eventIdToSlot(e.eventId))).toEqual(
			Array.from({ length: writers + 2 }, (_, i) => i + 1),
		);
		expect(data[2]?.eventType).toBe("step_created");
	});

	it("round-trips binary payloads through the Durable Object", async () => {
		const world = await setup();
		const runId = await startRun(world);
		const run = await world.runs.get(runId);
		expect(run.input).toEqual(input);
		expect(run.input).toBeInstanceOf(Uint8Array);
	});

	it("refuses step_started on a terminal run even while the step reads running", async () => {
		const world = await setup();
		const runId = await startRun(world);
		await world.events.create(runId, {
			eventType: "step_created",
			specVersion: SPEC_VERSION_CURRENT,
			correlationId: "step_a",
			eventData: { stepName: "s", input },
		});
		await world.events.create(runId, {
			eventType: "step_started",
			specVersion: SPEC_VERSION_CURRENT,
			correlationId: "step_a",
		});
		await world.events.create(runId, {
			eventType: "run_cancelled",
			specVersion: SPEC_VERSION_CURRENT,
		});

		await expect(
			world.events.create(runId, {
				eventType: "step_started",
				specVersion: SPEC_VERSION_CURRENT,
				correlationId: "step_a",
			}),
		).rejects.toMatchObject({ name: "RunExpiredError" });
		// An in-flight step can still record its outcome.
		await expect(
			world.events.create(runId, {
				eventType: "step_completed",
				specVersion: SPEC_VERSION_CURRENT,
				correlationId: "step_a",
				eventData: { result: input },
			}),
		).resolves.toMatchObject({ step: { status: "completed" } });
	});

	it("gives a hook token to one run at a time", async () => {
		const world = await setup();
		const [a, b] = [await startRun(world), await startRun(world)];
		const create = (runId: string, hookId: string) =>
			world.events.create(runId, {
				eventType: "hook_created",
				specVersion: SPEC_VERSION_CURRENT,
				correlationId: hookId,
				eventData: { token: "shared-token" },
			});

		const [ra, rb] = await Promise.all([create(a, "hook_a"), create(b, "hook_b")]);
		const types = [ra.event?.eventType, rb.event?.eventType].sort();
		expect(types).toEqual(["hook_conflict", "hook_created"]);
		const winner = ra.event?.eventType === "hook_created" ? a : b;
		expect((await world.hooks.getByToken("shared-token")).runId).toBe(winner);

		// Disposing frees the token for the other run.
		await world.events.create(winner, {
			eventType: "hook_disposed",
			specVersion: SPEC_VERSION_CURRENT,
			correlationId: winner === a ? "hook_a" : "hook_b",
		});
		const loser = winner === a ? b : a;
		const retry = await create(loser, loser === a ? "hook_a2" : "hook_b2");
		expect(retry.event?.eventType).toBe("hook_created");
	});

	it("tails a stream from before its first write until it is closed", async () => {
		const world = await setup();
		const runId = await startRun(world);
		const stream = await world.streams.get(runId, "out");
		const read = (async () => {
			const chunks: string[] = [];
			const decoder = new TextDecoder();
			for await (const chunk of stream) chunks.push(decoder.decode(chunk));
			return chunks.join("");
		})();

		await world.streams.write(runId, "out", "hello ");
		await world.streams.writeMulti?.(runId, "out", ["wor", "ld"]);
		await world.streams.close(runId, "out");

		expect(await read).toBe("hello world");
		expect(await world.streams.getInfo(runId, "out")).toEqual({
			tailIndex: 2,
			done: true,
		});
		const page = await world.streams.getChunks(runId, "out", { limit: 2 });
		expect(page.data.map((c) => c.index)).toEqual([0, 1]);
		expect(page.hasMore).toBe(true);
		expect(await world.streams.list(runId)).toEqual(["out"]);
	});

	it("filters runs.list by an array of statuses", async () => {
		const world = await setup();
		const running = await startRun(world);
		const cancelled = await startRun(world);
		await world.events.create(cancelled, {
			eventType: "run_cancelled",
			specVersion: SPEC_VERSION_CURRENT,
		});
		// The index is written after the response; give it a tick.
		await new Promise((r) => setTimeout(r, 10));

		const ids = async (status: never) =>
			(await world.runs.list({ status, resolveData: "none" })).data
				.map((r) => r.runId)
				.sort();
		expect(await ids(["running", "cancelled"] as never)).toEqual(
			[running, cancelled].sort(),
		);
		expect(await ids(["cancelled"] as never)).toEqual([cancelled]);
		expect(await ids([] as never)).toEqual([]);
	});
});
