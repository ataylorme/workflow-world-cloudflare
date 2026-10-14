/**
 * In-memory stand-ins for the Cloudflare bindings, so the World can run in
 * plain Node under `@workflow/world-testing`. They model the semantics the
 * World relies on, not the whole platform:
 *
 * - Durable Object storage: values are structured-cloned on the way in and
 *   out, keys list in lexicographic order, and a burst of puts/deletes issued
 *   without an `await` between them lands together (it is synchronous here).
 * - D1: a real SQLite database (`node:sqlite`), so unique constraints and
 *   `batch()` atomicity are the real thing.
 * - Queues: structured-cloned bodies, `delaySeconds`, per-message
 *   `ack()`/`retry()` and an `attempts` counter, delivered to a consumer
 *   callback in batches of one.
 */
import { DatabaseSync } from "node:sqlite";

type ListOptions = {
	prefix?: string;
	start?: string;
	startAfter?: string;
	end?: string;
	reverse?: boolean;
	limit?: number;
};

export class FakeDurableObjectStorage {
	private data = new Map<string, unknown>();

	async get(keyOrKeys: string | string[]): Promise<unknown> {
		if (Array.isArray(keyOrKeys)) {
			const out = new Map<string, unknown>();
			for (const k of keyOrKeys) {
				if (this.data.has(k)) out.set(k, structuredClone(this.data.get(k)));
			}
			return out;
		}
		const v = this.data.get(keyOrKeys);
		return v === undefined ? undefined : structuredClone(v);
	}

	async put(keyOrEntries: string | Record<string, unknown>, value?: unknown) {
		if (typeof keyOrEntries === "string") {
			this.data.set(keyOrEntries, structuredClone(value));
			return;
		}
		for (const [k, v] of Object.entries(keyOrEntries)) {
			this.data.set(k, structuredClone(v));
		}
	}

	async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
		if (Array.isArray(keyOrKeys)) {
			let n = 0;
			for (const k of keyOrKeys) if (this.data.delete(k)) n++;
			return n;
		}
		return this.data.delete(keyOrKeys);
	}

	async list(opts: ListOptions = {}): Promise<Map<string, unknown>> {
		let keys = [...this.data.keys()].sort();
		if (opts.prefix) keys = keys.filter((k) => k.startsWith(opts.prefix!));
		if (opts.start) keys = keys.filter((k) => k >= opts.start!);
		if (opts.startAfter) keys = keys.filter((k) => k > opts.startAfter!);
		if (opts.end) keys = keys.filter((k) => k < opts.end!);
		if (opts.reverse) keys.reverse();
		if (opts.limit !== undefined) keys = keys.slice(0, opts.limit);
		return new Map(keys.map((k) => [k, structuredClone(this.data.get(k))]));
	}
}

type DurableObjectCtor = new (
	state: { storage: FakeDurableObjectStorage },
	env: Record<string, unknown>,
) => { fetch(req: Request): Promise<Response> };

export class FakeDurableObjectNamespace {
	private instances = new Map<string, { fetch(req: Request): Promise<Response> }>();

	constructor(
		private ctor: DurableObjectCtor,
		private env: Record<string, unknown>,
	) {}

	idFromName(name: string) {
		return { name, toString: () => name };
	}

	get(id: { name: string }) {
		let instance = this.instances.get(id.name);
		if (!instance) {
			instance = new this.ctor({ storage: new FakeDurableObjectStorage() }, this.env);
			this.instances.set(id.name, instance);
		}
		const target = instance;
		return {
			fetch: (url: string, init?: RequestInit) =>
				target.fetch(new Request(url, init)),
		};
	}
}

const toSqlValue = (v: unknown) =>
	v === undefined ? null : typeof v === "boolean" ? (v ? 1 : 0) : v;

class FakeD1Statement {
	constructor(
		private db: DatabaseSync,
		private sql: string,
		private params: unknown[] = [],
	) {}

	bind(...params: unknown[]) {
		return new FakeD1Statement(this.db, this.sql, params.map(toSqlValue));
	}

	private stmt() {
		return this.db.prepare(this.sql);
	}

	async all() {
		return { results: this.allSync(), success: true, meta: {} };
	}

	allSync() {
		return this.stmt()
			.all(...(this.params as never[]))
			.map((row) => ({ ...row }));
	}

	async first(column?: string) {
		const row = this.allSync()[0] ?? null;
		if (row && column) return (row as Record<string, unknown>)[column] ?? null;
		return row;
	}

	async run() {
		const info = this.stmt().run(...(this.params as never[]));
		return { success: true, results: [], meta: { changes: info.changes } };
	}
}

export class FakeD1Database {
	private db = new DatabaseSync(":memory:");

	prepare(sql: string) {
		return new FakeD1Statement(this.db, sql);
	}

	async exec(sql: string) {
		this.db.exec(sql);
		return { count: 1, duration: 0 };
	}

	async batch(statements: FakeD1Statement[]) {
		this.db.exec("BEGIN");
		try {
			const results = statements.map((s) => ({
				results: s.allSync(),
				success: true,
				meta: {},
			}));
			this.db.exec("COMMIT");
			return results;
		} catch (err) {
			this.db.exec("ROLLBACK");
			throw err;
		}
	}
}

export interface FakeMessage<T> {
	id: string;
	timestamp: Date;
	body: T;
	attempts: number;
	ack(): void;
	retry(): void;
}

export class FakeQueue<T = unknown> {
	private nextId = 0;
	consumer?: (batch: { queue: string; messages: FakeMessage<T>[] }) => Promise<void>;

	constructor(private maxRetries = 10) {}

	async send(body: T, opts?: { delaySeconds?: number }) {
		this.schedule(structuredClone(body), (opts?.delaySeconds ?? 0) * 1000, 1);
	}

	async sendBatch(messages: { body: T; delaySeconds?: number }[]) {
		for (const m of messages) await this.send(m.body, m);
	}

	private schedule(body: T, delayMs: number, attempts: number) {
		const id = `fake_${++this.nextId}`;
		setTimeout(async () => {
			let settled: "ack" | "retry" | undefined;
			const message: FakeMessage<T> = {
				id,
				timestamp: new Date(),
				body: structuredClone(body),
				attempts,
				ack: () => {
					settled ??= "ack";
				},
				retry: () => {
					settled ??= "retry";
				},
			};
			try {
				await this.consumer?.({ queue: "workflow", messages: [message] });
			} catch {
				settled ??= "retry";
			}
			if (settled === "retry" && attempts < this.maxRetries) {
				this.schedule(body, 1000, attempts + 1);
			}
		}, delayMs);
	}
}
