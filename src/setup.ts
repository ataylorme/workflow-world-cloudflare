#!/usr/bin/env node
/// <reference types="node" />

import { execSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { createInterface } from "node:readline/promises";

const MIGRATION_SQL = `
CREATE TABLE IF NOT EXISTS workflow_runs_index (
  run_id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  status TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  spec_version INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  completed_at TEXT,
  expired_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_name ON workflow_runs_index(workflow_name);
CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs_index(status);

CREATE TABLE IF NOT EXISTS workflow_hooks_index (
  hook_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  token TEXT NOT NULL UNIQUE,
  owner_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  environment TEXT NOT NULL,
  created_at TEXT NOT NULL,
  is_webhook INTEGER DEFAULT 1,
  spec_version INTEGER
);
CREATE INDEX IF NOT EXISTS idx_hooks_run ON workflow_hooks_index(run_id);
CREATE INDEX IF NOT EXISTS idx_hooks_token ON workflow_hooks_index(token);
`.trim();

async function prompt(
	rl: ReturnType<typeof createInterface>,
	question: string,
): Promise<string> {
	const answer = (await rl.question(question)).trim();
	return answer;
}

function run(cmd: string): {
	success: boolean;
	stdout: string;
	stderr: string;
} {
	try {
		const stdout = execSync(cmd, {
			encoding: "utf-8",
			stdio: ["inherit", "pipe", "pipe"],
		});
		return { success: true, stdout, stderr: "" };
	} catch (err: unknown) {
		const e = err as { stdout?: string; stderr?: string };
		return {
			success: false,
			stdout: e.stdout ?? "",
			stderr: e.stderr ?? "",
		};
	}
}

function generateTomlBindings(
	dbName: string,
	databaseId: string,
	queueName: string,
): string {
	return `
# --- @ataylorme/workflow-world-cloudflare bindings ---

# Durable Objects — one DO class per workflow run
[durable_objects]
bindings = [
  { name = "WORKFLOW_RUNS", class_name = "WorkflowRunDO" }
]

# D1 Database — lightweight index for cross-run queries
[[d1_databases]]
binding = "WORKFLOW_DB"
database_name = "${dbName}"
database_id = "${databaseId}"

# Queue producer — for dispatching workflow/step messages
[[queues.producers]]
binding = "WORKFLOW_QUEUE"
queue = "${queueName}"

# Queue consumer — processes workflow/step messages
[[queues.consumers]]
queue = "${queueName}"
max_batch_size = 10
max_retries = 3

# DO migration (required when first deploying)
[[migrations]]
tag = "v1"
new_classes = ["WorkflowRunDO"]
`;
}

function generateJsonBindings(
	dbName: string,
	databaseId: string,
	queueName: string,
): Record<string, unknown> {
	return {
		durable_objects: {
			bindings: [{ name: "WORKFLOW_RUNS", class_name: "WorkflowRunDO" }],
		},
		d1_databases: [
			{
				binding: "WORKFLOW_DB",
				database_name: dbName,
				database_id: databaseId,
			},
		],
		queues: {
			producers: [{ binding: "WORKFLOW_QUEUE", queue: queueName }],
			consumers: [{ queue: queueName, max_batch_size: 10, max_retries: 3 }],
		},
		migrations: [{ tag: "v1", new_classes: ["WorkflowRunDO"] }],
	};
}

function stripJsonComments(text: string): string {
	// Remove single-line comments
	let result = text.replace(
		/("(?:[^"\\]|\\.)*")|\/\/[^\n]*/g,
		(_match, str) => {
			return str ? str : "";
		},
	);
	// Remove multi-line comments
	result = result.replace(
		/("(?:[^"\\]|\\.)*")|\/\*[\s\S]*?\*\//g,
		(_match, str) => {
			return str ? str : "";
		},
	);
	return result;
}

function updateWranglerToml(
	filePath: string,
	dbName: string,
	databaseId: string,
	queueName: string,
): void {
	const existing = readFileSync(filePath, "utf-8");
	const bindings = generateTomlBindings(dbName, databaseId, queueName);
	writeFileSync(filePath, `${existing.trimEnd()}\n${bindings}`);
	console.log(`\n✅ Updated ${filePath} with workflow bindings.`);
}

function updateWranglerJson(
	filePath: string,
	dbName: string,
	databaseId: string,
	queueName: string,
): void {
	const raw = readFileSync(filePath, "utf-8");
	const cleaned = stripJsonComments(raw);
	const config = JSON.parse(cleaned) as Record<string, unknown>;
	const bindings = generateJsonBindings(dbName, databaseId, queueName);

	// Merge bindings into config
	for (const [key, value] of Object.entries(bindings)) {
		config[key] = value;
	}

	writeFileSync(filePath, `${JSON.stringify(config, null, "\t")}\n`);
	console.log(`\n✅ Updated ${filePath} with workflow bindings.`);
}

function printConfigSnippet(
	dbName: string,
	databaseId: string,
	queueName: string,
): void {
	console.log(
		"\nNo wrangler.toml, wrangler.json, or wrangler.jsonc found in the current directory.",
	);
	console.log("Add the following to your wrangler.toml:\n");
	console.log(generateTomlBindings(dbName, databaseId, queueName));
}

async function main(): Promise<void> {
	const rl = createInterface({
		input: process.stdin,
		output: process.stdout,
	});

	console.log("@ataylorme/workflow-world-cloudflare setup");
	console.log("==========================================\n");
	console.log(
		"This will create Cloudflare resources and update your wrangler config.\n",
	);

	const baseName = await prompt(rl, "Base name for resources (e.g. my-app): ");
	if (!baseName) {
		console.error("Error: base name is required.");
		process.exit(1);
	}

	const envSuffix = await prompt(
		rl,
		"Environment suffix (dev, qa, prod, etc): ",
	);
	if (!envSuffix) {
		console.error("Error: environment suffix is required.");
		process.exit(1);
	}

	const dbName = `${baseName}-workflow-db-${envSuffix}`;
	const queueName = `${baseName}-workflow-queue-${envSuffix}`;

	console.log(`\nResources to create:`);
	console.log(`  D1 Database: ${dbName}`);
	console.log(`  Queue:       ${queueName}`);

	const confirm = await prompt(rl, "\nProceed? (y/N): ");
	rl.close();

	if (confirm.toLowerCase() !== "y") {
		console.log("Aborted.");
		process.exit(0);
	}

	// --- Create D1 database ---
	console.log(`\nCreating D1 database "${dbName}"...`);
	const d1Result = run(`npx wrangler d1 create "${dbName}"`);
	if (!d1Result.success) {
		console.error("Failed to create D1 database.");
		console.error(d1Result.stderr);
		console.error(
			"\nMake sure wrangler is installed and you are authenticated (npx wrangler login).",
		);
		process.exit(1);
	}
	console.log(d1Result.stdout);

	const dbIdMatch = d1Result.stdout.match(/database_id\s*=\s*"([^"]+)"/);
	if (!dbIdMatch) {
		console.error(
			"Could not parse database ID from wrangler output. Please update your wrangler config manually.",
		);
		process.exit(1);
	}
	const databaseId = dbIdMatch[1];
	console.log(`D1 database created (ID: ${databaseId})`);

	// --- Run D1 migration ---
	console.log(`\nRunning D1 schema migration on "${dbName}"...`);
	const statements = MIGRATION_SQL.split(";")
		.map((s) => s.trim())
		.filter(Boolean);

	let migrationFailed = false;
	for (const sql of statements) {
		const result = run(
			`npx wrangler d1 execute "${dbName}" --remote --command "${sql};"`,
		);
		if (!result.success) {
			console.error(`Migration statement failed: ${sql}`);
			console.error(result.stderr);
			migrationFailed = true;
			break;
		}
	}

	if (migrationFailed) {
		console.warn(
			"\n⚠️  D1 migration failed. You can run it manually later via:",
		);
		console.warn(
			'  import { migrate } from "@ataylorme/workflow-world-cloudflare/d1"',
		);
		console.warn("  await migrate(env.WORKFLOW_DB);");
	} else {
		console.log("D1 schema migration complete.");
	}

	// --- Create Queue ---
	console.log(`\nCreating queue "${queueName}"...`);
	const queueResult = run(`npx wrangler queues create "${queueName}"`);
	if (!queueResult.success) {
		console.error("Failed to create queue.");
		console.error(queueResult.stderr);
		console.error(
			"\nMake sure wrangler is installed and you are authenticated (npx wrangler login).",
		);
		process.exit(1);
	}
	console.log(queueResult.stdout);
	console.log(`Queue "${queueName}" created.`);

	// --- Update wrangler config ---
	const cwd = process.cwd();
	const tomlPath = resolve(cwd, "wrangler.toml");
	const jsoncPath = resolve(cwd, "wrangler.jsonc");
	const jsonPath = resolve(cwd, "wrangler.json");

	if (existsSync(tomlPath)) {
		updateWranglerToml(tomlPath, dbName, databaseId, queueName);
	} else if (existsSync(jsoncPath)) {
		updateWranglerJson(jsoncPath, dbName, databaseId, queueName);
	} else if (existsSync(jsonPath)) {
		updateWranglerJson(jsonPath, dbName, databaseId, queueName);
	} else {
		printConfigSnippet(dbName, databaseId, queueName);
	}

	console.log("\n==========================================");
	console.log("Setup complete! Next steps:\n");
	console.log("1. Re-export WorkflowRunDO from your worker entry point:");
	console.log(
		'   export { WorkflowRunDO } from "@ataylorme/workflow-world-cloudflare/durable-object";\n',
	);
	console.log("2. Deploy your worker:");
	console.log("   npx wrangler deploy\n");
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});
