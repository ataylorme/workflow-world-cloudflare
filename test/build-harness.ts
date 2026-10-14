import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

/** Bundle the harness World into one ESM file the test server can import. */
export default function setup() {
	const root = fileURLToPath(new URL("..", import.meta.url));
	execFileSync(
		"npx",
		["ncc", "build", "test/harness/index.ts", "-o", "test/.harness", "--no-source-map-register", "--transpile-only", "-q"],
		{ cwd: root, stdio: "inherit" },
	);
}
