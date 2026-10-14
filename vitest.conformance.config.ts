import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["test/conformance.test.ts"],
		globalSetup: ["test/build-harness.ts"],
		testTimeout: 60_000,
		hookTimeout: 60_000,
	},
});
