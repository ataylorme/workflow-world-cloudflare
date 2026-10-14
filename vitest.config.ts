import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		globals: false,
		include: ["src/**/*.test.ts", "test/**/*.test.ts"],
		exclude: ["test/conformance.test.ts", "**/node_modules/**"],
	},
});
