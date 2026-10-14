import { fileURLToPath } from "node:url";
import { createTestSuite } from "@workflow/world-testing";

// Built by `vitest.conformance.config.ts`'s global setup.
createTestSuite(
	fileURLToPath(new URL("./.harness/index.js", import.meta.url)),
);
