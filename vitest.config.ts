import { defineConfig } from "vitest/config";
import FailureOnlyReporter from "./scripts/vitest-failure-only-reporter";

export default defineConfig({
  test: {
    include: ["packages/bridge/**/*.test.ts", "packages/web/src/**/*.test.ts"],
    reporters: [new FailureOnlyReporter()],
    pool: "vmThreads",
  },
});
