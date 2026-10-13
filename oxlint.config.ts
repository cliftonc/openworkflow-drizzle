import { defineConfig } from "oxlint";

export default defineConfig({
  categories: {
    correctness: "error",
    perf: "error",
    suspicious: "error",
  },
  env: {
    node: true,
  },
  ignorePatterns: ["dist"],
  options: {
    denyWarnings: true,
    reportUnusedDisableDirectives: "error",
  },
  rules: {
    // transactions run their statements in order
    "eslint/no-await-in-loop": "allow",
  },
  plugins: [
    "eslint",
    "typescript",
    "unicorn",
    "oxc",
    "import",
    "node",
    "promise",
    "vitest",
  ],
});
