import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  build: {
    emptyOutDir: false,
    target: "chrome91",
    lib: {
      entry: resolve(import.meta.dirname, "src/index.ts"),
      formats: ["es", "cjs"],
      fileName: (format) => format === "es" ? "index.js" : "index.cjs",
    },
    sourcemap: true,
  },
  test: {
    environment: "jsdom",
  },
});
