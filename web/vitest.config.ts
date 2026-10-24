import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Standalone on purpose: it does NOT import vite.config.ts or mergeConfig it.
// The Module Federation plugin has nothing to do in a unit test, and a
// callback-form Vite config cannot be merged ("Cannot merge config in form of
// callback").
export default defineConfig({
  plugins: [react()],
  resolve: {
    // @warehouse/ui-kit is consumed via `file:../../warehouse-ui-kit` and has
    // its own react/react-dom; without dedupe its hooks run against a
    // SEPARATE React instance than the test tree ("Invalid hook call").
    dedupe: ["react", "react-dom"],
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    css: false,
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
