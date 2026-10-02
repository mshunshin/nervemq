import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const root = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@": root },
  },
  build: {
    // The Rust server embeds this directory at compile time (the `embed-ui`
    // feature; see build.rs).
    outDir: "out",
    // One bundle (~240 kB gzipped) is simpler than per-route chunks, and the
    // UI is served by the same binary as the API, not over a slow link.
    chunkSizeWarningLimit: 1000,
  },
  server: {
    port: 3000,
    // In development the admin API comes from a separately running server
    // (`just run`). Proxying keeps the UI on one origin, as it is when the
    // server embeds it, so the session cookie and relative URLs just work.
    // (SQS clients talk to the server directly: forwarding would break their
    // request signatures.)
    proxy: { "/api/admin": "http://localhost:8080" },
  },
});
