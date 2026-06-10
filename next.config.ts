import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Emit a static export (`out/`) so the Rust server can embed and serve the UI.
  output: "export",
  // The default Next image loader needs a server; disable optimization for export.
  images: { unoptimized: true },
};

export default nextConfig;
