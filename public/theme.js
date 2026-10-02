// Applies the saved theme before the first paint, so a dark-theme user
// doesn't see a flash of the light one; lib/theme.tsx takes over once the
// app has loaded. A file rather than an inline script because the server's
// Content-Security-Policy (src/lib.rs) allows only scripts from its own files.
try {
  const saved = localStorage.getItem("theme");
  const dark =
    saved === "dark" ||
    (saved !== "light" && matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.dataset.theme = dark ? "dark" : "light";
  document.documentElement.style.colorScheme = dark ? "dark" : "light";
} catch {
  // Storage blocked: the app falls back to the system theme.
}
