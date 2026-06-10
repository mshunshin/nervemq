"use client";

import { Button } from "@/components/ui/button";

/**
 * Route-level error boundary. Without one, an uncaught render error
 * white-screens the whole SPA; this works client-side even in a static
 * export.
 */
export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="min-h-svh w-full flex flex-col items-center justify-center gap-4">
      <h1 className="text-2xl font-bold">Something went wrong</h1>
      <p className="text-sm text-muted-foreground">
        {error.message || "An unexpected error occurred."}
      </p>
      <Button onClick={reset}>Try again</Button>
    </div>
  );
}
