import { isRouteErrorResponse, useRouteError } from "react-router";
import { Button } from "@/components/ui/button";

/**
 * The router's error boundary (see app/router.tsx). Without one, an uncaught
 * render error white-screens the whole app.
 */
export default function RouteError() {
  const error = useRouteError();
  const message = isRouteErrorResponse(error)
    ? `${error.status} ${error.statusText}`
    : error instanceof Error
      ? error.message
      : "";

  return (
    <div className="min-h-svh w-full flex flex-col items-center justify-center gap-4">
      <h1 className="text-2xl font-bold">Something went wrong</h1>
      <p className="text-sm text-muted-foreground">
        {message || "An unexpected error occurred."}
      </p>
      <Button onClick={() => window.location.reload()}>Try again</Button>
    </div>
  );
}
