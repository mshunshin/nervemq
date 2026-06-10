"use client";

import { SidebarProvider } from "@/components/ui/sidebar";
import { Toaster } from "@/components/ui/sonner";
import {
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import { useState } from "react";
import { toast } from "sonner";
import { ZodError } from "zod";

export default function Providers({
  sidebarOpen,
  children,
}: {
  sidebarOpen: boolean;
  children: React.ReactNode;
}) {
  // Lazily initialized inside the component so a fresh client is created per
  // app instance (the documented TanStack Query + React pattern).
  const [queryClient] = useState(
    () =>
      new QueryClient({
        queryCache: new QueryCache({
          // API wrappers throw on any failed request (see lib/actions/api.ts);
          // surface query failures globally instead of per call site. A
          // ZodError's message is a raw JSON issue dump — log it for
          // debugging but show something readable.
          onError: (error) => {
            if (error instanceof ZodError) {
              console.error("API response failed validation:", error);
              toast.error("Received an unexpected response from the server");
            } else {
              toast.error(error.message);
            }
          },
        }),
      }),
  );

  return (
    <ThemeProvider defaultTheme="system" enableSystem>
      <SidebarProvider defaultOpen={sidebarOpen}>
        <QueryClientProvider client={queryClient}>
          <Toaster closeButton={true} richColors={true} />
          {children}
        </QueryClientProvider>
      </SidebarProvider>
    </ThemeProvider>
  );
}
