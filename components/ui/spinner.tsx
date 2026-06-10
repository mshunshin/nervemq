import { Loader2 } from "lucide-react";

import { cn } from "@/lib/utils";

const sizes = {
  sm: "size-5",
  md: "size-8",
  lg: "size-10",
} as const;

/** Minimal replacement for HeroUI's <Spinner>: a spinning lucide loader that
 *  inherits the current text color. */
export function Spinner({
  size = "md",
  className,
}: {
  size?: keyof typeof sizes;
  className?: string;
}) {
  return (
    <Loader2
      aria-label="Loading"
      className={cn("animate-spin", sizes[size], className)}
    />
  );
}
