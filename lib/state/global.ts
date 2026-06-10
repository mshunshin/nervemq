import { create } from "zustand";
import { Role, type AdminSession } from "@/lib/types";

// Re-exported for existing importers; the definitions live in lib/types.
export { Role } from "@/lib/types";
export type { AdminSession } from "@/lib/types";

/**
 * `session` is tri-state:
 * - `undefined`: not yet verified (initial load)
 * - `null`: verified unauthenticated / logged out
 * - `AdminSession`: authenticated
 */
export type GlobalState = {
  session: AdminSession | null | undefined;
};

export const useGlobalState = create<GlobalState>(() => ({
  session: undefined,
}));

export function useSession(): AdminSession | null | undefined {
  return useGlobalState((state) => state.session);
}

/** `undefined` while the session is still being verified. */
export function useIsAdmin(): boolean | undefined {
  return useGlobalState((state) =>
    state.session === undefined
      ? undefined
      : state.session?.role === Role.Admin,
  );
}
