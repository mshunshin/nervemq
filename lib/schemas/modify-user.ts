import { Role } from "@/lib/state/global";
import { z } from "zod";

// Validates the form values, where namespaces are tracked as Sets. `owned`
// is the subset of namespaces the user owns.
export const modifyUserSchema = z.object({
  namespaces: z.set(z.string()),
  owned: z.set(z.string()),
  role: z.enum(Role),
});

export type ModifyUserRequest = z.infer<typeof modifyUserSchema>;
