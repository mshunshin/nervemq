import { Role } from "@/lib/state/global";
import { z } from "zod";

// Validates the form values, where namespaces are tracked as a Set. The form
// also carries email/password fields, which this dialog never edits — they are
// typed but not constrained so the schema matches the form's value shape.
export const modifyUserSchema = z.object({
  email: z.string(),
  password: z.string(),
  namespaces: z.set(z.string()),
  role: z.enum(Role),
});

export type ModifyUserRequest = z.infer<typeof modifyUserSchema>;
