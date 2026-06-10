import { z } from "zod";

// Validates the form values, where namespaces are tracked as a Set.
export const createUserSchema = z.object({
  email: z.email(),
  password: z.string().min(8).max(32),
  namespaces: z.set(z.string()),
  role: z.enum(["admin", "user"]),
});

// The API payload carries namespaces as an array (see create-user.tsx onSubmit).
export type CreateUserRequest = {
  email: string;
  password: string;
  namespaces?: string[];
  role: string;
};
