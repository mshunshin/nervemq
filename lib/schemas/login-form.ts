import { z } from "zod";

export const loginFormSchema = z.object({
  email: z.email(),
  password: z.string().min(8).max(32),
});

export type LoginRequest = z.infer<typeof loginFormSchema>;
