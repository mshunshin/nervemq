import { z } from "zod";

/** The password rule the create-user form already applies. */
export const passwordSchema = z
  .string()
  .min(8, "Password must be at least 8 characters")
  .max(32, "Password must be at most 32 characters");
