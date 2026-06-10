import { isAlphaNumeric } from "@/lib/utils";
import { z } from "zod";

export const createNamespaceSchema = z.object({
  name: z
    .string()
    .min(1)
    .max(32)
    .refine(isAlphaNumeric, "name should be alphanumeric"),
  role: z.enum(["admin", "user"], "Role must be either 'admin' or 'user'"),
});

export type CreateNamespaceRequest = z.infer<typeof createNamespaceSchema>;
