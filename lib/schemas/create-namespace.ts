import { z } from "zod";
import { namespaceNameSchema } from "@/lib/schemas/name";

export const createNamespaceSchema = z.object({
  name: namespaceNameSchema,
  role: z.enum(["admin", "user"], "Role must be either 'admin' or 'user'"),
});

export type CreateNamespaceRequest = z.infer<typeof createNamespaceSchema>;
