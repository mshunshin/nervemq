import { z } from "zod";
import { queueNameSchema } from "@/lib/schemas/name";

export const createQueueSchema = z.object({
  name: queueNameSchema,
  // Picked from the existing namespaces, so not re-validated here.
  namespace: z.string().min(1),
  attributes: z.map(z.string().min(1), z.string()),
  tags: z.map(z.string().min(1), z.string()),
});

export type CreateQueueRequest = z.infer<typeof createQueueSchema>;
