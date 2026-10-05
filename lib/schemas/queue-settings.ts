import { z } from "zod";

export const updateQueueConfigSchema = z.object({
  // Every receive counts, the first included: 0 would stop the queue
  // delivering anything, so the server refuses it.
  maxRetries: z.number().int().min(1, "Must be at least 1").max(999),
  deadLetterQueue: z.string().optional(),
});

export type QueueConfig = z.infer<typeof updateQueueConfigSchema>;

export type UpdateQueueConfigRequest = {
  queue: string;
  namespace: string;
  maxRetries: number;
  deadLetterQueue?: string;
};
