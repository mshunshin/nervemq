import { z } from "zod";

export const updateQueueConfigSchema = z.object({
  maxRetries: z.number().min(0).max(999),
  deadLetterQueue: z.string().optional(),
});

export type QueueConfig = z.infer<typeof updateQueueConfigSchema>;

export type UpdateQueueConfigRequest = {
  queue: string;
  namespace: string;
  maxRetries: number;
  deadLetterQueue?: string;
};
